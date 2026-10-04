"""仓干配协同：订单谱系、多维预约、作业链、逐段交接与改配。

设计要点：
- 订单下的箱货/托盘/批次是可拆分合并的量化单元(unit)，登记、拆分、合并、损耗、
  交铁路事件都写入 logistics_lineage，数量守恒可由 verify_conservation 复核。
- 预约同时约束车型、温区、货类、平台能力、库位温区、容量和铁路截关窗口。
- 作业链按到场→卸货→质检→上架→拣选→装箱→交铁路逐段推进；每段交接逐单元
  确认数量，破损/拒收/短少生成差异单，提交人不能自行复核。
- 列车改期、车辆迟到、拒收、破损、转仓等扰动只重排尚未完成且真正受影响的环节；
  已完成环节（含签收责任）随链迁移，绝不回滚。
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import timedelta

from .database import Database
from .errors import ConflictError, InvariantViolation, NotFoundError, PermissionDenied, ValidationError
from .identifiers import new_id, require_safe
from .idempotency import IdempotencyStore
from .jsonutil import canonical_json, digest_json
from .security import AccessContext, assert_distinct
from .timeutil import Clock, canonical_instant, parse_instant

STAGES = ("arrival", "unload", "qc", "putaway", "picking", "loading", "rail_handover")
STAGE_LABELS = {
    "arrival": "到场", "unload": "卸货", "qc": "质检", "putaway": "上架",
    "picking": "拣选", "loading": "装箱", "rail_handover": "交铁路",
}
# 每个环节要求资源具备的平台能力。
STAGE_CAPABILITY = {
    "arrival": "dock_in", "unload": "dock_in", "qc": "qc_station",
    "putaway": "storage", "picking": "forklift", "loading": "dock_out",
    "rail_handover": "rail_slot",
}
UNIT_KINDS = {"box", "pallet", "batch"}
TERMINAL_TASK_STATES = {"completed", "cancelled"}
OPEN_TASK_STATES = {"planned", "assigned", "claimed"}
DWELL_BACKOFF_SECONDS = 600
REPLAN_KINDS = {"train_reschedule", "vehicle_late", "rejection", "damage", "transfer"}


def _loads(raw: str) -> object:
    return json.loads(raw)


@dataclass(frozen=True)
class LogisticsService:
    database: Database
    clock: Clock
    idempotency: IdempotencyStore

    # ---------- 目录：参与方与资源 ----------

    def register_party(self, context: AccessContext, *, party_id: str, kind: str, name: str) -> dict:
        context.require("write:logistics")
        require_safe(party_id, "参与方标识")
        if kind not in {"customer", "carrier", "warehouse", "railway"}:
            raise ValidationError("参与方类型不合法")
        with self.database.transaction() as connection:
            if connection.execute("SELECT 1 FROM logistics_parties WHERE party_id=?", (party_id,)).fetchone():
                raise ConflictError("参与方已存在")
            connection.execute("INSERT INTO logistics_parties(party_id,kind,name,created_at) VALUES(?,?,?,?)",
                               (party_id, kind, name, self.clock.now()))
        return {"party_id": party_id, "kind": kind, "name": name}

    def register_resource(self, context: AccessContext, *, resource_id: str, kind: str, name: str, capacity: int,
                          vehicle_types: list[str] | None = None, temp_zones: list[str] | None = None,
                          cargo_types: list[str] | None = None, capabilities: list[str] | None = None,
                          zone: str = "") -> dict:
        context.require("write:logistics")
        require_safe(resource_id, "资源标识")
        if not isinstance(capacity, int) or capacity <= 0:
            raise ValidationError("容量必须是正整数")
        with self.database.transaction() as connection:
            if connection.execute("SELECT 1 FROM logistics_resources WHERE resource_id=?", (resource_id,)).fetchone():
                raise ConflictError("资源已存在")
            connection.execute(
                "INSERT INTO logistics_resources(resource_id,kind,name,accepted_vehicle_types_json,accepted_temp_zones_json,accepted_cargo_types_json,capabilities_json,zone,capacity,active) VALUES(?,?,?,?,?,?,?,?,?,1)",
                (resource_id, kind, name, canonical_json(vehicle_types or []), canonical_json(temp_zones or []),
                 canonical_json(cargo_types or []), canonical_json(capabilities or []), zone, capacity))
        return {"resource_id": resource_id, "kind": kind, "name": name, "capacity": capacity, "zone": zone}

    # ---------- 订单 ----------

    def create_order(self, context: AccessContext, *, customer_id: str, cargo_type: str, temp_zone: str,
                     destination: str, rail_cutoff_at: str, request_key: str) -> dict:
        context.require("write:logistics")
        require_safe(customer_id, "客户标识"); require_safe(cargo_type, "货类"); require_safe(temp_zone, "温区")
        cutoff = canonical_instant(rail_cutoff_at)
        with self.database.transaction() as connection:
            def operation() -> dict:
                self._require_party(connection, customer_id, "customer")
                order_id = new_id("order"); now = self.clock.now()
                connection.execute(
                    "INSERT INTO logistics_orders(order_id,customer_id,cargo_type,temp_zone,destination,rail_cutoff_at,state,version,created_at,updated_at) VALUES(?,?,?,?,?,?,'open',1,?,?)",
                    (order_id, customer_id, cargo_type, temp_zone, destination, cutoff, now, now))
                return dict(connection.execute("SELECT * FROM logistics_orders WHERE order_id=?", (order_id,)).fetchone())
            return self.idempotency.execute(connection, scope="logistics:create_order", request_key=request_key,
                                            request={"customer_id": customer_id, "cargo_type": cargo_type,
                                                     "temp_zone": temp_zone, "destination": destination, "cutoff": cutoff},
                                            operation=operation)

    # ---------- 单元谱系 ----------

    def register_units(self, context: AccessContext, *, order_id: str, units: list[dict], request_key: str) -> dict:
        context.require("write:logistics")
        if not units:
            raise ValidationError("至少登记一个单元")
        with self.database.transaction() as connection:
            def operation() -> dict:
                order = self._require_open_order(connection, order_id)
                if connection.execute("SELECT 1 FROM logistics_chains WHERE order_id=? AND state='active'", (order_id,)).fetchone():
                    raise ConflictError("作业链已生成，新增单元请先走改配流程")
                now = self.clock.now(); created = []
                for item in units:
                    unit_id = require_safe(str(item["unit_id"]), "单元标识")
                    kind = str(item["kind"]); qty = item["qty"]
                    if kind not in UNIT_KINDS:
                        raise ValidationError(f"未知单元类型: {kind}")
                    if not isinstance(qty, int) or qty <= 0:
                        raise ValidationError("单元数量必须是正整数")
                    if connection.execute("SELECT 1 FROM logistics_units WHERE unit_id=?", (unit_id,)).fetchone():
                        raise ConflictError(f"单元 {unit_id} 已存在")
                    connection.execute(
                        "INSERT INTO logistics_units(unit_id,kind,order_id,customer_id,parent_id,root_id,qty,cargo_type,temp_zone,status,created_at) VALUES(?,?,?,?,?,?,?,?,?, 'active',?)",
                        (unit_id, kind, order_id, order["customer_id"], None, unit_id, qty,
                         order["cargo_type"], order["temp_zone"], now))
                    self._lineage(connection, event="register", parent_id=None, child_id=unit_id, qty=qty,
                                  order_id=order_id, actor=context.actor_id, ref_key=request_key)
                    created.append({"unit_id": unit_id, "kind": kind, "qty": qty})
                return {"order_id": order_id, "units": created}
            return self.idempotency.execute(connection, scope=f"logistics:register_units:{order_id}", request_key=request_key,
                                            request={"units": units}, operation=operation)

    def split_unit(self, context: AccessContext, *, unit_id: str, splits: list[dict], request_key: str) -> dict:
        """把一个活动单元按数量拆成多个子单元，子数量之和必须等于当前数量。"""
        context.require("write:logistics")
        if len(splits) < 2:
            raise ValidationError("拆分至少产生两个单元")
        with self.database.transaction() as connection:
            def operation() -> dict:
                row = self._require_unit(connection, unit_id)
                self._require_open_order(connection, row["order_id"])
                if row["status"] != "active":
                    raise ConflictError("只有活动单元可以拆分")
                self._assert_no_stage_in_progress(connection, unit_id)
                total = 0
                for item in splits:
                    qty = item["qty"]
                    if not isinstance(qty, int) or qty <= 0:
                        raise ValidationError("拆分数量必须是正整数")
                    total += qty
                if total != row["qty"]:
                    raise ValidationError(f"拆分数量 {total} 不等于原数量 {row['qty']}，数量守恒被拒绝")
                now = self.clock.now(); children = []
                for item in splits:
                    child_id = require_safe(str(item["unit_id"]), "子单元标识")
                    if connection.execute("SELECT 1 FROM logistics_units WHERE unit_id=?", (child_id,)).fetchone():
                        raise ConflictError(f"单元 {child_id} 已存在")
                    connection.execute(
                        "INSERT INTO logistics_units(unit_id,kind,order_id,customer_id,parent_id,root_id,qty,cargo_type,temp_zone,status,location,created_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)",
                        (child_id, row["kind"], row["order_id"], row["customer_id"], unit_id, row["root_id"], item["qty"],
                         row["cargo_type"], row["temp_zone"], "active", row["location"], now))
                    self._lineage(connection, event="split", parent_id=unit_id, child_id=child_id, qty=item["qty"],
                                  order_id=row["order_id"], actor=context.actor_id, ref_key=request_key)
                    children.append({"unit_id": child_id, "qty": item["qty"]})
                connection.execute("UPDATE logistics_units SET status='split',ended_at=? WHERE unit_id=?", (now, unit_id))
                # 把拆分传播到活动链上尚未开始的后续环节，正在进行的环节已被上面的检查排除。
                self._rebase_task_units(connection, row["order_id"], [unit_id],
                                        [(c["unit_id"], c["qty"]) for c in children])
                return {"parent_id": unit_id, "children": children}
            return self.idempotency.execute(connection, scope=f"logistics:split:{unit_id}", request_key=request_key,
                                            request={"splits": splits}, operation=operation)

    def merge_units(self, context: AccessContext, *, unit_ids: list[str], new_unit_id: str, request_key: str) -> dict:
        """合并同订单的活动单元，新单元数量等于各单元当前数量之和。"""
        context.require("write:logistics")
        if len(set(unit_ids)) < 2:
            raise ValidationError("合并不少于两个不同单元")
        require_safe(new_unit_id, "新单元标识")
        with self.database.transaction() as connection:
            def operation() -> dict:
                rows = [self._require_unit(connection, uid) for uid in dict.fromkeys(unit_ids)]
                order_id = rows[0]["order_id"]
                self._require_open_order(connection, order_id)
                if any(r["order_id"] != order_id or r["status"] != "active" for r in rows):
                    raise ConflictError("只能合并同一订单下的活动单元")
                for r in rows:
                    self._assert_no_stage_in_progress(connection, r["unit_id"])
                if connection.execute("SELECT 1 FROM logistics_units WHERE unit_id=?", (new_unit_id,)).fetchone():
                    raise ConflictError(f"单元 {new_unit_id} 已存在")
                total = sum(int(r["qty"]) for r in rows); now = self.clock.now(); first = rows[0]
                connection.execute(
                    "INSERT INTO logistics_units(unit_id,kind,order_id,customer_id,parent_id,root_id,qty,cargo_type,temp_zone,status,location,created_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)",
                    (new_unit_id, "pallet", order_id, first["customer_id"], None, first["root_id"], total,
                     first["cargo_type"], first["temp_zone"], "active", first["location"], now))
                for r in rows:
                    self._lineage(connection, event="merge", parent_id=r["unit_id"], child_id=new_unit_id,
                                  qty=int(r["qty"]), order_id=order_id, actor=context.actor_id, ref_key=request_key)
                    connection.execute("UPDATE logistics_units SET status='merged',ended_at=? WHERE unit_id=?", (now, r["unit_id"]))
                self._rebase_task_units(connection, order_id, [r["unit_id"] for r in rows], [(new_unit_id, total)])
                return {"new_unit_id": new_unit_id, "qty": total, "parents": [r["unit_id"] for r in rows]}
            return self.idempotency.execute(connection, scope="logistics:merge", request_key=request_key,
                                            request={"unit_ids": unit_ids, "new_unit_id": new_unit_id}, operation=operation)

    def lineage(self, context: AccessContext, unit_id: str) -> list[dict]:
        context.require("read:logistics")
        with self.database.connect() as connection:
            row = connection.execute("SELECT order_id FROM logistics_units WHERE unit_id=?", (unit_id,)).fetchone()
            if not row:
                raise NotFoundError("单元不存在")
            return [dict(r) for r in connection.execute(
                "SELECT * FROM logistics_lineage WHERE order_id=? ORDER BY lineage_id", (row["order_id"],))]

    # ---------- 作业链与多维预约 ----------

    def plan_chain(self, context: AccessContext, *, order_id: str, train_id: str, stages: list[dict], request_key: str) -> dict:
        context.require("dispatch:logistics")
        require_safe(train_id, "列车标识")
        if [s["stage"] for s in stages] != list(STAGES):
            raise ValidationError("作业链必须按到场、卸货、质检、上架、拣选、装箱、交铁路的顺序各排一段")
        with self.database.transaction() as connection:
            def operation() -> dict:
                order = self._require_open_order(connection, order_id)
                if connection.execute("SELECT 1 FROM logistics_chains WHERE order_id=? AND state='active'", (order_id,)).fetchone():
                    raise ConflictError("该订单已有进行中的作业链，扰动请走 replan")
                chain_id = new_id("chain"); now = self.clock.now()
                unit_rows = connection.execute(
                    "SELECT * FROM logistics_units WHERE order_id=? AND status='active' AND qty>0", (order_id,)).fetchall()
                if not unit_rows:
                    raise ValidationError("订单还没有可作业的活动单元")
                total_qty = sum(int(u["qty"]) for u in unit_rows)
                schedule: list[tuple] = []
                task_ids: list[str] = []
                prev_end = None
                for seq, item in enumerate(stages):
                    stage = item["stage"]
                    start = canonical_instant(item["start_at"]); end = canonical_instant(item["end_at"])
                    if parse_instant(start) >= parse_instant(end):
                        raise ValidationError(f"{STAGE_LABELS[stage]}时段不合法")
                    if prev_end and parse_instant(start) < parse_instant(prev_end):
                        raise ValidationError(f"{STAGE_LABELS[stage]}早于上一环节结束时间")
                    prev_end = end
                    if stage == "rail_handover" and parse_instant(end) > parse_instant(order["rail_cutoff_at"]):
                        raise ConflictError("交铁路环节晚于铁路截关窗口")
                    reservation_id = self._create_reservation(
                        connection, order=order, resource_id=item["resource_id"], subject_id=chain_id,
                        quantity=int(item.get("quantity", total_qty)), vehicle_type=item["vehicle_type"],
                        start_at=start, end_at=end, chain_id=chain_id, actor=context.actor_id,
                        cutoff=order["rail_cutoff_at"], stage=stage)
                    task_id = new_id("task")
                    connection.execute(
                        "INSERT INTO logistics_tasks(task_id,chain_id,order_id,stage,seq,planned_start_at,planned_end_at,reservation_id,resource_id,state,version) VALUES(?,?,?,?,?,?,?,?,?, 'planned',1)",
                        (task_id, chain_id, order_id, stage, seq, start, end, reservation_id, item["resource_id"]))
                    connection.execute("UPDATE logistics_reservations SET task_id=? WHERE reservation_id=?", (task_id, reservation_id))
                    for u in unit_rows:
                        connection.execute("INSERT INTO logistics_task_units(task_id,unit_id,qty) VALUES(?,?,?)",
                                           (task_id, u["unit_id"], int(u["qty"])))
                    schedule.append((task_id, stage, start, end)); task_ids.append(task_id)
                connection.execute(
                    "INSERT INTO logistics_chains(chain_id,order_id,train_id,rail_cutoff_at,state,generated_at,revision) VALUES(?,?,?,?,'active',?,1)",
                    (chain_id, order_id, train_id, order["rail_cutoff_at"], now))
                self._arm_chain_watches(connection, chain_id, schedule)
                connection.execute("UPDATE logistics_orders SET updated_at=? WHERE order_id=?", (now, order_id))
                return {"chain_id": chain_id, "order_id": order_id, "train_id": train_id, "tasks": task_ids}
            return self.idempotency.execute(connection, scope=f"logistics:plan:{order_id}", request_key=request_key,
                                            request={"train_id": train_id, "stages": stages}, operation=operation)

    def _create_reservation(self, connection, *, order, resource_id: str, subject_id: str, quantity: int,
                            vehicle_type: str, start_at: str, end_at: str, chain_id: str,
                            actor: str, cutoff: str, stage: str) -> str:
        require_safe(resource_id, "资源标识"); require_safe(vehicle_type, "车型")
        if quantity <= 0:
            raise ValidationError("预约数量必须为正数")
        resource = connection.execute("SELECT * FROM logistics_resources WHERE resource_id=?", (resource_id,)).fetchone()
        if not resource:
            raise NotFoundError(f"资源 {resource_id} 不存在")
        if not resource["active"]:
            raise ConflictError(f"资源 {resource_id} 已停用")
        vehicles = _loads(resource["accepted_vehicle_types_json"]); zones = _loads(resource["accepted_temp_zones_json"])
        cargos = _loads(resource["accepted_cargo_types_json"]); capabilities = _loads(resource["capabilities_json"])
        if vehicles and vehicle_type not in vehicles:
            raise ConflictError(f"{resource_id} 不接受车型 {vehicle_type}（可接纳：{','.join(vehicles)}）")
        if zones and order["temp_zone"] not in zones:
            raise ConflictError(f"{resource_id} 不支持温区 {order['temp_zone']}")
        if cargos and order["cargo_type"] not in cargos:
            raise ConflictError(f"{resource_id} 不承接货类 {order['cargo_type']}")
        required = STAGE_CAPABILITY[stage]
        if required not in capabilities:
            raise ConflictError(f"{resource_id} 缺少 {STAGE_LABELS[stage]} 所需平台能力 {required}")
        if stage == "putaway" and resource["zone"] and resource["zone"] != order["temp_zone"]:
            raise ConflictError("上架库位温区与货物温区不一致")
        if stage == "rail_handover" and parse_instant(start_at) > parse_instant(cutoff):
            raise ConflictError("交铁路预约开始时间已经晚于截关窗口")
        used = connection.execute(
            "SELECT COALESCE(SUM(quantity),0) AS used FROM logistics_reservations WHERE resource_id=? AND status IN ('held','confirmed') AND start_at<? AND end_at>?",
            (resource_id, end_at, start_at)).fetchone()["used"]
        if int(used) + quantity > int(resource["capacity"]):
            raise ConflictError(f"资源 {resource_id} 在该时段容量不足：已占 {used}/{resource['capacity']}，申请 {quantity}")
        reservation_id = new_id("reservation"); now = self.clock.now()
        connection.execute(
            "INSERT INTO logistics_reservations(reservation_id,resource_id,subject_id,order_id,vehicle_type,temp_zone,cargo_type,required_capability,quantity,start_at,end_at,rail_cutoff_at,chain_id,status,version,created_by,created_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?, 'confirmed',1,?,?)",
            (reservation_id, resource_id, subject_id, order["order_id"], vehicle_type, order["temp_zone"], order["cargo_type"],
             required, quantity, start_at, end_at, cutoff if stage == "rail_handover" else None, chain_id, actor, now))
        return reservation_id

    def assign_task(self, context: AccessContext, *, task_id: str, party_id: str, request_key: str) -> dict:
        context.require("dispatch:logistics")
        with self.database.transaction() as connection:
            def operation() -> dict:
                task = self._require_task(connection, task_id)
                if task["state"] != "planned":
                    raise ConflictError("只有待分配任务可以指派")
                self._require_party(connection, party_id, "carrier")
                connection.execute(
                    "UPDATE logistics_tasks SET assignee_party_id=?,state='assigned',version=version+1 WHERE task_id=?",
                    (party_id, task_id))
                return dict(self._require_task(connection, task_id))
            return self.idempotency.execute(connection, scope=f"logistics:assign:{task_id}", request_key=request_key,
                                            request={"party_id": party_id}, operation=operation)

    def claim_task(self, context: AccessContext, *, task_id: str, party_id: str) -> dict:
        """承运方只能领取已分配给自己的任务；并发领取只有一方成功。"""
        context.require("claim:tasks")
        if "*" not in context.scopes and f"carrier:{party_id}" not in context.scopes:
            raise PermissionDenied("不能以其他承运方身份领取任务")
        with self.database.transaction() as connection:
            changed = connection.execute(
                "UPDATE logistics_tasks SET state='claimed',claimed_at=?,version=version+1 "
                "WHERE task_id=? AND state='assigned' AND assignee_party_id=?",
                (self.clock.now(), task_id, party_id)).rowcount
            if changed != 1:
                row = connection.execute("SELECT state,assignee_party_id FROM logistics_tasks WHERE task_id=?", (task_id,)).fetchone()
                if not row:
                    raise NotFoundError("任务不存在")
                if row["assignee_party_id"] is None:
                    raise ConflictError("任务尚未分配，承运方不能自行领取")
                if row["assignee_party_id"] != party_id:
                    raise PermissionDenied("该任务没有分配给该承运方，不能领取")
                raise ConflictError("任务已被领取或已结束")
            return dict(self._require_task(connection, task_id))

    def available_tasks(self, context: AccessContext, *, party_id: str) -> list[dict]:
        """承运方只能看到已分配给自己、尚未完成的任务。"""
        context.require("claim:tasks")
        if "*" not in context.scopes and f"carrier:{party_id}" not in context.scopes:
            raise PermissionDenied("只能查询本承运方的任务")
        with self.database.connect() as connection:
            rows = connection.execute(
                "SELECT * FROM logistics_tasks WHERE assignee_party_id=? AND state IN ('assigned','claimed') ORDER BY planned_start_at",
                (party_id,)).fetchall()
            return [dict(r) for r in rows]

    # ---------- 逐段交接 ----------

    def confirm_handover(self, context: AccessContext, *, task_id: str, receipts: list[dict], request_key: str) -> dict:
        """逐段确认数量。

        每个回执：unit_id/receipt_key/received_qty/damaged_qty/rejected_qty。
        守恒：received + rejected + short = expected，其中 short 由系统按
        expected-received-rejected 计算；0 <= damaged <= received。
        破损、拒收、短少分别生成差异单；重复 receipt_key 直接回放，不再占用资源。
        """
        context.require("confirm:handovers")
        if not receipts:
            raise ValidationError("回执不能为空")
        batch_digest = digest_json(receipts)
        with self.database.transaction() as connection:
            def operation() -> dict:
                keys = [str(r.get("receipt_key", "")).strip() for r in receipts]
                if any(not k for k in keys):
                    raise ValidationError("每条回执必须带 receipt_key")
                if len(set(keys)) != len(keys):
                    raise ValidationError("同批回执键重复")
                placeholders = ",".join("?" for _ in keys)
                existing = connection.execute(
                    f"SELECT receipt_key FROM logistics_handovers WHERE receipt_key IN ({placeholders})", keys).fetchall()
                if existing:
                    # 回执重发（哪怕请求键不同）：识别为重复，绝不再次占用资源。
                    existing_keys = {r["receipt_key"] for r in existing}
                    if existing_keys == set(keys):
                        return {"status": "duplicate", "task_id": task_id, "receipt_keys": sorted(existing_keys)}
                    raise ConflictError("部分回执键已经使用，不能混在新批次中再次提交")
                task = self._require_task(connection, task_id)
                if task["state"] != "claimed":
                    raise ConflictError(f"任务处于 {task['state']}，只有已领取任务可以回执")
                now = self.clock.now(); order = self._require_order(connection, task["order_id"])
                plan_rows = connection.execute("SELECT * FROM logistics_task_units WHERE task_id=?", (task_id,)).fetchall()
                plan_units = {r["unit_id"]: r for r in plan_rows}
                results = []; discrepancy_ids = []
                for receipt in receipts:
                    unit_id = receipt["unit_id"]
                    if unit_id not in plan_units:
                        raise ValidationError(f"单元 {unit_id} 不属于本环节作业范围")
                    unit = self._require_unit(connection, unit_id)
                    if unit["status"] != "active":
                        raise ConflictError(f"单元 {unit_id} 当前状态为 {unit['status']}，不能交接")
                    expected = int(unit["qty"])
                    received = int(receipt.get("received_qty", 0)); damaged = int(receipt.get("damaged_qty", 0))
                    rejected = int(receipt.get("rejected_qty", 0))
                    if min(received, damaged, rejected) < 0:
                        raise ValidationError("数量不能为负")
                    if damaged > received:
                        raise ValidationError("破损数量不能大于收到数量")
                    short = expected - received - rejected
                    if short < 0:
                        raise ConflictError(
                            f"{STAGE_LABELS[task['stage']]}数量不守恒：收到 {received}+拒收 {rejected} 超过应收 {expected}")
                    handover_id = new_id("handover")
                    status = "confirmed" if short == 0 and damaged == 0 and rejected == 0 else "discrepancy"
                    connection.execute(
                        "INSERT INTO logistics_handovers(handover_id,task_id,stage,unit_id,expected_qty,received_qty,damaged_qty,rejected_qty,status,receipt_key,actor,confirmed_at,created_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)",
                        (handover_id, task_id, task["stage"], unit_id, expected, received, damaged, rejected,
                         status, receipt["receipt_key"], context.actor_id, now, now))
                    flowing = received - damaged
                    if flowing == 0:
                        connection.execute(
                            "UPDATE logistics_units SET qty=0,status='lost',ended_at=? WHERE unit_id=?", (now, unit_id))
                    else:
                        connection.execute("UPDATE logistics_units SET qty=? WHERE unit_id=?", (flowing, unit_id))
                    for kind, qty in (("破损", damaged), ("拒收", rejected), ("短少", short)):
                        if qty <= 0:
                            continue
                        discrepancy_id = new_id("discrepancy")
                        connection.execute(
                            "INSERT INTO logistics_discrepancies(discrepancy_id,handover_id,task_id,unit_id,kind,qty,note,submitter,state,created_at) VALUES(?,?,?,?,?,?,?,?,'submitted',?)",
                            (discrepancy_id, handover_id, task_id, unit_id, kind, qty,
                             f"{STAGE_LABELS[task['stage']]}{kind}{qty}件", context.actor_id, now))
                        self._lineage(connection, event="loss", parent_id=unit_id, child_id=unit_id, qty=qty,
                                      order_id=order["order_id"], actor=context.actor_id,
                                      detail={"stage": task["stage"], "kind": kind, "task_id": task_id,
                                              "discrepancy_id": discrepancy_id})
                        discrepancy_ids.append(discrepancy_id)
                    results.append({"unit_id": unit_id, "expected": expected, "received": received,
                                    "damaged": damaged, "rejected": rejected, "short": short, "flowing": flowing})
                if len(receipts) != len(plan_rows):
                    missing = set(plan_units) - {r["unit_id"] for r in receipts}
                    raise ValidationError("仍有单元缺少回执: " + ", ".join(sorted(missing)))
                self._complete_task(connection, task, order, now, context.actor_id)
                return {"status": "confirmed", "task_id": task_id, "items": results, "discrepancies": discrepancy_ids}
            return self.idempotency.execute(connection, scope=f"logistics:handover:{task_id}", request_key=request_key,
                                            request={"digest": batch_digest}, operation=operation)

    def _complete_task(self, connection, task, order, now: str, actor: str) -> None:
        pending = connection.execute(
            "SELECT 1 FROM logistics_task_units tu JOIN logistics_units u ON u.unit_id=tu.unit_id "
            "WHERE tu.task_id=? AND NOT EXISTS (SELECT 1 FROM logistics_handovers h WHERE h.task_id=? AND h.unit_id=tu.unit_id)",
            (task["task_id"], task["task_id"])).fetchall()
        if pending:
            raise ValidationError("仍有单元缺少交接回执")
        connection.execute(
            "UPDATE logistics_tasks SET state='completed',completed_at=?,version=version+1 WHERE task_id=?",
            (now, task["task_id"]))
        connection.execute("UPDATE logistics_reservations SET status='consumed' WHERE reservation_id=?",
                           (task["reservation_id"],))
        connection.execute("UPDATE logistics_units SET location=? WHERE order_id=? AND status='active'",
                           (task["resource_id"], order["order_id"]))
        connection.execute("UPDATE logistics_watches SET state='cancelled',fired_at=? WHERE subject_type='task' AND subject_id=? AND state='active'",
                           (now, task["task_id"]))
        if task["stage"] == "rail_handover":
            delivering = [dict(r) for r in connection.execute(
                "SELECT unit_id,qty FROM logistics_units WHERE order_id=? AND status='active' AND qty>0", (order["order_id"],))]
            for u in delivering:
                self._lineage(connection, event="deliver", parent_id=u["unit_id"], child_id=u["unit_id"],
                              qty=u["qty"], order_id=order["order_id"], actor=actor,
                              detail={"task_id": task["task_id"]})
            connection.execute(
                "UPDATE logistics_units SET status='delivered',ended_at=? WHERE order_id=? AND status='active'",
                (now, order["order_id"]))
            connection.execute("UPDATE logistics_orders SET state='delivered',updated_at=? WHERE order_id=?", (now, order["order_id"]))
            connection.execute("UPDATE logistics_chains SET state='completed' WHERE chain_id=?", (task["chain_id"],))
            connection.execute("UPDATE logistics_watches SET state='cancelled',fired_at=? WHERE subject_type='chain' AND subject_id=?",
                               (now, task["chain_id"]))

    def review_discrepancy(self, context: AccessContext, *, discrepancy_id: str, accept: bool, note: str, request_key: str) -> dict:
        """差异复核：复核人不能是差异提交人本人。"""
        context.require("review:discrepancies")
        note = (note or "").strip()
        if not note:
            raise ValidationError("复核必须写明意见")
        with self.database.transaction() as connection:
            def operation() -> dict:
                row = connection.execute("SELECT * FROM logistics_discrepancies WHERE discrepancy_id=?", (discrepancy_id,)).fetchone()
                if not row:
                    raise NotFoundError("差异不存在")
                if row["state"] != "submitted":
                    raise ConflictError("差异已经复核，不能重复处理")
                assert_distinct(row["submitter"], context.actor_id)
                state = "accepted" if accept else "rejected"
                connection.execute(
                    "UPDATE logistics_discrepancies SET state=?,reviewer=?,resolution=?,reviewed_at=? WHERE discrepancy_id=?",
                    (state, context.actor_id, note, self.clock.now(), discrepancy_id))
                return {"discrepancy_id": discrepancy_id, "state": state, "reviewer": context.actor_id}
            return self.idempotency.execute(connection, scope=f"logistics:review:{discrepancy_id}", request_key=request_key,
                                            request={"accept": accept, "note": note}, operation=operation)

    def list_discrepancies(self, context: AccessContext, *, state: str = "submitted") -> list[dict]:
        context.require("review:discrepancies")
        with self.database.connect() as connection:
            return [dict(r) for r in connection.execute(
                "SELECT * FROM logistics_discrepancies WHERE state=? ORDER BY created_at", (state,))]

    # ---------- 改配 ----------

    def replan(self, context: AccessContext, *, order_id: str, reason: str, kind: str, request_key: str,
               new_train_id: str | None = None, new_cutoff_at: str | None = None,
               shifts: dict | None = None, transfer_from_stage: str | None = None) -> dict:
        """扰动后重排。

        kind=train_reschedule/vehicle_late/rejection/damage/transfer。
        只重排尚未完成且真正受影响的环节；已完成环节随旧任务原样挂到新链，
        预约 consumed 状态不变，签收责任不回滚。shifts 给出受影响环节的新排法。
        """
        context.require("dispatch:logistics")
        if kind not in REPLAN_KINDS:
            raise ValidationError("未知扰动类型")
        if not reason or not reason.strip():
            raise ValidationError("改配必须说明原因")
        shifts = shifts or {}
        with self.database.transaction() as connection:
            def operation() -> dict:
                order = self._require_open_order(connection, order_id)
                chain_row = connection.execute(
                    "SELECT * FROM logistics_chains WHERE order_id=? AND state='active' ORDER BY revision DESC LIMIT 1",
                    (order_id,)).fetchone()
                if not chain_row:
                    raise NotFoundError("没有进行中的作业链")
                chain_id = chain_row["chain_id"]
                old_tasks = connection.execute(
                    "SELECT * FROM logistics_tasks WHERE chain_id=? ORDER BY seq", (chain_id,)).fetchall()
                new_cutoff = canonical_instant(new_cutoff_at) if new_cutoff_at else chain_row["rail_cutoff_at"]
                if kind == "train_reschedule" and not new_train_id:
                    raise ValidationError("列车改期必须提供新列车标识")
                if kind == "transfer" and transfer_from_stage not in STAGES:
                    raise ValidationError("转仓必须指定起始环节")
                for stage in shifts:
                    if stage not in STAGES:
                        raise ValidationError(f"未知环节 {stage}")
                    done = next((t for t in old_tasks if t["stage"] == stage), None)
                    if done and done["state"] in TERMINAL_TASK_STATES:
                        raise ConflictError(f"{STAGE_LABELS[stage]}环节已完成，签收责任不能回滚")

                affected: set[str] = set()
                if kind in ("rejection", "damage"):
                    # 损耗发生环节之后、尚未完成的下游环节才真正受影响。
                    loss_seqs = [int(r["seq"]) for r in connection.execute(
                        "SELECT DISTINCT t.seq FROM logistics_discrepancies d "
                        "JOIN logistics_tasks t ON t.task_id=d.task_id WHERE t.chain_id=? "
                        "AND d.kind IN ('拒收','破损','短少')", (chain_id,)).fetchall()]
                    for t in old_tasks:
                        if t["state"] not in TERMINAL_TASK_STATES and loss_seqs and int(t["seq"]) > min(loss_seqs):
                            affected.add(t["stage"])
                elif kind == "transfer":
                    started = False
                    for t in old_tasks:
                        if t["stage"] == transfer_from_stage:
                            started = True
                        if started and t["state"] not in TERMINAL_TASK_STATES:
                            affected.add(t["stage"])
                elif kind == "vehicle_late":
                    affected.update(shifts.keys())
                    arrival = next(t for t in old_tasks if t["stage"] == "arrival")
                    if arrival["state"] in TERMINAL_TASK_STATES and "arrival" in affected:
                        raise ConflictError("到场已完成，不能因迟到改配")
                    if arrival["state"] not in TERMINAL_TASK_STATES and "arrival" not in affected:
                        raise ValidationError("车辆迟到至少要重排尚未完成的到场环节")
                else:  # train_reschedule
                    for t in old_tasks:
                        if t["state"] not in TERMINAL_TASK_STATES and parse_instant(t["planned_end_at"]) > parse_instant(new_cutoff):
                            affected.add(t["stage"])
                    affected.update(shifts.keys())

                missing = affected - set(shifts)
                if missing:
                    raise ValidationError("受影响环节缺少新排法: " + ", ".join(STAGE_LABELS[s] for s in STAGES if s in missing))
                extra = set(shifts) - affected
                if extra:
                    raise ValidationError("未受影响环节不应重排: " + ", ".join(STAGE_LABELS[s] for s in STAGES if s in extra))

                now = self.clock.now(); revision = int(chain_row["revision"]) + 1
                new_chain_id = new_id("chain")
                old_task_ids = [t["task_id"] for t in old_tasks]
                self._cancel_watches(connection, chain_id, old_task_ids, now)
                connection.execute("UPDATE logistics_chains SET state='superseded',superseded_at=? WHERE chain_id=?", (now, chain_id))
                connection.execute(
                    "INSERT INTO logistics_chains(chain_id,order_id,train_id,rail_cutoff_at,state,generated_at,revision) VALUES(?,?,?,?,'active',?,?)",
                    (new_chain_id, order_id, new_train_id or chain_row["train_id"], new_cutoff, now, revision))
                if new_cutoff_at:
                    connection.execute("UPDATE logistics_orders SET rail_cutoff_at=?,updated_at=? WHERE order_id=?", (new_cutoff, now, order_id))

                carried = []
                for t in old_tasks:
                    if t["stage"] in affected:
                        connection.execute("UPDATE logistics_reservations SET status='superseded' WHERE reservation_id=?",
                                           (t["reservation_id"],))
                        connection.execute(
                            "UPDATE logistics_tasks SET state='cancelled',terminal_at=?,terminal_reason=?,affected_reason=?,version=version+1 WHERE task_id=?",
                            (now, f"replan:{kind}", reason, t["task_id"]))
                        continue
                    # 未受影响环节（含已完成签收）整体迁入新链，交接凭证与预约占用原样保留。
                    connection.execute("UPDATE logistics_tasks SET chain_id=?,version=version+1 WHERE task_id=?",
                                       (new_chain_id, t["task_id"]))
                    if t["state"] not in TERMINAL_TASK_STATES:
                        connection.execute("UPDATE logistics_reservations SET chain_id=? WHERE reservation_id=?",
                                           (new_chain_id, t["reservation_id"]))
                        carried.append(t["stage"])
                    else:
                        carried.append(t["stage"])

                schedule: list[tuple] = []
                for stage in (s for s in STAGES if s in shifts):
                    spec = shifts[stage]; seq = STAGES.index(stage)
                    start = canonical_instant(spec["start_at"]); end = canonical_instant(spec["end_at"])
                    if stage == "rail_handover" and parse_instant(end) > parse_instant(new_cutoff):
                        raise ConflictError("新交铁路时段仍晚于截关窗口")
                    unit_rows = connection.execute(
                        "SELECT * FROM logistics_units WHERE order_id=? AND status='active' AND qty>0", (order_id,)).fetchall()
                    reservation_id = self._create_reservation(
                        connection, order=order, resource_id=spec["resource_id"], subject_id=new_chain_id,
                        quantity=int(spec.get("quantity", sum(int(u["qty"]) for u in unit_rows))),
                        vehicle_type=spec["vehicle_type"], start_at=start, end_at=end,
                        chain_id=new_chain_id, actor=context.actor_id, cutoff=new_cutoff, stage=stage)
                    new_task_id = new_id("task")
                    connection.execute(
                        "INSERT INTO logistics_tasks(task_id,chain_id,order_id,stage,seq,planned_start_at,planned_end_at,reservation_id,resource_id,state,affected_reason,version) VALUES(?,?,?,?,?,?,?,?,?, 'planned',?,1)",
                        (new_task_id, new_chain_id, order_id, stage, seq, start, end, reservation_id,
                         spec["resource_id"], f"{kind}:{reason}"))
                    connection.execute("UPDATE logistics_reservations SET task_id=? WHERE reservation_id=?",
                                       (new_task_id, reservation_id))
                    for u in unit_rows:
                        connection.execute("INSERT INTO logistics_task_units(task_id,unit_id,qty) VALUES(?,?,?)",
                                           (new_task_id, u["unit_id"], int(u["qty"])))
                    schedule.append((new_task_id, stage, start, end))

                # 整条新链的时序必须单调，并满足截关。
                merged = connection.execute(
                    "SELECT stage,planned_start_at,planned_end_at FROM logistics_tasks WHERE chain_id=? ORDER BY seq",
                    (new_chain_id,)).fetchall()
                prev_end = None
                for row in merged:
                    if prev_end and parse_instant(row["planned_start_at"]) < parse_instant(prev_end):
                        raise ConflictError(f"改配后 {STAGE_LABELS[row['stage']]} 与前序环节时间冲突")
                    prev_end = row["planned_end_at"]
                carried_stages = [t["stage"] for t in old_tasks if t["stage"] not in affected]
                self._arm_chain_watches(connection, new_chain_id, schedule + [
                    (r["task_id"], r["stage"], r["planned_start_at"], r["planned_end_at"])
                    for r in connection.execute(
                        "SELECT task_id,stage,planned_start_at,planned_end_at FROM logistics_tasks "
                        "WHERE chain_id=? AND state IN ('planned','assigned','claimed') ORDER BY seq", (new_chain_id,)).fetchall()])
                self._lineage(connection, event="replan", parent_id=chain_id, child_id=new_chain_id, qty=0,
                              order_id=order_id, actor=context.actor_id, ref_key=request_key,
                              detail={"kind": kind, "reason": reason, "affected": sorted(affected),
                                      "carried": carried_stages, "revision": revision})
                return {"chain_id": new_chain_id, "revision": revision,
                        "affected": sorted(affected), "carried": carried_stages}
            return self.idempotency.execute(connection, scope=f"logistics:replan:{order_id}", request_key=request_key,
                                            request={"kind": kind, "reason": reason, "new_train_id": new_train_id,
                                                     "new_cutoff_at": new_cutoff_at, "shifts": shifts,
                                                     "transfer_from_stage": transfer_from_stage}, operation=operation)

    # ---------- 守望与告警（重启后继续） ----------

    def _arm_chain_watches(self, connection, chain_id: str, open_tasks: list[tuple]) -> None:
        seen: set[str] = set()
        for task_id, stage, start, end in open_tasks:
            if task_id in seen:
                continue
            seen.add(task_id)
            payload = canonical_json({"chain_id": chain_id, "stage": stage})
            connection.execute(
                "INSERT INTO logistics_watches(watch_id,subject_type,subject_id,due_at,kind,state,payload_json) VALUES(?, 'task',?,?, 'reservation_due','active',?)",
                (new_id("watch"), task_id, start, payload))
            connection.execute(
                "INSERT INTO logistics_watches(watch_id,subject_type,subject_id,due_at,kind,state,payload_json) VALUES(?, 'task',?,?, 'dwell','active',?)",
                (new_id("watch"), task_id, end, payload))
        cutoff = connection.execute("SELECT rail_cutoff_at FROM logistics_chains WHERE chain_id=?", (chain_id,)).fetchone()["rail_cutoff_at"]
        connection.execute(
            "INSERT INTO logistics_watches(watch_id,subject_type,subject_id,due_at,kind,state,payload_json) VALUES(?, 'chain',?,?, 'cutoff','active',?)",
            (new_id("watch"), chain_id, cutoff, canonical_json({})))

    def _cancel_watches(self, connection, chain_id: str, task_ids: list[str], now: str) -> None:
        connection.execute("UPDATE logistics_watches SET state='cancelled',fired_at=? WHERE subject_type='chain' AND subject_id=? AND state='active'",
                           (now, chain_id))
        if task_ids:
            placeholders = ",".join("?" for _ in task_ids)
            connection.execute(
                f"UPDATE logistics_watches SET state='cancelled',fired_at=? WHERE subject_type='task' AND subject_id IN ({placeholders}) AND state='active'",
                [now, *task_ids])

    def sweep(self) -> dict:
        """处理到期预约、滞留/未完成交接告警和截关提醒。

        守望是持久化行：进程重启后再次 sweep 即继续处理；滞留告警在任务仍未结束时
        按退避间隔重新武装。
        """
        now = self.clock.now(); alerts: list[str] = []
        with self.database.transaction() as connection:
            due = connection.execute(
                "SELECT * FROM logistics_watches WHERE state='active' AND due_at<=? ORDER BY due_at,watch_id",
                (now,)).fetchall()
            for watch in due:
                if watch["subject_type"] == "task":
                    task = connection.execute("SELECT * FROM logistics_tasks WHERE task_id=?", (watch["subject_id"],)).fetchone()
                    if not task or task["state"] in TERMINAL_TASK_STATES:
                        self._mark_watch(connection, watch["watch_id"], now); continue
                    chain = connection.execute("SELECT state FROM logistics_chains WHERE chain_id=?", (task["chain_id"],)).fetchone()
                    if not chain or chain["state"] != "active":
                        self._mark_watch(connection, watch["watch_id"], now); continue
                    payload = _loads(watch["payload_json"]); stage = payload["stage"]
                    if watch["kind"] == "reservation_due":
                        if task["state"] == "claimed":
                            self._mark_watch(connection, watch["watch_id"], now)  # 已开工，滞留由 dwell 守望负责
                        else:
                            alerts.append(self._raise_alert(
                                connection, watch, f"{STAGE_LABELS[stage]}预约已到期，任务仍为{task['state']}，请尽快派车/领取", now))
                            self._mark_watch(connection, watch["watch_id"], now)
                    else:
                        tail = "交接未完成" if task["state"] == "claimed" else "环节滞留未开始"
                        alerts.append(self._raise_alert(connection, watch, f"{STAGE_LABELS[stage]}{tail}", now))
                        self._mark_watch(connection, watch["watch_id"], now)
                        next_at = (parse_instant(now) + timedelta(seconds=DWELL_BACKOFF_SECONDS)).isoformat().replace("+00:00", "Z")
                        connection.execute(
                            "INSERT INTO logistics_watches(watch_id,subject_type,subject_id,due_at,kind,state,payload_json) VALUES(?, 'task',?,?, 'dwell','active',?)",
                            (new_id("watch"), task["task_id"], next_at, watch["payload_json"]))
                else:
                    chain = connection.execute("SELECT * FROM logistics_chains WHERE chain_id=?", (watch["subject_id"],)).fetchone()
                    if not chain or chain["state"] != "active":
                        self._mark_watch(connection, watch["watch_id"], now); continue
                    alerts.append(self._raise_alert(connection, watch, "铁路截关窗口已到，作业链仍未完成交铁路", now))
                    self._mark_watch(connection, watch["watch_id"], now)
        return {"swept_at": now, "alerts": alerts}

    @staticmethod
    def _mark_watch(connection, watch_id: str, now: str) -> None:
        connection.execute("UPDATE logistics_watches SET state='cancelled',fired_at=? WHERE watch_id=?", (now, watch_id))

    @staticmethod
    def _raise_alert(connection, watch, message: str, now: str) -> str:
        alert_id = new_id("alert")
        connection.execute(
            "INSERT INTO logistics_alerts(alert_id,watch_id,kind,subject_type,subject_id,message,raised_at) VALUES(?,?,?,?,?,?,?)",
            (alert_id, watch["watch_id"], watch["kind"], watch["subject_type"], watch["subject_id"], message, now))
        return alert_id

    def acknowledge_alert(self, context: AccessContext, alert_id: str) -> dict:
        context.require("write:logistics")
        with self.database.transaction() as connection:
            changed = connection.execute(
                "UPDATE logistics_alerts SET acknowledged_at=? WHERE alert_id=? AND acknowledged_at IS NULL",
                (self.clock.now(), alert_id)).rowcount
            if changed != 1:
                raise ConflictError("告警不存在或已确认")
        return {"alert_id": alert_id, "acknowledged": True}

    def list_alerts(self, context: AccessContext, *, include_acked: bool = False) -> list[dict]:
        context.require("read:logistics")
        sql = "SELECT * FROM logistics_alerts"
        if not include_acked:
            sql += " WHERE acknowledged_at IS NULL"
        sql += " ORDER BY raised_at"
        with self.database.connect() as connection:
            return [dict(r) for r in connection.execute(sql)]

    # ---------- 运营查询 ----------

    def track(self, context: AccessContext, order_id: str) -> dict:
        """直接回答：货物当前去向、下一责任人、预计出库区间。"""
        with self.database.connect() as connection:
            order = self._require_order(connection, order_id)
            self._authorize_order_read(context, order)
            chain = connection.execute(
                "SELECT * FROM logistics_chains WHERE order_id=? ORDER BY revision DESC,generated_at DESC LIMIT 1",
                (order_id,)).fetchone()
            units = [dict(r) for r in connection.execute(
                "SELECT unit_id,kind,qty,status,location,parent_id,root_id FROM logistics_units WHERE order_id=? ORDER BY unit_id",
                (order_id,))]
            tasks = connection.execute("SELECT * FROM logistics_tasks WHERE chain_id=? ORDER BY seq", (chain["chain_id"],)).fetchall() if chain else []
            current = next((t for t in tasks if t["state"] not in TERMINAL_TASK_STATES), None)
            if current:
                stage = current["stage"]
                if current["state"] == "planned":
                    next_responsible = "pending_assignment"
                elif current["state"] == "assigned":
                    next_responsible = current["assignee_party_id"]
                else:
                    next_responsible = f"{current['assignee_party_id']}（作业中）"
            elif order["state"] == "delivered":
                stage, next_responsible = "delivered", "railway:delivered"
            else:
                stage, next_responsible = None, None
            loading = next((t for t in tasks if t["stage"] == "loading"), None)
            rail = next((t for t in tasks if t["stage"] == "rail_handover"), None)
            return {
                "order_id": order_id, "state": order["state"],
                "train_id": chain["train_id"] if chain else None,
                "current_stage": stage,
                "current_stage_label": STAGE_LABELS.get(stage, "已交付铁路"),
                "current_location": next((u["location"] for u in reversed(units) if u["status"] in ("active", "delivered") and u["location"]), ""),
                "next_responsible": next_responsible,
                "units": units,
                "stages": [{"stage": t["stage"], "label": STAGE_LABELS[t["stage"]], "state": t["state"],
                            "assignee": t["assignee_party_id"], "resource_id": t["resource_id"],
                            "planned_start_at": t["planned_start_at"], "planned_end_at": t["planned_end_at"]} for t in tasks],
                "estimated_outbound": {"earliest": loading["planned_start_at"] if loading and loading["state"] != "cancelled" else None,
                                       "latest": rail["planned_end_at"] if rail and rail["state"] != "cancelled" else None},
            }

    def resource_gaps(self, context: AccessContext, *, horizon_end: str | None = None) -> dict:
        """资源缺口：窗口内峰值用满的资源、已到期未指派任务、没有作业链的订单。"""
        context.require("read:logistics")
        horizon = canonical_instant(horizon_end) if horizon_end else self.clock.now()
        with self.database.connect() as connection:
            saturated = []
            for r in connection.execute("SELECT * FROM logistics_resources WHERE active=1"):
                peak = 0
                points = connection.execute(
                    "SELECT DISTINCT start_at FROM logistics_reservations WHERE resource_id=? AND status IN ('held','confirmed') AND start_at<=?",
                    (r["resource_id"], horizon)).fetchall()
                for p in points:
                    used = connection.execute(
                        "SELECT COALESCE(SUM(quantity),0) AS n FROM logistics_reservations WHERE resource_id=? AND status IN ('held','confirmed') AND start_at<=? AND end_at>?",
                        (r["resource_id"], p["start_at"], p["start_at"])).fetchone()["n"]
                    peak = max(peak, int(used))
                if peak >= int(r["capacity"]):
                    saturated.append({"resource_id": r["resource_id"], "capacity": int(r["capacity"]), "peak_used": peak})
            unassigned = [dict(t) for t in connection.execute(
                "SELECT t.task_id,t.chain_id,t.stage,t.planned_start_at FROM logistics_tasks t "
                "JOIN logistics_chains c ON c.chain_id=t.chain_id WHERE c.state='active' AND t.state='planned' AND t.planned_start_at<=? ORDER BY t.planned_start_at",
                (horizon,))]
            chainless = [r["order_id"] for r in connection.execute(
                "SELECT o.order_id FROM logistics_orders o LEFT JOIN logistics_chains c ON c.order_id=o.order_id AND c.state='active' "
                "WHERE o.state='open' AND c.chain_id IS NULL")]
            return {"saturated_resources": saturated, "unassigned_tasks_due": unassigned,
                    "orders_without_chain": chainless, "as_of": self.clock.now()}

    def earliest_slot(self, context: AccessContext, *, resource_id: str, quantity: int, after: str, duration_minutes: int) -> dict:
        """回答资源最早可预约时刻，用于缺口补救。"""
        context.require("read:logistics")
        if not isinstance(quantity, int) or quantity <= 0 or duration_minutes <= 0:
            raise ValidationError("数量与时长必须为正整数")
        with self.database.connect() as connection:
            resource = connection.execute("SELECT * FROM logistics_resources WHERE resource_id=?", (resource_id,)).fetchone()
            if not resource:
                raise NotFoundError("资源不存在")
            cap = int(resource["capacity"])
            if quantity > cap:
                raise ConflictError("需求量超过资源总容量，任何窗口都无法满足")
            rows = connection.execute(
                "SELECT start_at,end_at FROM logistics_reservations WHERE resource_id=? AND status IN ('held','confirmed') AND end_at>? ORDER BY start_at",
                (resource_id, self.clock.now())).fetchall()
            intervals = sorted((parse_instant(r["start_at"]), parse_instant(r["end_at"])) for r in rows)
            candidates = sorted({parse_instant(canonical_instant(after)), *(e for _, e in intervals)})
            for cand in candidates:
                if cand < parse_instant(self.clock.now()):
                    continue
                end = cand + timedelta(minutes=duration_minutes)
                start_s = cand.isoformat().replace("+00:00", "Z"); end_s = end.isoformat().replace("+00:00", "Z")
                used = connection.execute(
                    "SELECT COALESCE(SUM(quantity),0) AS n FROM logistics_reservations WHERE resource_id=? AND status IN ('held','confirmed') AND start_at<? AND end_at>?",
                    (resource_id, end_s, start_s)).fetchone()["n"]
                if int(used) + quantity <= cap:
                    return {"resource_id": resource_id, "start_at": start_s, "end_at": end_s}
            raise ConflictError("现有占用之后仍找不到可行窗口")

    # ---------- 不变量校验 ----------

    def verify_conservation(self) -> dict:
        """数量守恒、容量不失真、单元不跨两条活动作业链。"""
        with self.database.connect() as connection:
            # 1) 数量守恒按订单核对（合并不限定同一根树）：
            #    登记总量 == 现存(活动+交付) + 累计损耗。
            for order in connection.execute("SELECT order_id FROM logistics_orders"):
                oid = order["order_id"]
                registered = connection.execute(
                    "SELECT COALESCE(SUM(qty),0) AS n FROM logistics_lineage WHERE event='register' AND order_id=?",
                    (oid,)).fetchone()["n"]
                live = connection.execute(
                    "SELECT COALESCE(SUM(qty),0) AS n FROM logistics_units WHERE order_id=? AND status IN ('active','delivered')",
                    (oid,)).fetchone()["n"]
                lost = connection.execute(
                    "SELECT COALESCE(SUM(qty),0) AS n FROM logistics_lineage WHERE event='loss' AND order_id=?",
                    (oid,)).fetchone()["n"]
                if int(registered) != int(live) + int(lost):
                    raise InvariantViolation(
                        f"订单 {oid} 数量不守恒：登记 {registered} != 现存 {live}+损耗 {lost}")
            # 2) 每次拆分：子行数量之和应等于父行当时数量（谱系留痕即凭据）。
            for parent in connection.execute(
                    "SELECT parent_id, COALESCE(SUM(qty),0) AS n FROM logistics_lineage WHERE event='split' GROUP BY parent_id"):
                child_rows = connection.execute(
                    "SELECT COUNT(*) AS n FROM logistics_units WHERE parent_id=?", (parent["parent_id"],)).fetchone()["n"]
                if child_rows < 2 or int(parent["n"]) <= 0:
                    raise InvariantViolation(f"拆分 {parent['parent_id']} 谱系不完整")
            for r in connection.execute("SELECT * FROM logistics_resources"):
                points = connection.execute(
                    "SELECT DISTINCT start_at AS p FROM logistics_reservations WHERE resource_id=? AND status IN ('held','confirmed')",
                    (r["resource_id"],)).fetchall()
                for p in points:
                    used = connection.execute(
                        "SELECT COALESCE(SUM(quantity),0) AS n FROM logistics_reservations WHERE resource_id=? AND status IN ('held','confirmed') AND start_at<=? AND end_at>?",
                        (r["resource_id"], p["p"], p["p"])).fetchone()["n"]
                    if int(used) > int(r["capacity"]):
                        raise InvariantViolation(f"资源 {r['resource_id']} 在 {p['p']} 超出容量")
            dup = connection.execute(
                "SELECT tu.unit_id FROM logistics_task_units tu JOIN logistics_tasks t ON t.task_id=tu.task_id "
                "JOIN logistics_chains c ON c.chain_id=t.chain_id WHERE c.state='active' AND t.state NOT IN ('completed','cancelled') "
                "GROUP BY tu.unit_id HAVING COUNT(DISTINCT t.chain_id) > 1").fetchall()
            if dup:
                raise InvariantViolation("同一批箱货被安排进两条活动作业链")
            # 同一链内，同一未完成环节对同一单元至多一行作业量。
            bad = connection.execute(
                "SELECT task_id,unit_id,COUNT(*) AS n FROM logistics_task_units GROUP BY task_id,unit_id HAVING n>1").fetchall()
            if bad:
                raise InvariantViolation("环节内出现重复单元作业行")
            return {
                "unit_roots": connection.execute("SELECT COUNT(DISTINCT root_id) AS n FROM logistics_units").fetchone()["n"],
                "loss_events": connection.execute("SELECT COUNT(*) AS n FROM logistics_lineage WHERE event='loss'").fetchone()["n"],
                "resources_checked": connection.execute("SELECT COUNT(*) AS n FROM logistics_resources").fetchone()["n"],
                "open_tasks": connection.execute(
                    "SELECT COUNT(*) AS n FROM logistics_tasks WHERE state NOT IN ('completed','cancelled')").fetchone()["n"],
            }

    # ---------- 内部辅助 ----------

    def _authorize_order_read(self, context: AccessContext, order) -> None:
        if context.allows("read:logistics"):
            return
        if not context.allows("read:own_orders"):
            raise PermissionDenied("缺少订单读取权限")
        if "*" not in context.scopes and f"customer:{order['customer_id']}" not in context.scopes:
            raise PermissionDenied("客户只能查询自己的货物")

    def _rebase_task_units(self, connection, order_id: str, old_ids: list[str], new_rows: list[tuple]) -> None:
        """拆分/合并后，把活动链上未开始环节的作业行替换为新单元。"""
        chain = connection.execute(
            "SELECT chain_id FROM logistics_chains WHERE order_id=? AND state='active'", (order_id,)).fetchone()
        if not chain:
            return
        placeholders = ",".join("?" for _ in old_ids)
        task_rows = connection.execute(
            f"SELECT DISTINCT t.task_id FROM logistics_tasks t JOIN logistics_task_units tu ON tu.task_id=t.task_id "
            f"WHERE t.chain_id=? AND t.state IN ('planned','assigned') AND tu.unit_id IN ({placeholders})",
            [chain["chain_id"], *old_ids]).fetchall()
        for row in task_rows:
            task_id = row["task_id"]
            connection.execute(f"DELETE FROM logistics_task_units WHERE task_id=? AND unit_id IN ({placeholders})",
                               [task_id, *old_ids])
            for unit_id, qty in new_rows:
                connection.execute(
                    "INSERT INTO logistics_task_units(task_id,unit_id,qty) VALUES(?,?,?) "
                    "ON CONFLICT(task_id,unit_id) DO UPDATE SET qty=excluded.qty",
                    (task_id, unit_id, qty))

    def _assert_no_stage_in_progress(self, connection, unit_id: str) -> None:
        row = connection.execute(
            "SELECT 1 FROM logistics_task_units tu JOIN logistics_tasks t ON t.task_id=tu.task_id "
            "JOIN logistics_chains c ON c.chain_id=t.chain_id WHERE tu.unit_id=? AND c.state='active' AND t.state='claimed' LIMIT 1",
            (unit_id,)).fetchone()
        if row:
            raise ConflictError("该单元所在环节正在交接，不能拆分或合并")

    def _lineage(self, connection, *, event: str, parent_id: str | None, child_id: str, qty: int, order_id: str,
                 actor: str, ref_key: str = "", detail: dict | None = None) -> None:
        connection.execute(
            "INSERT INTO logistics_lineage(event,parent_id,child_id,qty,order_id,actor,occurred_at,ref_key,detail_json) VALUES(?,?,?,?,?,?,?,?,?)",
            (event, parent_id, child_id, qty, order_id, actor, self.clock.now(), ref_key, canonical_json(detail or {})))

    @staticmethod
    def _require_task(connection, task_id: str):
        row = connection.execute("SELECT * FROM logistics_tasks WHERE task_id=?", (task_id,)).fetchone()
        if not row:
            raise NotFoundError("任务不存在")
        return row

    @staticmethod
    def _require_unit(connection, unit_id: str):
        row = connection.execute("SELECT * FROM logistics_units WHERE unit_id=?", (unit_id,)).fetchone()
        if not row:
            raise NotFoundError(f"单元 {unit_id} 不存在")
        return row

    @staticmethod
    def _require_order(connection, order_id: str):
        row = connection.execute("SELECT * FROM logistics_orders WHERE order_id=?", (order_id,)).fetchone()
        if not row:
            raise NotFoundError("订单不存在")
        return row

    def _require_open_order(self, connection, order_id: str):
        row = self._require_order(connection, order_id)
        if row["state"] != "open":
            raise ConflictError(f"订单当前状态为 {row['state']}，不能再调整")
        return row

    @staticmethod
    def _require_party(connection, party_id: str, kind: str):
        row = connection.execute("SELECT * FROM logistics_parties WHERE party_id=?", (party_id,)).fetchone()
        if not row:
            raise NotFoundError(f"参与方 {party_id} 不存在")
        if row["kind"] != kind:
            raise ValidationError(f"{party_id} 不是 {kind}")
        return row
