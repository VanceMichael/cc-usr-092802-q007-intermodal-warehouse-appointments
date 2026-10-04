"""仓干配协同流程测试：谱系守恒、多维预约、逐段交接、改配、并发领取、权限与恢复。"""

from __future__ import annotations

import tempfile
import threading
import unittest
from pathlib import Path

from civicflow.application import CivicFlow
from civicflow.errors import ConflictError, InvariantViolation, NotFoundError, PermissionDenied, ValidationError
from civicflow.security import AccessContext

DAY = "2026-10-03"
# arrival unload qc putaway picking loading rail
WINDOWS = [("06:00", "07:00"), ("07:00", "08:00"), ("08:00", "09:00"), ("09:00", "10:00"),
           ("10:00", "11:00"), ("11:00", "12:00"), ("12:00", "13:00")]
STAGES = ("arrival", "unload", "qc", "putaway", "picking", "loading", "rail_handover")
VEHICLES = {"arrival": "reefer_truck", "unload": "reefer_truck", "qc": "van", "putaway": "forklift",
            "picking": "forklift", "loading": "container_truck", "rail_handover": "rail_wagon"}
RESOURCES = {
    "arrival": "dock:in-1", "unload": "dock:in-2", "qc": "qc:1", "putaway": "loc:frozen-a",
    "picking": "forklift:1", "loading": "dock:out-1", "rail_handover": "rail:slot-3",
}


def iso(hhmm: str, day: str = DAY) -> str:
    return f"{day}T{hhmm}:00Z"


def stage_specs(*, overrides: dict | None = None) -> list[dict]:
    overrides = overrides or {}
    specs = []
    for stage, (s, e) in zip(STAGES, WINDOWS):
        spec = {"stage": stage, "resource_id": RESOURCES[stage], "vehicle_type": VEHICLES[stage],
                "start_at": iso(s), "end_at": iso(e)}
        spec.update(overrides.get(stage, {}))
        specs.append(spec)
    return specs


class LogisticsTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.db_path = str(Path(self.temp.name) / "log.sqlite3")
        self.app = CivicFlow.open(self.db_path, fixed_now=iso("05:00"))
        self.dispatch = AccessContext(
            actor_id="planner:zhang",
            permissions=frozenset({"write:logistics", "dispatch:logistics", "read:logistics",
                                   "review:discrepancies", "claim:tasks"}),
            scopes=frozenset({"*"}), reveal_sensitive=True)
        self.carrier_a = AccessContext(
            actor_id="driver:a", permissions=frozenset({"claim:tasks", "confirm:handovers"}),
            scopes=frozenset({"carrier:carrier:a"}))
        self.carrier_b = AccessContext(
            actor_id="driver:b", permissions=frozenset({"claim:tasks", "confirm:handovers"}),
            scopes=frozenset({"carrier:carrier:b"}))
        self.customer_a = AccessContext(
            actor_id="portal:a", permissions=frozenset({"read:own_orders"}),
            scopes=frozenset({"customer:customer:a"}))
        self.customer_b = AccessContext(
            actor_id="portal:b", permissions=frozenset({"read:own_orders"}),
            scopes=frozenset({"customer:customer:b"}))
        self._catalog()

    def tearDown(self):
        self.temp.cleanup()

    # ---------- 装配辅助 ----------

    def _catalog(self, *, capacity: int = 20) -> None:
        log = self.app.logistics; ctx = self.dispatch
        log.register_party(ctx, party_id="customer:a", kind="customer", name="甲客户")
        log.register_party(ctx, party_id="customer:b", kind="customer", name="乙客户")
        log.register_party(ctx, party_id="carrier:a", kind="carrier", name="甲承运")
        log.register_party(ctx, party_id="carrier:b", kind="carrier", name="乙承运")
        items = [
            ("dock:in-1", "platform", "冷链入港平台", ["reefer_truck"], ["frozen", "chilled"], ["seafood"], ["dock_in"], ""),
            ("dock:in-2", "platform", "冷链卸货平台", ["reefer_truck"], ["frozen", "chilled"], ["seafood"], ["dock_in"], ""),
            ("qc:1", "station", "质检台", ["van", "forklift"], ["frozen", "chilled"], ["seafood"], ["qc_station"], ""),
            ("loc:frozen-a", "location", "冷冻库位", ["forklift"], ["frozen"], ["seafood"], ["storage"], "frozen"),
            ("forklift:1", "equipment", "叉车组", ["forklift"], ["frozen", "chilled"], ["seafood"], ["forklift"], ""),
            ("dock:out-1", "platform", "装箱平台", ["container_truck"], ["frozen", "chilled"], ["seafood"], ["dock_out"], ""),
            ("rail:slot-3", "rail", "铁路接口三道", ["rail_wagon"], ["frozen", "chilled"], ["seafood"], ["rail_slot"], ""),
        ]
        for rid, kind, name, vt, tz, ct, caps, zone in items:
            log.register_resource(ctx, resource_id=rid, kind=kind, name=name, capacity=capacity,
                                  vehicle_types=vt, temp_zones=tz, cargo_types=ct, capabilities=caps, zone=zone)

    def _open_order(self, *, customer: str = "customer:a", cutoff: str = iso("14:00"),
                    cargo: str = "seafood", zone: str = "frozen"):
        return self.app.logistics.create_order(
            self.dispatch, customer_id=customer, cargo_type=cargo, temp_zone=zone,
            destination="杜伊斯堡", rail_cutoff_at=cutoff, request_key=f"order-{customer}-{cargo}-{zone}-{cutoff}")

    def _register(self, order_id: str, units: list[tuple[str, str, int]], key: str = "units"):
        return self.app.logistics.register_units(
            self.dispatch, order_id=order_id,
            units=[{"unit_id": u, "kind": k, "qty": q} for u, k, q in units], request_key=key)

    def _plan(self, order_id: str, *, specs: list[dict] | None = None, train: str = "train:CE-1", key: str = "chain"):
        return self.app.logistics.plan_chain(
            self.dispatch, order_id=order_id, train_id=train, stages=specs or stage_specs(), request_key=key)

    def _task_ids(self, chain_id: str) -> dict[str, str]:
        with self.app.database.connect() as conn:
            return {r["stage"]: r["task_id"] for r in conn.execute(
                "SELECT stage,task_id FROM logistics_tasks WHERE chain_id=? ORDER BY seq", (chain_id,))}

    def _active_qtys(self, order_id: str) -> dict[str, int]:
        with self.app.database.connect() as conn:
            return {r["unit_id"]: r["qty"] for r in conn.execute(
                "SELECT unit_id,qty FROM logistics_units WHERE order_id=? AND status='active'", (order_id,))}

    def _run_stage(self, task_id: str, order_id: str, stage: str, carrier: AccessContext,
                   *, damage: dict | None = None, reject: dict | None = None) -> dict:
        self.app.logistics.assign_task(self.dispatch, task_id=task_id, party_id=f"carrier:{carrier.actor_id.split(':')[1]}",
                                       request_key=f"assign-{task_id}")
        self.app.logistics.claim_task(carrier, task_id=task_id, party_id=f"carrier:{carrier.actor_id.split(':')[1]}")
        qtys = self._active_qtys(order_id)
        damage = damage or {}; reject = reject or {}
        receipts = [{
            "unit_id": uid,
            "receipt_key": f"rcpt-{stage}-{uid}-{task_id[-6:]}",
            "received_qty": qty - reject.get(uid, 0),
            "damaged_qty": damage.get(uid, 0),
            "rejected_qty": reject.get(uid, 0),
        } for uid, qty in qtys.items()]
        return self.app.logistics.confirm_handover(
            carrier, task_id=task_id, request_key=f"handover-{task_id}", receipts=receipts)

    def _happy_path(self, order_id: str, chain_id: str) -> None:
        for stage in STAGES:
            self._run_stage(self._task_ids(chain_id)[stage], order_id, stage, self.carrier_a)

    # ---------- 谱系 ----------

    def test_split_merge_lineage_and_conservation(self):
        log = self.app.logistics
        order = self._open_order()
        self._register(order["order_id"], [("box:1", "box", 10), ("box:2", "box", 4)])
        with self.assertRaises(ValidationError):
            log.split_unit(self.dispatch, unit_id="box:1",
                           splits=[{"unit_id": "box:1a", "qty": 6}, {"unit_id": "box:1b", "qty": 3}],
                           request_key="bad-split")
        log.split_unit(self.dispatch, unit_id="box:1",
                       splits=[{"unit_id": "box:1a", "qty": 7}, {"unit_id": "box:1b", "qty": 3}],
                       request_key="split-1")
        log.merge_units(self.dispatch, unit_ids=["box:1b", "box:2"], new_unit_id="pallet:p1", request_key="merge-1")
        events = log.lineage(self.dispatch, "pallet:p1")
        kinds = {e["event"] for e in events}
        self.assertEqual(kinds, {"register", "split", "merge"})
        summary = log.verify_conservation()
        self.assertEqual(summary["unit_roots"], 2)
        with self.app.database.connect() as conn:
            live = conn.execute("SELECT COALESCE(SUM(qty),0) AS n FROM logistics_units WHERE status IN ('active','delivered')").fetchone()["n"]
        self.assertEqual(live, 14)

    # ---------- 多维预约 ----------

    def test_reservation_rejects_vehicle_zone_cargo_capability(self):
        log = self.app.logistics
        order = self._open_order()
        self._register(order["order_id"], [("box:1", "box", 1)])
        cases = [
            ({"arrival": {"vehicle_type": "tank_truck"}}, "车型"),
            ({"unload": {"resource_id": "dock:out-1"}}, "能力"),  # out 平台缺 dock_in
            ({"qc": {"resource_id": "loc:frozen-a"}}, "能力"),
        ]
        for overrides, label in cases:
            with self.subTest(label=label):
                with self.assertRaises((ConflictError, ValidationError, NotFoundError)):
                    log.plan_chain(self.dispatch, order_id=order["order_id"], train_id="t:x",
                                   stages=stage_specs(overrides=overrides), request_key=f"plan-{label}")

    def test_putaway_zone_mismatch_rejected(self):
        log = self.app.logistics
        # 冷藏货不能进冷冻库位。
        order = self._open_order(zone="chilled")
        self._register(order["order_id"], [("box:1", "box", 1)])
        with self.assertRaises(ConflictError):
            self._plan(order["order_id"], key="zone-plan")

    def test_capacity_enforced_on_overlap_not_on_boundary(self):
        log = self.app.logistics
        o1 = self._open_order()
        self._register(o1["order_id"], [("box:1", "box", 20)], key="u1")
        self._plan(o1["order_id"], train="train:1", key="c1")
        o2 = self._open_order(customer="customer:b")
        self._register(o2["order_id"], [("box:9", "box", 1)], key="u2")
        # 与第一个订单的到场时段重叠（06:30-07:30）→ 平台已满。
        overlap = stage_specs(overrides={"arrival": {"start_at": iso("06:30"), "end_at": iso("07:30")}})
        with self.assertRaises(ConflictError):
            self._plan(o2["order_id"], specs=overlap, train="train:2", key="c2-bad")
        # 边界相接（整体顺延一小时：07:00 起）→ 各资源容量在上一预约结束时已释放。
        shifted = [("07:00", "08:00"), ("08:00", "09:00"), ("09:00", "10:00"), ("10:00", "11:00"),
                   ("11:00", "12:00"), ("12:00", "13:00"), ("13:00", "14:00")]
        boundary = stage_specs(overrides={stage: {"start_at": iso(s), "end_at": iso(e)}
                                          for stage, (s, e) in zip(STAGES, shifted)})
        chain = self._plan(o2["order_id"], specs=boundary, train="train:2", key="c2-ok")
        self.assertTrue(chain["chain_id"])

    def test_rail_cutoff_window_enforced(self):
        order = self._open_order(cutoff=iso("12:30"))
        self._register(order["order_id"], [("box:1", "box", 1)])
        with self.assertRaises(ConflictError):
            self._plan(order["order_id"], key="late-chain")

    # ---------- 交接与差异 ----------

    def test_handover_conservation_and_segregation_of_duties(self):
        log = self.app.logistics
        order = self._open_order()
        self._register(order["order_id"], [("box:1", "box", 10)])
        chain = self._plan(order["order_id"])
        tasks = self._task_ids(chain["chain_id"])
        # 前两段正常。
        self._run_stage(tasks["arrival"], order["order_id"], "arrival", self.carrier_a)
        self._run_stage(tasks["unload"], order["order_id"], "unload", self.carrier_a)
        # 质检：2 破损、1 拒收，其余 7 继续。
        result = self._run_stage(tasks["qc"], order["order_id"], "qc", self.carrier_a,
                                 damage={"box:1": 2}, reject={"box:1": 1})
        self.assertEqual(result["items"][0]["flowing"], 7)
        self.assertEqual(len(result["discrepancies"]), 2)
        # 提交差异的仓库人员/司机不能复核自己的差异。
        disc = log.list_discrepancies(self.dispatch)[0]
        submitter_ctx = AccessContext(actor_id=disc["submitter"],
                                      permissions=frozenset({"review:discrepancies"}), scopes=frozenset({"*"}))
        with self.assertRaises(PermissionDenied):
            log.review_discrepancy(submitter_ctx, discrepancy_id=disc["discrepancy_id"],
                                   accept=True, note="自认", request_key="self-review")
        # 守恒：现存 7，损耗 3。
        log.verify_conservation()
        # 超收被守恒拒绝。
        self.app.logistics.assign_task(self.dispatch, task_id=tasks["putaway"], party_id="carrier:a",
                                       request_key="assign-pa")
        self.app.logistics.claim_task(self.carrier_a, task_id=tasks["putaway"], party_id="carrier:a")
        with self.assertRaises(ConflictError):
            self.app.logistics.confirm_handover(
                self.carrier_a, task_id=tasks["putaway"], request_key="over",
                receipts=[{"unit_id": "box:1", "receipt_key": "rcpt-over",
                           "received_qty": 8, "damaged_qty": 0, "rejected_qty": 0}])

    def test_duplicate_receipt_does_not_reoccupy(self):
        log = self.app.logistics
        order = self._open_order()
        self._register(order["order_id"], [("box:1", "box", 5)])
        chain = self._plan(order["order_id"])
        tid = self._task_ids(chain["chain_id"])["arrival"]
        self.app.logistics.assign_task(self.dispatch, task_id=tid, party_id="carrier:a", request_key="a1")
        self.app.logistics.claim_task(self.carrier_a, task_id=tid, party_id="carrier:a")
        receipts = [{"unit_id": "box:1", "receipt_key": "rcpt-x", "received_qty": 5,
                     "damaged_qty": 0, "rejected_qty": 0}]
        first = log.confirm_handover(self.carrier_a, task_id=tid, request_key="req-x", receipts=receipts)
        self.assertEqual(first["status"], "confirmed")
        # 外部重发：相同回执键、不同平台请求号 → duplicate，不新增交接行/差异，单元数量不变。
        replay = log.confirm_handover(self.carrier_a, task_id=tid, request_key="req-x-retry", receipts=receipts)
        self.assertEqual(replay["status"], "duplicate")
        # 同一平台请求号重放 → 幂等缓存，同样无副作用。
        replay2 = log.confirm_handover(self.carrier_a, task_id=tid, request_key="req-x", receipts=receipts)
        self.assertEqual(replay2["status"], "confirmed")
        with self.app.database.connect() as conn:
            self.assertEqual(conn.execute("SELECT COUNT(*) AS n FROM logistics_handovers").fetchone()["n"], 1)
            self.assertEqual(conn.execute("SELECT COUNT(*) AS n FROM logistics_discrepancies").fetchone()["n"], 0)
            self.assertEqual(conn.execute("SELECT qty FROM logistics_units WHERE unit_id='box:1'").fetchone()["qty"], 5)
            self.assertEqual(conn.execute(
                "SELECT status FROM logistics_reservations WHERE task_id=?", (tid,)).fetchone()["status"], "consumed")
        # 任务已经完成：本人再领冲突，其他承运方无权领取。
        with self.assertRaises(ConflictError):
            log.claim_task(self.carrier_a, task_id=tid, party_id="carrier:a")
        with self.assertRaises(PermissionDenied):
            log.claim_task(self.carrier_b, task_id=tid, party_id="carrier:b")

    def test_inbox_conflicting_message_quarantined_until_review(self):
        inbox = self.app.inbox
        first = inbox.receive(source="gate", source_key="truck:1", sequence=1,
                              payload={"arrived": True}, occurred_at=iso("05:30"))
        self.assertEqual(first["status"], "accepted")
        second = inbox.receive(source="gate", source_key="truck:1", sequence=1,
                               payload={"arrived": False}, occurred_at=iso("05:31"), quarantine=True)
        self.assertEqual(second["status"], "quarantined")
        conflicts = inbox.list_conflicts()
        self.assertEqual(len(conflicts), 1)
        # 原消息未被污染。
        self.assertEqual(inbox.timeline("gate", "truck:1")[0]["payload_json"], '{"arrived":true}')
        # 核对前不能再次核对不存在之外的分支：采纳冲突内容后原消息被更正。
        resolved = inbox.resolve_conflict(conflicts[0]["conflict_id"], decision="accept_incoming", actor="supervisor:li")
        self.assertTrue(resolved["status"].startswith("resolved"))
        with self.assertRaises(ConflictError):
            inbox.resolve_conflict(conflicts[0]["conflict_id"], decision="keep_existing", actor="supervisor:li")
        self.assertEqual(inbox.timeline("gate", "truck:1")[0]["status"], "corrected")

    # ---------- 权限隔离 ----------

    def test_customer_sees_only_own_cargo(self):
        log = self.app.logistics
        oa = self._open_order(customer="customer:a")
        ob = self._open_order(customer="customer:b")
        self.assertEqual(log.track(self.customer_a, oa["order_id"])["order_id"], oa["order_id"])
        with self.assertRaises(PermissionDenied):
            log.track(self.customer_b, oa["order_id"])
        with self.assertRaises(PermissionDenied):
            log.track(self.customer_a, ob["order_id"])

    def test_carrier_claims_only_assigned_tasks(self):
        log = self.app.logistics
        order = self._open_order()
        self._register(order["order_id"], [("box:1", "box", 3)])
        chain = self._plan(order["order_id"])
        tid = self._task_ids(chain["chain_id"])["arrival"]
        # 未分配不能领。
        with self.assertRaises(ConflictError):
            log.claim_task(self.carrier_a, task_id=tid, party_id="carrier:a")
        log.assign_task(self.dispatch, task_id=tid, party_id="carrier:a", request_key="asg")
        # 其他承运方看不到也领不了。
        self.assertEqual(log.available_tasks(self.carrier_b, party_id="carrier:b"), [])
        with self.assertRaises(PermissionDenied):
            log.claim_task(self.carrier_b, task_id=tid, party_id="carrier:b")
        visible = log.available_tasks(self.carrier_a, party_id="carrier:a")
        self.assertEqual([t["task_id"] for t in visible], [tid])
        log.claim_task(self.carrier_a, task_id=tid, party_id="carrier:a")

    def test_concurrent_claim_single_winner(self):
        order = self._open_order()
        self._register(order["order_id"], [("box:1", "box", 2)])
        chain = self._plan(order["order_id"])
        tid = self._task_ids(chain["chain_id"])["arrival"]
        self.app.logistics.assign_task(self.dispatch, task_id=tid, party_id="carrier:a", request_key="cc-asg")

        barrier = threading.Barrier(2)
        outcomes: list[str] = []
        lock = threading.Lock()

        def worker(actor: str) -> None:
            app = CivicFlow.open(self.db_path, fixed_now=iso("05:00"))
            ctx = AccessContext(actor_id=actor, permissions=frozenset({"claim:tasks"}),
                                scopes=frozenset({"carrier:carrier:a"}))
            barrier.wait()
            try:
                app.logistics.claim_task(ctx, task_id=tid, party_id="carrier:a")
                result = "won"
            except ConflictError:
                result = "lost"
            with lock:
                outcomes.append(result)

        t1 = threading.Thread(target=worker, args=("driver:a1",))
        t2 = threading.Thread(target=worker, args=("driver:a2",))
        t1.start(); t2.start(); t1.join(); t2.join()
        self.assertEqual(sorted(outcomes), ["lost", "won"])

    # ---------- 改配 ----------

    def test_train_reschedule_keeps_signed_off_stages(self):
        log = self.app.logistics
        order = self._open_order(cutoff=iso("14:00"))
        self._register(order["order_id"], [("box:1", "box", 10)])
        chain = self._plan(order["order_id"])
        tasks = self._task_ids(chain["chain_id"])
        self._run_stage(tasks["arrival"], order["order_id"], "arrival", self.carrier_a)
        self._run_stage(tasks["unload"], order["order_id"], "unload", self.carrier_a)
        with self.app.database.connect() as conn:
            handovers_before = conn.execute("SELECT COUNT(*) AS n FROM logistics_handovers").fetchone()["n"]

        # 已签收环节不能被重排。
        with self.assertRaises(ConflictError):
            log.replan(self.dispatch, order_id=order["order_id"], reason="试图回滚到场", kind="train_reschedule",
                       new_train_id="train:CE-2", new_cutoff_at=iso("09:45"), request_key="rollback-x",
                       shifts={"arrival": {"resource_id": RESOURCES["arrival"], "vehicle_type": "reefer_truck",
                                           "start_at": iso("06:30"), "end_at": iso("07:00")}})
        # 截关提前到 09:45：qc(09:00 结束) 不晚于截关，不受影响；其后四段重排。
        shifts = {
            "putaway": {"resource_id": RESOURCES["putaway"], "vehicle_type": "forklift",
                        "start_at": iso("09:00"), "end_at": iso("09:10")},
            "picking": {"resource_id": RESOURCES["picking"], "vehicle_type": "forklift",
                        "start_at": iso("09:10"), "end_at": iso("09:20")},
            "loading": {"resource_id": RESOURCES["loading"], "vehicle_type": "container_truck",
                        "start_at": iso("09:20"), "end_at": iso("09:30")},
            "rail_handover": {"resource_id": RESOURCES["rail_handover"], "vehicle_type": "rail_wagon",
                              "start_at": iso("09:30"), "end_at": iso("09:40")},
        }
        result = log.replan(self.dispatch, order_id=order["order_id"], reason="班列临时改点提前截关",
                            kind="train_reschedule", new_train_id="train:CE-2",
                            new_cutoff_at=iso("09:45"), request_key="reschedule-1", shifts=shifts)
        self.assertEqual(result["affected"], ["loading", "picking", "putaway", "rail_handover"])
        self.assertIn("arrival", result["carried"])
        self.assertIn("unload", result["carried"])
        self.assertIn("qc", result["carried"])
        with self.app.database.connect() as conn:
            # 旧交接凭证原样保留，签收责任不回滚。
            self.assertEqual(conn.execute("SELECT COUNT(*) AS n FROM logistics_handovers").fetchone()["n"], handovers_before)
            states = {r["stage"]: r["state"] for r in conn.execute(
                "SELECT stage,state FROM logistics_tasks WHERE chain_id=?", (result["chain_id"],))}
            self.assertEqual(states["arrival"], "completed")
            self.assertEqual(states["unload"], "completed")
            self.assertEqual(states["qc"], "planned")  # 未受影响、未开始的环节原样挂到新链
            self.assertEqual(states["putaway"], "planned")
            self.assertEqual(conn.execute(
                "SELECT COUNT(*) AS n FROM logistics_chains WHERE order_id=? AND state='active'",
                (order["order_id"],)).fetchone()["n"], 1)
        log.verify_conservation()

    def test_damage_replan_reschedules_only_downstream_with_real_qty(self):
        log = self.app.logistics
        order = self._open_order()
        self._register(order["order_id"], [("box:1", "box", 10)])
        chain = self._plan(order["order_id"])
        tasks = self._task_ids(chain["chain_id"])
        self._run_stage(tasks["arrival"], order["order_id"], "arrival", self.carrier_a)
        self._run_stage(tasks["unload"], order["order_id"], "unload", self.carrier_a)
        self._run_stage(tasks["qc"], order["order_id"], "qc", self.carrier_a, damage={"box:1": 3})
        # 旧下游预约释放后，同窗口容量可被新预约使用（这里仍订同一资源同时段）。
        shifts = {stage: {"resource_id": RESOURCES[stage], "vehicle_type": VEHICLES[stage],
                          "start_at": WINDOWS[i][0] and iso(WINDOWS[i][0]), "end_at": iso(WINDOWS[i][1])}
                  for i, stage in enumerate(STAGES[STAGES.index("putaway"):], start=STAGES.index("putaway"))}
        result = log.replan(self.dispatch, order_id=order["order_id"], reason="三箱破损退出",
                            kind="damage", request_key="damage-1", shifts=shifts)
        self.assertEqual(result["affected"], ["loading", "picking", "putaway", "rail_handover"])
        with self.app.database.connect() as conn:
            planned_qty = conn.execute(
                "SELECT COALESCE(SUM(qty),0) AS n FROM logistics_task_units tu JOIN logistics_tasks t ON t.task_id=tu.task_id "
                "WHERE t.chain_id=? AND t.stage='rail_handover'", (result["chain_id"],)).fetchone()["n"]
        self.assertEqual(planned_qty, 7)  # 真实在流数量，破损不继续占资源
        # 旧下游预约释放后，同窗口容量可被其他订单使用：库位窗口余 13 件容量。
        other = self._open_order(customer="customer:b")
        self._register(other["order_id"], [("box:2", "box", 13)], key="other-units")
        alt_chain = self._plan(other["order_id"], specs=stage_specs(), train="train:9", key="other-chain")
        self.assertTrue(alt_chain["chain_id"])
        log.verify_conservation()

    def test_vehicle_late_and_transfer_replan(self):
        log = self.app.logistics
        order = self._open_order()
        self._register(order["order_id"], [("box:1", "box", 4)])
        chain = self._plan(order["order_id"])
        tasks = self._task_ids(chain["chain_id"])
        # 车辆迟到：只重排到场，后续环节不受影响。
        shifts = {"arrival": {"resource_id": RESOURCES["arrival"], "vehicle_type": "reefer_truck",
                              "start_at": iso("06:30"), "end_at": iso("07:00")}}
        result = log.replan(self.dispatch, order_id=order["order_id"], reason="冷链车高速拥堵",
                            kind="vehicle_late", request_key="late-1", shifts=shifts)
        self.assertEqual(result["affected"], ["arrival"])
        with self.app.database.connect() as conn:
            self.assertEqual(conn.execute(
                "SELECT state FROM logistics_tasks WHERE chain_id=? AND stage='unload'",
                (result["chain_id"],)).fetchone()["state"], "planned")
        # 到场完成后转仓：从卸货起的未完成环节全部重排。
        new_tasks = self._task_ids(result["chain_id"])
        self._run_stage(new_tasks["arrival"], order["order_id"], "arrival", self.carrier_a)
        shifts = {stage: {"resource_id": RESOURCES[stage], "vehicle_type": VEHICLES[stage],
                          "start_at": iso(WINDOWS[i][0]), "end_at": iso(WINDOWS[i][1])}
                  for i, stage in enumerate(STAGES) if stage != "arrival"}
        moved = log.replan(self.dispatch, order_id=order["order_id"], reason="整批转至邻仓",
                           kind="transfer", request_key="transfer-1",
                           transfer_from_stage="unload", shifts=shifts)
        self.assertEqual(moved["affected"], ["loading", "picking", "putaway", "qc", "rail_handover", "unload"])
        self.assertIn("arrival", moved["carried"])
        log.verify_conservation()

    def test_replan_is_idempotent(self):
        log = self.app.logistics
        order = self._open_order()
        self._register(order["order_id"], [("box:1", "box", 4)])
        self._plan(order["order_id"])
        shifts = {"arrival": {"resource_id": RESOURCES["arrival"], "vehicle_type": "reefer_truck",
                              "start_at": iso("06:30"), "end_at": iso("07:00")}}
        r1 = log.replan(self.dispatch, order_id=order["order_id"], reason="迟到", kind="vehicle_late",
                        request_key="idem", shifts=shifts)
        r2 = log.replan(self.dispatch, order_id=order["order_id"], reason="迟到", kind="vehicle_late",
                        request_key="idem", shifts=shifts)
        self.assertEqual(r1["chain_id"], r2["chain_id"])

    # ---------- 运营接口 ----------

    def test_track_gaps_and_earliest_slot(self):
        log = self.app.logistics
        order = self._open_order()
        self._register(order["order_id"], [("box:1", "box", 10)])
        self._plan(order["order_id"])
        tracking = log.track(self.dispatch, order["order_id"])
        self.assertEqual(tracking["current_stage"], "arrival")
        self.assertEqual(tracking["next_responsible"], "pending_assignment")
        self.assertTrue(tracking["estimated_outbound"]["earliest"])
        gaps = log.resource_gaps(self.dispatch, horizon_end=iso("23:59"))
        self.assertEqual(len(gaps["unassigned_tasks_due"]), 7)
        # 第二个大订单使用顺延一小时的窗口（边界相接），在铁路接口形成满载。
        other = self._open_order(customer="customer:b")
        self._register(other["order_id"], [("box:2", "box", 20)], key="u-big")
        shifted = [("07:00", "08:00"), ("08:00", "09:00"), ("09:00", "10:00"), ("10:00", "11:00"),
                   ("11:00", "12:00"), ("12:00", "13:00"), ("13:00", "14:00")]
        specs = stage_specs(overrides={stage: {"start_at": iso(s), "end_at": iso(e)}
                                       for stage, (s, e) in zip(STAGES, shifted)})
        self._plan(other["order_id"], specs=specs, train="t:big", key="big-chain")
        slot = log.earliest_slot(self.dispatch, resource_id="rail:slot-3", quantity=15,
                                 after=iso("12:00"), duration_minutes=60)
        # 12:00-13:00 余 10 不够 15、13:00-14:00 满载，最早可行窗口在 14:00 之后。
        self.assertGreaterEqual(slot["start_at"], iso("14:00"))
        saturated = {r["resource_id"] for r in log.resource_gaps(
            self.dispatch, horizon_end=iso("23:59"))["saturated_resources"]}
        self.assertIn("rail:slot-3", saturated)
        self.assertEqual(len(log.resource_gaps(self.dispatch, horizon_end=iso("23:59"))["unassigned_tasks_due"]), 14)

    # ---------- 重启续跑 ----------

    def test_sweep_continues_after_restart_and_rearms_dwell(self):
        order = self._open_order()
        self._register(order["order_id"], [("box:1", "box", 3)])
        self._plan(order["order_id"])
        # 06:00 到场预约到期但任务仍 planned → 到期告警。
        app1 = CivicFlow.open(self.db_path, fixed_now=iso("06:00"))
        swept = app1.logistics.sweep()
        self.assertTrue(swept["alerts"])
        # 模拟进程重启并把时钟推到 07:30：未开工，滞留告警触发并重新武装。
        app2 = CivicFlow.open(self.db_path, fixed_now=iso("07:30"))
        swept2 = app2.logistics.sweep()
        self.assertTrue(swept2["alerts"])
        with app2.database.connect() as conn:
            rearms = conn.execute("SELECT COUNT(*) AS n FROM logistics_watches WHERE kind='dwell' AND state='active'").fetchone()["n"]
        self.assertGreaterEqual(rearms, 1)
        # 再推 5 分钟（退避 600 秒内）不应重复告警。
        app3 = CivicFlow.open(self.db_path, fixed_now=iso("07:35"))
        before = len(app3.logistics.list_alerts(self.dispatch))
        app3.logistics.sweep()
        self.assertEqual(len(app3.logistics.list_alerts(self.dispatch)), before)
        # 再推过退避点，滞留继续告警。
        app4 = CivicFlow.open(self.db_path, fixed_now=iso("07:45"))
        app4.logistics.sweep()
        self.assertGreater(len(app4.logistics.list_alerts(self.dispatch)), before)

    def test_job_lease_recovered_after_crash(self):
        job = self.app.jobs.schedule(job_type="reservation_due", subject_id="chain:1",
                                     run_at=iso("04:00"), payload={})
        claimed = self.app.jobs.claim_due(seconds=1)
        self.assertEqual([j["job_id"] for j in claimed], [job])
        # 进程崩溃（没有 finish），超过租约后新进程必须能重新领取。
        app2 = CivicFlow.open(self.db_path, fixed_now=iso("05:30"))
        reclaimed = app2.jobs.claim_due(seconds=30)
        self.assertEqual([j["job_id"] for j in reclaimed], [job])
        app2.jobs.finish(job)
        self.assertEqual(app2.jobs.claim_due(), [])

    def test_full_delivery_conservation_and_chain_guard(self):
        order = self._open_order()
        self._register(order["order_id"], [("box:1", "box", 6), ("box:2", "box", 4)])
        chain = self._plan(order["order_id"])
        # 同一订单不能并排生成第二条活动链（同一批箱货进两条作业链）。
        with self.assertRaises(ConflictError):
            self._plan(order["order_id"], train="train:dup", key="dup-chain")
        self._happy_path(order["order_id"], chain["chain_id"])
        tracking = self.app.logistics.track(self.dispatch, order["order_id"])
        self.assertEqual(tracking["current_stage"], "delivered")
        self.assertEqual(tracking["next_responsible"], "railway:delivered")
        with self.app.database.connect() as conn:
            delivered = conn.execute("SELECT COALESCE(SUM(qty),0) AS n FROM logistics_units WHERE status='delivered'").fetchone()["n"]
        self.assertEqual(delivered, 10)
        summary = self.app.logistics.verify_conservation()
        self.assertEqual(summary["open_tasks"], 0)


if __name__ == "__main__":
    unittest.main()
