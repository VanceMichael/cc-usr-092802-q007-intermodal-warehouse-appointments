"""命令行入口。"""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path

from .application import CivicFlow
from .cases import CaseService
from .logistics import STAGES
from .security import AccessContext


def emit(value: object) -> None:
    print(json.dumps(value, ensure_ascii=False, sort_keys=True, indent=2))


def demo(app: CivicFlow) -> dict:
    context = AccessContext.system("demo-operator")
    cases = CaseService(app.repository)
    created = cases.create(context, {"case_type": "协同事项", "subject": "示例联合处置", "owner_org": "org:demo", "priority": "high", "opened_at": app.clock.now()}, request_key="demo-case")
    accepted = app.inbox.receive(source="demo", source_key=created["entity_id"], sequence=1, payload={"kind": "opened"}, occurred_at=app.clock.now())
    reservation = app.reservations.reserve(resource_id="room:joint", subject_id=created["entity_id"], quantity=2, capacity=10, start_at="2026-09-28T10:00:00+08:00", end_at="2026-09-28T11:00:00+08:00", actor=context.actor_id)
    debit = app.ledger.post(journal_key="demo", account="coordination", currency="CNY", amount="12.34", direction="debit", reference="demo-debit", actor=context.actor_id)
    credit = app.ledger.post(journal_key="demo", account="coordination", currency="CNY", amount="12.34", direction="credit", reference="demo-credit", actor=context.actor_id)
    message = app.outbox.enqueue(topic="case.opened", aggregate_id=created["entity_id"], payload={"case_id": created["entity_id"]})
    return {"case": created, "inbox": accepted, "reservation": reservation, "entries": [debit, credit], "balance": app.ledger.balance("demo", currency="CNY"), "message_id": message, "verification": app.verify()}


def _stage_specs(day: str, resources: dict[str, str]) -> list[dict]:
    windows = [("08:00", "09:00"), ("09:00", "10:00"), ("10:00", "11:00"), ("11:00", "12:00"),
               ("13:00", "14:00"), ("14:00", "15:00"), ("15:00", "16:00")]
    vehicles = ["reefer_truck", "reefer_truck", "van", "forklift", "forklift", "container_truck", "rail_wagon"]
    return [{"stage": stage, "resource_id": resources[stage], "vehicle_type": vehicle,
             "start_at": f"{day}T{s}+08:00", "end_at": f"{day}T{e}+08:00"}
            for stage, (s, e), vehicle in zip(STAGES, windows, vehicles)]


def demo_logistics(app: CivicFlow) -> dict:
    """国际班列临时改点的仓干配协同演示。"""
    ctx = AccessContext.system("demo-operator")
    log = app.logistics
    log.register_party(ctx, party_id="customer:acme", kind="customer", name="ACME 贸易")
    log.register_party(ctx, party_id="carrier:shunfeng", kind="carrier", name="顺峰干线")
    log.register_party(ctx, party_id="warehouse:gaobiao", kind="warehouse", name="高标仓")
    resources = {}
    catalog = [
        ("dock:in-1", "platform", "冷链卸货平台 1", 20, ["reefer_truck"], ["frozen", "chilled"], ["seafood"], ["dock_in"], ""),
        ("dock:in-2", "platform", "冷链卸货平台 2", 20, ["van", "reefer_truck"], ["frozen", "chilled", "ambient"], ["seafood", "general"], ["dock_in"], ""),
        ("qc:1", "station", "质检台 1", 20, ["van"], ["frozen", "chilled", "ambient"], ["seafood", "general"], ["qc_station"], ""),
        ("loc:frozen-a", "location", "冷冻库位 A", 50, ["forklift"], ["frozen"], ["seafood"], ["storage"], "frozen"),
        ("forklift:1", "equipment", "叉车 1 组", 20, ["forklift"], ["frozen", "chilled", "ambient"], ["seafood", "general"], ["forklift"], ""),
        ("dock:out-1", "platform", "装箱平台 1", 20, ["container_truck", "reefer_truck"], ["frozen", "chilled"], ["seafood"], ["dock_out"], ""),
        ("rail:slot-3", "rail", "铁路交接口 3 道", 20, ["rail_wagon"], ["frozen", "chilled"], ["seafood"], ["rail_slot"], ""),
    ]
    for rid, kind, name, cap, vt, tz, ct, caps, zone in catalog:
        stage = {"dock:in-1": "arrival", "dock:in-2": "unload", "qc:1": "qc", "loc:frozen-a": "putaway",
                 "forklift:1": "picking", "dock:out-1": "loading", "rail:slot-3": "rail_handover"}[rid]
        resources[stage] = rid
        log.register_resource(ctx, resource_id=rid, kind=kind, name=name, capacity=cap,
                              vehicle_types=vt, temp_zones=tz, cargo_types=ct, capabilities=caps, zone=zone)
    order = log.create_order(ctx, customer_id="customer:acme", cargo_type="seafood", temp_zone="frozen",
                             destination="杜伊斯堡", rail_cutoff_at="2026-10-03T18:00:00+08:00",
                             request_key="demo-order")
    log.register_units(ctx, order_id=order["order_id"],
                       units=[{"unit_id": "box:b001", "kind": "box", "qty": 10},
                              {"unit_id": "box:b002", "kind": "box", "qty": 6}], request_key="demo-units")
    log.split_unit(ctx, unit_id="box:b001",
                   splits=[{"unit_id": "box:b001a", "qty": 7}, {"unit_id": "box:b001b", "qty": 3}], request_key="demo-split")
    chain = log.plan_chain(ctx, order_id=order["order_id"], train_id="train:CE-2026",
                           stages=_stage_specs("2026-10-03", resources), request_key="demo-chain")
    # 读取任务清单并逐段指派/领取/交接。
    with app.database.connect() as conn:
        task_ids = {r["stage"]: r["task_id"] for r in conn.execute(
            "SELECT stage,task_id FROM logistics_tasks WHERE chain_id=?", (chain["chain_id"],))}
    carrier = AccessContext(actor_id="carrier:shunfeng", permissions=frozenset({"claim:tasks", "confirm:handovers"}),
                            scopes=frozenset({"carrier:carrier:shunfeng"}))
    active_units = ["box:b001a", "box:b001b", "box:b002"]
    for stage in STAGES:
        tid = task_ids[stage]
        log.assign_task(ctx, task_id=tid, party_id="carrier:shunfeng", request_key=f"assign-{stage}")
        log.claim_task(carrier, task_id=tid, party_id="carrier:shunfeng")
        with app.database.connect() as conn:
            qtys = {r["unit_id"]: r["qty"] for r in conn.execute(
                "SELECT unit_id,qty FROM logistics_units WHERE order_id=? AND status='active'", (order["order_id"],))}
        log.confirm_handover(carrier, task_id=tid, request_key=f"handover-{stage}", receipts=[
            {"unit_id": uid, "receipt_key": f"rcpt-{stage}-{uid}", "received_qty": qtys[uid],
             "damaged_qty": 0, "rejected_qty": 0} for uid in qtys])
    tracking = log.track(ctx, order["order_id"])
    return {"order_id": order["order_id"], "chain_id": chain["chain_id"],
            "delivered_units": tracking["units"], "next_responsible": tracking["next_responsible"],
            "estimated_outbound": tracking["estimated_outbound"], "verification": app.verify()}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="协同事务平台")
    parser.add_argument("--db", default=os.getenv("CIVICFLOW_DB", "civicflow.sqlite3"))
    parser.add_argument("--now", default=None, help="测试或演示使用的固定时间")
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("demo")
    commands.add_parser("demo-logistics")
    commands.add_parser("verify")
    commands.add_parser("list-cases")
    track_p = commands.add_parser("track", help="查询仓干配订单当前去向")
    track_p.add_argument("order_id")
    commands.add_parser("sweep", help="处理到期预约、滞留告警与未完成交接")
    gaps_p = commands.add_parser("gaps", help="资源缺口与未指派临期任务")
    gaps_p.add_argument("--horizon", default=None)
    alerts_p = commands.add_parser("alerts", help="列出未确认告警")
    alerts_p.add_argument("--all", action="store_true")
    args = parser.parse_args(argv)
    app = CivicFlow.open(Path(args.db), fixed_now=args.now)
    ctx = AccessContext.system("cli")
    if args.command == "demo":
        emit(demo(app))
    elif args.command == "demo-logistics":
        emit(demo_logistics(app))
    elif args.command == "verify":
        emit(app.verify())
    elif args.command == "list-cases":
        emit(CaseService(app.repository).list_current(ctx))
    elif args.command == "track":
        emit(app.logistics.track(ctx, args.order_id))
    elif args.command == "sweep":
        emit(app.logistics.sweep())
    elif args.command == "gaps":
        emit(app.logistics.resource_gaps(ctx, horizon_end=args.horizon))
    elif args.command == "alerts":
        emit(app.logistics.list_alerts(ctx, include_acked=args.all))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
