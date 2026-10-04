"""SQLite 连接、事务和数据库初始化。"""

from __future__ import annotations

import sqlite3
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator


SCHEMA = r"""
PRAGMA foreign_keys = ON;
CREATE TABLE IF NOT EXISTS entities (
    entity_type TEXT NOT NULL,
    entity_id TEXT NOT NULL,
    version INTEGER NOT NULL,
    state TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    created_by TEXT NOT NULL,
    updated_by TEXT NOT NULL,
    PRIMARY KEY(entity_type, entity_id)
);
CREATE TABLE IF NOT EXISTS entity_versions (
    entity_type TEXT NOT NULL,
    entity_id TEXT NOT NULL,
    version INTEGER NOT NULL,
    state TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    valid_from TEXT NOT NULL,
    actor_id TEXT NOT NULL,
    request_key TEXT NOT NULL,
    PRIMARY KEY(entity_type, entity_id, version)
);
CREATE INDEX IF NOT EXISTS entity_versions_asof ON entity_versions(entity_type, entity_id, valid_from, version);
CREATE TABLE IF NOT EXISTS idempotency_keys (
    scope TEXT NOT NULL,
    request_key TEXT NOT NULL,
    request_digest TEXT NOT NULL,
    response_json TEXT NOT NULL,
    created_at TEXT NOT NULL,
    PRIMARY KEY(scope, request_key)
);
CREATE TABLE IF NOT EXISTS audit_entries (
    audit_id INTEGER PRIMARY KEY AUTOINCREMENT,
    occurred_at TEXT NOT NULL,
    actor_id TEXT NOT NULL,
    action TEXT NOT NULL,
    entity_type TEXT NOT NULL,
    entity_id TEXT NOT NULL,
    version INTEGER NOT NULL,
    detail_json TEXT NOT NULL,
    previous_digest TEXT NOT NULL,
    entry_digest TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS inbox_messages (
    source TEXT NOT NULL,
    source_key TEXT NOT NULL,
    sequence INTEGER NOT NULL,
    payload_digest TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    occurred_at TEXT NOT NULL,
    received_at TEXT NOT NULL,
    status TEXT NOT NULL,
    PRIMARY KEY(source, source_key, sequence)
);
CREATE TABLE IF NOT EXISTS inbox_conflicts (
    conflict_id INTEGER PRIMARY KEY AUTOINCREMENT,
    source TEXT NOT NULL,
    source_key TEXT NOT NULL,
    sequence INTEGER NOT NULL,
    existing_digest TEXT NOT NULL,
    incoming_digest TEXT NOT NULL,
    received_at TEXT NOT NULL DEFAULT '',
    payload_json TEXT NOT NULL DEFAULT '',
    occurred_at TEXT NOT NULL DEFAULT '',
    status TEXT NOT NULL DEFAULT 'pending_review',
    resolved_by TEXT NOT NULL DEFAULT '',
    resolved_at TEXT
);

CREATE TABLE IF NOT EXISTS outbox_messages (
    message_id TEXT PRIMARY KEY,
    topic TEXT NOT NULL,
    aggregate_id TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    available_at TEXT NOT NULL,
    lease_until TEXT,
    attempts INTEGER NOT NULL DEFAULT 0,
    status TEXT NOT NULL,
    delivered_at TEXT
);
CREATE INDEX IF NOT EXISTS outbox_ready ON outbox_messages(status, available_at, lease_until);
CREATE TABLE IF NOT EXISTS journal_entries (
    entry_id TEXT PRIMARY KEY,
    journal_key TEXT NOT NULL,
    account TEXT NOT NULL,
    currency TEXT NOT NULL,
    amount_minor INTEGER NOT NULL,
    direction TEXT NOT NULL,
    reference TEXT NOT NULL,
    reversed_entry_id TEXT,
    occurred_at TEXT NOT NULL,
    posted_by TEXT NOT NULL,
    FOREIGN KEY(reversed_entry_id) REFERENCES journal_entries(entry_id)
);
CREATE INDEX IF NOT EXISTS journal_reference ON journal_entries(journal_key, reference, occurred_at);
CREATE TABLE IF NOT EXISTS resource_reservations (
    reservation_id TEXT PRIMARY KEY,
    resource_id TEXT NOT NULL,
    subject_id TEXT NOT NULL,
    quantity INTEGER NOT NULL,
    start_at TEXT NOT NULL,
    end_at TEXT NOT NULL,
    status TEXT NOT NULL,
    version INTEGER NOT NULL,
    created_by TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS reservation_window ON resource_reservations(resource_id, start_at, end_at, status);
CREATE TABLE IF NOT EXISTS scheduled_jobs (
    job_id TEXT PRIMARY KEY,
    job_type TEXT NOT NULL,
    subject_id TEXT NOT NULL,
    run_at TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    status TEXT NOT NULL,
    attempt INTEGER NOT NULL DEFAULT 0,
    lease_until TEXT,
    last_error TEXT NOT NULL DEFAULT ''
);
CREATE INDEX IF NOT EXISTS jobs_due ON scheduled_jobs(status, run_at, lease_until);
CREATE TABLE IF NOT EXISTS logistics_parties (
    party_id TEXT PRIMARY KEY,
    kind TEXT NOT NULL,
    name TEXT NOT NULL,
    created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS logistics_orders (
    order_id TEXT PRIMARY KEY,
    customer_id TEXT NOT NULL,
    cargo_type TEXT NOT NULL,
    temp_zone TEXT NOT NULL,
    destination TEXT NOT NULL,
    rail_cutoff_at TEXT NOT NULL,
    state TEXT NOT NULL,
    version INTEGER NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS logistics_units (
    unit_id TEXT PRIMARY KEY,
    kind TEXT NOT NULL,
    order_id TEXT NOT NULL,
    customer_id TEXT NOT NULL,
    parent_id TEXT,
    root_id TEXT NOT NULL,
    qty INTEGER NOT NULL,
    cargo_type TEXT NOT NULL,
    temp_zone TEXT NOT NULL,
    status TEXT NOT NULL,
    location TEXT NOT NULL DEFAULT '',
    owner_task_id TEXT,
    created_at TEXT NOT NULL,
    ended_at TEXT,
    terminal_reason TEXT NOT NULL DEFAULT ''
);
CREATE INDEX IF NOT EXISTS units_order ON logistics_units(order_id, status);
CREATE INDEX IF NOT EXISTS units_parent ON logistics_units(parent_id);
CREATE INDEX IF NOT EXISTS units_root ON logistics_units(root_id);
CREATE INDEX IF NOT EXISTS units_owner ON logistics_units(owner_task_id);
CREATE TABLE IF NOT EXISTS logistics_lineage (
    lineage_id INTEGER PRIMARY KEY AUTOINCREMENT,
    event TEXT NOT NULL,
    parent_id TEXT,
    child_id TEXT,
    qty INTEGER NOT NULL,
    order_id TEXT NOT NULL,
    actor TEXT NOT NULL,
    occurred_at TEXT NOT NULL,
    ref_key TEXT NOT NULL DEFAULT '',
    detail_json TEXT NOT NULL DEFAULT '{}'
);
CREATE INDEX IF NOT EXISTS lineage_child ON logistics_lineage(child_id);
CREATE INDEX IF NOT EXISTS lineage_parent ON logistics_lineage(parent_id);
CREATE TABLE IF NOT EXISTS logistics_resources (
    resource_id TEXT PRIMARY KEY,
    kind TEXT NOT NULL,
    name TEXT NOT NULL,
    accepted_vehicle_types_json TEXT NOT NULL DEFAULT '[]',
    accepted_temp_zones_json TEXT NOT NULL DEFAULT '[]',
    accepted_cargo_types_json TEXT NOT NULL DEFAULT '[]',
    capabilities_json TEXT NOT NULL DEFAULT '[]',
    zone TEXT NOT NULL DEFAULT '',
    capacity INTEGER NOT NULL,
    active INTEGER NOT NULL DEFAULT 1
);
CREATE TABLE IF NOT EXISTS logistics_reservations (
    reservation_id TEXT PRIMARY KEY,
    resource_id TEXT NOT NULL,
    subject_id TEXT NOT NULL,
    order_id TEXT NOT NULL,
    vehicle_type TEXT NOT NULL,
    temp_zone TEXT NOT NULL,
    cargo_type TEXT NOT NULL,
    required_capability TEXT NOT NULL DEFAULT '',
    quantity INTEGER NOT NULL,
    start_at TEXT NOT NULL,
    end_at TEXT NOT NULL,
    rail_cutoff_at TEXT,
    chain_id TEXT NOT NULL DEFAULT '',
    task_id TEXT NOT NULL DEFAULT '',
    status TEXT NOT NULL,
    replaced_by TEXT NOT NULL DEFAULT '',
    version INTEGER NOT NULL,
    created_by TEXT NOT NULL,
    created_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS logres_window ON logistics_reservations(resource_id, start_at, end_at, status);
CREATE INDEX IF NOT EXISTS logres_chain ON logistics_reservations(chain_id, task_id);
CREATE TABLE IF NOT EXISTS logistics_chains (
    chain_id TEXT PRIMARY KEY,
    order_id TEXT NOT NULL,
    train_id TEXT NOT NULL,
    rail_cutoff_at TEXT NOT NULL,
    state TEXT NOT NULL,
    generated_at TEXT NOT NULL,
    superseded_at TEXT,
    revision INTEGER NOT NULL DEFAULT 1
);
CREATE INDEX IF NOT EXISTS chains_order ON logistics_chains(order_id, state);
CREATE TABLE IF NOT EXISTS logistics_tasks (
    task_id TEXT PRIMARY KEY,
    chain_id TEXT NOT NULL,
    order_id TEXT NOT NULL,
    stage TEXT NOT NULL,
    seq INTEGER NOT NULL,
    planned_start_at TEXT NOT NULL,
    planned_end_at TEXT NOT NULL,
    reservation_id TEXT,
    resource_id TEXT NOT NULL DEFAULT '',
    assignee_party_id TEXT,
    state TEXT NOT NULL,
    claimed_at TEXT,
    completed_at TEXT,
    terminal_at TEXT,
    terminal_reason TEXT NOT NULL DEFAULT '',
    affected_reason TEXT NOT NULL DEFAULT '',
    version INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS tasks_chain ON logistics_tasks(chain_id, seq);
CREATE INDEX IF NOT EXISTS tasks_assign ON logistics_tasks(state, assignee_party_id);
CREATE TABLE IF NOT EXISTS logistics_task_units (
    task_id TEXT NOT NULL,
    unit_id TEXT NOT NULL,
    qty INTEGER NOT NULL,
    PRIMARY KEY(task_id, unit_id)
);
CREATE TABLE IF NOT EXISTS logistics_handovers (
    handover_id TEXT PRIMARY KEY,
    task_id TEXT NOT NULL,
    stage TEXT NOT NULL,
    unit_id TEXT NOT NULL,
    expected_qty INTEGER NOT NULL,
    received_qty INTEGER,
    damaged_qty INTEGER NOT NULL DEFAULT 0,
    rejected_qty INTEGER NOT NULL DEFAULT 0,
    status TEXT NOT NULL,
    receipt_key TEXT NOT NULL DEFAULT '',
    actor TEXT NOT NULL,
    confirmed_at TEXT,
    created_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS handovers_task ON logistics_handovers(task_id, status);
CREATE INDEX IF NOT EXISTS handovers_receipt ON logistics_handovers(receipt_key);
CREATE TABLE IF NOT EXISTS logistics_discrepancies (
    discrepancy_id TEXT PRIMARY KEY,
    handover_id TEXT NOT NULL,
    task_id TEXT NOT NULL,
    unit_id TEXT NOT NULL,
    kind TEXT NOT NULL,
    qty INTEGER NOT NULL,
    note TEXT NOT NULL,
    submitter TEXT NOT NULL,
    state TEXT NOT NULL,
    reviewer TEXT NOT NULL DEFAULT '',
    resolution TEXT NOT NULL DEFAULT '',
    created_at TEXT NOT NULL,
    reviewed_at TEXT
);
CREATE TABLE IF NOT EXISTS logistics_watches (
    watch_id TEXT PRIMARY KEY,
    subject_type TEXT NOT NULL,
    subject_id TEXT NOT NULL,
    due_at TEXT NOT NULL,
    kind TEXT NOT NULL,
    state TEXT NOT NULL,
    fired_at TEXT,
    payload_json TEXT NOT NULL DEFAULT '{}'
);
CREATE INDEX IF NOT EXISTS watches_due ON logistics_watches(state, due_at);
CREATE TABLE IF NOT EXISTS logistics_alerts (
    alert_id TEXT PRIMARY KEY,
    watch_id TEXT NOT NULL,
    kind TEXT NOT NULL,
    subject_type TEXT NOT NULL,
    subject_id TEXT NOT NULL,
    message TEXT NOT NULL,
    raised_at TEXT NOT NULL,
    acknowledged_at TEXT
);
CREATE INDEX IF NOT EXISTS alerts_unacked ON logistics_alerts(acknowledged_at);
"""


class Database:
    def __init__(self, path: str | Path):
        self.path = str(path)

    def connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.path, timeout=30, isolation_level=None)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys=ON")
        connection.execute("PRAGMA journal_mode=WAL")
        connection.execute("PRAGMA busy_timeout=30000")
        return connection

    def initialize(self) -> None:
        with self.connect() as connection:
            connection.executescript(SCHEMA)
            self._migrate(connection)

    @staticmethod
    def _migrate(connection: sqlite3.Connection) -> None:
        """对早期版本的库做幂等加列，不破坏既有数据。"""
        columns = {row["name"] for row in connection.execute("PRAGMA table_info(inbox_conflicts)")}
        additions = {
            "payload_json": "TEXT NOT NULL DEFAULT ''",
            "occurred_at": "TEXT NOT NULL DEFAULT ''",
            "status": "TEXT NOT NULL DEFAULT 'pending_review'",
            "resolved_by": "TEXT NOT NULL DEFAULT ''",
            "resolved_at": "TEXT",
        }
        for name, declaration in additions.items():
            if name not in columns:
                connection.execute(f"ALTER TABLE inbox_conflicts ADD COLUMN {name} {declaration}")
        connection.execute("CREATE UNIQUE INDEX IF NOT EXISTS inbox_conflict_digest ON inbox_conflicts(source, source_key, sequence, incoming_digest)")
        connection.execute("CREATE INDEX IF NOT EXISTS inbox_conflict_status ON inbox_conflicts(status)")

    @contextmanager
    def transaction(self, *, immediate: bool = True) -> Iterator[sqlite3.Connection]:
        connection = self.connect()
        try:
            connection.execute("BEGIN IMMEDIATE" if immediate else "BEGIN")
            yield connection
            connection.commit()
        except BaseException:
            connection.rollback()
            raise
        finally:
            connection.close()
