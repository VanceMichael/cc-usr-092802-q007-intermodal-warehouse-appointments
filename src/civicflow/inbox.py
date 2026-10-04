"""带来源序号的消息接入、冲突隔离与核对。"""

from __future__ import annotations

from dataclasses import dataclass

from .database import Database
from .errors import ConflictError, NotFoundError, ValidationError
from .identifiers import require_safe
from .jsonutil import canonical_json, digest_json
from .timeutil import Clock, canonical_instant


@dataclass(frozen=True)
class Inbox:
    database: Database
    clock: Clock

    def receive(self, *, source: str, source_key: str, sequence: int, payload: dict, occurred_at: str, quarantine: bool = False) -> dict:
        """接入一条消息。

        相同来源序号且摘要一致：重复回执，直接返回 duplicate，不产生任何副作用
        （调用方据此跳过资源占用）。摘要冲突：原始消息保持不变，冲突内容进入
        inbox_conflicts 等待人工核对；quarantine=True 时不抛异常而返回 quarantined。
        """
        require_safe(source, "来源"); require_safe(source_key, "来源标识")
        if sequence < 0:
            raise ValidationError("来源序号不能为负数")
        occurred_at = canonical_instant(occurred_at); digest = digest_json(payload)
        with self.database.transaction() as connection:
            row = connection.execute("SELECT * FROM inbox_messages WHERE source=? AND source_key=? AND sequence=?", (source, source_key, sequence)).fetchone()
            if row:
                if row["payload_digest"] != digest:
                    connection.execute("INSERT OR IGNORE INTO inbox_conflicts(source,source_key,sequence,existing_digest,incoming_digest,received_at,payload_json,occurred_at,status) VALUES(?,?,?,?,?,?,?,?, 'pending_review')", (source, source_key, sequence, row["payload_digest"], digest, self.clock.now(), canonical_json(payload), occurred_at))
                    if quarantine:
                        return {"status": "quarantined", "digest": digest}
                    raise ConflictError("相同来源序号出现不同内容，冲突消息已隔离待核对")
                return {"status": "duplicate", "digest": digest}
            connection.execute("INSERT INTO inbox_messages(source,source_key,sequence,payload_digest,payload_json,occurred_at,received_at,status) VALUES(?,?,?,?,?,?,?,?)", (source, source_key, sequence, digest, canonical_json(payload), occurred_at, self.clock.now(), "accepted"))
            return {"status": "accepted", "digest": digest}

    def timeline(self, source: str, source_key: str) -> list[dict]:
        with self.database.connect() as connection:
            return [dict(row) for row in connection.execute("SELECT * FROM inbox_messages WHERE source=? AND source_key=? ORDER BY occurred_at,sequence", (source, source_key))]

    def list_conflicts(self, *, status: str = "pending_review") -> list[dict]:
        with self.database.connect() as connection:
            return [dict(row) for row in connection.execute("SELECT * FROM inbox_conflicts WHERE status=? ORDER BY conflict_id", (status,))]

    def resolve_conflict(self, conflict_id: int, *, decision: str, actor: str) -> dict:
        """核对冲突消息：accept_incoming 用新内容替换，keep_existing 保留原消息。

        未核对前冲突内容绝不参与业务处理；核对结果与操作人留痕，重复核对被拒绝。
        """
        if decision not in {"accept_incoming", "keep_existing"}:
            raise ValidationError("decision 必须是 accept_incoming 或 keep_existing")
        with self.database.transaction() as connection:
            row = connection.execute("SELECT * FROM inbox_conflicts WHERE conflict_id=?", (conflict_id,)).fetchone()
            if not row:
                raise NotFoundError("冲突记录不存在")
            if row["status"] != "pending_review":
                raise ConflictError("冲突已经核对，不能重复处理")
            now = self.clock.now()
            if decision == "accept_incoming":
                changed = connection.execute(
                    "UPDATE inbox_messages SET payload_digest=?,payload_json=?,occurred_at=?,status='corrected' WHERE source=? AND source_key=? AND sequence=? AND payload_digest=?",
                    (row["incoming_digest"], row["payload_json"], row["occurred_at"], row["source"], row["source_key"], row["sequence"], row["existing_digest"])).rowcount
                if changed != 1:
                    raise ConflictError("原消息已被其他核对改动，请重新核对")
            connection.execute("UPDATE inbox_conflicts SET status=?,resolved_by=?,resolved_at=? WHERE conflict_id=?", (f"resolved_{decision}", actor, now, conflict_id))
            return {"conflict_id": conflict_id, "status": f"resolved_{decision}", "resolved_by": actor, "resolved_at": now}
