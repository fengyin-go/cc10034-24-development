"""检查点管理领域服务：管理CDC采集的位点检查点提交和回退。

上游：cdc_channel 启动后记录检查点
消费：position_store 消费检查点更新位点记录；主备配对登记确认
下游：failover_group 通过恢复位置包判定是否满足切换条件
"""
from __future__ import annotations

import hashlib
import json
from datetime import datetime
from typing import Any

from app.services.base import BaseModuleService
from app.pipeline import CheckpointRecord, PositionSnapshot, pipeline_bus
from app.services.recovery_position import recovery_registry


class CheckpointManagerService(BaseModuleService):
    MODULE = "checkpoint_manager"
    ENTITY = "检查点"
    OPERATION_TERM = "采集"
    LIST_FIELDS = ["检查点ID", "所属通道", "Binlog文件", "偏移量", "确认时间", "是否提交", "数据摘要"]
    REQUIRED_FIELDS = ["检查点ID", "所属通道", "Binlog文件"]
    STATUS_ORDER = ["已记录", "已提交", "已过期", "已回退"]
    ACTION_RULES = {"提交检查点": "已记录", "回退检查点": "已记录", "标记过期": "已提交"}
    NEGATIVE_ACTIONS = []

    def register_pair_confirmation(
        self, values: dict[str, Any]
    ) -> tuple[dict[str, Any] | None, list[str], str]:
        """登记主端 / 备用端检查点确认；两端到齐且可比时自动生成恢复位置包。"""
        return recovery_registry.register_confirmation(values)

    def pair_status(self, group: str | None = None) -> dict[str, Any]:
        """当前主备配对与恢复位置包状态。"""
        with recovery_registry.lock:
            confirmations = [
                row.to_dict() for row in recovery_registry._confirmations(group)
            ]
            packages = [
                package.to_dict() for package in recovery_registry._packages(group)
            ]
            latest = packages[-1] if packages else None
            pending: dict[str, list[str]] = {}
            for row in recovery_registry._confirmations(group):
                missing = recovery_registry._pending_roles_locked(
                    row.group_name, row.generation
                )
                if missing:
                    pending[f"{row.group_name}#代次{row.generation}"] = missing
            return {
                "group": group,
                "confirmation_count": len(confirmations),
                "confirmations": confirmations,
                "package_count": len(packages),
                "packages": packages,
                "latest_package_version": (
                    latest["package_version"] if latest else None
                ),
                "latest_continuation_start": (
                    latest["continuation_start"] if latest else None
                ),
                "pending_generations": pending,
            }

    def resolve_recovery_package(
        self,
        group: str,
        generation: int | None = None,
        strict: bool = False,
    ) -> dict[str, Any]:
        """下载时解析要交给演练的恢复位置包（只取已确认的完整配对）。"""
        return recovery_registry.resolve_delivery(group, generation, strict=strict)

    def lookup_checkpoint_package(self, entry_id: int) -> dict[str, Any] | None:
        """按检查点记录反查它所属的恢复位置包版本和接续起点。"""
        return recovery_registry.lookup_checkpoint(entry_id)

    def list_handoffs(self, group: str | None = None, limit: int = 50) -> dict[str, Any]:
        """下载交接记录：哪笔下载把哪个包版本交给了哪次演练。"""
        handoffs = recovery_registry.list_handoffs(group, limit)
        return {
            "group": group,
            "count": len(handoffs),
            "items": handoffs,
        }

    def create_checkpoint(self, channel_name: str, binlog_file: str, offset: int,
                          data_payload: dict[str, Any] | None = None) -> CheckpointRecord:
        """创建检查点记录。

        基于通道当前位点和缓冲数据摘要生成检查点，
        自动注册到 Pipeline 和 position_store 中。
        """
        payload = data_payload or {}
        data_checksum = self.checksum_payload(payload)

        checkpoint = CheckpointRecord(
            checkpoint_id=f"ckpt_{datetime.now().strftime('%Y%m%d%H%M%S')}_{hashlib.md5(channel_name.encode()).hexdigest()[:6]}",
            channel_name=channel_name,
            binlog_file=binlog_file,
            offset=offset,
            confirmed_at=datetime.now(),
            committed=False,
            data_checksum=data_checksum,
        )
        pipeline_bus.checkpoints.append(checkpoint)

        # 联动更新位点存储
        ps = PositionSnapshot(
            instance=channel_name,
            binlog_name=binlog_file,
            position=offset,
            gtid_set=payload.get("gtid_set", ""),
            updated_at=datetime.now(),
            source="checkpoint_manager",
            channel_id=hash(channel_name) % 10000,
        )
        pipeline_bus.register_position(ps)

        pipeline_bus.write_audit(
            "创建检查点", channel_name,
            f"检查点 {checkpoint.checkpoint_id} 位点 {binlog_file}:{offset}",
            "checkpoint_manager", "已记录"
        )
        return checkpoint

    def commit_checkpoint(self, checkpoint_id: str) -> dict[str, Any]:
        """提交检查点。

        将检查点标记为已提交，向管道总线确认位点固化。
        """
        for cp in pipeline_bus.checkpoints:
            if cp.checkpoint_id == checkpoint_id:
                cp.committed = True
                cp.confirmed_at = datetime.now()
                pipeline_bus.write_audit(
                    "提交检查点", cp.channel_name,
                    f"检查点 {checkpoint_id} 已提交，位点 {cp.binlog_file}:{cp.offset}",
                    "checkpoint_manager", "已提交"
                )
                return {"checkpoint_id": checkpoint_id, "committed": True, "position": f"{cp.binlog_file}:{cp.offset}"}
        return {"checkpoint_id": checkpoint_id, "committed": False, "reason": "not found"}

    def rollback_to_checkpoint(self, checkpoint_id: str) -> dict[str, Any]:
        """回退到指定检查点。

        标记中间检查点为已回退状态，触发 failover_group 消费回退状态。
        """
        target_idx = -1
        for i, cp in enumerate(pipeline_bus.checkpoints):
            if cp.checkpoint_id == checkpoint_id:
                target_idx = i
                break
        if target_idx < 0:
            return {"success": False, "reason": "checkpoint not found"}
        # 标记目标之后的检查点为已回退
        for cp in pipeline_bus.checkpoints[target_idx:]:
            cp.committed = False
        pipeline_bus.write_audit(
            "回退检查点", pipeline_bus.checkpoints[target_idx].channel_name,
            f"回退至检查点 {checkpoint_id}",
            "checkpoint_manager", "已回退"
        )
        return {"success": True, "checkpoint_id": checkpoint_id}

    def checksum_payload(self, payload: dict[str, Any]) -> str:
        """对数据负载计算摘要。"""
        raw = json.dumps(payload, sort_keys=True, default=str)
        return hashlib.sha256(raw.encode()).hexdigest()[:16]

    def compute_backup_position(self, channel_name: str) -> dict[str, Any]:
        """计算备份位点（最近一次提交的检查点）。"""
        commits = [cp for cp in pipeline_bus.checkpoints
                   if cp.channel_name == channel_name and cp.committed]
        if not commits:
            return {"has_backup": False, "reason": "no committed checkpoint"}
        latest = commits[-1]
        return {
            "has_backup": True,
            "checkpoint_id": latest.checkpoint_id,
            "position": f"{latest.binlog_file}:{latest.offset}",
            "confirmed_at": latest.confirmed_at.isoformat(),
        }

    def compare_checkpoint_lag(self, cp_a: CheckpointRecord, cp_b: CheckpointRecord) -> dict[str, Any]:
        """比较两个检查点的位点距离。"""
        offset_diff = 0
        if cp_a.binlog_file == cp_b.binlog_file:
            offset_diff = abs(cp_a.offset - cp_b.offset)
        else:
            offset_diff = -1
        return {
            "offset_diff": offset_diff,
            "time_diff_sec": abs((cp_a.confirmed_at - cp_b.confirmed_at).total_seconds()),
            "same_file": cp_a.binlog_file == cp_b.binlog_file,
        }

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        entry, msg = super().run_action(entry_id, action)
        if entry and action == "提交检查点":
            channel = entry.get("所属通道", "")
            binlog = entry.get("Binlog文件", "mysql-bin.000001")
            offset = int(entry.get("偏移量", "0"))
            self.create_checkpoint(channel, binlog, offset)
        elif entry and action == "回退检查点":
            cid = entry.get("检查点ID", "")
            self.rollback_to_checkpoint(cid)
        return entry, msg

    def list_entries(self, **kwargs: Any):
        """列表记录带上所属恢复位置包版本和接续起点，供值班员直接查看。"""
        rows, total = super().list_entries(**kwargs)
        return [self._decorate(dict(row)) for row in rows], total

    def get_entry(self, entry_id: int) -> dict[str, Any] | None:
        row = super().get_entry(entry_id)
        return self._decorate(dict(row)) if row is not None else None

    def _decorate(self, row: dict[str, Any]) -> dict[str, Any]:
        checkpoint_id = str(row.get("检查点ID") or "")
        link = (
            recovery_registry.checkpoint_links.get(checkpoint_id)
            if checkpoint_id
            else None
        )
        if link is not None and not row.get("恢复包版本"):
            row["恢复包版本"] = f"v{link['package_version']}"
            row["接续起点"] = link["continuation_start"]
        return row