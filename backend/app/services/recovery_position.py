"""主备检查点配对与恢复位置包。

职责：
- 登记主端 / 备用端各自的检查点确认（同一组、同一代次、同一 Binlog 文件才能比较）。
- 把校验通过的完整配对固化成不可变的「恢复位置包」，供故障切换演练读取。
- 下载时按已确认的完整配对解析出包版本和接续起点；配对不完整时沿用上一份
  已确认配对，绝不用「一端已确认、另一端未确认」的半成品出包。
- 同一包版本重复下载返回完全相同的文件内容（payload 在出包时固化）。
- 并发出包只接收完整配对：登记、配对、出包、交接都在同一把锁里完成。

上游：checkpoint_manager 登记确认
消费：failover_group 演练读取恢复位置包
下游：checkpoint_manager 检查点记录可反查包版本与接续起点
"""
from __future__ import annotations

import hashlib
import json
import re
from datetime import datetime, timedelta
from typing import Any

from app.pipeline import (
    CheckpointConfirmation,
    RecoveryPositionPackage,
    pipeline_bus,
)
from app.store import store

ROLE_PRIMARY = "primary"
ROLE_STANDBY = "standby"
ROLE_LABELS = {ROLE_PRIMARY: "主端", ROLE_STANDBY: "备用端"}
ROLE_ALIASES = {
    "primary": ROLE_PRIMARY,
    "主端": ROLE_PRIMARY,
    "主": ROLE_PRIMARY,
    "主库": ROLE_PRIMARY,
    "standby": ROLE_STANDBY,
    "secondary": ROLE_STANDBY,
    "备用端": ROLE_STANDBY,
    "备端": ROLE_STANDBY,
    "备用": ROLE_STANDBY,
    "备库": ROLE_STANDBY,
}

FIELD_ALIASES = {
    "group": ("组名", "group", "group_name", "切换组"),
    "role": ("角色", "role", "端", "位置角色"),
    "channel": ("所属通道", "通道", "channel", "channel_name"),
    "checkpoint_id": ("检查点ID", "checkpoint_id", "检查点"),
    "binlog_file": ("Binlog文件", "binlog_file", "Binlog名称"),
    "offset": ("偏移量", "offset", "位点", "位点值"),
    "generation": ("代次", "generation", "配对代次", "版本代次"),
}

CHECKPOINT_MODULE = "checkpoint_manager"
FAILOVER_MODULE = "failover_group"


def _pick(values: dict[str, Any], key: str) -> Any:
    for alias in FIELD_ALIASES[key]:
        if alias in values and str(values.get(alias) or "").strip() != "":
            return values.get(alias)
    return None


def _to_int(value: Any, default: int = -1) -> int:
    try:
        return int(str(value).replace(",", "").replace("_", "").strip())
    except (TypeError, ValueError):
        return default


def _slug(text: str) -> str:
    cleaned = re.sub(r"[^0-9A-Za-z一-龥]+", "", text or "")
    return cleaned[:18] or "group"


def _position(binlog_file: str, offset: int) -> str:
    return f"{binlog_file}:{offset}"


class RecoveryPositionRegistry:
    """恢复位置包的唯一状态机。所有变更都在 pairing_lock 内完成。"""

    def __init__(self) -> None:
        # 检查点ID → 最近一次参与的包信息，供检查点记录反查。
        self.checkpoint_links: dict[str, dict[str, Any]] = {}
        # 组名 → 展示用的主备通道，由确认登记或切换组补齐。
        self._bootstrapped = False

    # ---------- 基础工具 ----------

    @property
    def lock(self):
        return pipeline_bus.pairing_lock

    def _confirmations(self, group: str | None = None) -> list[CheckpointConfirmation]:
        rows = pipeline_bus.confirmations
        if group:
            rows = [row for row in rows if row.group_name == group]
        return sorted(rows, key=lambda row: (row.confirmed_at, row.confirmation_id))

    def _packages(self, group: str | None = None) -> list[RecoveryPositionPackage]:
        rows = pipeline_bus.recovery_packages
        if group:
            rows = [row for row in rows if row.group_name == group]
        return sorted(rows, key=lambda row: row.package_version)

    def _next_confirmation_id(self) -> str:
        return f"pcnf_{len(pipeline_bus.confirmations) + 1:04d}"

    def _next_package_version(self, group: str) -> int:
        versions = [pkg.package_version for pkg in self._packages(group)]
        return (max(versions) + 1) if versions else 1

    def _next_handoff_id(self) -> str:
        return f"hdl_{len(pipeline_bus.package_handoffs) + 1:04d}"

    # ---------- 确认登记 ----------

    def register_confirmation(
        self, values: dict[str, Any]
    ) -> tuple[dict[str, Any] | None, list[str], str]:
        """登记一条主端 / 备用端确认。

        返回值：(确认记录, 缺失字段, 说明)。只有完整、合法的确认才会入库；
        同组同代次同角色重复提交相同位点时按幂等处理，不会产生第二条记录。
        """
        group = str(_pick(values, "group") or "").strip()
        role_raw = str(_pick(values, "role") or "").strip()
        binlog_file = str(_pick(values, "binlog_file") or "").strip()
        offset_raw = _pick(values, "offset")
        generation_raw = _pick(values, "generation")
        channel = str(_pick(values, "channel") or "").strip()
        checkpoint_id = str(_pick(values, "checkpoint_id") or "").strip()

        missing: list[str] = []
        if not group:
            missing.append("组名")
        if not role_raw:
            missing.append("角色")
        if not binlog_file:
            missing.append("Binlog文件")
        if offset_raw is None or str(offset_raw).strip() == "":
            missing.append("偏移量")
        if generation_raw is None or str(generation_raw).strip() == "":
            missing.append("代次")
        if missing:
            return None, missing, f"缺少必填字段：{'、'.join(missing)}"

        role = ROLE_ALIASES.get(role_raw.casefold(), ROLE_ALIASES.get(role_raw, ""))
        if not role:
            return None, ["角色"], f"角色「{role_raw}」不可识别，应为主端或备用端"

        offset = _to_int(offset_raw)
        generation = _to_int(generation_raw)
        if offset < 0:
            return None, ["偏移量"], "偏移量必须是非负整数"
        if generation < 1:
            return None, ["代次"], "代次必须是从 1 开始的整数"

        # 上传的 Binlog 文件与位点必须是可比代次的前提之一：文件名固定后
        # 才谈得上同一文件内的偏移量比较。
        if not re.match(r"^[\w.\-]+$", binlog_file):
            return None, ["Binlog文件"], f"Binlog 文件「{binlog_file}」格式不合法"

        with self.lock:
            for existing in pipeline_bus.confirmations:
                if (
                    existing.group_name == group
                    and existing.generation == generation
                    and existing.role == role
                ):
                    if (
                        existing.binlog_file == binlog_file
                        and existing.offset == offset
                    ):
                        return (
                            existing.to_dict(),
                            [],
                            f"{ROLE_LABELS[role]}代次{generation}的相同位点已确认，按幂等忽略",
                        )
                    return (
                        None,
                        [],
                        f"{ROLE_LABELS[role]}代次{generation}已存在位点 "
                        f"{_position(existing.binlog_file, existing.offset)}，"
                        f"不能再登记 {_position(binlog_file, offset)}；"
                        "同代次同角色只允许一个确认位点",
                    )

            if not channel:
                channel = f"{group}-{ROLE_LABELS[role]}"
            if not checkpoint_id:
                checkpoint_id = (
                    f"ckpt_{_slug(group)}_{role[0]}{generation:03d}_{offset}"
                )
            elif any(
                row.get("检查点ID") == checkpoint_id for row in store.rows(CHECKPOINT_MODULE)
            ):
                checkpoint_id = f"{checkpoint_id}_g{generation}"

            confirmation = CheckpointConfirmation(
                confirmation_id=self._next_confirmation_id(),
                group_name=group,
                role=role,
                channel_name=channel,
                checkpoint_id=checkpoint_id,
                binlog_file=binlog_file,
                offset=offset,
                generation=generation,
                confirmed_at=datetime.now(),
                data_checksum=hashlib.sha256(
                    f"{binlog_file}:{offset}:{generation}".encode()
                ).hexdigest()[:16],
            )
            pipeline_bus.confirmations.append(confirmation)
            self._append_checkpoint_row(confirmation)

            package, reason = self._ensure_package_locked(group, generation)
            pipeline_bus.write_audit(
                "登记主备确认", group,
                f"{ROLE_LABELS[role]}代次{generation}确认位点 "
                f"{_position(binlog_file, offset)}；{reason}",
                "checkpoint_manager", "已记录",
            )
            message = f"{ROLE_LABELS[role]}代次{generation}位点 {_position(binlog_file, offset)} 已登记"
            if package is not None:
                message += (
                    f"；已生成恢复位置包 v{package.package_version}，"
                    f"接续起点 {package.continuation_start}"
                )
            else:
                message += f"；{reason}"
            return confirmation.to_dict(), [], message

    def _append_checkpoint_row(self, confirmation: CheckpointConfirmation) -> None:
        rows = store.rows(CHECKPOINT_MODULE)
        row = {
            "id": max((int(item.get("id", 0)) for item in rows), default=0) + 1,
            "status": "已记录",
            "pending": True,
            "abnormal": False,
            "检查点ID": confirmation.checkpoint_id,
            "所属通道": confirmation.channel_name,
            "Binlog文件": confirmation.binlog_file,
            "偏移量": str(confirmation.offset),
            "确认时间": confirmation.confirmed_at.isoformat(),
            "是否提交": "是",
            "数据摘要": confirmation.data_checksum,
            # 以下字段不占列表列位，但会随记录返回，供反查包版本和接续起点。
            "配对组": confirmation.group_name,
            "配对代次": str(confirmation.generation),
            "配对角色": ROLE_LABELS[confirmation.role],
            "恢复包版本": "",
            "接续起点": "",
        }
        rows.append(row)

    # ---------- 配对校验 ----------

    def validate_pair(
        self, primary: CheckpointConfirmation, standby: CheckpointConfirmation
    ) -> dict[str, Any]:
        """校验两端确认是否属于可比代次，返回逐项检查结果。"""
        checks = [
            {
                "name": "代次一致",
                "ok": primary.generation == standby.generation,
                "detail": f"主端代次{primary.generation} / 备用端代次{standby.generation}",
            },
            {
                "name": "Binlog文件相同",
                "ok": primary.binlog_file == standby.binlog_file,
                "detail": f"主端 {primary.binlog_file} / 备用端 {standby.binlog_file}",
            },
            {
                "name": "组一致",
                "ok": primary.group_name == standby.group_name,
                "detail": f"主端 {primary.group_name} / 备用端 {standby.group_name}",
            },
            {
                "name": "备用端位点不超前",
                "ok": standby.offset <= primary.offset,
                "detail": (
                    f"备用端 {standby.offset} / 主端 {primary.offset}"
                    + ("" if standby.offset <= primary.offset else "，重放范围为空或倒挂")
                ),
            },
        ]
        return {
            "comparable": all(item["ok"] for item in checks),
            "checks": checks,
        }

    def _pending_roles_locked(self, group: str, generation: int) -> list[str]:
        present = {
            row.role
            for row in pipeline_bus.confirmations
            if row.group_name == group and row.generation == generation
        }
        return [
            ROLE_LABELS[role]
            for role in (ROLE_PRIMARY, ROLE_STANDBY)
            if role not in present
        ]

    def _generation_validation_locked(
        self, group: str, generation: int
    ) -> dict[str, Any]:
        """对指定代次已到的一端做可比较性预检，供下载响应说明缺口。"""
        rows = [
            row
            for row in pipeline_bus.confirmations
            if row.group_name == group and row.generation == generation
        ]
        primary_rows = [row for row in rows if row.role == ROLE_PRIMARY]
        standby_rows = [row for row in rows if row.role == ROLE_STANDBY]
        if primary_rows and standby_rows:
            return self.validate_pair(primary_rows[-1], standby_rows[-1])
        arrived = rows[-1] if rows else None
        return {
            "comparable": False,
            "checks": [],
            "reason": (
                f"代次{generation}只收到{ROLE_LABELS[arrived.role]}确认"
                f"（{_position(arrived.binlog_file, arrived.offset)}），无法配对"
                if arrived
                else f"代次{generation}还没有任何确认"
            ),
        }

    # ---------- 出包 ----------

    def _ensure_package_locked(
        self, group: str, generation: int
    ) -> tuple[RecoveryPositionPackage | None, str]:
        """在锁内为指定组 / 代次出包；配对不完整时不出包并说明原因。"""
        existing = [
            pkg
            for pkg in pipeline_bus.recovery_packages
            if pkg.group_name == group and pkg.generation == generation
        ]
        if existing:
            latest = max(existing, key=lambda pkg: pkg.package_version)
            return latest, f"代次{generation}已存在恢复位置包 v{latest.package_version}"

        rows = [
            row
            for row in pipeline_bus.confirmations
            if row.group_name == group and row.generation == generation
        ]
        primary_rows = [row for row in rows if row.role == ROLE_PRIMARY]
        standby_rows = [row for row in rows if row.role == ROLE_STANDBY]
        if not primary_rows or not standby_rows:
            pending = self._pending_roles_locked(group, generation)
            return None, (
                f"代次{generation}配对不完整，缺少{'、'.join(pending)}确认，"
                "暂不出包"
            )

        primary = primary_rows[-1]
        standby = standby_rows[-1]
        validation = self.validate_pair(primary, standby)
        if not validation["comparable"]:
            failed = "；".join(
                f"{item['name']}：{item['detail']}"
                for item in validation["checks"]
                if not item["ok"]
            )
            return None, f"代次{generation}确认不可比，暂不出包（{failed}）"

        package_version = self._next_package_version(group)
        package_id = f"rpkg_{_slug(group)}_{package_version:04d}"
        created_at = datetime.now().isoformat()
        payload: dict[str, Any] = {
            "package_type": "cdc-recovery-position",
            "package_id": package_id,
            "package_version": package_version,
            "group": group,
            "generation": generation,
            "binlog_file": primary.binlog_file,
            "primary": {
                "role": ROLE_LABELS[ROLE_PRIMARY],
                "role_key": ROLE_PRIMARY,
                "channel": primary.channel_name,
                "checkpoint_id": primary.checkpoint_id,
                "binlog_file": primary.binlog_file,
                "offset": primary.offset,
                "position": _position(primary.binlog_file, primary.offset),
                "generation": primary.generation,
                "confirmed_at": primary.confirmed_at.isoformat(),
                "confirmation_id": primary.confirmation_id,
                "data_checksum": primary.data_checksum,
            },
            "standby": {
                "role": ROLE_LABELS[ROLE_STANDBY],
                "role_key": ROLE_STANDBY,
                "channel": standby.channel_name,
                "checkpoint_id": standby.checkpoint_id,
                "binlog_file": standby.binlog_file,
                "offset": standby.offset,
                "position": _position(standby.binlog_file, standby.offset),
                "generation": standby.generation,
                "confirmed_at": standby.confirmed_at.isoformat(),
                "confirmation_id": standby.confirmation_id,
                "data_checksum": standby.data_checksum,
            },
            # 接续起点：备用端接管后继续实时消费的日志位置。
            "continuation_start": _position(primary.binlog_file, primary.offset),
            "continuation_offset": primary.offset,
            # 本轮备用端需要重放的范围（左闭右开）。
            "replay_range": {
                "binlog_file": primary.binlog_file,
                "from_offset": standby.offset,
                "to_offset": primary.offset,
                "from_position": _position(primary.binlog_file, standby.offset),
                "to_position": _position(primary.binlog_file, primary.offset),
                "offset_span": primary.offset - standby.offset,
            },
            "pair_validation": validation,
            "created_at": created_at,
        }
        checksum = hashlib.sha256(
            json.dumps(payload, sort_keys=True, ensure_ascii=False).encode()
        ).hexdigest()
        payload["checksum"] = checksum
        payload_json = json.dumps(payload, sort_keys=True, ensure_ascii=False, indent=2)

        package = RecoveryPositionPackage(
            package_version=package_version,
            package_id=package_id,
            group_name=group,
            generation=generation,
            primary=payload["primary"],
            standby=payload["standby"],
            continuation_start=payload["continuation_start"],
            replay_range=payload["replay_range"],
            created_at=created_at,
            checksum=checksum,
            payload_json=payload_json,
        )
        pipeline_bus.recovery_packages.append(package)
        self._link_checkpoints(package, primary, standby)
        pipeline_bus.write_audit(
            "生成恢复位置包", group,
            f"代次{generation}配对校验通过，包 v{package_version} 接续起点 "
            f"{package.continuation_start}，备用端重放 "
            f"{package.replay_range['from_position']} → {package.replay_range['to_position']}",
            "checkpoint_manager", "已生成",
        )
        return package, f"已生成恢复位置包 v{package_version}"

    def _link_checkpoints(
        self,
        package: RecoveryPositionPackage,
        primary: CheckpointConfirmation,
        standby: CheckpointConfirmation,
    ) -> None:
        for confirmation in (primary, standby):
            link = {
                "package_version": package.package_version,
                "package_id": package.package_id,
                "generation": package.generation,
                "role": ROLE_LABELS[confirmation.role],
                "continuation_start": package.continuation_start,
                "replay_from": package.replay_range["from_position"],
                "replay_to": package.replay_range["to_position"],
                "checkpoint_position": _position(
                    confirmation.binlog_file, confirmation.offset
                ),
            }
            self.checkpoint_links[confirmation.checkpoint_id] = link
            for row in store.rows(CHECKPOINT_MODULE):
                if row.get("检查点ID") == confirmation.checkpoint_id:
                    row["恢复包版本"] = f"v{package.package_version}"
                    row["接续起点"] = package.continuation_start
                    row["配对代次"] = str(package.generation)
                    row["配对角色"] = ROLE_LABELS[confirmation.role]

    def ensure_package(
        self, group: str, generation: int
    ) -> tuple[RecoveryPositionPackage | None, str]:
        with self.lock:
            return self._ensure_package_locked(group, generation)

    # ---------- 交接 / 下载 ----------

    def latest_confirmed_package(self, group: str) -> RecoveryPositionPackage | None:
        packages = self._packages(group)
        return packages[-1] if packages else None

    def has_pairing(self, group: str) -> bool:
        """该组是否登记过主备确认（决定切换是否必须凭恢复位置包起飞）。"""
        return any(
            row.group_name == group for row in pipeline_bus.confirmations
        )

    def resolve_delivery(
        self,
        group: str,
        generation: int | None = None,
        strict: bool = False,
    ) -> dict[str, Any]:
        """为一笔下载请求解析要交付的包版本。

        只读取已确认的完整配对：
        - 请求代次已配对 → 交付该代次的包；
        - 请求代次配对未完整 → 交付该组上一份已确认配对，并明确标注缺口，
          绝不把未确认的一端拼进包里；
        - strict=True 时配对不完整直接拒绝。
        """
        if not group:
            return {"ok": False, "status": 400, "reason": "缺少组名"}
        with self.lock:
            pending_roles: list[str] = []
            requested_pkg: RecoveryPositionPackage | None = None
            requested_validation: dict[str, Any] | None = None
            if generation is not None:
                # 两端确认都到齐时允许在下载这一刻补出该代次的包；
                # 未配齐时 _ensure_package_locked 不会出包（只接收完整配对）。
                built, _reason = self._ensure_package_locked(group, generation)
                candidates = [
                    pkg
                    for pkg in pipeline_bus.recovery_packages
                    if pkg.group_name == group and pkg.generation == generation
                ]
                requested_pkg = (
                    max(candidates, key=lambda pkg: pkg.package_version)
                    if candidates
                    else None
                )
                if requested_pkg is None:
                    pending_roles = self._pending_roles_locked(group, generation)
                    requested_validation = self._generation_validation_locked(
                        group, generation
                    )

            if generation is not None and requested_pkg is None:
                fallback = self.latest_confirmed_package(group)
                if strict:
                    return {
                        "ok": False,
                        "status": 409,
                        "reason": (
                            f"代次{generation}配对不完整，缺少{'、'.join(pending_roles)}确认，"
                            "按严格模式拒绝交付"
                        ),
                        "group": group,
                        "requested_generation": generation,
                        "pending_roles": pending_roles,
                        "pair_validation": requested_validation,
                    }
                if fallback is None:
                    return {
                        "ok": False,
                        "status": 409,
                        "reason": (
                            f"代次{generation}配对不完整且没有可用的历史已确认配对，"
                            f"缺少{'、'.join(pending_roles)}确认"
                        ),
                        "group": group,
                        "requested_generation": generation,
                        "pending_roles": pending_roles,
                        "pair_validation": requested_validation,
                    }
                package = fallback
                resolution = "fallback_confirmed_pair"
                note = (
                    f"代次{generation}配对未完整（缺{'、'.join(pending_roles)}确认），"
                    f"本次沿用上一份已确认配对代次{package.generation}，"
                    f"包版本 v{package.package_version}，接续起点 "
                    f"{package.continuation_start}；"
                    f"本轮备用端实际重放 {package.replay_range['from_position']} → "
                    f"{package.replay_range['to_position']}，"
                    f"代次{generation}的新位点未被带入本包"
                )
                handoff_pending = list(pending_roles)
            elif requested_pkg is not None:
                package = requested_pkg
                resolution = "exact_generation"
                note = (
                    f"代次{generation}主备确认完整，交付包版本 "
                    f"v{package.package_version}，接续起点 "
                    f"{package.continuation_start}"
                )
                handoff_pending = []
            else:
                package = self.latest_confirmed_package(group)
                if package is None:
                    return {
                        "ok": False,
                        "status": 409,
                        "reason": f"组「{group}」还没有已确认的完整主备配对",
                        "group": group,
                    }
                resolution = "latest_confirmed"
                handoff_pending = []
                note = (
                    f"交付最新已确认配对代次{package.generation}，包版本 "
                    f"v{package.package_version}，接续起点 {package.continuation_start}"
                )

            handoff = {
                "handoff_id": self._next_handoff_id(),
                "group": group,
                "requested_generation": generation,
                "package_version": package.package_version,
                "package_id": package.package_id,
                "generation": package.generation,
                "continuation_start": package.continuation_start,
                "replay_range": dict(package.replay_range),
                "resolution": resolution,
                "note": note,
                "created_at": datetime.now().isoformat(),
                "consumed_by_drill": None,
                "pending_roles": handoff_pending,
            }
            pipeline_bus.package_handoffs.append(handoff)
            pipeline_bus.write_audit(
                "下载恢复位置包", group,
                f"{handoff['handoff_id']} 交付包 v{package.package_version}，"
                f"接续起点 {package.continuation_start}；{note}",
                "checkpoint_manager", "已交付",
            )
            return {
                "ok": True,
                "status": 200,
                "group": group,
                "requested_generation": generation,
                "pending_roles": handoff_pending,
                "pair_validation": requested_validation,
                "resolution": resolution,
                "package": package.to_dict(),
                "package_version": package.package_version,
                "continuation_start": package.continuation_start,
                "handoff": handoff,
                "note": note,
            }

    # ---------- 检查点反查 ----------

    def lookup_checkpoint(self, entry_id: int) -> dict[str, Any] | None:
        row = store.find(CHECKPOINT_MODULE, entry_id)
        if row is None:
            return None
        checkpoint_id = str(row.get("检查点ID") or "")
        link = self.checkpoint_links.get(checkpoint_id)
        if link is None:
            # 兜底：按确认记录里的检查点ID查一次，兼容进程内重新登记的情况。
            for confirmation in pipeline_bus.confirmations:
                if confirmation.checkpoint_id == checkpoint_id:
                    for package in self._packages(confirmation.group_name):
                        if package.generation != confirmation.generation:
                            continue
                        if (
                            package.primary["checkpoint_id"] == checkpoint_id
                            or package.standby["checkpoint_id"] == checkpoint_id
                        ):
                            link = {
                                "package_version": package.package_version,
                                "package_id": package.package_id,
                                "generation": package.generation,
                                "role": ROLE_LABELS[confirmation.role],
                                "continuation_start": package.continuation_start,
                                "replay_from": package.replay_range["from_position"],
                                "replay_to": package.replay_range["to_position"],
                                "checkpoint_position": _position(
                                    confirmation.binlog_file, confirmation.offset
                                ),
                            }
                            break
                if link:
                    break
        result = {
            "entry_id": entry_id,
            "checkpoint_id": checkpoint_id,
            "channel": row.get("所属通道"),
            "binlog_file": row.get("Binlog文件"),
            "offset": row.get("偏移量"),
            "confirmed_at": row.get("确认时间"),
            "group": row.get("配对组"),
            "generation": row.get("配对代次"),
            "role": row.get("配对角色"),
            "found": link is not None,
        }
        if link is None:
            result.update({
                "package_version": None,
                "package_id": None,
                "continuation_start": None,
                "message": "该检查点还没有进入已确认的完整配对，暂无可查的包版本",
            })
            return result
        result.update({
            "package_version": link["package_version"],
            "package_version_label": f"v{link['package_version']}",
            "package_id": link["package_id"],
            "continuation_start": link["continuation_start"],
            "replay_from": link["replay_from"],
            "replay_to": link["replay_to"],
            "message": (
                f"检查点 {checkpoint_id} 属于恢复位置包 v{link['package_version']}，"
                f"接续起点 {link['continuation_start']}"
            ),
        })
        return result

    # ---------- 演练交接 ----------

    def handoff_by_id(self, handoff_id: str) -> dict[str, Any] | None:
        for handoff in pipeline_bus.package_handoffs:
            if handoff["handoff_id"] == handoff_id:
                return handoff
        return None

    def package_by_version(
        self, group: str, version: int
    ) -> RecoveryPositionPackage | None:
        for package in self._packages(group):
            if package.package_version == version:
                return package
        return None

    def latest_handoff(self, group: str) -> dict[str, Any] | None:
        rows = [
            handoff
            for handoff in pipeline_bus.package_handoffs
            if handoff["group"] == group
        ]
        return rows[-1] if rows else None

    def list_handoffs(
        self, group: str | None = None, limit: int = 50
    ) -> list[dict[str, Any]]:
        """下载交接日志：每笔下载把哪个包版本交给了哪次演练。"""
        rows = [
            dict(handoff)
            for handoff in pipeline_bus.package_handoffs
            if group is None or handoff["group"] == group
        ]
        return rows[-limit:]

    # ---------- 演示基线 ----------

    def bootstrap_baseline(self) -> None:
        """准备一份值班员手上已经可用的旧配对。

        基线：订单库主备切换组，代次1，主端 mysql-bin.000042:8000，
        备用端 mysql-bin.000042:7800，已生成恢复位置包 v1。
        只初始化一次，不覆盖运行期新登记的确认。
        """
        if self._bootstrapped:
            return
        with self.lock:
            if self._bootstrapped:
                return
            self._bootstrapped = True
            group = "订单库主备切换组"
            if any(row.get("组名") == group for row in store.rows(FAILOVER_MODULE)):
                return
            store.rows(FAILOVER_MODULE).append({
                "id": max(
                    (int(row.get("id", 0)) for row in store.rows(FAILOVER_MODULE)),
                    default=0,
                ) + 1,
                "status": "主运行",
                "pending": True,
                "abnormal": False,
                "组名": group,
                "主通道": "订单库主通道",
                "备用通道": "订单库备用通道",
                "切换条件": "checkpoint_stale",
                "自动切换": "是",
                "心跳超时": "30",
                "上次切换时间": "",
                "最近演练": "",
                "演练包版本": "",
                "演练接续起点": "",
                "演练重放范围": "",
            })
            baseline = [
                (ROLE_PRIMARY, "订单库主通道", "CKPT-ORDER-P-G1", 8000, 1),
                (ROLE_STANDBY, "订单库备用通道", "CKPT-ORDER-S-G1", 7800, 1),
            ]
            base_time = datetime.now() - timedelta(minutes=30)
            for index, (role, channel, checkpoint_id, offset, generation) in enumerate(
                baseline
            ):
                confirmation = CheckpointConfirmation(
                    confirmation_id=self._next_confirmation_id(),
                    group_name=group,
                    role=role,
                    channel_name=channel,
                    checkpoint_id=checkpoint_id,
                    binlog_file="mysql-bin.000042",
                    offset=offset,
                    generation=generation,
                    confirmed_at=base_time + timedelta(seconds=index * 30),
                    data_checksum=hashlib.sha256(
                        f"mysql-bin.000042:{offset}:{generation}".encode()
                    ).hexdigest()[:16],
                )
                pipeline_bus.confirmations.append(confirmation)
                self._append_checkpoint_row(confirmation)
            package, _ = self._ensure_package_locked(group, 1)
            if package is not None:
                pipeline_bus.write_audit(
                    "基线恢复位置包", group,
                    f"值班员已有可用配对：包 v{package.package_version}，接续起点 "
                    f"{package.continuation_start}",
                    "checkpoint_manager", "已生成",
                )


recovery_registry = RecoveryPositionRegistry()
recovery_registry.bootstrap_baseline()
