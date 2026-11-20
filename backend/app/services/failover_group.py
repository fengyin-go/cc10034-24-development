"""故障切换领域服务：管理CDC通道的主备切换。

上游：cdc_channel + checkpoint_manager 产出通道状态和检查点 → failover_group 消费
消费：恢复位置包校验切换组的启动条件；演练结果回写本模块切换清单
下游：position_store 按练过的接续起点更新当前位点
"""
from __future__ import annotations

import time
from datetime import datetime
from typing import Any

from app.services.base import BaseModuleService
from app.pipeline import pipeline_bus
from app.services.recovery_position import recovery_registry


class FailoverGroupService(BaseModuleService):
    MODULE = "failover_group"
    ENTITY = "切换组"
    OPERATION_TERM = "校验"
    LIST_FIELDS = ["组名", "主通道", "备用通道", "切换条件", "自动切换", "心跳超时", "上次切换时间"]
    REQUIRED_FIELDS = ["组名", "主通道", "备用通道"]
    STATUS_ORDER = ["主运行", "备运行", "切换中", "已失效"]
    ACTION_RULES = {"执行切换": "主运行", "标记失效": "主运行"}
    NEGATIVE_ACTIONS = []

    def check_start_conditions(
        self,
        config: dict[str, Any],
        package: dict[str, Any] | None = None,
        handoff: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """用恢复位置包核对切换启动条件。

        包内两个位置必须属于可比代次、同一 Binlog 文件，并且接续起点
        要与包版本一致；不满足就不允许起飞演练。
        """
        checks: list[dict[str, Any]] = []

        def add(name: str, ok: bool, detail: str) -> None:
            checks.append({"name": name, "ok": ok, "detail": detail})

        primary = config.get("主通道", "")
        standby = config.get("备用通道", "")
        add("主备通道已配置", bool(primary and standby), f"主 {primary} / 备 {standby}")

        if package is None:
            add("恢复位置包可用", False, "没有可用的已确认恢复位置包")
            return {"passed": False, "checks": checks, "package_version": None}

        pkg_primary = package["primary"]
        pkg_standby = package["standby"]
        add(
            "包内两位置同代次",
            pkg_primary.get("generation", package["generation"])
            == pkg_standby.get("generation", package["generation"])
            == package["generation"],
            (
                f"主端代次{pkg_primary.get('generation')} / "
                f"备用端代次{pkg_standby.get('generation')} / 包代次{package['generation']}"
            ),
        )
        add(
            "包内两位置同 Binlog 文件",
            pkg_primary["binlog_file"] == pkg_standby["binlog_file"],
            f"{pkg_primary['position']} / {pkg_standby['position']}",
        )
        add(
            "备用端配套",
            pkg_standby["channel"] == standby,
            f"包内备用 {pkg_standby['channel']} / 切换组备用 {standby or '未配置'}",
        )
        add(
            "备用端位点不超前",
            pkg_standby["offset"] <= pkg_primary["offset"],
            f"备 {pkg_standby['offset']} / 主 {pkg_primary['offset']}",
        )
        add(
            "接续起点与包一致",
            package["continuation_start"]
            == f"{pkg_primary['binlog_file']}:{pkg_primary['offset']}",
            package["continuation_start"],
        )
        add(
            "交接包版本一致",
            handoff is None
            or handoff.get("package_version") == package["package_version"],
            (
                "本次演练由下载交接直接交付"
                if handoff is None
                else f"下载交付 v{handoff['package_version']} / 当前包 v{package['package_version']}"
            ),
        )
        return {
            "passed": all(item["ok"] for item in checks),
            "checks": checks,
            "package_version": package["package_version"],
        }

    def run_drill(
        self,
        config: dict[str, Any],
        generation: int | None = None,
        handoff_id: str | None = None,
        strict: bool = False,
    ) -> dict[str, Any]:
        """执行切换演练：按下载交接的包版本核对启动条件并回写演练结果。"""
        group_name = config["组名"]

        handoff = recovery_registry.handoff_by_id(handoff_id) if handoff_id else None
        if handoff_id and handoff is None:
            return {
                "ok": False,
                "reason": f"下载交接 {handoff_id} 不存在",
                "group": group_name,
            }
        if handoff is not None:
            package = recovery_registry.package_by_version(
                group_name, int(handoff["package_version"])
            )
            if package is not None and handoff.get("group") not in (None, group_name):
                return {
                    "ok": False,
                    "reason": (
                        f"下载交接 {handoff['handoff_id']} 属于组「{handoff['group']}」，"
                        f"与演练组「{group_name}」不一致"
                    ),
                    "group": group_name,
                }
        else:
            resolution = recovery_registry.resolve_delivery(
                group_name, generation, strict=strict
            )
            if not resolution.get("ok"):
                return {
                    "ok": False,
                    "reason": resolution.get("reason", "没有可用的恢复位置包"),
                    "group": group_name,
                }
            package = recovery_registry.package_by_version(
                group_name, int(resolution["package_version"])
            )
            handoff = resolution["handoff"]

        if package is None:
            return {"ok": False, "reason": "恢复位置包不存在", "group": group_name}
        package_view = package.to_dict()

        conditions = self.check_start_conditions(config, package_view, handoff)
        if not conditions["passed"]:
            failed = "；".join(
                f"{item['name']}：{item['detail']}"
                for item in conditions["checks"]
                if not item["ok"]
            )
            pipeline_bus.write_audit(
                "切换演练", group_name,
                f"启动条件未通过，演练未起飞（{failed}）",
                "failover_group", "未通过",
            )
            return {
                "ok": False,
                "reason": f"启动条件不满足：{failed}",
                "group": group_name,
                "conditions": conditions,
                "package_version": package.package_version,
            }

        replay = package.replay_range
        result = {
            "ok": True,
            "group": group_name,
            "generation": package.generation,
            "package_id": package.package_id,
            "package_version": package.package_version,
            "continuation_start": package.continuation_start,
            "replay_from": replay["from_position"],
            "replay_to": replay["to_position"],
            "replay_span": replay["offset_span"],
            "handoff_id": handoff["handoff_id"] if handoff else None,
            "download_resolution": (handoff or {}).get("resolution", "drill_only"),
            "pending_roles": (handoff or {}).get("pending_roles", []) or [],
            "conditions": conditions,
            "drilled_at": datetime.now().isoformat(),
            "status": "演练通过",
        }

        if handoff is not None:
            handoff["consumed_by_drill"] = result["drilled_at"]
            handoff["drill_package_version"] = package.package_version

        # 演练结果回写到切换清单，值班员在列表里就能看到用过的包和接续起点。
        entry = config
        entry["最近演练"] = result["drilled_at"]
        entry["演练包版本"] = f"v{package.package_version}"
        entry["演练接续起点"] = package.continuation_start
        entry["演练重放范围"] = f"{replay['from_position']} → {replay['to_position']}"
        entry["演练结论"] = "通过"
        pipeline_bus.write_audit(
            "切换演练", group_name,
            f"演练通过：包 v{package.package_version}，接续起点 "
            f"{package.continuation_start}，备用端重放 "
            f"{replay['from_position']} → {replay['to_position']}",
            "failover_group", "演练通过",
        )
        return result

    # 兼容旧动作名：执行切换现在走恢复位置包校验后的切换演练。
    def execute_failover(self, config: dict[str, Any]) -> dict[str, Any]:
        """执行主备切换（按恢复位置包核对启动条件）。

        已经登记过主备确认的组，必须凭已确认的完整配对才能起飞；完全没有
        配对记录的组保留原有的手动切换行为，避免历史数据无法操作。
        """
        has_pairing = recovery_registry.has_pairing(config.get("组名", ""))
        if not has_pairing:
            return self._legacy_failover(config)

        drill = self.run_drill(config)
        if not drill.get("ok"):
            return {
                "group": config["组名"],
                "primary": config["主通道"],
                "standby": config["备用通道"],
                "switch_triggered": False,
                "switch_reason": drill.get("reason", "无可用恢复位置包"),
                "new_primary": config["主通道"],
                "package_version": drill.get("package_version"),
            }

        switched_at = datetime.now().isoformat()
        pipeline_bus.write_audit(
            "故障切换", config["组名"],
            f"切换组 {config['组名']} 主通道从 {config['主通道']} 切换至 "
            f"{config['备用通道']}，接续起点 {drill['continuation_start']}",
            "failover_group", "已切换",
        )
        return {
            "group": config["组名"],
            "primary": config["主通道"],
            "standby": config["备用通道"],
            "switch_triggered": True,
            "switch_reason": "recovery_package_verified",
            "new_primary": config["备用通道"],
            "new_standby": config["主通道"],
            "switched_at": switched_at,
            "last_switch": switched_at,
            "package_version": drill["package_version"],
            "continuation_start": drill["continuation_start"],
        }

    def _legacy_failover(self, config: dict[str, Any]) -> dict[str, Any]:
        """没有主备配对历史的组，沿用按条件评估的原始切换逻辑。"""
        group_name = config["组名"]
        primary_channel = config["主通道"]
        standby_channel = config["备用通道"]
        switch_condition = config.get("切换条件", "heartbeat_timeout")
        auto_switch = config.get("自动切换", "否") == "是"
        heartbeat_timeout = int(config.get("心跳超时", "30"))

        should_switch = self._evaluate_condition(
            switch_condition, primary_channel, heartbeat_timeout
        )
        result: dict[str, Any] = {
            "group": group_name,
            "primary": primary_channel,
            "standby": standby_channel,
            "switch_triggered": False,
            "switch_reason": "",
            "new_primary": primary_channel,
        }
        if should_switch or not auto_switch:
            switched_at = datetime.now().isoformat()
            result.update({
                "switch_triggered": True,
                "switch_reason": "heartbeat_timeout" if should_switch else "manual",
                "new_primary": standby_channel,
                "new_standby": primary_channel,
                "switched_at": switched_at,
                "last_switch": switched_at,
            })
            pipeline_bus.write_audit(
                "故障切换", group_name,
                f"切换组 {group_name} 主通道从 {primary_channel} 切换至 {standby_channel}",
                "failover_group", "已切换",
            )
        else:
            result["switch_reason"] = "healthy"
        return result

    def _evaluate_condition(self, condition: str, channel: str, timeout: int) -> bool:
        """评估切换条件。"""
        if condition == "heartbeat_timeout":
            # 检查通道位点的最后更新时间
            pos = pipeline_bus.get_position(channel)
            if pos is None:
                return True
            elapsed = (datetime.now() - pos.updated_at).total_seconds()
            return elapsed > timeout
        if condition == "checkpoint_stale":
            commits = [cp for cp in pipeline_bus.checkpoints
                       if cp.channel_name == channel and cp.committed]
            if not commits:
                return True
            latest = commits[-1]
            elapsed = (datetime.now() - latest.confirmed_at).total_seconds()
            return elapsed > timeout * 3
        if condition == "always":
            return True
        return False

    def heartbeat_check(self, channel_name: str, timeout_sec: float = 30.0) -> dict[str, Any]:
        """心跳检查。"""
        pos = pipeline_bus.get_position(channel_name)
        healthy = True
        elapsed = 0.0
        if pos:
            elapsed = (datetime.now() - pos.updated_at).total_seconds()
            healthy = elapsed <= timeout_sec
        else:
            elapsed = -1

        return {
            "channel": channel_name,
            "healthy": healthy,
            "last_seen_sec_ago": round(elapsed, 1),
            "timeout_sec": timeout_sec,
            "needs_failover": not healthy and channel_name in {
                cp.channel_name for cp in pipeline_bus.checkpoints
            },
        }

    def compute_failover_risk(self, config: dict[str, Any]) -> dict[str, Any]:
        """计算故障风险评分。"""
        primary = config["主通道"]
        heartbeat = self.heartbeat_check(primary)
        checkpoint_commits = sum(1 for cp in pipeline_bus.checkpoints
                                 if cp.channel_name == primary and cp.committed)
        risk_score = 0
        if not heartbeat["healthy"]:
            risk_score += 50
        if checkpoint_commits == 0:
            risk_score += 30
        elapsed = heartbeat.get("last_seen_sec_ago", -1)
        if elapsed > 120:
            risk_score += 20
        risk_level = "低" if risk_score < 30 else ("中" if risk_score < 60 else "高")
        return {
            "group": config["组名"],
            "primary_channel": primary,
            "risk_score": min(risk_score, 100),
            "risk_level": risk_level,
            "healthy": heartbeat["healthy"],
            "checkpoint_commits": checkpoint_commits,
            "recommends_manual_check": risk_score >= 50,
        }

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        entry, msg = super().run_action(entry_id, action)
        if entry and action == "执行切换":
            result = self.execute_failover(entry)
            if result["switch_triggered"]:
                entry["status"] = "切换中"
                entry["上次切换时间"] = result["last_switch"]
                msg = (
                    f"切换组已按恢复位置包 v{result['package_version']} 校验，"
                    f"接续起点 {result['continuation_start']}，切换已起飞"
                )
            else:
                msg = f"切换组未起飞：{result['switch_reason']}"
        return entry, msg