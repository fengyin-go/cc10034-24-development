"""统一资源接口：查询、导出、登记和动作按模块 key 分派到领域服务。"""
from __future__ import annotations

import csv
import io
import json
from typing import Any
from urllib.parse import quote

from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import StreamingResponse

from app.schemas import ActionPayload, ActionResult, EntryPayload, PageResult
from app.services import service_for

router = APIRouter(prefix="/api", tags=["业务资源"])


def require_service(module: str):
    service = service_for(module)
    if service is None:
        raise HTTPException(status_code=404, detail=f"未知业务模块：{module}")
    return service


@router.get("/{module}", response_model=PageResult[dict])
def list_entries(
    module: str,
    request: Request,
    keyword: str | None = Query(default=None),
    status: str | None = Query(default=None),
    page: int = Query(default=1, ge=1),
    size: int = Query(default=20, ge=1, le=200),
) -> PageResult[dict]:
    service = require_service(module)
    filters = {
        key: value.strip()
        for key, value in request.query_params.items()
        if key in service.LIST_FIELDS and value.strip()
    }
    items, total = service.list_entries(
        filters=filters, keyword=keyword, status=status, page=page, size=size)
    return PageResult(items=items, total=total, page=page, size=size)


@router.get("/{module}/export")
def export_entries(
    module: str,
    request: Request,
    keyword: str | None = Query(default=None),
    status: str | None = Query(default=None),
) -> StreamingResponse:
    service = require_service(module)
    filters = {
        key: value.strip()
        for key, value in request.query_params.items()
        if key in service.LIST_FIELDS and value.strip()
    }
    items, _ = service.list_entries(
        filters=filters, keyword=keyword, status=status, page=1, size=1_000_000)
    output = io.StringIO()
    output.write("\ufeff")
    writer = csv.writer(output, lineterminator="\n")
    writer.writerow(["id", *service.LIST_FIELDS, "status"])
    for item in items:
        values = [item.get("id"), *(item.get(field, "") for field in service.LIST_FIELDS),
                  item.get("status", "")]
        writer.writerow([
            "'" + str(value) if str(value or "").startswith(("=", "+", "-", "@")) else value
            for value in values
        ])
    return StreamingResponse(
        iter([output.getvalue()]), media_type="text/csv; charset=utf-8",
        headers={"Content-Disposition": f'attachment; filename="{module}.csv"'},
    )


# ---------- 检查点主备配对 / 恢复位置包 ----------
# 固定路径必须注册在 /{module}/{entry_id} 之前，否则 pairing 会被当成 entry_id。

@router.post("/checkpoint_manager/confirmations", response_model=ActionResult)
def register_pair_confirmation(payload: EntryPayload) -> ActionResult:
    """登记主端 / 备用端检查点确认。

    两端确认到齐且属于可比代次时，自动固化为一份新的恢复位置包。
    """
    service = require_service("checkpoint_manager")
    entry, missing, message = service.register_pair_confirmation(payload.values)
    return ActionResult(ok=entry is not None, message=message, entry=entry)


@router.get("/checkpoint_manager/pairing", response_model=dict)
def get_pairing_status(group: str | None = Query(default=None)) -> dict[str, Any]:
    """查看主备配对、已生成的恢复位置包和尚未配齐的代次。"""
    service = require_service("checkpoint_manager")
    return service.pair_status(group)


@router.get("/checkpoint_manager/recovery-package/handoffs", response_model=dict)
def list_recovery_handoffs(
    group: str | None = Query(default=None), limit: int = Query(default=50, ge=1, le=500)
) -> dict[str, Any]:
    """下载交接记录，排查「某次演练到底用了哪个包版本」。"""
    service = require_service("checkpoint_manager")
    return service.list_handoffs(group, limit)


@router.get("/checkpoint_manager/recovery-package/download")
def download_recovery_package(
    group: str = Query(...),
    generation: int | None = Query(default=None),
    strict: bool = Query(default=False),
) -> StreamingResponse:
    """下载恢复位置包（供切换演练读取）。

    只交付已确认的完整配对：请求代次未配齐时回退到上一份已确认配对，
    并在响应头里写明实际交付的包版本和接续起点。同一包版本重复下载
    返回完全相同的文件内容。
    """
    service = require_service("checkpoint_manager")
    resolved = service.resolve_recovery_package(group, generation, strict=strict)
    if not resolved.get("ok"):
        raise HTTPException(status_code=resolved.get("status", 409), detail=resolved)
    package = resolved["package"]
    body = json.dumps(package, ensure_ascii=False, sort_keys=True, indent=2)
    # HTTP 头只能是 latin-1：文件名走 RFC 5987，中文角色名做百分号编码。
    filename = f"{package['package_id']}.json"
    ascii_filename = (
        f"recovery-package-v{package['package_version']}-g{package['generation']}.json"
    )
    quoted_filename = quote(filename, safe="")
    return StreamingResponse(
        iter([body]), media_type="application/json; charset=utf-8",
        headers={
            "Content-Disposition": (
                f'attachment; filename="{ascii_filename}"; '
                f"filename*=UTF-8''{quoted_filename}"
            ),
            "X-Recovery-Package-Version": str(package["package_version"]),
            "X-Recovery-Package-Generation": str(package["generation"]),
            "X-Recovery-Continuation-Start": package["continuation_start"],
            "X-Recovery-Handoff-Id": resolved["handoff"]["handoff_id"],
            "X-Recovery-Resolution": resolved["resolution"],
            "X-Recovery-Pending-Roles": quote(
                ",".join(resolved.get("pending_roles", [])), safe=","
            ),
            "X-Recovery-Package-Checksum": package["checksum"],
        },
    )


@router.get("/checkpoint_manager/{entry_id}/recovery-package", response_model=dict)
def get_checkpoint_recovery_package(entry_id: int) -> dict[str, Any]:
    """按检查点记录反查它所属的恢复位置包版本和接续起点。"""
    service = require_service("checkpoint_manager")
    result = service.lookup_checkpoint_package(entry_id)
    if result is None:
        raise HTTPException(status_code=404, detail=f"检查点 {entry_id} 不存在或已归档")
    return result


@router.get("/{module}/{entry_id}", response_model=dict)
def get_entry(module: str, entry_id: int) -> dict[str, Any]:
    service = require_service(module)
    entry = service.get_entry(entry_id)
    if entry is None:
        raise HTTPException(status_code=404, detail=f"{service.ENTITY} {entry_id} 不存在或已归档")
    return entry


@router.post("/{module}", response_model=ActionResult)
def create_entry(module: str, payload: EntryPayload) -> ActionResult:
    service = require_service(module)
    entry, missing = service.create_entry(payload.values)
    if missing:
        return ActionResult(ok=False, message=f"缺少必填字段：{'、'.join(missing)}")
    return ActionResult(ok=True, message=f"{service.ENTITY}已登记", entry=entry)


@router.post("/{module}/{entry_id}/actions", response_model=ActionResult)
def run_action(module: str, entry_id: int, payload: ActionPayload) -> ActionResult:
    service = require_service(module)
    entry, message = service.run_action(entry_id, payload.action.strip())
    return ActionResult(ok=entry is not None, message=message, entry=entry)


@router.post("/failover_group/{entry_id}/drill", response_model=dict)
def run_failover_drill(
    entry_id: int, payload: EntryPayload | None = None
) -> dict[str, Any]:
    """切换演练：按下载交接的包版本核对启动条件，结果回写切换清单。"""
    service = require_service("failover_group")
    entry = service.get_entry(entry_id)
    if entry is None:
        raise HTTPException(status_code=404, detail=f"切换组 {entry_id} 不存在或已归档")
    values = payload.values if payload else {}
    generation = values.get("代次") or values.get("generation")
    handoff_id = values.get("交接ID") or values.get("handoff_id")
    strict = values.get("严格") in (True, "是", "true", "True")
    result = service.run_drill(
        entry,
        generation=int(generation) if generation not in (None, "") else None,
        handoff_id=str(handoff_id) if handoff_id else None,
        strict=strict,
    )
    if result.get("ok"):
        entry["status"] = "切换中"
        entry["pending"] = True
    return result

