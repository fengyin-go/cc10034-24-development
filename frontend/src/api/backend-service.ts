import { request } from './client'
import { moduleMeta } from '@/data/modules'
import type { ActionResult, EntryRow, OverviewResult, PageResult } from '@/data/types'

export { moduleMeta }

export async function listEntries(
  key: string,
  filters: Record<string, string> = {},
): Promise<PageResult> {
  const query = new URLSearchParams(filters).toString()
  const response = await request(`/api/${key}?${query}`)
  if (!response.ok) throw new Error('业务数据读取失败')
  return await response.json()
}

export async function createEntry(
  key: string,
  values: Record<string, string>,
): Promise<ActionResult> {
  const response = await request(`/api/${key}`, {
    method: 'POST',
    body: JSON.stringify({ values }),
  })
  return await response.json()
}

export async function runAction(
  key: string,
  id: number,
  action: string,
): Promise<ActionResult> {
  const response = await request(`/api/${key}/${id}/actions`, {
    method: 'POST',
    body: JSON.stringify({ action }),
  })
  return await response.json()
}

export async function downloadEntries(
  key: string,
  filters: Record<string, string> = {},
): Promise<void> {
  const query = new URLSearchParams(filters).toString()
  const response = await request(`/api/${key}/export?${query}`)
  if (!response.ok) throw new Error('业务数据导出失败')
  const url = URL.createObjectURL(await response.blob())
  const anchor = document.createElement('a')
  anchor.href = url
  anchor.download = `${key}.csv`
  document.body.appendChild(anchor)
  anchor.click()
  document.body.removeChild(anchor)
  URL.revokeObjectURL(url)
}

export type PairConfirmationResult = {
  ok: boolean
  message: string
  entry?: Record<string, unknown> | null
}

export type RecoveryDownload = {
  packageVersion: number
  generation: number
  continuationStart: string
  resolution: string
  handoffId: string
  pendingRoles: string[]
  filename: string
}

/** 登记一条主端 / 备用端检查点确认；两端配齐后后端自动固化为恢复位置包。 */
export async function registerPairConfirmation(
  values: Record<string, string>,
): Promise<PairConfirmationResult> {
  const response = await request('/api/checkpoint_manager/confirmations', {
    method: 'POST',
    body: JSON.stringify({ values }),
  })
  return await response.json()
}

/** 下载恢复位置包：只取已确认的完整配对，返回头里带回实际交付的版本和接续起点。 */
export async function downloadRecoveryPackage(
  group: string,
  generation?: string,
  strict = false,
): Promise<RecoveryDownload> {
  const query = new URLSearchParams({ group })
  if (generation) query.set('generation', generation)
  if (strict) query.set('strict', 'true')
  const response = await request(`/api/checkpoint_manager/recovery-package/download?${query}`)
  if (!response.ok) {
    const detail = await response.json().catch(() => null)
    throw new Error(detail?.detail?.reason ?? '恢复位置包下载失败')
  }
  const blob = await response.blob()
  const filename =
    response.headers.get('Content-Disposition')?.match(/filename="([^"]+)"/)?.[1] ??
    'recovery-position.json'
  const url = URL.createObjectURL(blob)
  const anchor = document.createElement('a')
  anchor.href = url
  anchor.download = filename
  document.body.appendChild(anchor)
  anchor.click()
  document.body.removeChild(anchor)
  URL.revokeObjectURL(url)
  return {
    packageVersion: Number(response.headers.get('X-Recovery-Package-Version') ?? 0),
    generation: Number(response.headers.get('X-Recovery-Package-Generation') ?? 0),
    continuationStart: response.headers.get('X-Recovery-Continuation-Start') ?? '',
    resolution: response.headers.get('X-Recovery-Resolution') ?? '',
    handoffId: response.headers.get('X-Recovery-Handoff-Id') ?? '',
    pendingRoles: (response.headers.get('X-Recovery-Pending-Roles') ?? '')
      .split(',')
      .filter(Boolean)
      .map((role) => decodeURIComponent(role)),
    filename,
  }
}

/** 切换演练：按下载交接的包版本核对启动条件，结果回写切换清单。 */
export async function runFailoverDrill(
  id: number,
  values: Record<string, string> = {},
): Promise<{ ok: boolean; reason?: string; package_version?: number; continuation_start?: string }> {
  const response = await request(`/api/failover_group/${id}/drill`, {
    method: 'POST',
    body: JSON.stringify({ values }),
  })
  return await response.json()
}

/** 按检查点记录反查所属恢复位置包版本和接续起点。 */
export async function lookupCheckpointPackage(id: number): Promise<Record<string, unknown>> {
  const response = await request(`/api/checkpoint_manager/${id}/recovery-package`)
  if (!response.ok) throw new Error('恢复位置包反查失败')
  return await response.json()
}

export async function loadOverview(): Promise<OverviewResult> {
  const response = await request('/api/overview')
  if (!response.ok) throw new Error('运营概览读取失败')
  return await response.json()
}
