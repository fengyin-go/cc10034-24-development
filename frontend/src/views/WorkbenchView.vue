<template>
  <section class="page" :data-module="meta.key">
    <header class="page-head">
      <div>
        <h2>{{ meta.name }}</h2>
        <p class="page-desc">{{ meta.desc }}</p>
      </div>
      <div class="page-actions">
        <button class="btn primary" data-testid="open-create" type="button" @click="openCreate">
          登记{{ meta.entity }}
        </button>
        <button class="btn" data-testid="export" type="button" @click="exportRows">
          导出{{ meta.name }}清单
        </button>
        <button
          v-if="meta.key === 'checkpoint_manager'"
          class="btn"
          data-testid="export-packages"
          type="button"
          @click="exportPackages"
        >
          导出主备配对文件
        </button>
      </div>
    </header>

    <section v-if="meta.key === 'checkpoint_manager'" class="pair-panel" data-testid="pair-panel">
      <header class="pair-head">
        <div>
          <h3>主备配对与恢复位置包</h3>
          <p class="page-desc">
            主端、备用端各自确认位点；同一代次、同一 Binlog 文件的完整配对才会固化成恢复位置包。
          </p>
        </div>
        <span v-if="pairing?.latest_package_version" class="pair-badge" data-testid="latest-package">
          最新包 v{{ pairing.latest_package_version }} · 接续起点
          {{ pairing.latest_continuation_start }}
        </span>
      </header>

      <form class="pair-form" data-testid="confirm-form" @submit.prevent="submitConfirmation">
        <label v-for="field in CONFIRM_FIELDS" :key="field" class="pair-field">
          <span>{{ field }}</span>
          <input v-model="confirmValues[field]" :data-testid="`confirm-${field}`" />
        </label>
        <button class="btn primary" data-testid="submit-confirmation" type="submit">
          登记主备确认
        </button>
      </form>

      <p v-if="pairingMessage" class="pair-message" data-testid="pair-message">{{ pairingMessage }}</p>

      <table class="data-table pair-table">
        <thead>
          <tr><th>代次</th><th>角色</th><th>通道</th><th>位点</th><th>确认时间</th></tr>
        </thead>
        <tbody>
          <tr v-for="item in pairing?.confirmations ?? []" :key="String(item.confirmation_id)">
            <td>G{{ item.generation }}</td>
            <td>{{ item.role === 'primary' ? '主端' : '备用端' }}</td>
            <td>{{ item.channel_name }}</td>
            <td>{{ item.binlog_file }}:{{ item.offset }}</td>
            <td>{{ item.confirmed_at }}</td>
          </tr>
          <tr v-if="!(pairing?.confirmations ?? []).length">
            <td colspan="5" class="empty-state">还没有主备确认。值守班组可先登记主端位点，再补备用端确认。</td>
          </tr>
        </tbody>
      </table>

      <div v-if="Object.keys(pairing?.pending_generations ?? {}).length" class="pair-warning" data-testid="pair-pending">
        <strong>配对未完整：</strong>
        <span v-for="(roles, key) in pairing?.pending_generations" :key="key">
          {{ key }} 缺少{{ roles.join('、') }}确认；
        </span>
        此时下载会沿用上一份已确认配对。
      </div>

      <div class="pair-download">
        <label class="pair-field">
          <span>下载组名</span>
          <input v-model="downloadGroup" data-testid="download-group" placeholder="切换组名" />
        </label>
        <label class="pair-field">
          <span>请求代次（可空）</span>
          <input v-model="downloadGeneration" data-testid="download-generation" placeholder="例如 2" />
        </label>
        <button class="btn" data-testid="download-package" type="button" @click="downloadPackage">
          下载恢复位置包
        </button>
      </div>
      <p v-if="downloadMessage" class="pair-message" data-testid="download-message">{{ downloadMessage }}</p>

      <table v-if="(pairing?.packages ?? []).length" class="data-table pair-table">
        <thead>
          <tr><th>包版本</th><th>代次</th><th>主端位点</th><th>备用端位点</th><th>接续起点</th><th>重放范围</th></tr>
        </thead>
        <tbody>
          <tr v-for="pkg in pairing?.packages ?? []" :key="String(pkg.package_id)">
            <td>v{{ pkg.package_version }}</td>
            <td>G{{ pkg.generation }}</td>
            <td>{{ pkg.primary.position }}</td>
            <td>{{ pkg.standby.position }}</td>
            <td>{{ pkg.continuation_start }}</td>
            <td>{{ pkg.replay_range.from_position }} → {{ pkg.replay_range.to_position }}</td>
          </tr>
        </tbody>
      </table>
    </section>

    <section v-if="meta.key === 'failover_group'" class="pair-panel" data-testid="drill-panel">
      <header class="pair-head">
        <div>
          <h3>切换演练 · 恢复位置包</h3>
          <p class="page-desc">
            演练读取下载交接的包版本，核对启动条件后把包版本、接续起点和重放范围回写到切换清单。
          </p>
        </div>
        <span class="pair-badge" data-testid="drill-handoff">
          交接 {{ lastHandoffId || '—' }}
        </span>
      </header>
      <form class="pair-form" @submit.prevent="submitDrill">
        <label class="pair-field">
          <span>演练组（记录 ID）</span>
          <input v-model="drillId" data-testid="drill-id" placeholder="切换组记录 id" />
        </label>
        <label class="pair-field">
          <span>下载交接 ID（可空）</span>
          <input v-model="drillHandoff" data-testid="drill-handoff-input" placeholder="hdl_0002" />
        </label>
        <label class="pair-field">
          <span>请求代次（可空）</span>
          <input v-model="drillGeneration" data-testid="drill-generation" placeholder="例如 2" />
        </label>
        <button class="btn primary" data-testid="submit-drill" type="submit">执行切换演练</button>
      </form>
      <p v-if="drillMessage" class="pair-message" data-testid="drill-message">{{ drillMessage }}</p>
    </section>

    <div class="stat-row">
      <article v-for="item in stats" :key="item.label" class="stat-card">
        <span class="stat-label">{{ item.label }}</span>
        <strong class="stat-value">{{ item.value }}</strong>
      </article>
    </div>

    <p class="status-legend">
      <span v-for="item in statusSummary" :key="item.status" class="legend-item">
        {{ item.status }}：{{ item.count }}
      </span>
    </p>

    <form class="filter-bar" @submit.prevent="reload">
      <label v-for="field in filterFields" :key="field" class="filter-item">
        <span>{{ field }}</span>
        <input v-model="filters[field]" :data-testid="`filter-${field}`" :placeholder="`按${field}检索`" />
      </label>
      <button class="btn" data-testid="query" type="submit">查询</button>
      <button class="btn ghost" data-testid="reset" type="button" @click="resetFilters">重置条件</button>
    </form>

    <table class="data-table">
      <thead>
        <tr>
          <th v-for="column in meta.fields" :key="column">{{ column }}</th>
          <th v-if="meta.key === 'failover_group'">最近演练</th>
          <th v-if="meta.key === 'failover_group'">演练包版本</th>
          <th v-if="meta.key === 'failover_group'">接续起点 / 重放范围</th>
          <th v-if="meta.key === 'checkpoint_manager'">恢复包版本</th>
          <th v-if="meta.key === 'checkpoint_manager'">接续起点</th>
          <th>当前状态</th>
          <th>可执行动作</th>
        </tr>
      </thead>
      <tbody>
        <tr v-for="row in rows" :key="String(row.id)" :data-testid="`row-${row.id}`" :data-status="String(row.status)">
          <td v-for="column in meta.fields" :key="column">{{ row[column] ?? '—' }}</td>
          <td v-if="meta.key === 'failover_group'">{{ row['最近演练'] || '—' }}</td>
          <td v-if="meta.key === 'failover_group'">{{ row['演练包版本'] || '—' }}</td>
          <td v-if="meta.key === 'failover_group'">{{ row['演练接续起点'] || '—' }}<template v-if="row['演练重放范围']"> · {{ row['演练重放范围'] }}</template></td>
          <td v-if="meta.key === 'checkpoint_manager'">{{ row['恢复包版本'] || '—' }}</td>
          <td v-if="meta.key === 'checkpoint_manager'">{{ row['接续起点'] || '—' }}</td>
          <td>{{ row.status }}</td>
          <td class="row-actions">
            <button
              v-for="action in meta.actions"
              :key="action"
              class="link"
              type="button"
              :data-testid="`action-${row.id}-${action}`"
              @click="runAction(action, row)"
            >
              {{ action }}
            </button>
          </td>
        </tr>
        <tr v-if="!rows.length">
          <td :colspan="emptyColspan" class="empty-state">
            暂无{{ meta.name }}数据，可先登记{{ meta.entity }}
          </td>
        </tr>
      </tbody>
    </table>

    <div v-if="showCreate" class="modal-backdrop" data-testid="create-modal">
      <form class="modal" @submit.prevent="submitCreate">
        <h3>登记{{ meta.entity }}</h3>
        <label v-for="field in meta.fields" :key="field" class="create-field">
          <span>{{ field }}</span>
          <input v-model="createValues[field]" :data-testid="`create-${field}`" required />
        </label>
        <div class="page-actions">
          <button class="btn" type="button" @click="showCreate = false">取消</button>
          <button class="btn primary" data-testid="submit-create" type="submit" :disabled="creating">
            {{ creating ? '提交中…' : '确认登记' }}
          </button>
        </div>
      </form>
    </div>

    <footer class="page-foot">
      <span>共 {{ total }} 条{{ meta.name }}记录</span>
      <span v-if="errorMessage" class="error-text">{{ errorMessage }}</span>
    </footer>
  </section>
</template>

<script setup lang="ts">
import { computed, onMounted, ref } from 'vue'

import {
  createEntry,
  downloadEntries,
  downloadRecoveryPackage,
  listEntries,
  moduleMeta,
  registerPairConfirmation,
  runAction as applyAction,
  runFailoverDrill,
} from '@/api/backend-service'
import type { EntryRow } from '@/data/types'

type PairingConfirmation = {
  confirmation_id: string
  group_name: string
  role: string
  channel_name: string
  binlog_file: string
  offset: number
  generation: number
  confirmed_at: string
}

type PairingPackage = {
  package_id: string
  package_version: number
  generation: number
  continuation_start: string
  primary: { position: string }
  standby: { position: string }
  replay_range: { from_position: string; to_position: string }
}

type PairingStatus = {
  confirmations: PairingConfirmation[]
  packages: PairingPackage[]
  latest_package_version: number | null
  latest_continuation_start: string | null
  pending_generations: Record<string, string[]>
}

const CONFIRM_FIELDS = ['组名', '角色', '通道', 'Binlog文件', '偏移量', '代次']

const props = defineProps<{ moduleKey: string }>()
const meta = moduleMeta(props.moduleKey)
const rows = ref<EntryRow[]>([])
const total = ref(0)
const errorMessage = ref('')
const filters = ref<Record<string, string>>({})
const filterFields = meta.fields.slice(0, 3)
const showCreate = ref(false)
const creating = ref(false)
const createValues = ref<Record<string, string>>({})
const pairing = ref<PairingStatus | null>(null)
const confirmValues = ref<Record<string, string>>({})
const pairingMessage = ref('')
const downloadGroup = ref('')
const downloadGeneration = ref('')
const downloadMessage = ref('')
const drillId = ref('')
const drillHandoff = ref('')
const drillGeneration = ref('')
const drillMessage = ref('')
const lastHandoffId = ref('')
const stats = computed(() => [
  { label: meta.metrics[0], value: total.value },
  { label: meta.metrics[1], value: rows.value.filter((row) => row.status === meta.statuses[1]).length },
  {
    label: meta.metrics[2],
    value: rows.value.filter((row) => row.status === meta.statuses[meta.statuses.length - 1]).length,
  },
])
const statusSummary = computed(() =>
  meta.statuses.map((status) => ({
    status,
    count: rows.value.filter((row) => String(row.status) === status).length,
  })),
)
const extraColumns = computed(() => {
  if (meta.key === 'failover_group') return 3
  if (meta.key === 'checkpoint_manager') return 2
  return 0
})
const emptyColspan = computed(() => meta.fields.length + extraColumns.value + 2)

function resetFilters() {
  filters.value = {}
  void reload()
}

async function exportRows() {
  errorMessage.value = ''
  try {
    await downloadEntries(meta.key, filters.value)
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '导出失败'
  }
}

function openCreate() {
  createValues.value = Object.fromEntries(meta.fields.map((field) => [field, '']))
  errorMessage.value = ''
  showCreate.value = true
}

async function submitCreate() {
  creating.value = true
  errorMessage.value = ''
  try {
    const result = await createEntry(meta.key, createValues.value)
    if (!result.ok) throw new Error(result.message)
    showCreate.value = false
    await reload()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '登记失败'
  } finally {
    creating.value = false
  }
}

async function runAction(action: string, row: EntryRow) {
  errorMessage.value = ''
  try {
    const result = await applyAction(meta.key, Number(row.id), action)
    if (!result.ok) throw new Error(result.message)
    await reload()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '操作失败'
  }
}

async function reload() {
  errorMessage.value = ''
  try {
    const payload = await listEntries(meta.key, filters.value)
    rows.value = payload.items
    total.value = payload.total
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '业务列表读取失败'
  }
}

async function reloadPairing() {
  if (meta.key !== 'checkpoint_manager') return
  try {
    const response = await fetch('/api/checkpoint_manager/pairing')
    if (!response.ok) return
    pairing.value = (await response.json()) as PairingStatus
    const latest = pairing.value.packages[pairing.value.packages.length - 1] as
      | (PairingPackage & { group?: string })
      | undefined
    if (latest?.group && !downloadGroup.value) downloadGroup.value = latest.group
  } catch {
    pairing.value = null
  }
}

async function submitConfirmation() {
  pairingMessage.value = ''
  try {
    const result = await registerPairConfirmation(confirmValues.value)
    pairingMessage.value = result.message
    if (result.ok) {
      confirmValues.value = {}
      await Promise.all([reload(), reloadPairing()])
    }
  } catch (error) {
    pairingMessage.value = error instanceof Error ? error.message : '登记失败'
  }
}

async function downloadPackage() {
  downloadMessage.value = ''
  try {
    const result = await downloadRecoveryPackage(
      downloadGroup.value,
      downloadGeneration.value || undefined,
    )
    const pending = result.pendingRoles.length
      ? `；请求代次缺少${result.pendingRoles.join('、')}确认，已沿用上一份已确认配对`
      : ''
    downloadMessage.value =
      `已下载 ${result.filename}：包版本 v${result.packageVersion}，` +
      `代次 G${result.generation}，接续起点 ${result.continuationStart}${pending}`
    lastHandoffId.value = result.handoffId
    if (!drillHandoff.value) drillHandoff.value = result.handoffId
    if (!drillGeneration.value && downloadGeneration.value) {
      drillGeneration.value = downloadGeneration.value
    }
  } catch (error) {
    downloadMessage.value = error instanceof Error ? error.message : '下载失败'
  }
}

async function exportPackages() {
  errorMessage.value = ''
  try {
    const result = await downloadRecoveryPackage(
      downloadGroup.value,
      downloadGeneration.value || undefined,
    )
    downloadMessage.value =
      `已导出主备配对文件 ${result.filename}：包版本 v${result.packageVersion}，` +
      `接续起点 ${result.continuationStart}`
    lastHandoffId.value = result.handoffId
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '导出配对文件失败'
  }
}

async function submitDrill() {  drillMessage.value = ''
  try {
    const values: Record<string, string> = {}
    if (drillHandoff.value) values.交接ID = drillHandoff.value
    if (drillGeneration.value) values.代次 = drillGeneration.value
    const result = await runFailoverDrill(Number(drillId.value), values)
    if (!result.ok) {
      drillMessage.value = `演练未起飞：${result.reason ?? '启动条件不满足'}`
      return
    }
    drillMessage.value =
      `演练通过：包版本 v${result.package_version}，接续起点 ${result.continuation_start}`
    await reload()
  } catch (error) {
    drillMessage.value = error instanceof Error ? error.message : '演练失败'
  }
}

onMounted(() => {
  void reload()
  void reloadPairing()
})
</script>
