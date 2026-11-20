# 实时数仓CDC变更捕获中心

面向实时数仓建设场景，提供Binlog解析、变更事件捕获、DDL追踪、SCD处理和下游分发同步的CDC治理平台

这是一个前后端分离的管理平台：前端 Vue 3 + Vite + TypeScript，后端 FastAPI（Python）。
两边各自独立启动，前端 dev server 已关掉自动打开页面，启动后按终端打印的地址手工打开。

## 目录结构

```text
.
├── frontend/                 Vue 3 + Vite + TypeScript 前端
│   ├── src/views/WorkbenchView.vue|tsx  全部模块共享工作台
│   ├── src/data/modules.ts              18个业务模块配置
│   ├── src/api/              统一请求封装
│   └── vite.config.ts        dev server 配置（open: false）
├── backend/                  FastAPI（Python） 后端
│   ├── app/routers/resource.py  通用查询/导出/登记/动作接口
│   ├── app/services/<key>.py    每个模块独立领域逻辑
│   ├── app/services/base.py     共享服务骨架
│   └── app/store.py          内存数据仓库与示例数据
├── .gitignore
└── docker-compose.yml
```

## 启动

### 后端

```bash
cd backend
python3 -m venv .venv && .venv/bin/pip install -r requirements.txt
./run.sh
```

健康检查：`curl http://127.0.0.1:8000/api/health`

### 前端

```bash
cd frontend
npm ci
npm run dev
```

前端默认监听 `http://127.0.0.1:5173/`，dev server 不会自动打开浏览器，
需要自己访问。`/api` 由 vite 代理到后端 `http://127.0.0.1:8000`。

## 业务模块

| 模块 | 目录 | 业务对象 | 主要字段 |
| --- | --- | --- | --- |
| Binlog解析 | `binlog_parser` | 解析配置 | 配置名称、源数据库、解析模式 |
| 变更事件 | `change_event` | 变更记录 | 事件ID、来源表、操作类型 |
| DDL追踪 | `ddl_tracker` | DDL记录 | 记录ID、变更数据库、变更表名 |
| Schema同步 | `schema_sync` | 同步任务 | 任务名称、源表、目标表 |
| 采集通道 | `cdc_channel` | 通道实例 | 通道名称、Binlog源、输出目标 |
| 检查点管理 | `checkpoint_manager` | 检查点 | 检查点ID、所属通道、Binlog文件 |
| 位点存储 | `position_store` | 位点记录 | 记录ID、数据库实例、Binlog名称 |
| 格式适配 | `format_adapter` | 适配规则 | 规则名称、输入格式、输出格式 |
| 缓慢变化维 | `scd_handler` | SCD配置 | 配置名称、维度表、SCD类型 |
| 去重窗口 | `dedup_window` | 窗口配置 | 配置名称、去重键、窗口大小 |
| 路由分发 | `route_dispatcher` | 路由规则 | 规则名称、匹配条件、目标通道 |
| 回填位点 | `backfill_position` | 回填任务 | 任务名称、源位点、目标位点 |
| 延迟监控 | `lag_monitor` | 延迟指标 | 指标时间、所属通道、延迟秒数 |
| 快照比对 | `snapshot_compare` | 比对报告 | 报告名称、源表名、目标表名 |
| 事件过滤 | `event_filter` | 过滤规则 | 规则名称、过滤类型、匹配表达式 |
| 分区同步 | `partition_sync` | 同步配置 | 配置名称、源分区策略、目标分区策略 |
| 审计轨迹 | `audit_trail` | 审计日志 | 审计时间、操作类型、目标对象 |
| 故障切换 | `failover_group` | 切换组 | 组名、主通道、备用通道 |

## 约定

- 全部模块共享`frontend/src/views/WorkbenchView.vue|tsx`和一套资源路由。
- 每个模块仍保留独立`backend/app/services/<模块>.py`；纯前端保留
  `frontend/src/domain/<模块>.ts`，用于承载真实业务差异并维持36题位容量。
- 列表接口统一返回 `{ items, total, page, size }`，动作接口统一返回 `{ ok, message }`。
- 状态流转只允许在 `app/services` 里改，路由层不做业务判断。

## 检查点主备配对 / 恢复位置包

值班员需要一份能直接交给切换演练的主备配对文件。流程是：主端、备用端各自
登记检查点确认 → 同一组、同一代次、同一 Binlog 文件的完整配对固化成
**恢复位置包** → 下载给演练 → 演练用包核对启动条件 → 结果回写切换清单。

相关接口：

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/api/checkpoint_manager/confirmations` | 登记主端 / 备用端确认（两端到齐自动出包） |
| GET | `/api/checkpoint_manager/pairing?group=` | 配对状态、包列表、未配齐代次 |
| GET | `/api/checkpoint_manager/recovery-package/download?group=&generation=&strict=` | 下载恢复位置包 |
| GET | `/api/checkpoint_manager/recovery-package/handoffs?group=` | 下载交接记录 |
| GET | `/api/checkpoint_manager/{id}/recovery-package` | 检查点反查包版本与接续起点 |
| POST | `/api/failover_group/{id}/drill` | 切换演练（可传 `交接ID` / `代次` / `严格`） |

关键约束（`backend/app/services/recovery_position.py`）：

- **只接收已确认的完整配对**：一端确认、另一端未到时不产生新包。
- **可比代次**：包内两个位置必须同代次、同 Binlog 文件，且备用端位点不超前。
- **下载不拼半成品**：请求代次未配齐时沿用该组上一份已确认配对，并在响应头
  `X-Recovery-Resolution`、`X-Recovery-Pending-Roles` 标注；`strict=true` 直接 409。
- **同版幂等**：同一包版本重复下载返回完全相同的文件内容（payload 出包时固化）。
- **并发安全**：登记、配对、出包、交接在同一把 `pairing_lock` 内完成。
- **演练回写**：包版本、接续起点、重放范围写回切换清单，并可在检查点记录反查。


领域：数据工程
子方向：数仓与数据湖
