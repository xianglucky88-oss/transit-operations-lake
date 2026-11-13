# 城市公共交通运行数据湖

采集公交、地铁和有轨电车的线路、站点、车辆状态与运营事件，统一多来源数据并形成可重放的运行事实数据。

## 数据工程链路

`source adapters -> transport -> raw landing -> schema normalization -> identity resolution -> quality control -> deduplication -> batch state -> structured storage -> replay`

## 模块职责

- `sources.py`：fixture 和 HTTP(S) JSON 数据源；认证只从运行环境读取。
- `models.py`：不可变领域记录和来源批次模型。
- `normalize.py`：字段标准化、类型转换、领域枚举校验和稳定哈希。
- `identity.py`：领域主键和去重键。
- `quality.py`：确定性、可扩展的质量规则引擎（规则码、严重级别、三类结果和可解释消息）。
- `events.py`：运营事件分类与统一状态注册表。
- `storage.py`：SQLite 结构化表、幂等写入、批次、原始清单和拒绝记录。
- `pipeline.py`：只负责编排标准化、质量校验和存储。
- `application.py`：组装依赖、保存原始响应和管理资源生命周期。
- `cli.py`：fixture/HTTP 两种运行入口。

## 质量规则引擎

`quality.validate(record) -> list[str]` 保留旧契约：可接收（含仅警告）记录返回空列表，拒绝/无法判定记录返回阻断性消息。
需要机器可读结果时使用 `quality.evaluate(record)`，返回 `EvaluationResult`：

- `outcome`：`accept`（可接收，可能带警告）、`reject`（应拒绝）、`inconclusive`（无法判定，隔离待人工/后续规则处理）。
- `findings`：按固定目录顺序聚合的 `Finding(rule_code, severity, message, field)`，单条记录最多保留 16 条，消息默认截断到 512 字符。
- `category` / `known_status`：与 `events.classify` 一致的事件分类。
- `rule_engine_version` 与 `config_signature`：随拒绝原因持久化，保证规则配置变更后仍可按规则码复现历史决策。

### 稳定规则码（目录顺序即聚合顺序）

| 规则码 | 严重级别 | 含义 |
| --- | --- | --- |
| `FIELD_MISSING` | error | route_id/stop_id 等必填标识为空 |
| `VEHICLE_ID_MISSING` | warning | 缺 vehicle_id，车辆级分析受限但记录可用 |
| `OBSERVED_AT_MISSING` / `OBSERVED_AT_UNPARSEABLE` | error | observed_at 缺失或不是 ISO-8601 |
| `OBSERVED_AT_NAIVE` | warning | 时间戳无时区，按来源本地时间暂存 |
| `STATUS_UNKNOWN` | unknown | 状态存在但不在注册表 → inconclusive，而非直接拒绝 |
| `DELAY_NOT_FINITE` / `DELAY_OUT_OF_RANGE` | error | 非有限数值或超出 ±86400 秒 |
| `DELAY_EXTREME_EARLY` | error | 延误严格小于 -3600 秒（沿用旧 `< -3600` 边界，-3600 本身不拒绝） |
| `DELAY_EARLY_ARRIVAL` | warning | 负延误表示早到，正常接收 |
| `CANCELLED_WITH_DELAY` | warning | 取消事件携带延误；取消统计忽略该延误 |
| `DELAYED_STATUS_NEGATIVE_DELAY` | warning | delayed 状态与负延误互相矛盾 |
| `RECORD_REVISION` | warning | 同一 record_key 出现不同 raw_hash 的内容修订 |
| `NORMALIZE_ERROR` / `UNEXPECTED_ERROR` | error | 记录未进入引擎（标准化失败/引擎异常）时的稳定兜底码 |
| `FINDINGS_TRUNCATED` | unknown | 触发单条问题数上限后的截断标记 |
| `LEGACY_REJECTION` | error | 旧数据库纯文本拒绝行迁移后的合成规则码 |

### 哪些变化是警告而不是破坏性拒绝

早到负延误（≥ -3600 秒）、无时区时间戳、缺 vehicle_id、取消事件携带延误、
delayed 状态配负延误、同 record_key 的内容修订——这些记录照常入库，只在 `warnings` 计数与
`evaluate()` 结果中体现；`validate()` 对它们仍返回空列表，旧调用方行为不变。

### 扩展规则

自定义规则通过 `Rule(code, severity, check, description)` 描述，用
`evaluate(record, extra_rules=(...))` 或构造 `RuleEngine` 接入；扩展规则追加在内置目录之后，
不修改全局默认引擎，规则顺序与多问题聚合不受字典哈希或来源处理顺序影响。
阈值通过冻结的 `RuleConfig` 调整，非法配置在构造时即报错。

### 持久化与旧库兼容

`rejected_records` 新增 `rule_code`、`rule_codes_json`、`severity`、`outcome`、
`rule_engine_version` 列；`DataStore` 打开旧库时自动 `ALTER TABLE` 迁移，并把历史纯文本行标记为
`LEGACY_REJECTION`。可通过 `DataStore.rejections_for_rule(code)` 按规则码复现全部拒绝行。
pipeline 返回的 `rejected`/`accepted` 等为批次内计数，全表总量改用 `total_*` 键，避免历史拒绝行污染当批计数。

## 运行

```bash
PYTHONPATH=src python3 -m transit_operations.cli \
  --fixture fixtures/sample.json \
  --database /tmp/transit-operations-lake.sqlite3 \
  --raw-dir /tmp/transit-operations-lake-raw
```

HTTP 模式：

```bash
PYTHONPATH=src python3 -m transit_operations.cli \
  --url https://example.invalid/api/records \
  --database /tmp/transit-operations-lake.sqlite3 \
  --raw-dir /tmp/transit-operations-lake-raw
```

接口需要返回 JSON 数组，或包含 `records`、`data`、`items`、`results` 的对象；其他嵌套结构使用 `--records-key`。认证变量 `DATA_SOURCE_BEARER_TOKEN` 和 `DATA_SOURCE_API_KEY` 只作为环境变量名称示例，不写入真实值。

## 外部依赖类型与版本

- Python 3.11+
- pytest 8.3.x（开发/测试）
- SQLite 3.x（本地结构化存储）
- HTTP(S) JSON 数据源（生产接入时按具体来源协议配置）

## 验证

```bash
PYTHONPATH=src python3 -m pytest --import-mode=importlib tests
PYTHONPATH=src python3 -m compileall -q src
```
