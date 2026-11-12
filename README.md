# 城市公共交通运行数据湖

采集公交、地铁和有轨电车的线路、站点、车辆状态与运营事件，统一多来源数据并形成可重放的运行事实数据。

## 数据工程链路

`source adapters -> transport -> raw landing -> schema normalization -> identity resolution -> quality control -> deduplication -> batch state -> structured storage -> replay`

## 模块职责

- `sources.py`：fixture 和 HTTP(S) JSON 数据源；认证只从运行环境读取。
- `models.py`：不可变领域记录和来源批次模型。
- `normalize.py`：字段标准化、类型转换、领域枚举校验和稳定哈希。
- `identity.py`：领域主键和去重键。
- `quality.py`：领域质量规则和拒绝原因。
- `storage.py`：SQLite 结构化表、幂等写入、批次、原始清单和拒绝记录。
- `pipeline.py`：只负责编排标准化、质量校验和存储。
- `application.py`：组装依赖、保存原始响应和管理资源生命周期。
- `cli.py`：fixture/HTTP 两种运行入口。
- `identity.py, quality.py, events.py`：领域事件、生命周期和后续扩展规则。

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
