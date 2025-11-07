# PandaAI Quantflow 量化工作流平台

## 概述

**PandaAI QuantFlow** 是一个集成的量化交易和机器学习工作流平台，旨在为量化研究人员提供完整的端到端解决方案，降低AI门槛，打破传统量化高门槛壁垒，让主观交易者、学生乃至普通投资者都能参与策略研发。
![预览](https://lyzj-ai.oss-cn-shanghai.aliyuncs.com/d712ad04032a0d70ea468cc1bfd0d144.png)
![预览](https://lyzj-ai.oss-cn-shanghai.aliyuncs.com/127c169482041aa8bd619a074665d8e2.png)
![预览](https://lyzj-ai.oss-cn-shanghai.aliyuncs.com/47f670c2e1c1d45a593cb3f6b2d707ef.png)
![预览](https://lyzj-ai.oss-cn-shanghai.aliyuncs.com/a5a35c798f3138bdbd145d647abaedee.png)


### 🚀 核心功能

**🔧 可视化工作流编排**
- 基于节点的可视化工作流设计器，支持拖拽式构建复杂的量化研究和交易策略流程
- 丰富的内置工作节点，涵盖数据处理、特征工程、机器学习、因子分析和回测等各个环节
- 灵活的插件系统，支持自定义工作节点开发和扩展

**🤖 机器学习集成**
- 支持主流机器学习算法：XGBoost、LightGBM、RandomForest、SVM、神经网络等
- 提供多任务学习(MTL)和深度学习解决方案
- 自动化模型训练、验证和预测流程

**📈 因子分析与回测策略验证**
- 高性能回测引擎，支持股票、期货
- 完整的事件驱动架构，精确模拟真实交易环境
- 兼容PandaFactor分析框架

**🌐 企业级服务架构**
- 基于FastAPI的高性能REST API服务
- 支持多用户工作流管理和权限控制
- 分布式任务执行，支持云端和本地部署
- 完善的日志系统和实时监控

### 🎯 适用场景

- **量化研究**：因子挖掘、策略开发、模型验证
- **算法交易**：策略回测、实盘验证、风险管理
- **金融数据分析**：市场研究、投资组合优化
- **机器学习应用**：金融预测模型、智能投顾

### 💡 技术特色

- **微服务架构**：模块化设计，易于扩展和维护
- **插件化设计**：通过 `@work_node` 装饰器轻松开发自定义节点
- **工作流支持json导入导出**：通过json文件快速传播和复现工作流
- **容器化部署**：支持Docker和Docker Compose快速部署

### ⚡ 实时数据管线

| 组件 | 默认配置 | 说明 |
| ---- | ---- | ---- |
| Kafka | `KAFKA_ENABLE_FUTURE_TICK=true` 时启用，Topic 默认为 `market.future.tick` | 行情采集端将 Tick 同步写入 Redis 与 Kafka，消费端按需订阅，多进程解耦。 |
| Kafka 信号 | `KAFKA_ENABLE_TRADE_SIGNAL=true` 时启用，Topic 前缀 `KAFKA_TRADE_SIGNAL_TOPIC_PREFIX` | 交易信号可从 Redis 切换到 Kafka，`trade.signal.<future|stock>`、`trade.signal.risk_reload_<run_id>`。 |
| QuestDB | `QUESTDB_ENABLE=true` 时启用，表名默认为 `future_ticks` | Tick 会通过 ILP 协议写入 QuestDB；若 Kafka/Redis 缓存缺失，读取器会自动从 QuestDB 回补最近一条。 |
| ClickHouse | `CLICKHOUSE_ENABLE=true` 时启用，表名默认为 `future_ticks` | Tick 通过 HTTP `JSONEachRow` 写入 ClickHouse，适合做批量分析与 OLAP 查询。 |
| 验证 | `tail -f logs/panda_info.log` 查看 `[Kafka]`、`[QuestDB]`、`[ClickHouse]` 日志；也可使用 Kafka CLI、QuestDB/ClickHouse HTTP 接口校验数据落库情况。 |
| 测试脚本 | `python tools/publish_sample_tick.py --symbol TEST.FUT --price 101.5` | 向 Kafka / QuestDB / ClickHouse 写入测试 Tick，便于确认链路。 |
| 性能对比 | `python tools/benchmark_publish_sample_tick.py --symbol TEST.FUT --price 101.5 --count 100` | 对比调优前后写入吞吐，评估批量调用收益。 |
| 性能采集 | `python tools/collect_metrics.py --messages 500` | 统计 Kafka 平均/分位延迟，并可测试 QuestDB、ClickHouse 查询耗时。 |


## 已规划功能，欢迎加群内测
LLM支持

API代码式调用工作流

CTP实盘交易

QMT实盘交易

数字货币支持

## 服务安装与运行

### 安装包方式 (简单，快速体验)
- 若是您不想花时间搭环境，可以用我们打包好的客户端，一键解压，可直接运行，因为内置数据库和行情数据，文件略大。
![预览](https://lyzj-ai.oss-cn-shanghai.aliyuncs.com/2a1a5cbb2a997f0f143192dc083cd906.png)
内含工作流、超级图表、因子模块。
- **MacOS 系统**: [准备中]
- **Windows 系统**: [下载 Windows 安装包](https://github.com/PandaAI-Tech/panda_quantflow/releases)（因为文件大小限制，做了分包，请下载全部文件再解压）


### 从源码安装（高度自定义，适合二次开发）
#### 前置条件
1. 安装 ANACONDA 环境 ([官网下载](https://www.anaconda.com/download))
2. 下载并启动PandaAI提供的数据库(含有行情数据)<br>
   请联系小助理从网盘下载最新的数据库
   - **MacOS 系统**: 下载并解压后运行
`chmod 600 conf/mongo.key`
`bin/mongod -replSet rs0 --dbpath data/db  --keyFile conf/mongo.key --port 27017 --quiet --auth`
   - **Windows 系统**: 下载并解压后运行 `bin\mongod.exe --replSet rs0 --dbpath data\db  --keyFile conf\mongo.key --port 27017 --quiet --auth`
3. 安装 panda_factor 相关依赖，并且启动factor服务
   ```bash
   git clone https://github.com/PandaAI-Tech/panda_factor.git
   cd panda_factor
   pip install -r requirements.txt
   pip install -e ./panda_common ./panda_factor ./panda_data ./panda_data_hub ./panda_llm ./panda_factor_server 
   python ./panda_factor_server/panda_factor_server/__main__.py
   ```
   panda_factor还有若干功能，例如数据自动更新等，具体请查看([PandaFactor](https://github.com/PandaAI-Tech/panda_factor))

  
#### 安装流程
1. 安装 panda_quantflow
   ```bash
   git clone https://github.com/PandaAI-Tech/panda_quantflow.git
   cd panda_quantflow
   pip install -e .
   ```
2. 启动 panda_quantflow 服务
   ```bash
   python src/panda_server/main.py
   ```
3. 打开 UI 图形界面
   ```bash
   超级图表：http://127.0.0.1:8000/charts/
   工作流：http://127.0.0.1:8000/quantflow/
   ```

## 编写自定义插件
- 开发者可以在项目目录`.src/panda_plugins/custom/`中编写自定义插件, 以在工作流中使用
- 自定义插件需要继承 `BaseWorkNode` 并实现 `input_model`, `output_model` 和 `run` 3 个方法
- 自定义插件示例 (更多示例在`.src/panda_plugins/custom/examples/`中)
   ```python
   from typing import Optional, Type
   from panda_plugins.base import BaseWorkNode, work_node
   from pydantic import BaseModel

   class InputModel(BaseModel):
      """
      Define the input model for the node.
      Use pydantic to define, which is a library for data validation and parsing.
      Reference: https://pydantic-docs.helpmanual.io
      
      
      为工作节点定义输入模型.
      使用 Pydantic 定义, Pydantic 是一个用于数据验证和解析的库.
      参考文档: https://pydantic-docs.helpmanual.io
      """
      number1: int
      number2: int

   class OutputModel(BaseModel):
      """
      Define the output model for the node.
      Use pydantic to define, which is a library for data validation and parsing.
      Reference: https://pydantic-docs.helpmanual.io
      
      为工作节点定义输出模型.
      使用 Pydantic 定义, Pydantic 是一个用于数据验证和解析的库.
      参考文档: https://pydantic-docs.helpmanual.io
      """
      result: int

   @work_node(name="示例-两数求和", group="测试节点")
   class ExamplePluginAddition(BaseWorkNode):
      """
      Implement a example node, which can add two numbers and return the result.
      实现一个示例节点, 完成一个简单的加法运算, 输入 2 个数值, 输出 2 个数值的和.
      """

      # Return the input model
      # 返回输入模型
      @classmethod
      def input_model(cls) -> Optional[Type[BaseModel]]:
         return InputModel

      # Return the output model
      # 返回输出模型
      @classmethod
      def output_model(cls) -> Optional[Type[BaseModel]]:
         return OutputModel

      # Node running logic
      # 节点运行逻辑
      def run(self, input: BaseModel) -> BaseModel:
         result = input.number1 + input.number2
         return OutputModel(result=result)

   if __name__ == "__main__":
      node = ExamplePluginAddition()
      input = InputModel(number1=1, number2=2)
      print(node.run(input))
   ```
**我们欢迎每一位用户加入节点贡献行列，共同点燃量化开源的火焰🔥** 

**如果有新的节点需求，也欢迎进群告诉我们，只要有助于项目发展，我们都愿意免费支持开发。**

## 项目结构** 
```
panda_workflow/
├── src/                    # 所有源代码
│   ├── common/             # 通用工具和配置
│   ├── panda_ml/           # 机器学习组件
│   ├── panda_plugins/      # 插件系统
│   ├── panda_server/       # API服务
│   ├── panda_backtest/     # 回测系统
│   └── panda_trading/      # 交易执行系统
├── tests/                  # 测试目录
├── pyproject.toml          # 项目配置和依赖
├── Dockerfile              # Docker配置
└── docker-compose.yml      # Docker Compose配置
```

## 提交历史概览（中文）

**Kickoff（起步）**
- `6774f35` – 为仓库加入宽松许可证与初始 README，确保整个流水线具备合规基础。
- `744a2b5` – 一次性导入 Quantflow 核心代码（连接器、日志、调度、回测 API 以及 Docker/pyproject 脚手架），策略只需引用提供的 API 模块并运行示例 `Test` 入口即可。
- `cfe58df` – 调整 README 结构，让安装与运行步骤更清晰。
- `92011a9` – 进一步补充 README 说明与格式，便于新同学上手。
- `fffa536` – 在 README 中加入更多上手提示，帮助贡献者快速熟悉仓库。

**Docs Refinements（文档优化）**
- `c7e92b9` – 优化 README 的段落与措辞，提升可读性。
- `be826b8` – 合并文档优化 PR，主干直接享受最新文本。
- `af13f0b` – 小幅更新 README，使示例与现状保持一致。
- `9049456` – 再次补充 README 的细节说明。
- `8499f40` – 修正文档中的细微错别字与排版问题。

**Factor & Config Prep（因子与配置准备）**
- `c7e891d` – 调整 `factor_analysis_node` 的默认设置以匹配新的节点命名。
- `5cedc03` – 同步更新 `factor_build_node_pro`，保持工作流逻辑一致。
- `b2ca95b` – 刷新配置与 Mongo 访问逻辑，并重新生成前端资源，确保 UI 与配置相符。
- `aa401f4` – 精简 README，移除重复描述。
- `5788c5d` – 更新 README 的链接与措辞。
- `053c25a` – 针对排版再做微调。

**Backtest Launch（回测发布）**
- `6c47f8e` – 在重大回测版本前完成最后的 README 打磨。
- `f88870b` – “backtest support” 大版本：引入定时任务、重构工具库、增加全面文档与示例策略。按 `panda_backtest/README` 的步骤实现 `initialize`/`handle_data`，再运行 `main_workflow` 启动策略。
- `59bd1cd` – 删除陈旧的编译产物，减小前端包体积。
- `3badc25` – 重命名并重新链接前端静态资源，确保网页指向正确。
- `03134ae` – 大规模重构：升级日志系统，新增多种示例策略，并把 LLM 助手与静态代码检查器整合进 `panda_server`，可在既有接口中唤起 AI 回测/因子/代码助手。
- `f96c39f` – README 增补新 AI 功能的使用方式。

**Stabilization（稳定性加强）**
- `9ef808c` – README 进一步覆盖最新基础设施。
- `400cec6` – 向 `pyproject.toml` 添加新增服务所需的依赖。
- `04eff7b` – 一组 bugfix，同时新增因子相关节点、通用错误码与时间工具；需要因子相关热力/报表时，可直接引用新的节点。
- `2eccb87` – 将工作流拆分为 `main_workflow_future` / `main_workflow_stock` 两套入口，并增强插件加载逻辑、同步全量 UI 资源；根据资产类别运行对应入口即可。
- `db1152e` – 修正打包配置，确保新模块可被正确发现。
- `d721951` – 移除工作流入口的多余调试输出。

**Plugin & Ops Enhancements（插件与运维增强）**
- `6ad53cf` – 新增 GRU/LSTM 因子节点与完整的用户插件存储链路（验证、API、数据库模型）；通过新的 plugin route 发布自定义插件即可完成持久化与校验。
- `4312c09` – README 中补充插件体验相关说明。
- `fb7eaec` – 移除过时的 docker-compose 样例，避免误导。
- `ad53981` – 精简 Dockerfile 以适配新部署结构。
- `4c75cbf` – 在 `pyproject.toml` 中补齐缺失的依赖。
- `688b90e` – README 再次同步最新部署指引。

**Kafka/QuestDB Tooling（行情链路工具）**
- `e24dac9` – 新增 Kafka、QuestDB 客户端封装，并把它们接入 CTP/tushare 行情生产端，附带示例 tick 发布脚本；配置好环境变量后运行脚本即可完成端到端测试。
- `f1a06df` – 引入 ClickHouse 客户端与 QuestDB 批量写入能力，增加配置开关，升级订阅端；启用对应开关后行情会同时落 Kafka/QuestDB/ClickHouse。
- `baca43d` – 添加 `collect_metrics.py`，用于在发布后采集 tick 吞吐/延迟；跑一次收集脚本即可得到基准。
- `fb5da43` – 提供自动化 tick 发布循环与发布性能对比 CLI，可通过新脚本批量推送或对比子进程/同进程吞吐，README 也新增了 benchmark 命令。
- `e5fd7b0` – 强化 benchmark CLI，新增配置参数并调整默认值，便于在实验室复现性能数字。
- `ae29a1b` – 将 QuestDB 深度融入回测：新增 QuestDB 数据源、Mongo→QuestDB ETL、以及专用入口；先用转换脚本导入历史 K 线，再运行 `python -m panda_backtest.main_questdb` 即可。

**Data Coverage & API（数据覆盖与 API）**
- `269836b` – 提供工具比较 Mongo vs QuestDB 全历史差异并生成统计；运行新脚本即可验证迁移质量。
- `8282ccf` – Web 端 API 全面切到 HTTPS，并刷新 `uv.lock`，确保客户端只调用安全接口。
- `b4b16e0` – 在 CTP 回调里加上异步 tick 队列，实时线程只负责写 Redis；通过 `TICK_ASYNC_QUEUE_SIZE` 等环境变量即可调节缓冲区。
- `e52b812` – 新增 `benchmark_tick_async.py` 性能基准脚本，用来验证 tick 管线的延迟；运行脚本即可复现 README 中的数据。

**Tick Pipeline Perf（行情性能）**
- `b709c41` – 行情整体改用 msgpack 序列化（并同步更新基准脚本）；请确保安装 `msgpack`，生产消费双方才能识别新格式。
- `84db407` – 进一步优化基准，对 Redis 写入引入批处理与刷盘间隔环境变量，单次 RTT 大幅下降；按需调整 `TICK_REDIS_FLUSH_BATCH` 和 `TICK_REDIS_FLUSH_INTERVAL_MS`。
- `c4da03c` – 将 Kafka/QuestDB/ClickHouse 拆成独立队列和线程，且可单独开关，Kafka 可优先发送；用 `ENABLE_KAFKA_TICK` / `ENABLE_QUESTDB_TICK` / `ENABLE_CLICKHOUSE_TICK` 及各自队列长度即可按机型启停。
- `5a94f63` – 引入 C 级别 Level2 二进制解析器、QuestDB 批量写工具、性能基准和示例深交所 Level2 数据；使用 `level2_cparser` 解码后，可通过 `import_level2_to_questdb` 装载，再用 `benchmark_level2_io` 做吞吐评估。

如需了解某个提交的具体 API 变更或脚本参数，欢迎指出具体 commit，我可以继续深挖。

### 近 30 天提交速览（含脚本示例）

**行情链路与监控**
- `e24dac9` / `f1a06df` – 行情生产端打通 Kafka+QuestDB+ClickHouse，示例 tick 发布脚本也一并落地。
- `baca43d` – 新增 `tools/collect_metrics.py`，可测 Kafka 延迟与 QuestDB/ClickHouse 查询耗时。
- `fb5da43` / `e5fd7b0` – 自动化 tick 发布、发布链路基准脚本及其配置项完善，便于比较子进程与内联写入吞吐。
示例命令：
```bash
# 单次写入 Kafka/QuestDB/ClickHouse，并打印耗时
python tools/publish_sample_tick.py --symbol TEST.FUT --price 101.5 --summary-only

# 按固定间隔循环推送 100 次示例 Tick
python tools/auto_publish_sample_tick.py --symbol TEST.FUT --price 101.5 --interval 0.5 --count 100

# 对比 Redis 旧链路、子进程写入与批处理写入的延迟表现
python tools/benchmark_publish_sample_tick.py --symbol TEST.FUT --price 101.5 --count 50 --warmup 10

# 采集 Kafka 延迟并可选跳过某些后端查询
python tools/collect_metrics.py --messages 500 --skip-clickhouse
```

**QuestDB 回测与批量工具**
- `ae29a1b` – QuestDB 接入回测主流程，提供 `main_questdb.py` 启动器。
- `269836b` – Mongo→QuestDB 批量迁移及数据一致性验证脚本（统计/全区间对比）。
示例命令：
```bash
# 使用 QuestDB 数据源回测指定策略
python src/panda_backtest/main_questdb.py --file strategy/future01.py --start-date 20240101 --end-date 20240201

# 将 Mongo 集合中的日线写入 QuestDB（支持多标的）
python src/panda_backtest/tools/mongo_to_questdb_bars.py \
  --collection stock_market \
  --symbols 000001.SH 000300.SH \
  --start-date 20230101 --end-date 20231231 \
  --quest-table stock_daily_bars

# 自动识别指定标的的最早/最晚交易日并分别跑 Mongo/QuestDB 对比回测
python src/panda_backtest/compare_data_source_full_range.py \
  --collection future_market \
  --symbol RB9999.SHF \
  --date-field trade_date \
  --date-type int \
  --print-summary
```

**API / 前端**
- `8282ccf` – Web 端接口全面升级为 HTTPS，默认使用 `uv` 锁文件中声明的新依赖；部署后可用 `curl -I https://127.0.0.1:8000/quantflow/api/v1/status` 快速检查服务存活。

**CTP 行情性能与 Level2**
- `b4b16e0` – CTP 回调仅负责 Redis 写入，慢链路移至后台队列，显著降低阻塞。
- `e52b812` / `b709c41` / `84db407` – 提供异步处理基准脚本、msgpack 序列化以及 Redis 批写参数，帮助验证优化效果。
- `c4da03c` – Kafka/QuestDB/ClickHouse 后端分离并可独立启停，适合按机器性能裁剪。
- `5a94f63` – Level2 C 解析器、批量 QuestDB 写入与导入脚本，方便离线数据回放。
示例命令：
```bash
# 基于 stub 组件压测异步写入耗时，支持模拟不同队列长度
python scripts/benchmark_tick_async.py --ticks 50000 --queue 8000

# 启用所有后端后压测真实链路
ENABLE_KAFKA_TICK=1 ENABLE_QUESTDB_TICK=1 ENABLE_CLICKHOUSE_TICK=1 \
python tools/publish_sample_tick.py --symbol AG.TEST --price 5200 --summary-only

# 将深交所 Level2 二进制数据导入 QuestDB，可先用 --dry-run 验证
python tools/import_level2_to_questdb.py \
  --date 20241107 \
  --tick-file 000001.SZ.tick \
  --order-file 000001.SZ.order \
  --trade-file 000001.SZ.trade \
  --dry-run
```

## 加群答疑（备注【开源】更快通过）
![PandaAI 交流群](https://zynf-test.oss-cn-shanghai.aliyuncs.com/github/wechat_2025-06-27_102633_615.png)

## 贡献
欢迎贡献代码、提出 Issue 或 PR:
- Fork 本项目
- 新建功能分支 git checkout -b feature/AmazingFeature
- 提交更改 git commit -m 'Add some AmazingFeature'
- 推送分支 git push origin feature/AmazingFeature
- 发起 Pull Request

## 致谢
感谢量化李不白的粉丝们对我们的支持<br>
感谢所有开源社区的贡献者

## 许可证
本项目采用 GPLV3 许可证
