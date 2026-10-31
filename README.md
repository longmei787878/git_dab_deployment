# GitHub + Databricks Asset Bundles 演示

本项目演示从 Git feature 分支部署到 Databricks 开发环境，运行测试，再通过 `main` 分支部署生产环境的完整流程。

## 数据流

源表：

- `claudecatalog.raw.bookings`
- `claudecatalog.raw.passengers`
- `claudecatalog.raw.airports`

关联关系：

- `bookings.passenger_id = passengers.passenger_id`
- `bookings.airport_id = airports.airport_id`

目标表：

- dev：`claudecatalog.silver.git_booking_enriched_dev`
- prod：`claudecatalog.silver.git_booking_enriched`

任务使用左关联保留所有 booking。维表 ID 重复时先按其属性排序并保留一行，防止关联后行数膨胀。找不到机场或乘客时，对应补充字段为 `NULL`。

## DAB 的作用

`databricks.yml` 是 Bundle 入口，`resources/booking_enrichment.job.yml` 声明 Databricks Job。DAB 将本地代码、Job 配置、定时计划和不同环境参数作为一个可版本控制的部署单元。

- `dev` 使用 development mode，写开发表并暂停定时器，供 feature 分支手工/CI 冒烟测试。
- `prod` 使用 production mode，写正式表并启用每小时定时器。
- Bundle 文件部署到当前部署身份自己的 Workspace 目录，不使用所有用户可写的 `/Workspace/Shared`。
- Cron `0 0 * * * ?` 表示每小时整点按 UTC 执行。
- Job 使用当前 serverless environment version 6，不需要维护传统 cluster。
- 两个 target 暂时使用同一个 Workspace，以模拟环境晋级。真实生产建议使用不同 Workspace。

## 本地验证与部署

当前机器已经配置默认 Databricks CLI profile。执行：

本地运行 PySpark 单元测试还需要 Java 17 和正确的 `JAVA_HOME`。GitHub Actions workflow 已自动安装并配置 Java 17。

```bash
python -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"
pytest
databricks bundle validate -t dev
databricks bundle deploy -t dev
databricks bundle run -t dev booking_enrichment
```

检查开发结果：

```sql
SELECT *
FROM claudecatalog.silver.git_booking_enriched_dev
LIMIT 20;
```

本地模拟生产部署：

```bash
databricks bundle validate -t prod
databricks bundle deploy -t prod
```

生产部署会启用每小时计划。首次学习时，建议先只部署 dev。

## Git 和 GitHub 流程

初始化仓库：

```bash
git init
git add .
git commit -m "Add booking enrichment bundle"
git branch -M main
git remote add origin <你的 GitHub 仓库 URL>
git push -u origin main
```

创建功能分支：

```bash
git switch -c feature/booking-enrichment
git push -u origin feature/booking-enrichment
```

推送 `feature/**` 后，`.github/workflows/feature-dev.yml` 将：

1. 安装 Python 依赖。
2. 执行 PySpark 单元测试。
3. 验证 dev Bundle。
4. 部署 dev Job。
5. 立即运行一次 Job 作为冒烟测试。

Pull Request 合并到 `main` 后，`.github/workflows/main-prod.yml` 将重新测试、验证、部署 prod，并立即执行一次生产冒烟测试。生产 Job 随后按每小时计划运行。

## GitHub 配置

在 GitHub 仓库中创建两个 Environments：`dev` 和 `prod`。

为两个 Environment 添加名为 `DATABRICKS_TOKEN` 的 secret。学习环境可使用个人访问令牌；正式项目应使用 Databricks service principal，并优先使用 GitHub OIDC/workload identity federation，避免长期 token。

为 `prod` Environment 配置 Required reviewers。这样 `main` workflow 到达生产部署前必须人工批准。

执行任务的身份至少需要：

- 读取 `claudecatalog.raw` 三个源表的权限。
- 使用 `claudecatalog` 和 `silver` schema 的权限。
- 创建、修改及写入目标表的权限。
- 在 Workspace 中创建和运行 Job 的权限。

## GitHub 与 Databricks Git folders 的区别

GitHub 是代码仓库和 CI/CD 执行入口。GitHub Actions 中的 Databricks CLI 调用 DAB API 完成部署。Databricks Git folders 主要用于在 Workspace UI 内交互式开发；使用 DAB 自动部署时不是必需组件。
