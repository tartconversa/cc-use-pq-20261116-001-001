# 展品体验事件湖

本项目使用 FastAPI 和 SQLite 管理场馆、展品、来源契约与事件批次。原始事件保留接收证据，正式报表只读取已发布的日期快照。

## 运行

在预装依赖的环境中，从项目根目录执行：

    uvicorn exhibit_lake.api:app --host 0.0.0.0 --port 8000

默认数据库为本地 SQLite 文件；用 EXHIBIT_LAKE_DB 可指定其他路径。

## 测试与构建

    python -m unittest discover -s tests -v
    python -m compileall -q exhibit_lake
    python -m exhibit_lake.demo

以上三条命令依次运行业务测试、编译检查和本地合成数据验收。依赖版本记录在 requirements.txt。

## 日分区的水位线与封账

每个「场馆 + 本地日期」分区都有独立的水位线（已发布事件的最大事件时间）和封账版本。所有规则都以本地日期为边界，事件按场馆时区归类，因此不同时区来源的同一瞬间会落到各自正确的日期。

1. **封账前**：继续补传会正常接收，重新发布后形成新的 `generation`；旧版本仍可按 `snapshot_id` 读取，报表以版本号区分口径。
2. **封账**：`POST /venues/{venue_id}/days/{local_day}/close` 冻结当前已发布的 head。封账会记录水位线、版本号、操作人、原因和幂等键。若还有已接收但未发布的事件，封账会把它们转为「留证」而不并入正式数字。
3. **封账后**：新到批次仍完整落原始行并生成可追溯的接收证据，但事件被标记为留证（`held_by_freeze`），不会进入任何自动发布的报表。通过 `GET /venues/{venue_id}/days/{local_day}/closure` 可以查看留证明细、封账历史和当前水位线。
4. **获准重开**：`POST /venues/{venue_id}/days/{local_day}/reopen` 必须提供 `approved_by` 与 `reason`。重开会留下新的封账版本并把留证事件释放为下一版回填的候选；再次封账会产生新的版本，历史版本全部保留在 `day_closures` 与审计日志中。
5. **一致性**：暂存、发布、封账、重开都在单个 SQLite 写事务内用条件更新完成。发布与封账并发时只有一方能推进 head，查询端读到的永远是一个已发布版本；留证事件不会混入正式报表。

相关接口还包括 `GET /venues/{venue_id}/days/{local_day}/recovery`（候选诊断）与 `GET /audits`（`day.closed`、`day.reopened`、`events.held_after_close`、`event.released_after_reopen` 等动作）。
