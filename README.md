# 巨潮资讯公告监控 (CninfoCrawler)

> 自动提取近 7 天的关键词监控公告。更新时间：2026-10-07 01:40:30

最近 7 天暂无匹配公告。

## 完整性校验使用示例

> `README.md` 由脚本自动生成，请优先修改 `update_readme.py` 或 `cninfo_service.py` 中的模板，避免被 GitHub Actions 覆盖。

- 校验指定时间范围，程序会自动按月份拆分抓取：
  - `python verify_csv_integrity.py --start-date 2022-01-01 --end-date 2022-12-31`
- 发现遗漏后追加补齐到 `announcements.csv` 末尾：
  - `python verify_csv_integrity.py --start-date 2022-01-01 --end-date 2022-12-31 --repair`
- 如果时间跨度较大，建议先不带 `--repair` 观察日志和缺失统计，再决定是否补写。

---
*更多历史数据请查看 [announcements.csv](./announcements.csv)*
