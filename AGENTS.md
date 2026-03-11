使用简体中文回答用户

# Repository Guidelines

## 项目结构与模块组织
- `CninfoCrawler1.py` 是主入口脚本，默认抓取最近 7 天公告并做增量更新。
- `cninfo_service.py` 集中维护抓取、按月拆分日期范围、分页日志、去重、CSV 读写与 README 内容生成逻辑。
- `verify_csv_integrity.py` 用于手动校验 `announcements.csv` 是否有遗漏，支持仅报告或用 `--repair` 追加补齐到 CSV 末尾。
- `update_readme.py` 根据最近 7 天的数据重建 `README.md` 中的公告表格。
- `requirements.txt` 维护运行依赖，目前包含 `requests`、`pandas`、`tabulate`。
- `.github/workflows/` 存放定时任务：`daily_crawl.yml` 负责抓取公告，`daily_update.yml` 负责刷新说明文档。
- 仓库当前采用扁平结构；新增脚本优先放在根目录，只有在模块明显增多时再拆分子目录。

## 构建、测试与开发命令
- `python -m venv .venv`：创建本地虚拟环境。
- `.\\.venv\\Scripts\\Activate.ps1`：在 PowerShell 中激活虚拟环境。
- `pip install -r requirements.txt`：安装项目依赖。
- `python CninfoCrawler1.py`：抓取最近 7 天关键词公告并更新 `announcements.csv`。
- `python verify_csv_integrity.py --start-date 2022-01-01 --end-date 2022-12-31`：校验指定时间范围的 CSV 完整性；程序会自动按月份拆分抓取，避免大区间分页回卷。
- `python verify_csv_integrity.py --start-date 2022-01-01 --end-date 2022-12-31 --repair`：发现遗漏后追加写入 CSV 末尾。
- `python update_readme.py`：重新生成 `README.md` 中的 Markdown 表格。
- `python -m py_compile CninfoCrawler1.py cninfo_service.py verify_csv_integrity.py update_readme.py`：提交前做快速语法检查。

## 代码风格与命名规范
- 遵循 PEP 8，统一使用 4 个空格缩进，导入按标准库、第三方、本地模块分组。
- 函数与变量使用 `snake_case`，类名使用 `PascalCase`，模块级常量使用 `UPPER_CASE`，如 `DEFAULT_KEYWORDS`、`DEFAULT_OUTPUT_FILE`。
- 保持现有风格：为关键逻辑补充简洁类型标注，使用 `logging` 输出运行信息，避免无意义注释。
- 读写 CSV 和 Markdown 时默认使用 UTF-8 相关编码，确保中文内容在本地和 GitHub 上都能正常显示。

## 测试指南
- 当前没有正式测试框架，至少执行语法检查和一次手工冒烟验证。
- 修改抓取逻辑后，运行 `python CninfoCrawler1.py`，确认 `announcements.csv` 新增记录格式正确且没有重复。
- 修改完整性校验逻辑后，先运行不带 `--repair` 的 `python verify_csv_integrity.py ...`，确认缺失统计、按月拆分和分页日志符合预期，再决定是否补写。
- 修改文档生成逻辑后，运行 `python update_readme.py`，检查 `README.md` 表格是否渲染正常、链接是否可读。
- 如需补充自动化测试，建议新建 `tests/` 目录，并采用 `test_*.py` 命名，便于后续接入 `pytest`。

## 提交与合并请求规范
- 定时任务提交沿用现有格式，例如 `Auto-update data: 2026-03-10 15:51` 与 `docs: 自动更新近7天公告表格 [skip ci]`。
- 人工修改建议使用简短前缀式提交，如 `fix: handle empty API responses` 或 `docs: clarify workflow schedule`。
- Pull Request 需说明变更目的、影响范围、验证命令，以及是否会影响 GitHub Actions 或生成文件。
- 只有在变更会影响 Markdown 展示或工作流结果时，才附示例输出或截图。
