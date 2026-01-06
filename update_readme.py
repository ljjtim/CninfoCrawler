import pandas as pd
from datetime import datetime, timedelta

def update_readme():
    # 1. 读取 CSV 文件
    df = pd.read_csv('announcements.csv')

    # 2. 转换时间格式并过滤近 7 天数据
    df['publish_time'] = pd.to_datetime(df['publish_time'])
    seven_days_ago = datetime.now() - timedelta(days=7)
    recent_df = df[df['publish_time'] >= seven_days_ago].copy()

    # 按时间倒序排序
    recent_df = recent_df.sort_values(by='publish_time', ascending=False)

    # 格式化日期为字符串，方便显示
    recent_df['publish_time'] = recent_df['publish_time'].dt.strftime('%Y-%m-%d')

    # 3. 生成 Markdown 表格
    # 将 URL 转换为 Markdown 链接格式
    recent_df['title'] = recent_df.apply(lambda x: f"[{x['title']}]({x['announcement_url']})", axis=1)
    
    # 选择需要的列并重命名
    display_df = recent_df[['publish_time', 'stock_code', 'stock_name', 'keyword', 'title']]
    display_df.columns = ['发布日期', '代码', '简称', '关键字', '公告标题 (点击跳转)']

    markdown_table = display_df.to_markdown(index=False)

    # 4. 写入 README.md
    readme_content = f"""# 巨潮资讯公告监控 (CninfoCrawler)

> 自动提取近 7 天的关键词监控公告。更新时间：{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

{markdown_table}

---
*更多历史数据请查看 [announcements.csv](./announcements.csv)*
"""

    with open('README.md', 'w', encoding='utf-8') as f:
        f.write(readme_content)

if __name__ == "__main__":
    update_readme()