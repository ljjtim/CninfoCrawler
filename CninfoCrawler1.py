import os
import time
import json
import logging
import random
import pandas as pd
import requests
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional

# ==================== 配置区域 ====================
KEYWORDS = ['警示', '责令改正', '行政监管', '立案', '行政处罚']        # 搜索关键词列表

END_DATE = datetime.now().strftime('%Y-%m-%d')                # 结束日期
START_DATE = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')              # 开始日期
OUTPUT_FILE = 'announcements.csv'      # 输出文件名
PROGRESS_FILE = 'crawl_progress.json'  # 进度记录文件
FORCE_UPDATE = False                   # 是否强制全量更新
DELAY_RANGE = (1, 3)                   # 随机延迟范围（秒）
PROXIES = None                         # 代理设置
# =================================================

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

class CninfoCrawler:
    def __init__(self):
        self.url = "http://www.cninfo.com.cn/new/hisAnnouncement/query"
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
            "X-Requested-With": "XMLHttpRequest",
            "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8"
        }
        self.existing_ids = self._load_existing_ids()

    def _load_existing_ids(self) -> set:
        """加载已爬取的数据ID，用于去重"""
        if os.path.exists(OUTPUT_FILE) and not FORCE_UPDATE:
            try:
                df = pd.read_csv(OUTPUT_FILE)
                # 注意：如果之前CSV没有keyword列，这里读取可能会报错，
                # 但由于我们使用 title+publish_time 去重，所以逻辑依然稳健
                return set(df['title'] + df['publish_time'])
            except Exception as e:
                logging.error(f"读取旧数据失败: {e}")
        return set()

    def load_progress(self) -> str:
        if os.path.exists(PROGRESS_FILE) and not FORCE_UPDATE:
            with open(PROGRESS_FILE, 'r') as f:
                progress = json.load(f)
                return progress.get('last_date', START_DATE)
        return START_DATE

    def save_progress(self, current_date: str):
        with open(PROGRESS_FILE, 'w') as f:
            json.dump({'last_date': current_date}, f)

    # 修改点 1：增加 keyword 参数
    def parse_page(self, data_list: List[Dict], keyword: str) -> List[Dict]:
        """解析页面数据并记录对应关键词"""
        parsed_results = []
        for item in data_list:
            title = item.get('announcementTitle', '').replace('<em>', '').replace('</em>', '')
            publish_time = datetime.fromtimestamp(item['announcementTime'] / 1000).strftime('%Y-%m-%d')
            
            # 唯一性检查
            uid = title + publish_time
            if uid in self.existing_ids:
                continue

            # 修改点 2：在字典中增加 keyword 列
            parsed_results.append({
                'keyword': keyword,         # 记录当前搜索的词
                'stock_code': item.get('secCode'),
                'stock_name': item.get('secName'),
                'title': title,
                'publish_time': publish_time,
                'announcement_url': f"http://static.cninfo.com.cn/{item.get('adjunctUrl')}"
            })
            self.existing_ids.add(uid)
        return parsed_results

    def crawl_data(self, keyword: str, start_date: str, end_date: str):
        page = 1
        all_data = []
        
        while True:
            logging.info(f"正在爬取关键词 [{keyword}] : 第 {page} 页...")
            
            payload = {
                "pageNum": page,
                "pageSize": 30,
                "column": "szse",
                "tabName": "fulltext",
                "searchkey": keyword,
                "seDate": f"{start_date}~{end_date}",
                "sortName": "pubdate",
                "sortType": "desc",
                "isHLtitle": "true"
            }

            try:
                response = requests.post(self.url, data=payload, headers=self.headers, proxies=PROXIES, timeout=15)
                response.raise_for_status()
                res_json = response.json()
                
                announcements = res_json.get('announcements', [])
                if not announcements:
                    break
                
                # 修改点 3：将 keyword 传给解析函数
                valid_data = self.parse_page(announcements, keyword)
                all_data.extend(valid_data)
                
                if not res_json.get('hasMore', False):
                    break
                
                page += 1
                time.sleep(random.uniform(*DELAY_RANGE))
                
            except Exception as e:
                logging.error(f"请求失败: {e}")
                self.save_progress(start_date)
                return all_data

        return all_data

    def save_to_csv(self, data: List[Dict]):
        if not data:
            return
        
        df = pd.DataFrame(data)
        file_exists = os.path.isfile(OUTPUT_FILE)
        
        # 写入 CSV
        df.to_csv(OUTPUT_FILE, mode='a', index=False, header=not file_exists, encoding='utf-8-sig')
        logging.info(f"成功保存 {len(data)} 条数据至 {OUTPUT_FILE}")

def main():
    crawler = CninfoCrawler()
    current_start = crawler.load_progress()
    logging.info(f"设置的日期范围: {START_DATE} 至 {END_DATE}")
    logging.info(f"程序启动。计划爬取日期范围: {current_start} 至 {END_DATE}")

    if datetime.strptime(current_start, '%Y-%m-%d') >= datetime.strptime(END_DATE, '%Y-%m-%d'):
        logging.info("当前数据已是最新，无需更新。")
        return

    total_new_records = 0
    for kw in KEYWORDS:
        new_data = crawler.crawl_data(kw, current_start, END_DATE)
        if new_data:
            crawler.save_to_csv(new_data)
            total_new_records += len(new_data)
    
    crawler.save_progress(END_DATE)
    logging.info(f"任务完成！本次共新增记录: {total_new_records} 条")

if __name__ == '__main__':
    main()
