import requests
import json
from datetime import datetime, timedelta

def fetch_etf_top5(date_str):
    url = "https://www.sse.com.cn/xhtml/js/lib/2021/bootstrap-select-v1.13.9.js?v=ssesite_V3.7.3_20260204"  # 真实URL
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Referer": "https://www.szse.cn/market/product/fund/etf/index.html",
        "Content-Type": "application/json",  # 根据实际情况调整
    }
    # 参数格式可能为 JSON 或 query string，按实际情况选择
    payload = {
        "catalogId": "ETF_VOLUME_RANK",
        "date": date_str,
        "top": 5
    }
    # 如果是 POST JSON 请求
    resp = requests.post(url, json=payload, headers=headers)
    # 如果是 GET 请求
    # resp = requests.get(url, params=payload, headers=headers)
    
    print(f"请求 {date_str} 状态码: {resp.status_code}")  # 调试输出
    print("响应前200字符:", resp.text[:200])  # 查看返回内容
    
    if resp.status_code == 200:
        try:
            data = resp.json()
            return data.get("data", [])  # 根据实际返回结构调整
        except json.JSONDecodeError:
            print("响应不是 JSON，请检查 URL 和参数。")
            return []
    else:
        return []