import requests
import pandas as pd
import re
import json
import time
import datetime
import plotly.graph_objects as go
from requests.adapters import HTTPAdapter

# 1. 核心底层加固：禁用 IPv6 干扰，绕开 SSL 报错
requests.packages.urllib3.util.connection.HAS_IPV6 = False
requests.packages.urllib3.disable_warnings()

class PureDirectAdapter(HTTPAdapter):
    def proxy_manager_for(self, *args, **kwargs): return None

class SSEQuantGladiatorShort:
    def __init__(self, target_count=5):
        self.target_count = target_count
        self.url = "https://query.sse.com.cn/commonQuery.do"
        self.session = requests.Session()
        # 强制直连隔离系统代理
        self.session.mount("https://", PureDirectAdapter())
        self.session.trust_env = False
        self.headers = {
            "Host": "query.sse.com.cn",
            "Referer": "https://www.sse.com.cn/market/funddata/volumn/etfvolumn/",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/123.0.0.0 Safari/537.36"
        }

    def fetch_data(self, date_str):
        params = {
            "jsonCallBack": f"jsonp{int(time.time()*1000)}",
            "isPagination": "true",
            "pageHelp.pageSize": "500",
            "sqlId": "COMMON_SSE_ZQPZ_ETFZL_XXPL_ETFGM_SEARCH_L",
            "STAT_DATE": date_str,
            "_": int(time.time() * 1000)
        }
        try:
            # 增加 timeout 到 15 秒，应对 IPv6 环境波动
            r = self.session.get(self.url, params=params, headers=self.headers, verify=False, timeout=15)
            if "jsonp" in r.text:
                json_str = re.search(r'\(({.*})\)', r.text).group(1)
                data = json.loads(json_str)
                items = data.get('pageHelp', {}).get('data', [])
                if items:
                    df = pd.DataFrame(items)
                    df['TOT_MARKET_VALUE'] = pd.to_numeric(df['TOT_MARKET_VALUE'], errors='coerce')
                    df['date'] = date_str
                    return df[['date', 'SEC_CODE', 'SEC_ABBR', 'TOT_MARKET_VALUE']]
        except:
            pass
        return None

    def run(self):
        print(f">>> 启动高频监控：正在捕获最近 {self.target_count} 个交易日的 Top 5 数据...")
        all_frames = []
        # 从今天开始回溯
        current_check = datetime.date.today()
        
        attempts = 0
        while len(all_frames) < self.target_count and attempts < 20:
            d_str = current_check.strftime("%Y-%m-%d")
            print(f"扫描周期: {d_str}...", end='\r')
            df = self.fetch_data(d_str)
            
            if df is not None:
                all_frames.append(df)
                print(f"\n[获取成功] 第 {len(all_frames)} 个交易日: {d_str}")
            
            current_check -= datetime.timedelta(days=1)
            attempts += 1
            time.sleep(1.0) # 稍微拉长间隔，防止被 WAF 临时封禁

        if not all_frames:
            print("\n>>> 错误：未能获取任何数据，请尝试：set NO_PROXY=sse.com.cn")
            return

        full_df = pd.concat(all_frames)
        latest_date = full_df['date'].max()
        
        # 自动识别前五大
        top5_list = full_df[full_df['date'] == latest_date].nlargest(5, 'TOT_MARKET_VALUE')
        top5_codes = top5_list['SEC_CODE'].tolist()

        fig = go.Figure()
        for code in top5_codes:
            sub = full_df[full_df['SEC_CODE'] == code].sort_values('date')
            name = sub['SEC_ABBR'].iloc[-1]
            fig.add_trace(go.Scatter(x=sub['date'], y=sub['TOT_MARKET_VALUE'], name=f"{name}({code})", mode='lines+markers'))

        # 下拉菜单切换
        buttons = [dict(label="全量对比汇总", method="update", args=[{"visible": [True]*5}])]
        for i, code in enumerate(top5_codes):
            v = [False]*5; v[i] = True
            buttons.append(dict(label=f"仅看 {top5_list.iloc[i]['SEC_ABBR']}", method="update", args=[{"visible": v}]))

        fig.update_layout(
            updatemenus=[dict(active=0, buttons=buttons, x=0, y=1.2, xanchor='left')],
            title=f"SSE Top 5 ETF 近 {len(all_frames)} 个交易日规模监测",
            template="plotly_dark", xaxis_title="日期", yaxis_title="市值(亿元)",
            hovermode="x unified"
        )
        
        fig.write_html("sse_5day_top5.html")
        print(f"\n>>> 任务完成！交互图表已生成: sse_5day_top5.html")

if __name__ == "__main__":
    SSEQuantGladiatorShort(target_count=5).run()