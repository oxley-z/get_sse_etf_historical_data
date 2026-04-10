import requests
import json
import time
import os
import re
import webbrowser
from datetime import datetime

# ── 1. 配置 ────────────────────────────────────────────────────────────────────
ETF_MAP = {
    '510300': '华泰柏瑞沪深300ETF',
    '510310': '易方达沪深300ETF',
    '510330': '华夏沪深300ETF',
    '510050': '华夏上证50ETF',
    '510500': '南方中证500ETF',
    '512100': '南方中证1000ETF',
    '510180': '华安上证180ETF',
    '560010': '广发中证1000ETF',
    '588080': '易方达上证科创板50ETF',
}

OUTPUT_HTML = 'sse_final_dashboard.html'
CHECKPOINT = 'sse_checkpoint.json'

# ── 2. 指数获取（修复 0 值问题） ───────────────────────────────────────────────

def get_shindex_data():
    """从搜狐获取上证指数历史数据，增加解析容错"""
    # 扩大日期范围，确保覆盖
    url = 'http://q.stock.sohu.com/hisHq?code=zs_000001&start=20200101&end=20261231&stat=1&order=A&period=d'
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
        'Referer': 'http://q.stock.sohu.com/'
    }
    try:
        # 直接请求 JSON 避免 callback 解析干扰
        resp = requests.get(url, headers=headers, timeout=10)
        data = resp.json()
        
        if isinstance(data, list) and len(data) > 0:
            hq_list = data[0].get('hq', [])
            # 格式：["2024-01-02", "2972.28", "2962.28", ...] 收盘价在 index 2
            result = {row[0]: float(row[2]) for row in hq_list if float(row[2]) > 0}
            print(f"成功获取指数数据，共 {len(result)} 条记录")
            return result
    except Exception as e:
        print(f"指数解析异常: {e}")
    return {}

# ── 3. 数据处理 ──────────────────────────────────────────────────────────────

def build_aligned_plot_data(results, index_dict):
    # 提取所有日期并去重排序
    all_dates = sorted(list({r['date'] for r in results}))
    
    plot_data = []
    
    # 1. 构建 ETF 数据
    for code, name in ETF_MAP.items():
        vals = []
        for d in all_dates:
            day_data = next((r for r in results if r['date'] == d), None)
            val = None
            if day_data:
                item = next((i for i in day_data['items'] if i.get('SEC_CODE', '').strip() == code), None)
                if item:
                    val = float(str(item.get('TOT_VOL', '0')).replace(',', ''))
            vals.append(val)
        
        plot_data.append({
            'x': all_dates,
            'y': vals,
            'name': f"{name}({code})",
            'type': 'scatter',
            'mode': 'lines',
            'yaxis': 'y1',
            'connectgaps': True  # 自动连接缺失的数据点
        })
    
    # 2. 构建指数数据（对齐日期）
    idx_vals = []
    for d in all_dates:
        # 如果当日没有指数数据，设为 None 避免连到 0 点
        price = index_dict.get(d, None)
        idx_vals.append(price)
        
    plot_data.append({
        'x': all_dates,
        'y': idx_vals,
        'name': '上证指数',
        'type': 'scatter',
        'mode': 'lines',
        'line': {'color': 'rgba(231, 76, 60, 0.8)', 'width': 2},
        'yaxis': 'y2',
        'connectgaps': True
    })
    
    return plot_data

# ── 4. HTML 模板 ──────────────────────────────────────────────────────────────

HTML_TEMPLATE = """<!DOCTYPE html>
<html>
<head>
    <meta charset="utf-8">
    <title>ETF 监控看板</title>
    <script src="https://cdn.plot.ly/plotly-2.27.0.min.js"></script>
    <style>
        body { font-family: -apple-system, sans-serif; margin: 20px; background: #f4f6f9; }
        .tabs { display: flex; flex-wrap: wrap; gap: 8px; margin-bottom: 15px; justify-content: center; }
        .tab { padding: 6px 14px; background: #fff; border: 1px solid #dcdfe6; border-radius: 4px; cursor: pointer; font-size: 13px; }
        .tab.active { background: #409EFF; color: #fff; border-color: #409EFF; }
        #chart { background: #fff; padding: 15px; border-radius: 8px; box-shadow: 0 4px 12px rgba(0,0,0,0.1); height: 75vh; }
    </style>
</head>
<body>
    <div class="tabs" id="tabs"></div>
    <div id="chart"></div>

    <script>
        const rawData = PLOT_DATA_JSON;
        const indexIdx = rawData.length - 1;

        const layout = {
            title: 'ETF 规模与上证指数对照',
            hovermode: 'x unified',
            xaxis: { gridcolor: '#f0f0f0', type: 'category' },
            yaxis: { title: 'ETF 规模 (万份)', side: 'left' },
            yaxis2: { 
                title: '上证指数', side: 'right', overlaying: 'y', 
                showgrid: false, tickfont: {color: '#e74c3c'}, 
                autorange: true // 确保单图时指数刻度自动适配
            },
            legend: { orientation: 'h', x: 0.5, xanchor: 'center', y: -0.15 },
            margin: { t: 50, r: 80, b: 80, l: 80 }
        };

        Plotly.newPlot('chart', rawData, layout);

        const tabsBox = document.getElementById('tabs');
        const makeBtn = (text, idx) => {
            const b = document.createElement('div');
            b.className = 'tab' + (idx === -1 ? ' active' : '');
            b.innerText = text;
            b.onclick = () => {
                document.querySelectorAll('.tab').forEach(t => t.classList.remove('active'));
                b.classList.add('active');
                const mask = rawData.map((_, i) => idx === -1 || i === idx || i === indexIdx);
                Plotly.restyle('chart', { visible: mask });
                Plotly.relayout('chart', { 'yaxis2.autorange': true });
            };
            tabsBox.appendChild(b);
        };

        makeBtn('📊 全部显示', -1);
        rawData.forEach((t, i) => i !== indexIdx && makeBtn(t.name.split('(')[0], i));
    </script>
</body>
</html>"""

# ── 5. 执行 ────────────────────────────────────────────────────────────────────

def main():
    if not os.path.exists(CHECKPOINT):
        print("错误: 找不到 sse_checkpoint.json 文件")
        return

    with open(CHECKPOINT, 'r', encoding='utf-8') as f:
        results = json.load(f).get('results', [])

    index_dict = get_shindex_data()
    
    if not index_dict:
        print("警告: 未能获取指数数据，请检查网络或稍后重试")

    plot_data = build_aligned_plot_data(results, index_dict)
    
    final_html = HTML_TEMPLATE.replace('PLOT_DATA_JSON', json.dumps(plot_data, ensure_ascii=False))
    
    with open(OUTPUT_HTML, 'w', encoding='utf-8') as f:
        f.write(final_html)
    
    print(f"看板已更新: {os.path.abspath(OUTPUT_HTML)}")
    webbrowser.open('file://' + os.path.abspath(OUTPUT_HTML))

if __name__ == '__main__':
    main()