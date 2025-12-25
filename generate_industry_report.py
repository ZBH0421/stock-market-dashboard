from market_data_db import MarketDataDB
from sqlalchemy import text
import pandas as pd
import datetime
import json

def generate_industry_report(target_industry: str):
    print(f"Generating Professional Report for: {target_industry}...")
    
    try:
        db = MarketDataDB()
        
        # 1. Fetch Fundamental Data
        with db.engine.connect() as conn:
            query_funds = text("""
                SELECT t.ticker, t.company_name, t.market_cap, t.revenue, t.pe_ratio
                FROM tickers t
                JOIN industries i ON t.industry_id = i.id
                WHERE i.name = :industry
            """)
            df_funds = pd.read_sql(query_funds, conn, params={"industry": target_industry})
            
            if df_funds.empty:
                print("No tickers found.")
                return

            # 2. Process Price History & Metrics
            stats_list = []
            stock_data_map = {}

            # Helper for % Change
            def get_pct_change(current_price, old_price):
                if old_price is None or old_price == 0: return 0.0
                return ((current_price - old_price) / old_price) * 100

            for index, row in df_funds.iterrows():
                ticker = row['ticker']
                
                # Fetch 400 days
                query_price = text("""
                    SELECT date, close, volume
                    FROM us_daily_prices
                    WHERE symbol = :ticker
                    ORDER BY date DESC
                    LIMIT 400
                """)
                df_price = pd.read_sql(query_price, conn, params={"ticker": ticker})
                
                # Metrics Calculation
                metrics = {'Volume': 0, '1D': 0.0, '1M': 0.0, '2M': 0.0, '3M': 0.0, '6M': 0.0, '12M': 0.0, 'YTD': 0.0}
                
                if not df_price.empty:
                    # 1. Prepare JSON Data for Chart
                    chart_series = []
                    for _, p_row in df_price.iloc[::-1].iterrows():
                         chart_series.append({
                             'x': p_row['date'].strftime('%Y-%m-%d'),
                             'y': float(p_row['close'])
                         })
                    stock_data_map[ticker] = chart_series

                    # 2. Logic Calc
                    df_price['date'] = pd.to_datetime(df_price['date'])
                    latest = df_price.iloc[0]
                    latest_date = latest['date']
                    latest_price = latest['close']
                    metrics['Volume'] = latest['volume']
                    
                    if len(df_price) >= 2:
                        metrics['1D'] = get_pct_change(latest_price, df_price.iloc[1]['close'])

                    def find_price_days_ago(days):
                        target_date = latest_date - pd.Timedelta(days=days)
                        for _, p_row in df_price.iterrows():
                            if p_row['date'] <= target_date: return p_row['close']
                        return None

                    metrics['1M'] = get_pct_change(latest_price, find_price_days_ago(30))
                    metrics['2M'] = get_pct_change(latest_price, find_price_days_ago(60))
                    metrics['3M'] = get_pct_change(latest_price, find_price_days_ago(90))
                    metrics['6M'] = get_pct_change(latest_price, find_price_days_ago(180))
                    metrics['12M'] = get_pct_change(latest_price, find_price_days_ago(365))

                    start_of_year = datetime.datetime(latest_date.year, 1, 1)
                    ytd_price = None
                    for _, p_row in df_price.iterrows():
                        if p_row['date'] <= start_of_year:
                             ytd_price = p_row['close']
                             break
                    if ytd_price is None: ytd_price = df_price.iloc[-1]['close']
                    metrics['YTD'] = get_pct_change(latest_price, ytd_price)

                stats_list.append({ 'Symbol': ticker, **metrics })
            
            df_final = pd.merge(df_funds, pd.DataFrame(stats_list), left_on='ticker', right_on='Symbol')
            
            # Serialize stock_data_map to JSON string
            json_stock_data = json.dumps(stock_data_map)

            # 3. Generate HTML with data attributes
            table_rows = ""
            for i, row in df_final.iterrows():
                sym = row['ticker']
                name = row['company_name'] if row['company_name'] else sym
                
                # Formatters
                def fmt_b(val): return f"{val/1e9:.2f}B" if val and val>0 else "-"
                def fmt_pct(val): return f"{val:+.2f}%"
                def get_cls(val): return "pos" if val > 0 else "neg" if val < 0 else "neu"

                mcap_raw = row['market_cap'] if pd.notnull(row['market_cap']) else 0
                rev_raw = row['revenue'] if pd.notnull(row['revenue']) else 0
                pe_raw = row['pe_ratio'] if pd.notnull(row['pe_ratio']) else 0
                pe_fmt = f"{pe_raw:.2f}" if pe_raw > 0 else "-"

                tf_cells = ""
                # Add data-symbol and data-tf attributes to cells
                for tf in ['1D', '1M', '2M', '3M', '6M', '12M', 'YTD']:
                    val = row[tf]
                    tf_cells += f'<td data-order="{val}" data-symbol="{sym}" data-tf="{tf}" class="tf-{tf} badge-pct pct-{get_cls(val)} chart-trigger">{fmt_pct(val)}</td>'

                table_rows += f"""
                <tr>
                    <td>{i + 1}</td>
                    <td><span class="symbol-badge">{sym}</span></td>
                    <td>{name}</td>
                    <td data-order="{mcap_raw}" class="fw-medium">{fmt_b(mcap_raw)}</td>
                    <td data-order="{pe_raw}">{pe_fmt}</td>
                    {tf_cells}
                    <td data-order="{row['Volume']}">{row['Volume']:,}</td>
                    <td data-order="{rev_raw}">{fmt_b(rev_raw)}</td>
                </tr>
                """

            # 4. Inject HTML + JS
            html_content = f"""
            <!DOCTYPE html>
            <html lang="zh-Hant">
            <head>
                <meta charset="UTF-8">
                <meta name="viewport" content="width=device-width, initial-scale=1.0">
                <title>{target_industry} Market Report (Professional)</title>

                <link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&family=Noto+Sans+TC:wght@400;500;700&display=swap" rel="stylesheet">
                <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0/dist/css/bootstrap.min.css" rel="stylesheet">
                <link rel="stylesheet" type="text/css" href="https://cdn.datatables.net/1.13.6/css/dataTables.bootstrap5.min.css">

                <style>
                    :root {{
                        --bg-body: #f1f5f9;
                        --bg-card: #ffffff;
                        --text-primary: #0f172a;
                        --text-secondary: #64748b;
                        --accent-blue: #3b82f6;
                        --pos-bg: #dcfce7; --pos-text: #166534;
                        --neg-bg: #fee2e2; --neg-text: #991b1b;
                        --neu-bg: #f1f5f9; --neu-text: #475569;
                    }}

                    body {{ font-family: 'Inter', sans-serif; background-color: var(--bg-body); color: var(--text-primary); padding-bottom: 60px; }}
                    
                    .page-header {{ padding: 2rem 0; margin-bottom: 1rem; }}
                    .page-title {{ font-weight: 800; color: #1e293b; letter-spacing: -0.025em; }}
                    .page-subtitle {{ color: var(--text-secondary); }}

                    .dashboard-card {{
                        background: var(--bg-card); border-radius: 16px; border: 1px solid #e2e8f0;
                        box-shadow: 0 4px 6px -1px rgba(0, 0, 0, 0.05); padding: 1.5rem; height: 100%;
                    }}

                    .table-card {{ border-radius: 16px; overflow: hidden; border: 1px solid #e2e8f0; background: white; box-shadow: 0 4px 6px -1px rgba(0, 0, 0, 0.05); }}
                    .table-header {{ padding: 1.25rem 1.5rem; border-bottom: 1px solid #e2e8f0; background: white; }}

                    table.dataTable thead th {{
                        background-color: #f8fafc !important; color: #475569; font-weight: 600; font-size: 0.8rem;
                        text-transform: uppercase; letter-spacing: 0.05em; border-bottom: 1px solid #e2e8f0 !important;
                    }}
                    table.dataTable tbody td {{ vertical-align: middle; font-size: 0.9rem; padding: 1rem !important; color: #334155; }}
                    
                    .badge-pct {{
                        display: inline-flex; align-items: center; justify-content: center; min-width: 68px;
                        padding: 6px 10px; border-radius: 8px; font-weight: 600; font-size: 0.85rem; cursor: pointer; transition: all 0.2s;
                    }}
                    .pct-pos {{ background-color: var(--pos-bg); color: var(--pos-text); }}
                    .pct-neg {{ background-color: var(--neg-bg); color: var(--neg-text); }}
                    .pct-neu {{ background-color: var(--neu-bg); color: var(--neu-text); }}
                    .badge-pct:hover {{ filter: brightness(0.96); transform: translateY(-1px); box-shadow: 0 2px 4px rgba(0,0,0,0.05); }}

                    .symbol-badge {{ background-color: #eff6ff; color: var(--accent-blue); padding: 4px 8px; border-radius: 6px; font-weight: 700; font-size: 0.85rem; }}

                    #chartTooltip {{
                        position: fixed; display: none; width: 300px; height: 220px;
                        background: rgba(255, 255, 255, 0.95); backdrop-filter: blur(8px);
                        border: 1px solid #e2e8f0; box-shadow: 0 20px 25px -5px rgba(0,0,0,0.1);
                        z-index: 9999; border-radius: 12px; padding: 16px; pointer-events: none;
                    }}
                    #tooltipHeader {{ font-size: 0.85rem; font-weight: 700; color: #1e293b; margin-bottom: 8px; border-bottom: 1px solid #f1f5f9; padding-bottom: 8px; }}
                </style>
            </head>

            <body>

                <div class="container main-container">
                    <div class="page-header row align-items-end">
                        <div class="col-md-8">
                            <h1 class="page-title">{target_industry} Market Report</h1>
                            <p class="page-subtitle">Interactive market analysis dashboard & visualization</p>
                        </div>
                        <div class="col-md-4 text-md-end">
                            <button class="btn btn-outline-secondary btn-sm" onclick="window.print()">Export PDF</button>
                        </div>
                    </div>

                    <div class="row g-4 mb-4">
                        <div class="col-lg-8">
                            <div class="dashboard-card d-flex flex-column">
                                <div class="d-flex justify-content-between align-items-center mb-3">
                                    <h5 class="fw-bold mb-0" style="color: #334155;">Market Cap Distribution</h5>
                                    <span class="badge bg-light text-secondary">Top 5 vs Others</span>
                                </div>
                                <div id="marketCapChart" class="flex-grow-1" style="min-height: 300px;"></div>
                            </div>
                        </div>

                        <div class="col-lg-4">
                            <div class="dashboard-card">
                                <h5 class="fw-bold mb-3" style="color: #334155;">Settings</h5>
                                <div class="mb-3">
                                    <label class="form-label text-secondary small fw-bold">PERFORMANCE TIMEFRAME</label>
                                    <select id="tfSelector" class="form-select form-select-lg">
                                        <option value="1D">1 Day</option>
                                        <option value="1M">1 Month</option>
                                        <option value="2M">2 Months</option>
                                        <option value="3M">3 Months</option>
                                        <option value="6M">6 Months</option>
                                        <option value="12M">1 Year</option>
                                        <option value="YTD">YTD</option>
                                    </select>
                                </div>
                                <div class="p-3 bg-light rounded-3 border mt-4">
                                    <small class="text-muted d-block mb-1"><strong>Pro Tip:</strong></small>
                                    <span style="font-size: 0.9rem; color: #475569;">
                                        Hover over any % value in the table to see the instant price trend chart.
                                    </span>
                                </div>
                            </div>
                        </div>
                    </div>

                    <div class="table-card mb-5">
                        <div class="table-header">
                            <h5 class="fw-bold mb-0" style="color: #334155;">Detailed Market Data</h5>
                        </div>
                        <div class="table-responsive">
                            <table id="stocksTable" class="table mb-0" style="width:100%">
                                <thead>
                                    <tr>
                                        <th>#</th> <th>Symbol</th> <th>Company</th> <th>Market Cap</th> <th>P/E</th>
                                        <th class="tf-1D">1D %</th> <th class="tf-1M">1M %</th> <th class="tf-2M">2M %</th>
                                        <th class="tf-3M">3M %</th> <th class="tf-6M">6M %</th> <th class="tf-12M">12M %</th>
                                        <th class="tf-YTD">YTD %</th>
                                        <th>Volume</th> <th>Revenue</th>
                                    </tr>
                                </thead>
                                <tbody>{table_rows}</tbody>
                            </table>
                        </div>
                    </div>
                </div>

                <div id="chartTooltip">
                    <div id="tooltipHeader">
                        <span id="tooltipSymbol"></span>
                        <span id="tooltipValue" style="float:right;"></span>
                    </div>
                    <div id="chartContainer"></div>
                </div>

                <script src="https://code.jquery.com/jquery-3.7.0.min.js"></script>
                <script src="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0/dist/js/bootstrap.bundle.min.js"></script>
                <script src="https://cdn.datatables.net/1.13.6/js/jquery.dataTables.min.js"></script>
                <script src="https://cdn.datatables.net/1.13.6/js/dataTables.bootstrap5.min.js"></script>
                <script src="https://cdn.jsdelivr.net/npm/apexcharts"></script>

                <script>
                    var stockData = {json_stock_data};
                    var tooltipChart;

                    $(document).ready(function () {{
                        // 1. Initialize DataTable
                        var table = $('#stocksTable').DataTable({{
                            "paging": false, "info": false, "searching": true,
                            "order": [[3, "desc"]],
                            "aoColumnDefs": [ 
                                {{ "bVisible": false, "aTargets": [6,7,8,9,10,11] }},
                                {{ "className": "text-center", "targets": [0,1,4,5,6,7,8,9,10,11] }},
                                {{ "className": "text-end", "targets": [3,12,13] }}
                            ]
                        }});

                        // 2. Market Cap Donut Chart Logic
                        var marketCaps = [];
                        $('#stocksTable tbody tr').each(function () {{
                            var symbol = $(this).find('td').eq(1).text().trim();
                            var mcapRaw = $(this).find('td').eq(3).attr('data-order'); // Get clean number
                            var mcap = parseFloat(mcapRaw);
                            if (mcap && !isNaN(mcap) && mcap > 0) {{
                                marketCaps.push({{ name: symbol, value: mcap }});
                            }}
                        }});

                        marketCaps.sort((a, b) => b.value - a.value);
                        var top5 = marketCaps.slice(0, 5);
                        var othersVal = marketCaps.slice(5).reduce((acc, curr) => acc + curr.value, 0);

                        var labels = top5.map(d => d.name);
                        var series = top5.map(d => d.value);
                        if (othersVal > 0) {{
                            labels.push('Others');
                            series.push(othersVal);
                        }}

                        var pieOptions = {{
                            series: series, labels: labels,
                            chart: {{ type: 'donut', height: 320, fontFamily: 'Inter, sans-serif' }},
                            colors: ['#3b82f6', '#60a5fa', '#93c5fd', '#bfdbfe', '#2563eb', '#cbd5e1'],
                            plotOptions: {{ 
                                pie: {{ 
                                    donut: {{ 
                                        size: '70%', 
                                        labels: {{ 
                                            show: true,
                                            name: {{ show: true, fontSize: '16px', fontWeight: 600 }},
                                            value: {{
                                                show: true,
                                                fontSize: '20px',
                                                fontWeight: 700,
                                                formatter: function (val, opts) {{
                                                    var total = opts.globals.seriesTotals.reduce((a, b) => a + b, 0);
                                                    return ((val / total) * 100).toFixed(1) + "%";
                                                }}
                                            }}, 
                                            total: {{ 
                                                show: true, 
                                                label: 'Total Cap', 
                                                fontSize: '14px',
                                                formatter: w => "$" + (w.globals.seriesTotals.reduce((a, b) => a + b, 0) / 1e9).toFixed(1) + "B" 
                                            }} 
                                        }} 
                                    }} 
                                }} 
                            }},
                            dataLabels: {{ enabled: false }}, legend: {{ position: 'bottom' }},
                            tooltip: {{ y: {{ formatter: val => "$" + (val / 1e9).toFixed(2) + "B" }} }}
                        }};

                        if (marketCaps.length > 0) {{
                            new ApexCharts(document.querySelector("#marketCapChart"), pieOptions).render();
                        }} else {{
                            $('#marketCapChart').html('<div class="text-center text-muted py-5">No Market Cap Data</div>');
                        }}

                        // 3. Timeframe Toggle
                        $('#tfSelector').on('change', function() {{
                            var val = $(this).val();
                            var map = {{ '1D': 5, '1M': 6, '2M': 7, '3M': 8, '6M': 9, '12M': 10, 'YTD': 11 }};
                            table.columns([5,6,7,8,9,10,11]).visible(false);
                            table.column(map[val]).visible(true);
                        }});

                        // 4. Hover Chart Logic
                        $('#stocksTable').on('mouseenter', '.chart-trigger', function (e) {{
                            var symbol = $(this).data('symbol');
                            var tf = $(this).data('tf');
                            if (!stockData || !stockData[symbol]) return;

                             // Slice Data based on TF
                            var daysMap = {{ '1D': 2, '1M': 30, '2M': 60, '3M': 90, '6M': 180, '12M': 365, 'YTD': 'YTD' }};
                            var sliceDays = daysMap[tf];
                            var allData = stockData[symbol];
                            var filteredData = [];

                            if (sliceDays === 'YTD') {{
                                var curYear = new Date().getFullYear();
                                filteredData = allData.filter(pt => pt.x.startsWith(curYear));
                            }} else {{
                                var tradingDays = {{ '1D': 2, '1M': 22, '2M': 44, '3M': 66, '6M': 130, '12M': 260 }};
                                var n_items = tradingDays[tf] || 22; 
                                filteredData = allData.slice(-n_items);
                            }}
                            if (filteredData.length < 2) filteredData = allData.slice(-5);

                            var val = $(this).text();
                            var isPos = $(this).hasClass('pct-pos') || val.includes('+');
                            var color = isPos ? '#166534' : '#991b1b';

                            var startP = filteredData[0].y;
                            var endP = filteredData[filteredData.length-1].y;

                            $('#tooltipSymbol').html(symbol + ' <span style="font-size:0.8em; color:#666; font-weight:normal;">(' + tf + ')</span>');
                            $('#tooltipValue').html(
                                '<div style="text-align:right;">' + val + '</div>' + 
                                '<div style="font-size:0.75em; color:#555; font-weight:normal;">$' + startP.toFixed(2) + ' &#8594; $' + endP.toFixed(2) + '</div>'
                            ).css('color', color);
                            
                            $('#chartTooltip').fadeIn(100);

                            var options = {{
                                series: [{{ data: filteredData }}],
                                chart: {{ type: 'area', height: 180, sparkline: {{ enabled: true }} }},
                                stroke: {{ curve: 'smooth', width: 2, colors: [color] }},
                                fill: {{ type: 'gradient', gradient: {{ opacityFrom: 0.4, opacityTo: 0.1, colorStops: [{{ offset: 0, color: color, opacity: 0.2 }}, {{ offset: 100, color: color, opacity: 0 }}] }} }},
                                tooltip: {{ fixed: {{ enabled: false }}, x: {{ show: false }}, y: {{ title: {{ formatter: () => '' }} }} }},
                                colors: [color],
                                yaxis: {{ min: Math.min(...filteredData.map(d=>d.y)) * 0.999 }}
                            }};

                            if (tooltipChart) tooltipChart.destroy();
                            tooltipChart = new ApexCharts(document.querySelector("#chartContainer"), options);
                            tooltipChart.render();

                        }}).on('mousemove', '.chart-trigger', function (e) {{
                            var tooltip = $('#chartTooltip');
                            var tY = e.clientY + 20;
                            var tX = e.clientX + 20;
                            if (tX + 320 > window.innerWidth) tX = e.clientX - 330;
                            if (tY + 240 > window.innerHeight) tY = e.clientY - 250;
                            tooltip.css({{ top: tY, left: tX }});
                        }}).on('mouseleave', '.chart-trigger', function () {{
                            $('#chartTooltip').hide();
                        }});
                    }});
                </script>
            </body>
            </html>
            """
            
            filename = f"{target_industry}_Report.html"
            with open(filename, "w", encoding='utf-8') as f:
                f.write(html_content)
                
            print(f"[SUCCESS] Professional Report saved to: {filename}")

    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    generate_industry_report("Airlines")
