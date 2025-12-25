from market_data_db import MarketDataDB
from sqlalchemy import text
import pandas as pd
import os

def generate_dashboard():
    print("Generating Dashboard (index.html)...")
    
    try:
        db = MarketDataDB()
        
        # Fetch Industry Stats
        with db.engine.connect() as conn:
            # Get Industry ID, Name, Ticker Count, Total Market Cap
            query = text("""
                SELECT i.name, COUNT(t.ticker) as ticker_count, SUM(t.market_cap) as total_cap
                FROM industries i
                JOIN tickers t ON i.id = t.industry_id
                GROUP BY i.name
                ORDER BY total_cap DESC NULLS LAST
            """)
            df = pd.read_sql(query, conn)
            
            if df.empty:
                print("No industry data found.")
                return

            print(f"Found {len(df)} industries.")

            # Generate HTML Cards
            cards_html = ""
            for _, row in df.iterrows():
                ind_name = row['name']
                count = row['ticker_count']
                mcap = row['total_cap']
                
                # Report Link (Dynamic)
                report_link = f"dynamic_report.html?industry={ind_name}"
                
                # Formatter
                def fmt_b(val): return f"${val/1e9:.1f}B" if val and val>0 else "-"
                
                cards_html += f"""
                <div class="col-md-4 col-lg-3 d-flex align-items-stretch industry-card-wrapper" data-name="{ind_name.lower()}">
                    <a href="{report_link}" class="card-link w-100 text-decoration-none">
                        <div class="dashboard-card h-100 d-flex flex-column justify-content-between">
                            <div>
                                <h5 class="card-title text-truncate" title="{ind_name}">{ind_name}</h5>
                                <div class="text-secondary small mb-3">{count} Companies</div>
                            </div>
                            <div class="mt-2">
                                <span class="badge bg-light text-dark border">Cap: {fmt_b(mcap)}</span>
                                <div class="arrow-icon mt-3 text-end">
                                    <svg xmlns="http://www.w3.org/2000/svg" width="20" height="20" fill="currentColor" class="bi bi-arrow-right-circle-fill text-primary" viewBox="0 0 16 16">
                                      <path d="M8 0a8 8 0 1 1 0 16A8 8 0 0 1 8 0zM4.5 7.5a.5.5 0 0 0 0 1h5.793l-2.147 2.146a.5.5 0 0 0 .708.708l3-3a.5.5 0 0 0 0-.708l-3-3a.5.5 0 1 0-.708.708L10.293 7.5H4.5z"/>
                                    </svg>
                                </div>
                            </div>
                        </div>
                    </a>
                </div>
                """

            html_content = f"""
            <!DOCTYPE html>
            <html lang="en">
            <head>
                <meta charset="UTF-8">
                <meta name="viewport" content="width=device-width, initial-scale=1.0">
                <title>Market Overview Dashboard</title>
                <link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap" rel="stylesheet">
                <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0/dist/css/bootstrap.min.css" rel="stylesheet">
                <style>
                    :root {{
                        --bg-body: #f8f9fa;
                        --bg-card: #ffffff;
                        --text-primary: #1e293b;
                    }}
                    body {{ font-family: 'Inter', sans-serif; background-color: var(--bg-body); color: var(--text-primary); padding-bottom: 60px; }}
                    
                    .page-header {{ padding: 3rem 0 2rem; text-align: center; }}
                    .page-title {{ font-weight: 800; color: #0f172a; margin-bottom: 0.5rem; }}
                    
                    .dashboard-card {{
                        background: var(--bg-card);
                        border-radius: 16px;
                        border: 1px solid #e2e8f0;
                        padding: 1.25rem;
                        transition: all 0.2s ease;
                    }}
                    .card-link:hover .dashboard-card {{
                        transform: translateY(-5px);
                        box-shadow: 0 10px 15px -3px rgba(0, 0, 0, 0.1), 0 4px 6px -2px rgba(0, 0, 0, 0.05);
                        border-color: #cbd5e1;
                    }}
                    .card-title {{ font-weight: 700; color: #334155; margin-bottom: 0.25rem; font-size: 1.1rem; }}
                    
                    .search-box {{
                        max-width: 500px;
                        margin: 0 auto 3rem;
                        position: relative;
                    }}
                    .search-input {{
                        border-radius: 50px;
                        padding: 12px 24px;
                        border: 2px solid #e2e8f0;
                        box-shadow: 0 4px 6px -1px rgba(0,0,0,0.02);
                        transition: all 0.2s;
                    }}
                    .search-input:focus {{
                        border-color: #3b82f6;
                        outline: none;
                        box-shadow: 0 0 0 4px rgba(59, 130, 246, 0.1);
                    }}
                </style>
            </head>
            <body>
                <div class="container">
                    <div class="page-header">
                        <h1 class="page-title">Market Dashboard</h1>
                        <p class="text-secondary">Overview of {len(df)} Industries</p>
                    </div>

                    <div class="search-box">
                        <input type="text" id="searchInput" class="form-control search-input" placeholder="Search industries...">
                    </div>

                    <div class="row g-4" id="industryGrid">
                        {cards_html}
                    </div>
                </div>

                <script src="https://code.jquery.com/jquery-3.7.0.min.js"></script>
                <script>
                    $(document).ready(function() {{
                        $('#searchInput').on('keyup', function() {{
                            var value = $(this).val().toLowerCase();
                            $(".industry-card-wrapper").filter(function() {{
                                $(this).toggle($(this).data('name').indexOf(value) > -1)
                            }});
                        }});
                    }});
                </script>
            </body>
            </html>
            """
            
            with open("index.html", "w", encoding='utf-8') as f:
                f.write(html_content)
                
            print(f"[SUCCESS] Dashboard saved to: index.html")

    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    generate_dashboard()
