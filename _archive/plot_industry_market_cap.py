from market_data_db import MarketDataDB
from sqlalchemy import text
import pandas as pd
import matplotlib.pyplot as plt
import os

def plot_industry_market_cap(industry_name: str):
    """
    Generates a pie chart showing market cap distribution for a specific industry.
    Includes the total industry market cap in the title.
    """
    print(f"Generating Market Cap Pie Chart for: {industry_name}")
    
    try:
        db = MarketDataDB()
        
        # 1. Fetch Data
        with db.engine.connect() as conn:
            query = text("""
                SELECT t.ticker, t.market_cap 
                FROM tickers t
                JOIN industries i ON t.industry_id = i.id
                WHERE i.name = :industry AND t.market_cap IS NOT NULL AND t.market_cap > 0
                ORDER BY t.market_cap DESC
            """)
            df = pd.read_sql(query, conn, params={"industry": industry_name})
            
        if df.empty:
            print(f"No market cap data found for industry: {industry_name}")
            return

        # 2. Prepare Data
        total_mcap = df['market_cap'].sum()
        df['percentage'] = df['market_cap'] / total_mcap * 100
        
        # Group small slices into "Others" to avoid clutter if too many
        # Strategy: Keep top 10, group rest
        if len(df) > 10:
            top_10 = df.head(10).copy()
            others_mcap = df.iloc[10:]['market_cap'].sum()
            others_row = pd.DataFrame([{'ticker': 'Others', 'market_cap': others_mcap}])
            df_plot = pd.concat([top_10, others_row], ignore_index=True)
        else:
            df_plot = df.copy()

        # 3. Plotting
        plt.figure(figsize=(10, 8))
        
        # Color palette - customized for professional look
        # using a colormap
        colors = plt.cm.tab20c.colors
        
        # Plot Pie
        wedges, texts, autotexts = plt.pie(
            df_plot['market_cap'], 
            labels=df_plot['ticker'],
            autopct='%1.1f%%',
            startangle=140,
            colors=colors,
            pctdistance=0.85, # Move % inside a bit
            explode=[0.05 if i == 0 else 0 for i in range(len(df_plot))] # Explode the largest slice slightly
        )
        
        # Draw circle for Donut Chart style (optional, but looks modern)
        centre_circle = plt.Circle((0,0),0.70,fc='white')
        fig = plt.gcf()
        fig.gca().add_artist(centre_circle)
        
        # Formatting
        plt.setp(texts, size=9, weight="bold")
        plt.setp(autotexts, size=9, color="white", weight="bold")
        
        # Title with Total Market Cap
        readable_total = f"${total_mcap/1_000_000_000:,.2f} B"
        plt.title(f"{industry_name} Market Cap Distribution\nTotal: {readable_total}", fontsize=14, weight='bold')
        
        plt.tight_layout()
        
        # Save
        output_file = f"{industry_name}_MarketCap_Pie.png"
        plt.savefig(output_file, dpi=300)
        print(f"[SUCCESS] Pie chart saved to: {output_file}")
            
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    plot_industry_market_cap("Airlines")
