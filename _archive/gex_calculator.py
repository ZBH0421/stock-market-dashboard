import yfinance as yf
import pandas as pd
import numpy as np
from scipy.stats import norm
from datetime import datetime, timedelta
import argparse

def black_scholes_gamma(S, K, T, r, sigma):
    """
    Calculate Gamma for a European option using Black-Scholes model.
    Gamma is the same for Calls and Puts.
    """
    if T <= 0 or sigma <= 0:
        return 0.0
    
    try:
        d1 = (np.log(S / K) + (r + 0.5 * sigma ** 2) * T) / (sigma * np.sqrt(T))
        pdf_d1 = norm.pdf(d1)
        gamma = pdf_d1 / (S * sigma * np.sqrt(T))
        return gamma
    except Exception:
        return 0.0

def fetch_option_data(ticker_symbol):
    """
    Fetches option chain for all expirations.
    """
    tk = yf.Ticker(ticker_symbol)
    
    # Get current price
    try:
        # Try fast info first
        current_price = tk.fast_info['last_price']
    except:
        try:
            current_price = tk.history(period="1d")['Close'].iloc[-1]
        except:
            print(f"Could not fetch price for {ticker_symbol}")
            return None, None

    expirations = tk.options
    if not expirations:
        print(f"No expirations found for {ticker_symbol}")
        return None, None

    all_opts = []
    print(f"Fetching data for {len(expirations)} expirations...")
    
    for exp_date in expirations:
        try:
            chain = tk.option_chain(exp_date)
            # Calls
            calls = chain.calls.copy()
            calls['type'] = 'call'
            calls['expirationDate'] = exp_date
            
            # Puts
            puts = chain.puts.copy()
            puts['type'] = 'put'
            puts['expirationDate'] = exp_date
            
            all_opts.append(pd.concat([calls, puts]))
        except Exception as e:
            print(f"Failed to fetch {exp_date}: {e}")
            continue
            
    if not all_opts:
        return None, None
        
    df_opts = pd.concat(all_opts)
    return current_price, df_opts


def calculate_gex(ticker_symbol, risk_free_rate=0.045, top_n=20, plot=False, days_n=None):
    """
    Calculates GEX profile.
    """
    S, df = fetch_option_data(ticker_symbol)
    if S is None or df is None:
        return
    
    # Pre-process
    # T = time to expiry in years
    today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    
    # Helper to parse yfinance date strings if needed, but they are usually 'YYYY-MM-DD'
    df['expirationDate'] = pd.to_datetime(df['expirationDate'])
    df['T'] = (df['expirationDate'] - today).dt.days / 365.0
    
    # Filter out expired or invalid (Allow T=0 for 0DTE)
    df = df[df['T'] >= 0].copy()
    
    # Filter for nearest expiration(s)
    if days_n is not None and days_n > 0 and not df.empty:
        unique_exps = sorted(df['expirationDate'].unique())
        target_exps = unique_exps[:days_n]
        # Format for printing
        target_dates_str = [d.strftime('%Y-%m-%d') for d in target_exps]
        print(f"\n[Mode] Focusing on nearest {len(target_exps)} expirations: {target_dates_str}")
        df = df[df['expirationDate'].isin(target_exps)].copy()
    else:
        print(f"\n[Mode] Aggregating ALL {df['expirationDate'].nunique()} expirations.")
    
    # Handle missing IV
    # yfinance 'impliedVolatility' is a decimal (e.g. 0.25 for 25%)
    df = df[df['impliedVolatility'] > 0].copy()

    print(f"Calculating Gamma for {len(df)} contracts...")
    
    # Vectorized Gamma Calculation
    # Function to safe calc gamma with min T
    def safe_gamma(S, K, T, r, sigma):
        # Use a minimum T of 1/365 (~1 day) for 0DTE to approximate 'during the day' or 'open' gamma.
        # Otherwise T=0 gives Gamma=0 which is wrong (it should be huge).
        eff_T = max(T, 0.0027) # 0.0027 is roughly 1 day (1/365)
        return black_scholes_gamma(S, K, eff_T, r, sigma)

    df['gamma'] = df.apply(
        lambda row: safe_gamma(S, row['strike'], row['T'], risk_free_rate, row['impliedVolatility']), 
        axis=1
    )
    
    # GEX Calculation (Naive)
    df['GEX_raw'] = df['gamma'] * df['openInterest'] * 100
    df['GEX'] = df.apply(lambda row: row['GEX_raw'] if row['type'] == 'call' else -row['GEX_raw'], axis=1)
    
    # Total GEX
    call_gex_total = df[df['type'] == 'call']['GEX'].sum()
    put_gex_total = df[df['type'] == 'put']['GEX'].sum()
    total_gex = df['GEX'].sum()
    
    # Calculate GEX by Strike (Shares)
    gex_by_strike_shares = df.groupby('strike')['GEX'].sum().sort_index()
    
    # Calculate GEX by Strike (Notional $ per 1% move)
    # Formula: GEX_shares * Spot * (Spot * 0.01)
    df['GEX_notional'] = df['GEX'] * S * S * 0.01
    gex_by_strike_notional = df.groupby('strike')['GEX_notional'].sum().sort_index()
    
    print(f"\n--- {ticker_symbol} GEX Analysis ---")
    print(f"Current Price: ${S:.2f}")
    print(f"Call GEX (Dealer Long): {call_gex_total:,.0f} shares (delta per $1 move)")
    print(f"Put GEX (Dealer Short): {put_gex_total:,.0f} shares (delta per $1 move)")
    print(f"Total Net GEX: {total_gex:,.0f} shares (delta per $1 move)")
    
    # Total Notional from shares (should match sum of GEX_notional approx)
    notional_1pct = total_gex * S * S * 0.01
    print(f"Notional GEX per 1% Move: ${notional_1pct:,.0f} ({notional_1pct/1e9:.1f}B)")
    
    # Helper to print tables in Billions
    def print_top_strikes(series, n, label, ascending=False):
        # Convert to Billions for display
        series_b = series / 1e9
        print(f"\nTop {n} {label}:")
        if ascending:
            print(series_b.nsmallest(n).map('{:,.2f}B'.format))
        else:
            print(series_b.nlargest(n).map('{:,.2f}B'.format))

    print_top_strikes(gex_by_strike_notional, top_n, "Positive GEX Strikes ($B - Support/Resist)", ascending=False)
    print_top_strikes(gex_by_strike_notional, top_n, "Negative GEX Strikes ($B - Volatility)", ascending=True)

    # Nearby Strikes Analysis
    print(f"\n--- GEX Nearby Spot (${S:.2f}) ---")
    all_strikes = gex_by_strike_notional.index.values
    idx = (np.abs(all_strikes - S)).argmin()
    start_idx = max(0, idx - 10)
    end_idx = min(len(all_strikes), idx + 11)
    nearby_gex = gex_by_strike_notional.iloc[start_idx:end_idx]
    print(nearby_gex.div(1e9).map('{:,.2f}B'.format))
    
    if plot:
        plot_gex_profile(ticker_symbol, gex_by_strike_notional, S)

    return df

def plot_gex_profile(ticker_symbol, gex_by_strike, spot_price):
    """
    Plots the GEX profile and saves it to a file.
    """
    plt.figure(figsize=(12, 6))
    
    # Color mapping
    colors = ['blue' if x >= 0 else 'red' for x in gex_by_strike.values]
    
    # Plot in Billions
    plt.bar(gex_by_strike.index, gex_by_strike.values / 1e9, color=colors, width=2.0)
    plt.axvline(x=spot_price, color='green', linestyle='--', label=f'Spot Price: {spot_price:.2f}')
    
    plt.title(f"{ticker_symbol} Gamma Exposure Profile")
    plt.xlabel("Strike Price")
    plt.ylabel("Gamma Exposure ($Bn per 1% Move)") # Updated label
    plt.legend()
    plt.grid(True, alpha=0.3)
    
    filename = f"{ticker_symbol}_gex_profile.png"
    plt.savefig(filename)
    print(f"\nChart saved to {filename}")

def check_atm_params(ticker_symbol, risk_free_rate=0.045):
    """
    Prints detailed parameters for ATM options to allow manual verification.
    """
    print(f"\n--- Verifying ATM Contract Data for {ticker_symbol} ---")
    S, df = fetch_option_data(ticker_symbol)
    if S is None or df is None:
        return

    # Find nearest expiration
    today = datetime.now()
    df['expirationDate'] = pd.to_datetime(df['expirationDate'])
    df['T'] = (df['expirationDate'] - today).dt.days / 365.0
    df = df[df['T'] > 0].copy()
    
    if df.empty:
        print("No valid options found.")
        return

    # Filter for nearest expiry (but not today/0 days)
    nearest_exp = df['expirationDate'].min()
    df_near = df[df['expirationDate'] == nearest_exp]
    
    # Find ATM Call and Put (Strike closest to S)
    atm_idx = (np.abs(df_near['strike'] - S)).argmin()
    atm_strike = df_near.iloc[atm_idx]['strike']
    
    # Get Call and Put at this strike
    atm_opts = df_near[df_near['strike'] == atm_strike]
    
    print(f"Spot Price (S): ${S:.2f}")
    print(f"Nearest Expiration: {nearest_exp.date()}")
    print(f"ATM Strike (K): {atm_strike}")
    print(f"Time to Expiry (T): {df_near.iloc[0]['T']:.4f} years")
    print(f"Risk-Free Rate (r): {risk_free_rate}")
    
    for _, row in atm_opts.iterrows():
        print(f"\n[{row['type'].upper()} Contract: {row['contractSymbol']}]")
        print(f"  Strike: {row['strike']}")
        print(f"  Market Price (Last): ${row['lastPrice']}")
        print(f"  Implied Volatility (IV): {row['impliedVolatility']:.4f}")
        print(f"  Open Interest (OI): {row['openInterest']}")
        
        # Calculate Gamma Manually
        my_gamma = black_scholes_gamma(S, row['strike'], row['T'], risk_free_rate, row['impliedVolatility'])
        print(f"  Calculated Gamma: {my_gamma:.6f}")
        
        # Explain GEX Calc
        gex_contrib = my_gamma * row['openInterest'] * 100
        sign = "(+)" if row['type'] == 'call' else "(-)"
        print(f"  GEX Contribution: {gex_contrib:,.2f} (Gamma * OI * 100)")
        print(f"  Assumption: Dealer is {sign} Gamma")
    
    print("\n[How to Verify]")
    print("1. Check 'Spot Price' and 'ATM Strike' against Yahoo Finance.")
    print("2. Check 'Implied Volatility' and 'Open Interest' for this contract on Yahoo Finance.")
    print("3. Use an online Black-Scholes calculator with S, K, T, r, IV to check 'Calculated Gamma'.")
    print("   (Note: Tiny differences may occur due to Time-to-Expiry precision)")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Calculate GEX for a stock")
    parser.add_argument("ticker", nargs='?', default="SPY", type=str, help="Stock Ticker (default: SPY)")
    parser.add_argument("--top", type=int, default=20, help="Number of top strikes to show (default: 20)")
    parser.add_argument("--plot", action="store_true", help="Generate GEX chart")
    parser.add_argument("--check", action="store_true", help="Verify ATM contract details")
    parser.add_argument("--days", type=int, help="Number of upcoming expiration dates to include (e.g. 2 for nearest 2 days)")
    args = parser.parse_args()
    
    if args.check:
        check_atm_params(args.ticker)
    else:
        calculate_gex(args.ticker, top_n=args.top, plot=args.plot, days_n=args.days)
