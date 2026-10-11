import sqlite3
import pandas as pd

DB_PATH = "system.db"

def run_cross_validation():
    # 1. Connect to the database
    try:
        conn = sqlite3.connect(DB_PATH)
    except Exception as e:
        print(f"Failed to connect to database: {e}")
        return

    print("==================================================")
    print(" 1. PORTFOLIO POSITIONS OVERVIEW")
    print("==================================================")
    
    # Extract the high-level positions
    positions_query = """
        SELECT strategy_id, symbol, status, quantity, 
               average_entry_price, realized_pnl, opened_at, updated_at
        FROM positions
        ORDER BY opened_at ASC
    """
    df_positions = pd.read_sql_query(positions_query, conn)
    
    if df_positions.empty:
        print("No positions found in the database.")
        return
        
    print(df_positions.to_string(index=False))
    
    print("\n==================================================")
    print(" 2. EXECUTION RECONCILIATION (MATH CHECK)")
    print("==================================================")

    # 2. Extract discrete fills and aggregate them by order
    # We join Fills to Orders to determine side (BUY/SELL)
    fills_query = """
        SELECT 
            o.strategy_id,
            o.symbol,
            o.side,
            SUM(f.quantity) as total_filled,
            SUM(f.price * f.quantity) / SUM(f.quantity) as vw_avg_price,
            SUM(f.fee) as total_fees,
            f.fee_asset
        FROM fills f
        JOIN orders o ON f.client_order_id = o.client_order_id
        GROUP BY o.client_order_id, o.side
        ORDER BY f.timestamp ASC
    """
    df_fills = pd.read_sql_query(fills_query, conn)

    # Variables to track a running manual calculation
    running_pnl = 0.0
    total_fees = 0.0
    
    # We simulate a FIFO matching engine to calculate gross PnL
    # For a simple system where 1 Buy is matched by 1 Sell, we can pair them up.
    buys = df_fills[df_fills['side'] == 'BUY'].reset_index()
    sells = df_fills[df_fills['side'] == 'SELL'].reset_index()

    print(f"{'Symbol':<10} | {'Qty':<8} | {'Entry Avg':<10} | {'Exit Avg':<10} | {'Gross PnL':<10} | {'Fees':<8} | {'Net PnL'}")
    print("-" * 80)

    for i in range(min(len(buys), len(sells))):
        print(f"{'Symbol':<10} | {'Qty':<8} | {'Entry Avg':<10} | {'Exit Avg':<10} | {'Gross PnL':<10} | {'Fees (USDT)':<12} | {'Net PnL'}")
    print("-" * 84)

    for i in range(min(len(buys), len(sells))):
        buy = buys.iloc[i]
        sell = sells.iloc[i]
        
        symbol = buy['symbol']
        qty = min(buy['total_filled'], sell['total_filled'])
        
        entry_price = buy['vw_avg_price']
        exit_price = sell['vw_avg_price']
        
        # Calculate gross PnL (Long position: Exit - Entry)
        gross_pnl = (exit_price - entry_price) * qty
        
        # FIX: Normalize fees to Quote Asset (USDT)
        buy_fee_usdt = buy['total_fees'] * entry_price if buy['fee_asset'] != 'USDT' else buy['total_fees']
        sell_fee_usdt = sell['total_fees'] * exit_price if sell['fee_asset'] != 'USDT' else sell['total_fees']
        
        fees_incurred = buy_fee_usdt + sell_fee_usdt
        net_pnl = gross_pnl - fees_incurred
        
        running_pnl += net_pnl
        total_fees += fees_incurred
        
        print(f"{symbol:<10} | {qty:<8.4f} | {entry_price:<10.2f} | {exit_price:<10.2f} | {gross_pnl:<10.4f} | {fees_incurred:<12.4f} | {net_pnl:<8.4f}")

    print("-" * 84)
    
    # 3. Compare the manual aggregate against the Portfolio Manager's aggregate
    db_total_pnl = df_positions['realized_pnl'].sum()
    
    print("\n==================================================")
    print(" 3. CROSS-VALIDATION VERDICT")
    print("==================================================")
    print(f"Total Net PnL (Calculated from Fills): ${running_pnl:.4f}")
    print(f"Total Net PnL (Reported by Portfolio): ${db_total_pnl:.4f}")
    
    difference = abs(running_pnl - db_total_pnl)
    if difference < 0.01:
        print("\n✅ VALIDATION PASSED: The Portfolio Manager's state perfectly matches the low-level Fills ledger.")
    else:
        print(f"\n❌ VALIDATION FAILED: State mismatch of ${difference:.4f} detected between ledgers.")

    conn.close()

if __name__ == "__main__":
    run_cross_validation()