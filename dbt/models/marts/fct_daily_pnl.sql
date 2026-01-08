{{
    config(
        materialized='table',
        schema='ANALYTICS',
        unique_key=['trade_date', 'account_id', 'symbol']
    )
}}

/*
    Fact table: Daily P&L by Account and Symbol
    
    Provides a complete view of daily trading activity and P&L,
    combining realized gains from trades with unrealized gains from positions.
    
    Grain: One row per account/symbol/date
*/

with trade_positions as (
    select * from {{ ref('int_trade_positions') }}
),

final as (
    select
        -- Dimensional keys
        trade_date,
        account_id,
        symbol,
        sector,
        asset_class,
        
        -- Trading activity
        total_trades,
        buy_quantity,
        sell_quantity,
        net_quantity,
        
        -- Notional values
        total_notional,
        
        -- Current position
        current_position,
        position_type,
        current_avg_cost,
        current_market_value,
        cost_basis,
        
        -- P&L breakdown
        realized_pnl,
        unrealized_pnl,
        total_pnl,
        unrealized_return_pct,
        
        -- P&L classification
        case
            when total_pnl > 0 then 'PROFIT'
            when total_pnl < 0 then 'LOSS'
            else 'BREAKEVEN'
        end as pnl_status,
        
        -- Metadata
        current_timestamp() as dbt_updated_at
        
    from trade_positions
)

select * from final
