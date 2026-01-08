{{
    config(
        materialized='table',
        schema='ANALYTICS'
    )
}}

/*
    Fact table: Account Summary
    
    Aggregates trading activity and P&L at the account level.
    Useful for portfolio-level reporting and account performance analysis.
    
    Grain: One row per account
*/

with daily_pnl as (
    select * from {{ ref('fct_daily_pnl') }}
),

account_metrics as (
    select
        account_id,
        
        -- Trading activity
        count(distinct trade_date) as active_trading_days,
        count(distinct symbol) as unique_symbols_traded,
        sum(total_trades) as total_trade_count,
        
        -- Volume
        sum(total_notional) as total_notional,
        
        -- P&L
        sum(realized_pnl) as total_realized_pnl,
        sum(unrealized_pnl) as total_unrealized_pnl,
        sum(total_pnl) as total_pnl,
        
        -- Position counts
        count(case when position_type = 'LONG' then 1 end) as long_position_count,
        count(case when position_type = 'SHORT' then 1 end) as short_position_count,
        
        -- P&L statistics
        count(case when total_pnl > 0 then 1 end) as profitable_positions,
        count(case when total_pnl < 0 then 1 end) as losing_positions,
        
        -- Best/worst
        max(total_pnl) as best_position_pnl,
        min(total_pnl) as worst_position_pnl
        
    from daily_pnl
    group by account_id
),

final as (
    select
        account_id,
        
        -- Activity metrics
        active_trading_days,
        unique_symbols_traded,
        total_trade_count,
        
        -- Volume metrics
        total_notional,
        round(total_notional / nullif(active_trading_days, 0), 2) as avg_daily_volume,
        
        -- P&L metrics
        total_realized_pnl,
        total_unrealized_pnl,
        total_pnl,
        
        -- Position breakdown
        long_position_count,
        short_position_count,
        long_position_count + short_position_count as total_positions,
        
        -- Win/loss metrics
        profitable_positions,
        losing_positions,
        round(
            profitable_positions::float / nullif(profitable_positions + losing_positions, 0) * 100, 
            2
        ) as win_rate_pct,
        
        -- Extremes
        best_position_pnl,
        worst_position_pnl,
        
        -- Risk metrics
        case
            when total_pnl >= 0 then 'PROFITABLE'
            else 'LOSING'
        end as account_status,
        
        -- Metadata
        current_timestamp() as dbt_updated_at
        
    from account_metrics
)

select * from final
