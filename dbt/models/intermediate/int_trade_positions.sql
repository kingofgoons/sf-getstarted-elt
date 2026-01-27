{{
    config(
        materialized='ephemeral'
    )
}}

/*
    Intermediate model joining trades with positions.
    
    Combines daily trade metrics with current position state
    for comprehensive account/symbol view.
*/

with trades as (
    select * from {{ ref('stg_trades') }}
),

positions as (
    select * from {{ ref('stg_positions') }}
),

joined as (
    select
        -- Keys
        t.trade_date,
        t.account_id,
        t.symbol,
        
        -- Trade metrics
        t.total_trades,
        t.buy_quantity,
        t.sell_quantity,
        t.net_quantity,
        t.total_notional,
        t.realized_pnl,
        
        -- Position context
        p.quantity as current_position,
        p.position_type,
        p.avg_cost as current_avg_cost,
        p.market_value as current_market_value,
        p.unrealized_pnl,
        p.unrealized_return_pct,
        p.cost_basis,
        
        -- Classification
        p.sector,
        p.asset_class,
        
        -- Total P&L
        t.realized_pnl + coalesce(p.unrealized_pnl, 0) as total_pnl
        
    from trades t
    left join positions p
        on t.account_id = p.account_id
        and t.symbol = p.symbol
)

select * from joined
