{{
    config(
        materialized='table',
        schema='ANALYTICS'
    )
}}

/*
    Dimension table: Instruments (Securities)
    
    Provides reference data for traded instruments including
    sector classification and aggregate trading statistics.
    
    Note: In production, this would typically come from a 
    reference data source. Here we derive it from position data.
*/

with positions as (
    select distinct
        symbol,
        sector,
        asset_class
    from {{ ref('stg_positions') }}
    where symbol is not null
),

trade_stats as (
    select
        symbol,
        count(distinct trade_date) as trading_days,
        count(distinct account_id) as accounts_traded,
        sum(total_trades) as total_trade_count,
        sum(total_notional) as total_notional_volume
    from {{ ref('stg_trades') }}
    group by symbol
),

final as (
    select
        -- Instrument identifier
        p.symbol,
        
        -- Classification
        p.sector,
        p.asset_class,
        
        -- Trading statistics
        coalesce(ts.trading_days, 0) as trading_days,
        coalesce(ts.accounts_traded, 0) as accounts_traded,
        coalesce(ts.total_trade_count, 0) as total_trade_count,
        coalesce(ts.total_notional_volume, 0) as total_notional_volume,
        
        -- Activity classification
        case
            when ts.total_trade_count > 50 then 'HIGH'
            when ts.total_trade_count > 20 then 'MEDIUM'
            when ts.total_trade_count > 0 then 'LOW'
            else 'NONE'
        end as trading_activity_level,
        
        -- Metadata
        current_timestamp() as dbt_updated_at
        
    from positions p
    left join trade_stats ts on p.symbol = ts.symbol
)

select * from final
