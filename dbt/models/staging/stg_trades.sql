{{
    config(
        materialized='view',
        schema='STAGING'
    )
}}

/*
    Staging model for trade metrics from the curated layer.
    
    Standardizes column names and adds computed fields for downstream models.
    
    Source columns: SYMBOL, METRIC_DATE, ACCOUNT_ID, BUY_QUANTITY, 
    SELL_QUANTITY, TOTAL_NOTIONAL, TOTAL_REALIZED_PNL, TRADE_COUNT, _UPDATED_AT
*/

with source as (
    select * from {{ source('curated', 'TRADE_METRICS') }}
),

renamed as (
    select
        -- Keys
        metric_date::date as trade_date,
        account_id,
        symbol,
        
        -- Quantities
        buy_quantity,
        sell_quantity,
        buy_quantity - sell_quantity as net_quantity,
        
        -- Notional
        total_notional,
        
        -- P&L
        total_realized_pnl as realized_pnl,
        
        -- Counts
        trade_count as total_trades,
        
        -- Metadata
        _updated_at as loaded_at
        
    from source
)

select * from renamed
