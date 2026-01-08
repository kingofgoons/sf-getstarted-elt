{{
    config(
        materialized='view',
        schema='STAGING'
    )
}}

/*
    Staging model for position data from the curated layer.
    
    Standardizes column names and adds position classification.
*/

with source as (
    select * from {{ source('curated', 'POSITION_SUMMARY') }}
),

renamed as (
    select
        -- Keys
        account_id,
        symbol,
        
        -- Position details
        quantity,
        avg_cost,
        market_value,
        
        -- P&L
        unrealized_pnl,
        
        -- Position classification
        case
            when quantity > 0 then 'LONG'
            when quantity < 0 then 'SHORT'
            else 'FLAT'
        end as position_type,
        
        -- Cost basis
        abs(quantity) * avg_cost as cost_basis,
        
        -- Return calculation
        case
            when abs(quantity) * avg_cost > 0 
            then (unrealized_pnl / (abs(quantity) * avg_cost)) * 100
            else 0
        end as unrealized_return_pct,
        
        -- Classification
        as_of_date,
        sector,
        asset_class,
        
        -- Metadata
        _updated_at as loaded_at
        
    from source
)

select * from renamed

