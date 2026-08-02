with source as (
    select * from {{ source('raw_data', 'psx_prices') }}
),

cleaned as (
    select
        symbol,
        sector,
        price,
        change_pct,
        change_1y_pct,
        pe_ratio,
        dividend_yield,
        volume_avg_30d,
        cast(fetched_at as timestamp) as fetched_at
    from source
)

select * from cleaned