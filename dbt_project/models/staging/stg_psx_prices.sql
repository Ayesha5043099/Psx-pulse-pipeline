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
),

deduplicated as (
    select *,
        row_number() over (
            partition by symbol
            order by fetched_at desc
        ) as row_num
    from cleaned
)

select
    symbol, sector, price, change_pct, change_1y_pct,
    pe_ratio, dividend_yield, volume_avg_30d, fetched_at
from deduplicated
where row_num = 1