with source as (
    select * from {{ source('raw_data', 'fx_rates') }}
),

cleaned as (
    select
        base_currency,
        target_currency,
        rate as pkr_per_usd,
        cast(fetched_at as timestamp) as fetched_at
    from source
)

select * from cleaned