with source as (
    select * from {{ source('raw_data', 'news_articles') }}
),

cleaned as (
    select
        source as news_source,
        title,
        link,
        sentiment_score,
        cast(fetched_at as timestamp) as fetched_at
    from source
)

select * from cleaned