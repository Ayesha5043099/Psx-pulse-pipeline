with stocks as (
    select * from {{ ref('stg_psx_prices') }}
),

fx as (
    select * from {{ ref('stg_fx_rates') }}
    order by fetched_at desc
    limit 1
),

news as (
    select
        avg(sentiment_score) as avg_market_sentiment,
        count(*) as news_article_count
    from {{ ref('stg_news_articles') }}
),

final as (
    select
        stocks.symbol,
        stocks.sector,
        stocks.price,
        stocks.change_pct,
        stocks.change_1y_pct,
        stocks.pe_ratio,
        stocks.dividend_yield,
        fx.pkr_per_usd,
        news.avg_market_sentiment,
        news.news_article_count,
        stocks.fetched_at
    from stocks
    cross join fx
    cross join news
)

select * from final