{{config(materialized='view'
    )
    }}

select 
    datetime_trunc(trade_timestamp,hour) as trade_day_hour,
    exchange_info,
    sum(quantity) as total_volume,
    avg(price) as average_price 
FROM {{ref('binance')}}
group by 1,2
order by 1 DESC