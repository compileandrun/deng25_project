
{{ config(materialized='table',
            partition_by={
                'field':'trade_timestamp',
                'data_type':'timestamp',
                'granularity':'hour'
            }) }}


select
    a as trade_id,
    p as price,
    q as quantity,
    f as first_trade_id,
    l as last_trade_id,
    TIMESTAMP_MILLIS(T) as trade_timestamp,
    CAST(date_trunc(TIMESTAMP_MILLIS(T),day) as DATE) as trade_date,
    'BTCUSDT' as exchange_info,
    m as is_buyer_maker

from {{source('elegant','binance_ext')}}
QUALIFY row_number() OVER (PARTITION BY t) = 1

/*
    Uncomment the line below to remove records with null `id` values
*/

-- where id is not null
