with source as (

    select * from {{ source('raw', 'raw_order') }}
    where order_id is not null

)

select
    order_id,
    customer_id,
    region,
    created_at::timestamp_ntz           as created_at,
    status,
    currency,
    promo_code,
    total_amount::decimal(18, 2)        as total_amount,
    promo_code is not null              as has_promo,
    _loaded_at,
    _source_file
from source
