with source as (

    select * from {{ source('raw', 'raw_order_item') }}
    where item_id is not null
      and order_id is not null

)

select
    item_id,
    order_id,
    product_id,
    quantity::int                       as quantity,
    unit_price::decimal(18, 2)          as unit_price,
    amount::decimal(18, 2)              as amount,
    _loaded_at
from source
