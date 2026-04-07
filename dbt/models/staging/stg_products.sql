with source as (

    select * from {{ source('raw', 'raw_product') }}
    where product_id is not null

)

select
    product_id,
    name,
    category,
    unit_price::decimal(18, 2)          as unit_price,
    is_active::boolean                  as is_active,
    _loaded_at
from source
