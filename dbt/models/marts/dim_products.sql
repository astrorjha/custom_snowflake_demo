with source as (

    select * from {{ ref('stg_products') }}
    where is_active = true

)

select
    {{ dbt_utils.generate_surrogate_key(['product_id']) }}  as product_key,
    product_id,
    name,
    category,
    unit_price,
    is_active,
    _loaded_at

from source
