with order_items as (

    select * from {{ ref('stg_order_items') }}

),

products as (

    select * from {{ ref('stg_products') }}

)

select
    -- item fields
    oi.item_id,
    oi.order_id,
    oi.product_id,
    oi.quantity,
    oi.unit_price,
    oi.amount,
    oi._loaded_at,

    -- product fields
    p.name                              as product_name,
    p.category,

    -- derived
    oi.amount / nullif(
        sum(oi.amount) over (partition by oi.order_id),
        0
    )                                   as revenue_contribution

from order_items oi
left join products p
    on oi.product_id = p.product_id
