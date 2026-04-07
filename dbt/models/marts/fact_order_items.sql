with items as (

    select * from {{ ref('int_order_items_enriched') }}

),

fact_orders as (

    select order_id, order_key from {{ ref('fact_orders') }}

),

dim_products as (

    select product_id, product_key from {{ ref('dim_products') }}

)

select
    {{ dbt_utils.generate_surrogate_key(['i.item_id']) }}   as item_key,
    i.item_id,
    i.order_id,
    fo.order_key,
    i.product_id,
    dp.product_key,
    i.quantity,
    i.unit_price,
    i.amount,
    i.revenue_contribution,
    i.product_name,
    i.category

from items i
left join fact_orders fo
    on i.order_id = fo.order_id
left join dim_products dp
    on i.product_id = dp.product_id
