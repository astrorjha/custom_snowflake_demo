with orders as (

    select * from {{ ref('int_orders_enriched') }}

),

dim_customers as (

    select customer_id, customer_key from {{ ref('dim_customers') }}

)

select
    {{ dbt_utils.generate_surrogate_key(['o.order_id']) }}  as order_key,
    o.order_id,
    o.customer_id,
    c.customer_key,
    o.region,
    o.order_date,
    o.order_hour,
    o.status,
    o.currency,
    o.promo_code,
    o.has_promo,
    o.total_amount,
    o.continent,
    o.customer_tier                                          as tier

from orders o
left join dim_customers c
    on o.customer_id = c.customer_id
