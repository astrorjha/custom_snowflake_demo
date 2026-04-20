with orders as (

    select * from {{ ref('int_orders_enriched') }}

)

select
    {{ dbt_utils.generate_surrogate_key(['o.order_id']) }}       as order_key,
    o.order_id,
    o.customer_id,
    {{ dbt_utils.generate_surrogate_key(['o.customer_id']) }}    as customer_key,
    o.region,
    o.order_date,
    o.order_hour,
    o.status,
    o.currency,
    o.promo_code,
    o.has_promo,
    o.total_amount,
    o.continent,
    o.customer_tier                                              as tier

from orders o
