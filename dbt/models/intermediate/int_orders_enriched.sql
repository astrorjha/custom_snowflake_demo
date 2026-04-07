with orders as (

    select * from {{ ref('stg_orders') }}

),

customers as (

    select * from {{ ref('stg_customers') }}

),

regions as (

    select * from {{ ref('regions') }}

)

select
    -- order fields
    o.order_id,
    o.customer_id,
    o.region,
    o.created_at,
    o.created_at::date                  as order_date,
    hour(o.created_at)                  as order_hour,
    o.status,
    o.currency,
    o.promo_code,
    o.total_amount,
    o.has_promo,
    o._loaded_at,
    o._source_file,

    -- customer fields
    c.email                             as customer_email,
    c.tier                              as customer_tier,

    -- region fields
    r.continent,
    r.timezone_offset

from orders o
left join customers c
    on o.customer_id = c.customer_id
left join regions r
    on o.region = r.region
