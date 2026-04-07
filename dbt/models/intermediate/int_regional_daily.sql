with orders_agg as (

    select
        region,
        order_date,
        count(order_id)                                     as total_orders,
        sum(total_amount)                                   as gross_revenue,
        avg(total_amount)                                   as avg_order_value,
        count_if(status = 'returned')                       as returned_orders,
        count(distinct customer_id)                         as unique_customers
    from {{ ref('int_orders_enriched') }}
    group by region, order_date

),

marketing_agg as (

    select
        region,
        event_date,
        count_if(event_type = 'impression')                 as impressions,
        count_if(event_type = 'click')                      as clicks,
        count_if(event_type = 'purchase')                   as purchases
    from {{ ref('stg_marketing_events') }}
    group by region, event_date

)

select
    o.region,
    o.order_date,
    o.total_orders,
    o.gross_revenue,
    o.avg_order_value,
    o.unique_customers,
    o.returned_orders,
    div0(o.returned_orders, o.total_orders)                 as return_rate,
    coalesce(m.impressions, 0)                              as impressions,
    coalesce(m.clicks, 0)                                   as clicks,
    coalesce(m.purchases, 0)                                as purchases

from orders_agg o
left join marketing_agg m
    on o.region = m.region
    and o.order_date = m.event_date
