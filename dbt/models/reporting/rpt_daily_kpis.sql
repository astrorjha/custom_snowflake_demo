with regional_daily as (

    select * from {{ ref('int_regional_daily') }}

),

-- Aggregate fact_orders to region + date to get continent and
-- order-level derived metrics not in int_regional_daily
orders_agg as (

    select
        region,
        order_date,
        -- continent is stable per region; pick any non-null value
        max(continent)                                      as continent,
        count_if(tier = 'gold')                             as gold_customer_orders,
        div0(
            count_if(has_promo),
            count(order_id)
        )                                                   as promo_order_pct
    from {{ ref('fact_orders') }}
    group by region, order_date

),

joined as (

    select
        rd.order_date                                       as report_date,
        rd.region,
        oa.continent,

        -- volume & revenue
        rd.total_orders,
        rd.gross_revenue,
        rd.avg_order_value,
        rd.unique_customers,

        -- returns
        rd.returned_orders,
        rd.return_rate,

        -- marketing funnel
        rd.impressions,
        rd.clicks,
        rd.purchases,
        div0(rd.clicks, nullif(rd.impressions, 0))          as ctr,
        div0(rd.purchases, nullif(rd.clicks, 0))            as conversion_rate,
        div0(rd.gross_revenue, nullif(rd.impressions, 0))   as revenue_per_impression,

        -- order mix
        oa.gold_customer_orders,
        oa.promo_order_pct

    from regional_daily rd
    left join orders_agg oa
        on rd.region    = oa.region
        and rd.order_date = oa.order_date

),

with_rolling as (

    select
        *,
        avg(avg_order_value) over (
            partition by region
            order by report_date
            rows between 6 preceding and current row
        )                                                   as avg_order_value_7d_avg,

        avg(gross_revenue) over (
            partition by region
            order by report_date
            rows between 6 preceding and current row
        )                                                   as gross_revenue_7d_avg

    from joined

)

select
    *,
    current_timestamp()                                     as _dbt_updated_at
from with_rolling
