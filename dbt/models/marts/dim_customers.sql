with source as (

    select * from {{ ref('stg_customers') }}

),

deduped as (

    select *
    from source
    qualify row_number() over (
        partition by customer_id
        order by _loaded_at desc
    ) = 1

)

select
    {{ dbt_utils.generate_surrogate_key(['customer_id']) }}  as customer_key,
    customer_id,
    name,
    email,
    region,
    tier,
    created_at,
    _loaded_at,
    true                                                      as is_current

from deduped
