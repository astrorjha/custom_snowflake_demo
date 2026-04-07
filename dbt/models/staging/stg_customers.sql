with source as (

    select * from {{ source('raw', 'raw_customer') }}
    where customer_id is not null

)

select
    customer_id,
    name,
    email,
    lower(region)                       as region,
    lower(tier)                         as tier,
    created_at::timestamp_ntz           as created_at,
    _loaded_at
from source
