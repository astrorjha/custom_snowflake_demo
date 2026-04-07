with source as (

    select * from {{ source('raw', 'raw_marketing_event') }}
    where event_id is not null

)

select
    event_id,
    customer_id,
    region,
    lower(event_type)                   as event_type,
    created_at::timestamp_ntz           as created_at,
    created_at::date                    as event_date,
    _loaded_at
from source
