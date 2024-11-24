{{
  config(
    materialized = 'ephemeral',
    )
}}

-- Query to select only id and createdAt columns
select
    distinct
    source_traffic
from {{ source('hubspot_source', 'll_hs_contacts') }}
