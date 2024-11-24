-- models/query_id_createdat.sql

{{
  config(
    materialized = 'ephemeral',
    )
}}

-- Query to select only id and createdAt columns
select
    distinct
    date(createdAt) as date
from {{ source('hubspot_source', 'll_hs_contacts') }}
order by date desc