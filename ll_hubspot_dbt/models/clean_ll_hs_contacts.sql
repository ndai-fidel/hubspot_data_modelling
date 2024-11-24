{{ config(materialized='ephemeral') }}

{% set source_relation = source('hubspot_source', 'll_hs_contacts') %}

with source_data as (
    select *
    from {{ source_relation }}
),

deduped as (
    select
        id,
        createdAt,
        updatedAt,
        {{ dbt_utils.star(from=source_relation, except=["id", "createdAt", "updatedAt"]) }}
    from source_data
    where createdAt is not null
    qualify row_number() over (partition by id order by updatedAt desc) = 1
)

select *
from deduped
order by createdAt desc