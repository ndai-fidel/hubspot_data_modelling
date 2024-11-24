-- models/owner_table.sql
{{
  config(
    materialized = 'ephemeral',
    )
}}

SELECT
    'dummy_owner_id' AS owner_id,
    'dummy_owner_name' AS owner_name,
    'dummy_team_name' AS team_name
