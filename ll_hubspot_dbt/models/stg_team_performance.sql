-- models/stg_team_performance.sql
{{
  config(
    materialized = 'table',
  )
}}

WITH team_data AS (
    SELECT
        sl.created_at,
        
        -- Break down created_at into hour in 24-hour format
        EXTRACT(HOUR FROM sl.created_at) AS hour,  -- 24-hour format
        
        -- Extract the day of the week
        FORMAT_TIMESTAMP('%A', sl.created_at) AS day,  -- Get the day of the week
        
        sl.team_id,
        sl.contact_owner_id,
        sl.traffic, 
        COUNT(DISTINCT sl.contact_id) AS total_leads,
        COUNT(DISTINCT CASE WHEN sl.lead_status = 'Retained' THEN sl.contact_id END) AS leads_converted,
        COUNT(DISTINCT CASE WHEN sl.lead_status = 'Lost' THEN sl.contact_id END) AS leads_lost,
        AVG(DATE_DIFF(CAST(sl.updated_at AS TIMESTAMP), CAST(sl.created_at AS TIMESTAMP), DAY)) AS avg_lead_conversion_time,
        AVG(DATE_DIFF(CAST(sl.last_activity_date AS TIMESTAMP), CAST(sl.created_at AS TIMESTAMP), DAY)) AS avg_lead_response_time
    FROM
        {{ ref('stg_leads') }} AS sl
    LEFT JOIN
        {{ source('hubspot_source', 'll_hs_owners') }} AS o
    ON
        sl.contact_owner_id = o.id
    WHERE
        sl.team_id IS NOT NULL
    GROUP BY
        sl.created_at,
        sl.team_id,
        sl.contact_owner_id,
        sl.traffic
),

calculations AS (
    SELECT
        td.*,
        SAFE_DIVIDE(td.leads_converted, td.total_leads) AS conversion_rate,
        SAFE_DIVIDE(td.leads_lost, td.total_leads) AS loss_rate
    FROM
        team_data AS td
),

owner_details AS (
    SELECT
        o.id,
        o.userId,
        o.teams AS team_name,
        o.firstName AS owner_firstName,  -- Include first name
        o.lastName AS owner_lastName  -- Include last name
    FROM
        {{ source('hubspot_source', 'll_hs_owners') }} AS o
)

SELECT
    c.created_at,
    c.hour,  -- 24-hour format
    c.day,  -- Day of the week
    c.team_id,
    c.contact_owner_id,
    c.traffic,  -- Include traffic
    c.total_leads,
    c.leads_converted,
    c.leads_lost,
    c.conversion_rate,
    c.loss_rate,
    c.avg_lead_conversion_time,
    c.avg_lead_response_time,
    od.team_name,
    od.owner_firstName,  -- Added owner_firstName
    od.owner_lastName  -- Added owner_lastName
FROM
    calculations AS c
LEFT JOIN
    owner_details AS od
ON
    c.contact_owner_id = od.id
