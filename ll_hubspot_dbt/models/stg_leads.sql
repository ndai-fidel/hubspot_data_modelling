-- models/stg_leads.sql
{{
  config(
    materialized = 'ephemeral',
  )
}}

WITH base_contacts AS (

    SELECT
        id AS contact_id,
        createdAt AS created_at,
        updatedAt AS updated_at,
        firstname,
        lastname,
        email,
        -- Lead qualification
        hs_lead_status as lead_status,
        qualified_score,
        marketing_organic_lead_status,
        lifecyclestage AS lifecycle_stage,
        -- Engagement metrics
        hs_email_last_open_date AS last_email_open,
        hs_last_sales_activity_date AS last_activity_date,
        CAST(num_notes AS INT64) AS num_notes,
        CAST(hs_email_open AS INT64) AS num_emails_opened,
        CAST(hs_email_click AS INT64) AS num_emails_clicked,
        CAST(num_contacted_notes AS INT64) AS num_contacted_notes,
        -- Marketing attribution
        utm_campaign,
        utm_source,
        utm_medium,
        utm_term,
        utm_content,
        gclid,
        -- Lead owner
        hubspot_owner_id AS contact_owner_id,
        hubspot_team_id AS team_id,
        -- Time metrics
        DATE_DIFF(CURRENT_DATE(), DATE(createdAt), DAY) AS lead_age_in_days,
        
        -- Define traffic variable based on source_traffic
        CASE
            WHEN LOWER(source_traffic) LIKE '%google%' THEN 'Google'
            WHEN LOWER(source_traffic) LIKE '%fb%' THEN 'Facebook'
            WHEN LOWER(source_traffic) LIKE '%facebook%' THEN 'Facebook'
            WHEN LOWER(source_traffic) LIKE '%tiktok%' THEN 'Tik Tok'
            WHEN LOWER(source_traffic) LIKE '%tik tok%' THEN 'Tik Tok'
            WHEN LOWER(source_traffic) LIKE '%youtube%' THEN 'YouTube'
            WHEN LOWER(source_traffic) LIKE '%bing%' THEN 'Bing'
            WHEN LOWER(source_traffic) LIKE '%quora%' THEN 'Quora'
            WHEN LOWER(source_traffic) LIKE '%snapchat%' THEN 'Snapchat'
            WHEN LOWER(source_traffic) LIKE '%instagram%' THEN 'Instagram'
            ELSE source_traffic
        END AS traffic  

    FROM
        {{ ref('clean_ll_hs_contacts') }} 

)

SELECT
    *
FROM
    base_contacts
