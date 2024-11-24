-- models/stg_marketing_performance.sql

WITH campaign_data AS (

    SELECT
        utm_campaign,
        utm_source,
        traffic,
        utm_medium,
        COUNT(DISTINCT contact_id) AS num_leads,
        COUNT(DISTINCT CASE WHEN lifecycle_stage = 'customer' THEN contact_id END) AS num_contacts_converted,
        COUNT(DISTINCT CASE WHEN lead_status = 'Lost' THEN contact_id END) AS num_lost_leads,
        SUM(num_emails_opened) AS total_emails_opened,
        SUM(num_emails_clicked) AS total_emails_clicked
    FROM
        {{ ref('stg_leads') }}
    WHERE
        utm_campaign IS NOT NULL
    GROUP BY
        utm_campaign,
        utm_source,
        traffic,
        utm_medium

),

calculations AS (

    SELECT
        *,
        SAFE_DIVIDE(num_contacts_converted, num_leads) AS conversion_rate,
        SAFE_DIVIDE(total_emails_clicked, total_emails_opened) AS email_click_through_rate
    FROM
        campaign_data

)

SELECT
    utm_campaign,
    utm_source,
    traffic,
    utm_medium,
    num_leads,
    num_contacts_converted,
    num_lost_leads,
    conversion_rate,
    email_click_through_rate
FROM
    calculations
