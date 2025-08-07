{{
  config(
    materialized='table',
    tags=['intermediate', 'campaign_metrics']
  )
}}

WITH campaign_data AS (
    SELECT
        *,
        -- Conversión
        CASE WHEN subscription = 'yes' THEN 1 ELSE 0 END as is_conversion,

        -- Duración exitosa (solo para conversiones)
        CASE
            WHEN subscription = 'yes' THEN duration
            ELSE NULL
        END as successful_duration,

        -- Tipo de campaña
        CASE
            WHEN campaign = 1 THEN 'first_contact'
            WHEN campaign BETWEEN 2 AND 3 THEN 'follow_up'
            WHEN campaign BETWEEN 4 AND 10 THEN 'persistent'
            ELSE 'aggressive'
        END as campaign_type,

        -- Método de contacto
        CASE
            WHEN contact = 'cellular' THEN 'mobile'
            WHEN contact = 'telephone' THEN 'phone'
            ELSE 'other'
        END as contact_method,

        -- Estacionalidad
        CASE
            WHEN month IN ('mar', 'apr', 'may') THEN 'spring'
            WHEN month IN ('jun', 'jul', 'aug') THEN 'summer'
            WHEN month IN ('sep', 'oct', 'nov') THEN 'autumn'
            ELSE 'winter'
        END as season,

        -- Día de la semana
        CASE
            WHEN day_of_week IN ('mon', 'tue', 'wed') THEN 'weekday_start'
            WHEN day_of_week IN ('thu', 'fri') THEN 'weekday_end'
            ELSE 'weekend'
        END as day_category

    FROM {{ ref('stg_bank_marketing') }}
    WHERE data_quality_flag = 'valid'
),

campaign_aggregates AS (
    SELECT
        campaign_type,
        contact_method,
        season,
        day_category,
        COUNT(*) as total_contacts,
        SUM(is_conversion) as successful_conversions,
        ROUND(SUM(is_conversion) * 100.0 / COUNT(*), 2) as conversion_rate,
        AVG(successful_duration) as avg_successful_duration,
        AVG(duration) as avg_total_duration,
        COUNT(DISTINCT campaign) as unique_campaigns,
        AVG(campaign) as avg_campaign_number
    FROM campaign_data
    GROUP BY 1, 2, 3, 4
)

SELECT
    *,
    -- ROI estimado (basado en conversión y duración)
    ROUND(
        (successful_conversions * 100) +
        (avg_successful_duration * 0.1) -
        (total_contacts * 5),
        2
    ) as estimated_roi,

    -- Rating de efectividad
    CASE
        WHEN conversion_rate >= 15 AND estimated_roi > 0 THEN 'excellent'
        WHEN conversion_rate >= 10 AND estimated_roi > -100 THEN 'good'
        WHEN conversion_rate >= 5 THEN 'fair'
        ELSE 'poor'
    END as effectiveness_rating

FROM campaign_aggregates 