{{
  config(
    materialized='table',
    tags=['marts', 'marketing', 'fact']
  )
}}

WITH customer_segments AS (
    SELECT * FROM {{ ref('int_customer_segments') }}
),

campaign_metrics AS (
    SELECT * FROM {{ ref('int_campaign_metrics') }}
),

customer_campaign_join AS (
    SELECT
        cs.*,
        cm.campaign_type,
        cm.contact_method,
        cm.season,
        cm.day_category,
        cm.total_contacts,
        cm.successful_conversions,
        cm.conversion_rate,
        cm.avg_successful_duration,
        cm.avg_total_duration,
        cm.estimated_roi,
        cm.effectiveness_rating,

        -- KPIs específicos por segmento
        CASE
            WHEN cs.customer_segment = 'young_professional' AND cm.conversion_rate > 10 THEN 'high_potential'
            WHEN cs.customer_segment = 'senior_conservative' AND cm.conversion_rate > 5 THEN 'stable_customer'
            WHEN cs.customer_segment = 'vulnerable' AND cm.conversion_rate < 3 THEN 'avoid_contact'
            ELSE 'standard'
        END as target_segment,

        -- Efectividad por segmento
        CASE
            WHEN cs.contact_priority = 'high' AND cm.conversion_rate >= 10 THEN 'excellent_match'
            WHEN cs.contact_priority = 'medium' AND cm.conversion_rate >= 5 THEN 'good_match'
            WHEN cs.contact_priority = 'low' AND cm.conversion_rate >= 2 THEN 'acceptable_match'
            ELSE 'poor_match'
        END as segment_effectiveness

    FROM customer_segments cs
    CROSS JOIN campaign_metrics cm
)

SELECT
    -- Identificadores
    ROW_NUMBER() OVER (ORDER BY age, job, campaign_type) as kpi_id,

    -- Dimensiones de cliente
    customer_segment,
    age_segment,
    job_segment,
    education_segment,
    marital_segment,
    financial_risk,
    contact_priority,

    -- Dimensiones de campaña
    campaign_type,
    contact_method,
    season,
    day_category,

    -- KPIs principales
    total_contacts,
    successful_conversions,
    conversion_rate,
    estimated_roi,
    effectiveness_rating,

    -- KPIs específicos
    target_segment,
    segment_effectiveness,

    -- Métricas adicionales
    avg_successful_duration,
    avg_total_duration,

    -- Metadatos
    _loaded_at,
    _source

FROM customer_campaign_join 