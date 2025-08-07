{{
  config(
    materialized='table',
    tags=['marts', 'marketing', 'dimension']
  )
}}

WITH customer_segments AS (
    SELECT * FROM {{ ref('int_customer_segments') }}
),

segment_aggregates AS (
    SELECT
        customer_segment,
        age_segment,
        job_segment,
        education_segment,
        marital_segment,
        financial_risk,
        contact_priority,

        -- Métricas agregadas por segmento
        COUNT(*) as segment_size,
        AVG(age) as avg_age,
        COUNT(CASE WHEN subscription = 'yes' THEN 1 END) as conversions_in_segment,
        ROUND(
            COUNT(CASE WHEN subscription = 'yes' THEN 1 END) * 100.0 / COUNT(*),
            2
        ) as segment_conversion_rate,

        -- Distribución por características
        COUNT(CASE WHEN job_segment = 'professional' THEN 1 END) as professional_count,
        COUNT(CASE WHEN education_segment = 'higher_education' THEN 1 END) as higher_education_count,
        COUNT(CASE WHEN financial_risk = 'low_risk' THEN 1 END) as low_risk_count,

        -- Métricas de contacto
        AVG(duration) as avg_contact_duration,
        COUNT(CASE WHEN contact = 'cellular' THEN 1 END) as mobile_contacts,
        COUNT(CASE WHEN contact = 'telephone' THEN 1 END) as phone_contacts

    FROM customer_segments
    GROUP BY 1, 2, 3, 4, 5, 6, 7
)

SELECT
    -- Identificadores
    ROW_NUMBER() OVER (ORDER BY customer_segment) as segment_id,

    -- Dimensiones del segmento
    customer_segment,
    age_segment,
    job_segment,
    education_segment,
    marital_segment,
    financial_risk,
    contact_priority,

    -- Métricas de tamaño
    segment_size,
    avg_age,

    -- Métricas de conversión
    conversions_in_segment,
    segment_conversion_rate,

    -- Distribución de características
    professional_count,
    higher_education_count,
    low_risk_count,

    -- Métricas de contacto
    avg_contact_duration,
    mobile_contacts,
    phone_contacts,

    -- Clasificación del segmento
    CASE
        WHEN segment_conversion_rate >= 15 AND segment_size >= 100 THEN 'premium'
        WHEN segment_conversion_rate >= 10 AND segment_size >= 50 THEN 'high_value'
        WHEN segment_conversion_rate >= 5 THEN 'medium_value'
        ELSE 'low_value'
    END as segment_classification,

    -- Score de rentabilidad
    ROUND(
        (segment_conversion_rate * 0.4) +
        (segment_size * 0.001) +
        (avg_contact_duration * 0.01) +
        (professional_count * 0.1),
        2
    ) as profitability_score,

    -- Recomendación de estrategia
    CASE
        WHEN segment_classification = 'premium' THEN 'increase_investment'
        WHEN segment_classification = 'high_value' THEN 'maintain_focus'
        WHEN segment_classification = 'medium_value' THEN 'optimize_approach'
        ELSE 'reduce_effort'
    END as strategy_recommendation,

    -- Metadatos
    CURRENT_TIMESTAMP() as _loaded_at,
    'customer_segments_dimension' as _source

FROM segment_aggregates 