{{
  config(
    materialized='table',
    tags=['intermediate', 'customer_segments']
  )
}}

WITH customer_data AS (
    SELECT
        *,
        -- Segmentación por edad
        CASE
            WHEN age < 30 THEN 'young'
            WHEN age BETWEEN 30 AND 50 THEN 'middle_aged'
            WHEN age > 50 THEN 'senior'
        END as age_segment,

        -- Segmentación por trabajo
        CASE
            WHEN job IN ('admin.', 'management', 'technician') THEN 'professional'
            WHEN job IN ('services', 'blue-collar', 'entrepreneur') THEN 'service_worker'
            WHEN job IN ('retired', 'student', 'unemployed') THEN 'non_working'
            ELSE 'other'
        END as job_segment,

        -- Segmentación por educación
        CASE
            WHEN education IN ('university.degree', 'professional.course') THEN 'higher_education'
            WHEN education IN ('high.school', 'basic.9y', 'basic.6y') THEN 'secondary_education'
            ELSE 'basic_education'
        END as education_segment,

        -- Segmentación por estado civil
        CASE
            WHEN marital = 'married' THEN 'married'
            WHEN marital IN ('single', 'divorced') THEN 'single'
            ELSE 'other'
        END as marital_segment,

        -- Riesgo financiero
        CASE
            WHEN default_credit = 'yes' OR housing = 'no' OR loan = 'yes' THEN 'high_risk'
            WHEN default_credit = 'no' AND housing = 'yes' AND loan = 'no' THEN 'low_risk'
            ELSE 'medium_risk'
        END as financial_risk

    FROM {{ ref('stg_bank_marketing') }}
    WHERE data_quality_flag = 'valid'
)

SELECT
    *,
    -- Segmento combinado
    CASE
        WHEN age_segment = 'young' AND job_segment = 'professional' THEN 'young_professional'
        WHEN age_segment = 'senior' AND financial_risk = 'low_risk' THEN 'senior_conservative'
        WHEN job_segment = 'non_working' AND financial_risk = 'high_risk' THEN 'vulnerable'
        WHEN education_segment = 'higher_education' AND job_segment = 'professional' THEN 'educated_professional'
        ELSE 'general'
    END as customer_segment,

    -- Prioridad de contacto
    CASE
        WHEN customer_segment IN ('young_professional', 'educated_professional') THEN 'high'
        WHEN customer_segment = 'senior_conservative' THEN 'medium'
        WHEN customer_segment = 'vulnerable' THEN 'low'
        ELSE 'medium'
    END as contact_priority

FROM customer_data 