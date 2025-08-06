-- Modelo de staging para probar dbt
SELECT 
    age,
    job,
    marital,
    education,
    y as subscription
FROM {{ source('bank_marketing', 'bank_marketing') }}
WHERE age >= 18 