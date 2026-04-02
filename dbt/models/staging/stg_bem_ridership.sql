-- stg_bem_ridership.sql
-- Clean and standardize BEM (MRT) ridership data
-- Source: raw.bem_ridership

WITH source AS (
    SELECT * FROM {{ source('raw', 'bem_ridership') }}
),

cleaned AS (
    SELECT
        id,
        report_month,
        
        -- Standardize line names
        CASE 
            WHEN LOWER(line_name) LIKE '%blue%' THEN 'MRT Blue Line'
            WHEN LOWER(line_name) LIKE '%purple%' THEN 'MRT Purple Line'
            ELSE line_name
        END AS line_name,
        
        -- Ensure positive numbers
        CASE WHEN daily_avg_passengers > 0 THEN daily_avg_passengers END AS daily_avg_passengers,
        CASE WHEN monthly_total_passengers > 0 THEN monthly_total_passengers END AS monthly_total_passengers,
        CASE WHEN revenue_thb > 0 THEN revenue_thb END AS revenue_thb,
        
        'BEM' AS data_source,
        extracted_at
        
    FROM source
    WHERE daily_avg_passengers IS NOT NULL
       OR monthly_total_passengers IS NOT NULL
)

SELECT * FROM cleaned
