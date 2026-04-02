-- stg_bts_ridership.sql
-- Clean and standardize BTS ridership data from PDF extraction
-- Source: raw.bts_ridership

WITH source AS (
    SELECT * FROM {{ source('raw', 'bts_ridership') }}
),

cleaned AS (
    SELECT
        id,
        fiscal_year,
        quarter,
        
        -- Standardize line names
        CASE 
            WHEN LOWER(line_name) LIKE '%sukhumvit%' THEN 'BTS Sukhumvit Line'
            WHEN LOWER(line_name) LIKE '%silom%' THEN 'BTS Silom Line'
            WHEN LOWER(line_name) LIKE '%gold%' THEN 'BTS Gold Line'
            WHEN LOWER(line_name) LIKE '%total%' THEN 'BTS Total'
            ELSE line_name
        END AS line_name,
        
        -- Convert fiscal year to calendar year period
        -- BTS fiscal year runs April to March
        -- FY2024 = April 2023 - March 2024
        MAKE_DATE(fiscal_year - 1, 4, 1) AS period_start,
        MAKE_DATE(fiscal_year, 3, 31) AS period_end,
        
        CASE WHEN daily_avg_passengers > 0 THEN daily_avg_passengers END AS daily_avg_passengers,
        CASE WHEN total_passengers > 0 THEN total_passengers END AS total_passengers,
        CASE WHEN revenue_thb > 0 THEN revenue_thb END AS revenue_thb,
        
        source_file,
        'BTS' AS data_source,
        extracted_at
        
    FROM source
    WHERE fiscal_year BETWEEN 2019 AND 2027
)

SELECT * FROM cleaned
