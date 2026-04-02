-- int_ridership_combined.sql
-- Combine BEM (MRT) and BTS ridership into a single unified view

WITH bem AS (
    SELECT
        report_month AS period_date,
        line_name,
        daily_avg_passengers,
        monthly_total_passengers AS total_passengers,
        revenue_thb,
        data_source,
        'monthly' AS grain
    FROM {{ ref('stg_bem_ridership') }}
),

bts AS (
    SELECT
        period_start AS period_date,
        line_name,
        daily_avg_passengers,
        total_passengers,
        revenue_thb,
        data_source,
        CASE 
            WHEN quarter IS NOT NULL THEN 'quarterly'
            ELSE 'annual'
        END AS grain
    FROM {{ ref('stg_bts_ridership') }}
    WHERE line_name != 'BTS Total'  -- Exclude totals to avoid double counting
),

combined AS (
    SELECT * FROM bem
    UNION ALL
    SELECT * FROM bts
)

SELECT
    ROW_NUMBER() OVER (ORDER BY period_date, line_name) AS ridership_id,
    period_date,
    line_name,
    
    -- Determine operator from line name
    CASE 
        WHEN line_name LIKE 'BTS%' THEN 'BTSC'
        WHEN line_name LIKE 'MRT%' THEN 'BEM'
        ELSE 'Other'
    END AS operator,
    
    daily_avg_passengers,
    total_passengers,
    revenue_thb,
    data_source,
    grain
    
FROM combined
ORDER BY period_date, line_name
