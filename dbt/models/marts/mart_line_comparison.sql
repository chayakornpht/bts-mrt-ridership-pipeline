-- mart_line_comparison.sql
-- Analytics-ready table comparing ridership across all lines
-- Includes YoY growth, market share, and ranking

WITH ridership AS (
    SELECT * FROM {{ ref('int_ridership_combined') }}
),

monthly_stats AS (
    SELECT
        DATE_TRUNC('month', period_date) AS report_month,
        line_name,
        operator,
        AVG(daily_avg_passengers) AS avg_daily_passengers,
        SUM(total_passengers) AS total_passengers,
        SUM(revenue_thb) AS total_revenue_thb
    FROM ridership
    GROUP BY 1, 2, 3
),

with_yoy AS (
    SELECT
        report_month,
        line_name,
        operator,
        avg_daily_passengers,
        total_passengers,
        total_revenue_thb,
        
        -- Year-over-Year growth
        LAG(avg_daily_passengers, 12) OVER (
            PARTITION BY line_name 
            ORDER BY report_month
        ) AS avg_daily_passengers_prev_year,
        
        CASE 
            WHEN LAG(avg_daily_passengers, 12) OVER (
                PARTITION BY line_name ORDER BY report_month
            ) > 0 THEN
                ROUND(
                    (avg_daily_passengers - LAG(avg_daily_passengers, 12) OVER (
                        PARTITION BY line_name ORDER BY report_month
                    )) / LAG(avg_daily_passengers, 12) OVER (
                        PARTITION BY line_name ORDER BY report_month
                    ) * 100, 2
                )
        END AS yoy_growth_pct,
        
        -- Month-over-Month growth
        LAG(avg_daily_passengers, 1) OVER (
            PARTITION BY line_name 
            ORDER BY report_month
        ) AS avg_daily_passengers_prev_month,
        
        -- Market share within the month
        ROUND(
            avg_daily_passengers / NULLIF(
                SUM(avg_daily_passengers) OVER (PARTITION BY report_month), 0
            ) * 100, 2
        ) AS market_share_pct,
        
        -- Ranking by ridership
        RANK() OVER (
            PARTITION BY report_month 
            ORDER BY avg_daily_passengers DESC
        ) AS ridership_rank
        
    FROM monthly_stats
)

SELECT
    report_month,
    line_name,
    operator,
    ROUND(avg_daily_passengers) AS avg_daily_passengers,
    total_passengers,
    total_revenue_thb,
    ROUND(avg_daily_passengers_prev_year) AS prev_year_daily_passengers,
    yoy_growth_pct,
    market_share_pct,
    ridership_rank,
    
    -- Recovery indicator (vs pre-COVID baseline if available)
    EXTRACT(YEAR FROM report_month) AS year,
    EXTRACT(MONTH FROM report_month) AS month
    
FROM with_yoy
ORDER BY report_month DESC, ridership_rank
