-- tests/assert_ridership_within_range.sql
-- Custom test: daily ridership should be between 0 and 2 million
-- (Bangkok total system capacity ~2M/day)

SELECT
    ridership_id,
    line_name,
    period_date,
    daily_avg_passengers
FROM {{ ref('int_ridership_combined') }}
WHERE daily_avg_passengers < 0
   OR daily_avg_passengers > 2000000
