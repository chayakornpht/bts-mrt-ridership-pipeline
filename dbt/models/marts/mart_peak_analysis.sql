-- mart_peak_analysis.sql
-- Analyze peak travel times and station congestion patterns

WITH travel AS (
    SELECT
        tt.from_station_code,
        tt.to_station_code,
        tt.time_slot,
        tt.duration_seconds,
        tt.distance_meters,
        tt.captured_date,
        
        fs.name_en AS from_station_name,
        fs.line_name AS from_line,
        ts.name_en AS to_station_name,
        ts.line_name AS to_line,
        
        d.day_of_week,
        d.day_name,
        d.is_weekend,
        d.month,
        d.year
        
    FROM raw.travel_times tt
    LEFT JOIN raw.stations fs ON tt.from_station_code = fs.station_code
    LEFT JOIN raw.stations ts ON tt.to_station_code = ts.station_code
    LEFT JOIN marts.dim_date d ON d.full_date = tt.captured_date
),

route_stats AS (
    SELECT
        from_station_code,
        to_station_code,
        from_station_name,
        to_station_name,
        from_line,
        to_line,
        time_slot,
        is_weekend,
        
        -- Average travel time
        AVG(duration_seconds) AS avg_duration_seconds,
        ROUND(AVG(duration_seconds) / 60.0, 1) AS avg_duration_minutes,
        
        -- Travel time variability (higher = less predictable)
        STDDEV(duration_seconds) AS duration_stddev,
        
        -- Min and max observed
        MIN(duration_seconds) AS min_duration_seconds,
        MAX(duration_seconds) AS max_duration_seconds,
        
        -- Distance
        AVG(distance_meters) AS avg_distance_meters,
        ROUND(AVG(distance_meters) / 1000.0, 2) AS avg_distance_km,
        
        -- Average speed (km/h)
        CASE 
            WHEN AVG(duration_seconds) > 0 THEN
                ROUND(
                    (AVG(distance_meters) / 1000.0) / (AVG(duration_seconds) / 3600.0), 1
                )
        END AS avg_speed_kmh,
        
        COUNT(*) AS sample_count
        
    FROM travel
    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8
),

ranked AS (
    SELECT
        *,
        
        -- Rank slowest routes per time slot
        RANK() OVER (
            PARTITION BY time_slot, is_weekend
            ORDER BY avg_duration_minutes DESC
        ) AS slowest_rank,
        
        -- Rank most variable routes (least predictable)
        RANK() OVER (
            PARTITION BY time_slot, is_weekend
            ORDER BY duration_stddev DESC
        ) AS most_variable_rank,
        
        -- Rush hour penalty: how much slower vs midday
        avg_duration_minutes - FIRST_VALUE(avg_duration_minutes) OVER (
            PARTITION BY from_station_code, to_station_code, is_weekend
            ORDER BY CASE time_slot WHEN 'midday' THEN 0 ELSE 1 END
        ) AS rush_hour_penalty_minutes
        
    FROM route_stats
    WHERE sample_count >= 3  -- Only include routes with enough data
)

SELECT * FROM ranked
ORDER BY time_slot, is_weekend, avg_duration_minutes DESC
