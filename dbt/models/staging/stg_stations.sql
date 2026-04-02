-- stg_stations.sql
-- Clean and standardize station master data

WITH source AS (
    SELECT * FROM {{ source('raw', 'stations') }}
),

cleaned AS (
    SELECT
        station_code,
        INITCAP(TRIM(name_en)) AS name_en,
        TRIM(name_th) AS name_th,
        
        -- Standardize line names
        CASE 
            WHEN LOWER(line_name) LIKE '%sukhumvit%' THEN 'BTS Sukhumvit Line'
            WHEN LOWER(line_name) LIKE '%silom%' THEN 'BTS Silom Line'
            WHEN LOWER(line_name) LIKE '%gold%' THEN 'BTS Gold Line'
            WHEN LOWER(line_name) LIKE '%blue%' THEN 'MRT Blue Line'
            WHEN LOWER(line_name) LIKE '%purple%' THEN 'MRT Purple Line'
            WHEN LOWER(line_name) LIKE '%yellow%' THEN 'MRT Yellow Line'
            WHEN LOWER(line_name) LIKE '%pink%' THEN 'MRT Pink Line'
            ELSE line_name
        END AS line_name,
        
        -- Standardize operator names
        CASE 
            WHEN UPPER(operator) IN ('BTSC', 'BTS') THEN 'BTSC'
            WHEN UPPER(operator) = 'BEM' THEN 'BEM'
            WHEN UPPER(operator) LIKE '%SRTET%' THEN 'SRTET'
            ELSE operator
        END AS operator,
        
        latitude,
        longitude,
        opened_date,
        COALESCE(num_exits, 2) AS num_exits,
        COALESCE(is_interchange, FALSE) AS is_interchange,
        interchange_lines
        
    FROM source
    WHERE station_code IS NOT NULL
)

SELECT * FROM cleaned
