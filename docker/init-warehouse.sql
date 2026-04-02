-- =============================================
-- BTS/MRT Ridership Data Warehouse
-- Star Schema Design
-- =============================================

-- Create schemas
CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS marts;

-- Create metabase database for Metabase metadata
CREATE DATABASE metabase OWNER warehouse;

-- =============================================
-- RAW LAYER - data as-is from sources
-- =============================================

CREATE TABLE raw.bem_ridership (
    id SERIAL PRIMARY KEY,
    report_month DATE NOT NULL,
    line_name VARCHAR(50) NOT NULL,
    daily_avg_passengers NUMERIC,
    monthly_total_passengers NUMERIC,
    revenue_thb NUMERIC,
    extracted_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE raw.bts_ridership (
    id SERIAL PRIMARY KEY,
    fiscal_year INT NOT NULL,
    quarter VARCHAR(10),
    line_name VARCHAR(50) NOT NULL,
    daily_avg_passengers NUMERIC,
    total_passengers NUMERIC,
    revenue_thb NUMERIC,
    source_file VARCHAR(255),
    extracted_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE raw.stations (
    id SERIAL PRIMARY KEY,
    station_code VARCHAR(20) UNIQUE NOT NULL,
    name_en VARCHAR(100) NOT NULL,
    name_th VARCHAR(100),
    line_name VARCHAR(50) NOT NULL,
    operator VARCHAR(50) NOT NULL,
    latitude NUMERIC(10, 7),
    longitude NUMERIC(10, 7),
    opened_date DATE,
    num_exits INT,
    is_interchange BOOLEAN DEFAULT FALSE,
    interchange_lines TEXT[],
    loaded_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE raw.travel_times (
    id SERIAL PRIMARY KEY,
    from_station_code VARCHAR(20) NOT NULL,
    to_station_code VARCHAR(20) NOT NULL,
    time_slot VARCHAR(20) NOT NULL,
    duration_seconds INT,
    distance_meters INT,
    captured_date DATE NOT NULL,
    captured_at TIMESTAMP DEFAULT NOW()
);

-- =============================================
-- DIMENSION TABLES
-- =============================================

CREATE TABLE marts.dim_date (
    date_id INT PRIMARY KEY,
    full_date DATE UNIQUE NOT NULL,
    day_of_week INT NOT NULL,
    day_name VARCHAR(20) NOT NULL,
    day_name_th VARCHAR(20),
    is_weekend BOOLEAN NOT NULL,
    is_thai_holiday BOOLEAN DEFAULT FALSE,
    holiday_name VARCHAR(100),
    week_of_year INT NOT NULL,
    month INT NOT NULL,
    month_name VARCHAR(20) NOT NULL,
    quarter INT NOT NULL,
    year INT NOT NULL,
    fiscal_year INT NOT NULL
);

CREATE TABLE marts.dim_station (
    station_key SERIAL PRIMARY KEY,
    station_code VARCHAR(20) UNIQUE NOT NULL,
    name_en VARCHAR(100) NOT NULL,
    name_th VARCHAR(100),
    line_name VARCHAR(50) NOT NULL,
    operator VARCHAR(50) NOT NULL,
    latitude NUMERIC(10, 7),
    longitude NUMERIC(10, 7),
    opened_date DATE,
    num_exits INT,
    is_interchange BOOLEAN DEFAULT FALSE,
    interchange_lines TEXT[],
    zone VARCHAR(50),
    effective_from DATE DEFAULT CURRENT_DATE,
    effective_to DATE DEFAULT '9999-12-31'
);

CREATE TABLE marts.dim_line (
    line_key SERIAL PRIMARY KEY,
    line_code VARCHAR(20) UNIQUE NOT NULL,
    line_name VARCHAR(50) NOT NULL,
    operator VARCHAR(50) NOT NULL,
    color VARCHAR(20),
    total_stations INT,
    total_length_km NUMERIC(6, 2),
    opened_date DATE,
    fare_min_thb NUMERIC(6, 2),
    fare_max_thb NUMERIC(6, 2)
);

-- =============================================
-- FACT TABLES
-- =============================================

CREATE TABLE marts.fact_ridership (
    ridership_id SERIAL PRIMARY KEY,
    date_id INT REFERENCES marts.dim_date(date_id),
    station_key INT REFERENCES marts.dim_station(station_key),
    line_key INT REFERENCES marts.dim_line(line_key),
    daily_passengers NUMERIC,
    monthly_passengers NUMERIC,
    revenue_thb NUMERIC,
    data_source VARCHAR(20) NOT NULL,
    grain VARCHAR(20) NOT NULL DEFAULT 'monthly'
);

CREATE TABLE marts.fact_travel_time (
    travel_time_id SERIAL PRIMARY KEY,
    date_id INT REFERENCES marts.dim_date(date_id),
    from_station_key INT REFERENCES marts.dim_station(station_key),
    to_station_key INT REFERENCES marts.dim_station(station_key),
    time_slot VARCHAR(20) NOT NULL,
    duration_seconds INT,
    distance_meters INT
);

-- =============================================
-- INDEXES for query performance
-- =============================================

CREATE INDEX idx_fact_ridership_date ON marts.fact_ridership(date_id);
CREATE INDEX idx_fact_ridership_station ON marts.fact_ridership(station_key);
CREATE INDEX idx_fact_ridership_line ON marts.fact_ridership(line_key);
CREATE INDEX idx_fact_travel_time_date ON marts.fact_travel_time(date_id);
CREATE INDEX idx_fact_travel_time_from ON marts.fact_travel_time(from_station_key);
CREATE INDEX idx_fact_travel_time_to ON marts.fact_travel_time(to_station_key);
CREATE INDEX idx_raw_bem_month ON raw.bem_ridership(report_month);
CREATE INDEX idx_raw_bts_year ON raw.bts_ridership(fiscal_year);

-- =============================================
-- Populate dim_date (2019-2027)
-- =============================================

INSERT INTO marts.dim_date (date_id, full_date, day_of_week, day_name, day_name_th, is_weekend, week_of_year, month, month_name, quarter, year, fiscal_year)
SELECT
    TO_CHAR(d, 'YYYYMMDD')::INT AS date_id,
    d AS full_date,
    EXTRACT(DOW FROM d)::INT AS day_of_week,
    TO_CHAR(d, 'Day') AS day_name,
    CASE EXTRACT(DOW FROM d)::INT
        WHEN 0 THEN 'อาทิตย์' WHEN 1 THEN 'จันทร์' WHEN 2 THEN 'อังคาร'
        WHEN 3 THEN 'พุธ' WHEN 4 THEN 'พฤหัสบดี' WHEN 5 THEN 'ศุกร์' WHEN 6 THEN 'เสาร์'
    END AS day_name_th,
    EXTRACT(DOW FROM d)::INT IN (0, 6) AS is_weekend,
    EXTRACT(WEEK FROM d)::INT AS week_of_year,
    EXTRACT(MONTH FROM d)::INT AS month,
    TO_CHAR(d, 'Month') AS month_name,
    EXTRACT(QUARTER FROM d)::INT AS quarter,
    EXTRACT(YEAR FROM d)::INT AS year,
    CASE WHEN EXTRACT(MONTH FROM d) >= 4 THEN EXTRACT(YEAR FROM d)::INT + 1
         ELSE EXTRACT(YEAR FROM d)::INT
    END AS fiscal_year
FROM generate_series('2019-01-01'::DATE, '2027-12-31'::DATE, '1 day'::INTERVAL) AS d;

-- =============================================
-- Populate dim_line (current lines)
-- =============================================

INSERT INTO marts.dim_line (line_code, line_name, operator, color, total_stations, total_length_km, opened_date, fare_min_thb, fare_max_thb) VALUES
('BTS_SUKHUMVIT', 'BTS Sukhumvit Line', 'BTSC', 'Light Green', 35, 36.25, '1999-12-05', 17, 62),
('BTS_SILOM', 'BTS Silom Line', 'BTSC', 'Dark Green', 13, 14.3, '1999-12-05', 17, 47),
('BTS_GOLD', 'BTS Gold Line', 'BTSC', 'Gold', 4, 2.7, '2020-12-16', 0, 0),
('MRT_BLUE', 'MRT Blue Line', 'BEM', 'Blue', 38, 47.0, '2004-07-03', 17, 43),
('MRT_PURPLE', 'MRT Purple Line', 'BEM', 'Purple', 16, 23.0, '2016-08-06', 15, 42),
('MRT_YELLOW', 'MRT Yellow Line', 'Northern Bangkok Monorail', 'Yellow', 23, 30.4, '2023-07-03', 15, 45),
('MRT_PINK', 'MRT Pink Line', 'Northern Bangkok Monorail', 'Pink', 30, 34.5, '2024-02-01', 15, 45),
('ARL', 'Airport Rail Link', 'SRTET', 'Red/Blue', 8, 28.6, '2010-08-23', 15, 45),
('SRT_RED', 'SRT Red Line', 'SRTET', 'Red', 13, 41.0, '2021-11-29', 12, 42);

-- =============================================
-- Populate raw.stations (major stations sample)
-- =============================================

INSERT INTO raw.stations (station_code, name_en, name_th, line_name, operator, latitude, longitude, opened_date, num_exits, is_interchange, interchange_lines) VALUES
-- BTS Sukhumvit Line (major stations)
('N8', 'Mo Chit', 'หมอชิต', 'BTS Sukhumvit', 'BTSC', 13.8024, 100.5533, '1999-12-05', 4, TRUE, ARRAY['MRT Blue']),
('N1', 'Ratchathewi', 'ราชเทวี', 'BTS Sukhumvit', 'BTSC', 13.7517, 100.5317, '1999-12-05', 4, FALSE, NULL),
('CEN', 'Siam', 'สยาม', 'BTS Sukhumvit', 'BTSC', 13.7454, 100.5342, '1999-12-05', 6, TRUE, ARRAY['BTS Silom']),
('E1', 'Chit Lom', 'ชิดลม', 'BTS Sukhumvit', 'BTSC', 13.7440, 100.5431, '1999-12-05', 6, FALSE, NULL),
('E2', 'Phloen Chit', 'เพลินจิต', 'BTS Sukhumvit', 'BTSC', 13.7434, 100.5490, '1999-12-05', 4, FALSE, NULL),
('E3', 'Nana', 'นานา', 'BTS Sukhumvit', 'BTSC', 13.7405, 100.5553, '1999-12-05', 4, FALSE, NULL),
('E4', 'Asok', 'อโศก', 'BTS Sukhumvit', 'BTSC', 13.7368, 100.5603, '1999-12-05', 6, TRUE, ARRAY['MRT Blue']),
('E5', 'Phrom Phong', 'พร้อมพงษ์', 'BTS Sukhumvit', 'BTSC', 13.7305, 100.5695, '1999-12-05', 6, FALSE, NULL),
('E6', 'Thong Lo', 'ทองหล่อ', 'BTS Sukhumvit', 'BTSC', 13.7246, 100.5783, '1999-12-05', 4, FALSE, NULL),
('E7', 'Ekkamai', 'เอกมัย', 'BTS Sukhumvit', 'BTSC', 13.7195, 100.5853, '1999-12-05', 4, FALSE, NULL),
('E9', 'On Nut', 'อ่อนนุช', 'BTS Sukhumvit', 'BTSC', 13.7059, 100.6013, '1999-12-05', 4, FALSE, NULL),
-- BTS Silom Line (major stations)
('S2', 'Sala Daeng', 'ศาลาแดง', 'BTS Silom', 'BTSC', 13.7282, 100.5341, '1999-12-05', 4, TRUE, ARRAY['MRT Blue']),
('S5', 'Chong Nonsi', 'ช่องนนทรี', 'BTS Silom', 'BTSC', 13.7225, 100.5293, '1999-12-05', 4, FALSE, NULL),
('S7', 'Krung Thon Buri', 'กรุงธนบุรี', 'BTS Silom', 'BTSC', 13.7207, 100.5027, '2009-05-15', 4, TRUE, ARRAY['BTS Gold']),
('S12', 'Bang Wa', 'บางหว้า', 'BTS Silom', 'BTSC', 13.7208, 100.4575, '2013-12-05', 4, TRUE, ARRAY['MRT Blue']),
-- MRT Blue Line (major stations)
('BL01', 'Tha Phra', 'ท่าพระ', 'MRT Blue', 'BEM', 13.7233, 100.4598, '2019-09-21', 4, TRUE, ARRAY['MRT Blue (loop)']),
('BL10', 'Hua Lamphong', 'หัวลำโพง', 'MRT Blue', 'BEM', 13.7379, 100.5170, '2004-07-03', 4, FALSE, NULL),
('BL13', 'Silom', 'สีลม', 'MRT Blue', 'BEM', 13.7295, 100.5348, '2004-07-03', 4, TRUE, ARRAY['BTS Silom']),
('BL15', 'Sukhumvit', 'สุขุมวิท', 'MRT Blue', 'BEM', 13.7360, 100.5610, '2004-07-03', 4, TRUE, ARRAY['BTS Sukhumvit']),
('BL18', 'Phahon Yothin', 'พหลโยธิน', 'MRT Blue', 'BEM', 13.8140, 100.5608, '2004-07-03', 4, TRUE, ARRAY['BTS Sukhumvit']),
('BL19', 'Lat Phrao', 'ลาดพร้าว', 'MRT Blue', 'BEM', 13.8064, 100.5732, '2004-07-03', 4, TRUE, ARRAY['MRT Yellow']),
('BL21', 'Thailand Cultural Centre', 'ศูนย์วัฒนธรรมฯ', 'MRT Blue', 'BEM', 13.7656, 100.5703, '2004-07-03', 4, FALSE, NULL),
('BL22', 'Phra Ram 9', 'พระราม 9', 'MRT Blue', 'BEM', 13.7570, 100.5650, '2004-07-03', 4, FALSE, NULL),
('BL26', 'Bang Sue', 'บางซื่อ', 'MRT Blue', 'BEM', 13.8060, 100.5250, '2004-07-03', 4, TRUE, ARRAY['SRT Red', 'MRT Purple']),
-- MRT Purple Line (major stations)
('PP01', 'Khlong Bang Phai', 'คลองบางไผ่', 'MRT Purple', 'BEM', 13.8949, 100.4244, '2016-08-06', 2, FALSE, NULL),
('PP16', 'Tao Poon', 'เตาปูน', 'MRT Purple', 'BEM', 13.8065, 100.5306, '2016-08-06', 4, TRUE, ARRAY['MRT Blue']);
