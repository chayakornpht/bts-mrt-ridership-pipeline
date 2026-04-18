# BTS/MRT Ridership Analytics Pipeline

End-to-end data pipeline ที่ดึงข้อมูลผู้โดยสารรถไฟฟ้า BTS และ MRT มาวิเคราะห์ ตั้งแต่ extract ข้อมูลจากหลายแหล่ง, transform ด้วย dbt, orchestrate ด้วย Airflow, แสดงผลบน Metabase dashboard และเทรน ML model ทำนายเวลาเดินทาง

## สิ่งที่โปรเจกต์นี้ทำ

- ดึงข้อมูล ridership จาก 3 แหล่ง (เว็บ BEM, PDF รายงาน BTS, Google Maps API)
- เก็บเข้า PostgreSQL data warehouse ที่ออกแบบเป็น star schema
- Transform ข้อมูลผ่าน dbt 3 ชั้น (staging → intermediate → marts)
- ตั้ง schedule อัตโนมัติด้วย Airflow (รายเดือน + ทุก 3 ชั่วโมง)
- แสดง insight บน Metabase dashboard 5 กราฟ
- เทรน XGBoost model ทำนายเวลาเดินทางระหว่างสถานี (R² > 0.95)

ทุกอย่างรันใน Docker Compose คำสั่งเดียว

## Tech Stack

**Pipeline:** Python, PostgreSQL, Apache Airflow, dbt  
**ML:** XGBoost, scikit-learn, Prophet, pandas  
**Infra:** Docker Compose, GitHub Actions (CI/CD)  
**Viz:** Metabase, matplotlib, folium

## โครงสร้างโปรเจกต์

- `dags/` — Airflow DAGs (monthly + daily)
- `dbt/models/` — dbt models แบ่งเป็น staging, intermediate, marts
- `scripts/` — Python ETL scripts ดึงข้อมูลจาก BEM, BTS, Google Maps
- `notebooks/` — Data Science notebooks (EDA, forecasting, ML model)
- `models/` — trained ML model (.pkl)
- `docker/` — init SQL สร้าง star schema
- `seed_data.sql` — ข้อมูลจริงจากรายงาน BEM/BTS

## Data Sources

ข้อมูลอ้างอิงจากรายงานจริง:
- **BEM Investor Relations** — ridership MRT Blue + Purple Line (investor.bemplc.co.th)
- **BTS Annual Reports** — ridership BTS Sukhumvit + Silom (bts.co.th)
- **Krungsri Research** — Industry Outlook 2024-2026

## Notebooks

**Ridership Analysis** — EDA วิเคราะห์ trend ผู้โดยสาร 2019-2024, ทำนาย ridership ด้วย Prophet, จัดกลุ่มสถานีด้วย K-Means, ทดสอบผลกระทบนโยบาย 20 บาทด้วย t-test

**Travel Time Prediction** — สร้าง synthetic data 15,000 records จำลอง pattern จริง (rush hour, weekend, ฤดูฝน), เทรน 3 models เปรียบเทียบ (Linear Regression, Random Forest, XGBoost), save model ด้วย joblib

## ผู้จัดทำ

Chayakorn Phuttharak — Computer Science, Kasetsart University
