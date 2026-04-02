"""
BTS/MRT Ridership Pipeline DAG

Schedule: Monthly (ridership data updates monthly)
Pipeline: Extract BEM + BTS → Load Raw → dbt Transform → dbt Test

Author: [Your Name]
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator

import sys
sys.path.insert(0, '/opt/airflow/scripts')

# =============================================
# Default args
# =============================================
default_args = {
    'owner': 'data-engineer',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2024, 1, 1),
}

# =============================================
# LINE Notify alerting (optional)
# =============================================
def send_line_notify(context):
    """Send failure alert via LINE Notify."""
    import requests
    import os
    
    token = os.environ.get('LINE_NOTIFY_TOKEN', '')
    if not token:
        return
    
    dag_id = context.get('dag').dag_id
    task_id = context.get('task_instance').task_id
    execution_date = context.get('execution_date')
    
    message = f"\n❌ Airflow Task Failed\nDAG: {dag_id}\nTask: {task_id}\nDate: {execution_date}"
    
    try:
        requests.post(
            'https://notify-api.line.me/api/notify',
            headers={'Authorization': f'Bearer {token}'},
            data={'message': message},
            timeout=10,
        )
    except Exception:
        pass


# =============================================
# DAG: Monthly Ridership Pipeline
# =============================================
with DAG(
    dag_id='bts_mrt_ridership_monthly',
    default_args=default_args,
    description='Monthly BTS/MRT ridership data pipeline',
    schedule_interval='0 8 5 * *',  # 5th of every month at 8 AM
    catchup=False,
    tags=['ridership', 'bts', 'mrt', 'monthly'],
    doc_md="""
    ## BTS/MRT Ridership Monthly Pipeline
    
    ### Data Sources
    - **BEM**: MRT Blue & Purple Line ridership from investor.bemplc.co.th
    - **BTS**: Sukhumvit & Silom Line ridership from annual report PDFs
    
    ### Pipeline Steps
    1. Extract data from BEM website
    2. Extract data from BTS PDF reports  
    3. Run dbt staging models (clean & standardize)
    4. Run dbt intermediate models (combine sources)
    5. Run dbt mart models (analytics-ready)
    6. Run dbt tests (data quality)
    """,
) as dag:

    start = EmptyOperator(task_id='start')

    # --- Extract ---
    extract_bem = PythonOperator(
        task_id='extract_bem_ridership',
        python_callable=lambda: __import__('extract_bem_ridership').extract_bem_ridership(),
        on_failure_callback=send_line_notify,
    )

    extract_bts = PythonOperator(
        task_id='extract_bts_pdf',
        python_callable=lambda: __import__('extract_bts_pdf').extract_bts_pdfs(),
        on_failure_callback=send_line_notify,
    )

    # --- dbt Transform ---
    dbt_staging = BashOperator(
        task_id='dbt_run_staging',
        bash_command='cd /opt/airflow/dbt && dbt run --select staging',
        on_failure_callback=send_line_notify,
    )

    dbt_intermediate = BashOperator(
        task_id='dbt_run_intermediate',
        bash_command='cd /opt/airflow/dbt && dbt run --select intermediate',
        on_failure_callback=send_line_notify,
    )

    dbt_marts = BashOperator(
        task_id='dbt_run_marts',
        bash_command='cd /opt/airflow/dbt && dbt run --select marts',
        on_failure_callback=send_line_notify,
    )

    # --- dbt Test ---
    dbt_test = BashOperator(
        task_id='dbt_test',
        bash_command='cd /opt/airflow/dbt && dbt test',
        on_failure_callback=send_line_notify,
    )

    end = EmptyOperator(task_id='end')

    # --- Dependencies ---
    start >> [extract_bem, extract_bts] >> dbt_staging >> dbt_intermediate >> dbt_marts >> dbt_test >> end


# =============================================
# DAG: Daily Travel Time Capture
# =============================================
with DAG(
    dag_id='bts_mrt_travel_times_daily',
    default_args=default_args,
    description='Daily travel time capture via Google Maps API',
    schedule_interval='0 7,12,18,22 * * *',  # 4 times daily
    catchup=False,
    tags=['travel_time', 'google_maps', 'daily'],
) as dag_travel:

    def extract_for_current_slot():
        """Determine current time slot and extract travel times."""
        from datetime import datetime
        
        hour = datetime.now().hour
        if hour < 10:
            slot = 'morning_rush'
        elif hour < 15:
            slot = 'midday'
        elif hour < 20:
            slot = 'evening_rush'
        else:
            slot = 'late_night'
        
        from extract_travel_times import extract_travel_times
        return extract_travel_times(time_slot_name=slot)

    capture_travel_time = PythonOperator(
        task_id='capture_travel_time',
        python_callable=extract_for_current_slot,
        on_failure_callback=send_line_notify,
    )

    capture_travel_time
