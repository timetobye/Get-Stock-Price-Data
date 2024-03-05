import pendulum

from datetime import timedelta
from utils.slack_alert import SlackAlert

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

slack_alert = SlackAlert()

default_args = {
    'retries': 1,
    'retry_delay': timedelta(seconds=30),
    # 'on_success_callback': slack_alert.create_success_alert,
}

with DAG(
    dag_id='us_market_trigger_dag_run_operator',
    start_date=pendulum.datetime(2023, 11, 30, 19, tz="America/New_York"),
    schedule='0 21 * * 1-5',
    default_args=default_args,
    catchup=False,
    tags=['us_market', 'stock']
) as dag:

    start_task = BashOperator(
        task_id='start_task',
        bash_command='echo "US Market start!"',
    )

    trigger_dag_task_1 = TriggerDagRunOperator(
        task_id='trigger_dag_task_1',
        trigger_dag_id='us_market_yf_ticker_list',
        wait_for_completion=True,
        allowed_states=['success']
    )

    trigger_dag_task_2 = TriggerDagRunOperator(
        task_id='trigger_dag_task_2',
        trigger_dag_id='us_market_yf_daily_price',
        wait_for_completion=True,
        allowed_states=['success']
    )

    trigger_dag_task_3 = TriggerDagRunOperator(
        task_id='trigger_dag_task_3',
        trigger_dag_id='us_market_yf_ticker_daily_price',
        wait_for_completion=True,
        allowed_states=['success']
    )

    trigger_dag_task_4 = EmptyOperator(
        task_id="Complete-TriggerRunDag",
        trigger_rule="none_failed",
        on_success_callback=[slack_alert.create_success_alert]
    )

    start_task >> trigger_dag_task_1 >> trigger_dag_task_2
    trigger_dag_task_2 >> trigger_dag_task_3 >> trigger_dag_task_4
