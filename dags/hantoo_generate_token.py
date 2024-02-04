import pendulum
from datetime import timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator

from utils.generate_hantoo_token import GenerateHantooToken
from utils.slack_alert import SlackAlert


"""
2023-08-01 기준 
- 한국투자증권에서 제공하는 API에는 토큰이 유효한 토큰인지 여부를 확인할 수 있는 API 는 제공하지 않은 상태
- 또한 토큰의 유효 기간이 24시간 이므로, 재발급 받는 것이 작업의 복잡성을 낮추는 것으로 판단
- 향후 API 가 유효한지 확인할 수 있는 환경이 갖추어 진다면, DAG 구성은 달라질 수 있음
"""

slack_alert = SlackAlert()

default_args = {
    'retries': 1,
    'retry_delay': timedelta(seconds=30),
    'on_failure_callback': slack_alert.create_failure_alert,
    # 'on_success_callback': slack_alert.create_success_alert,
}

with DAG(
    dag_id="hantoo_generate_token",
    start_date=pendulum.datetime(2023, 10, 20, 17, tz='Asia/Seoul'),
    schedule_interval='0 2 * * *',  # 한국 시간으로 새벽 2시 실행
    default_args=default_args,
    on_success_callback=slack_alert.create_success_alert,
    catchup=False,
    tags=["Hantoo", "Token"]

) as dag:
    def is_not_sunday(**context):
        data_interval_end = context["data_interval_end"]  # type : pendulum.datetime
        kst_data_interval_end = data_interval_end.in_timezone('Asia/Seoul')

        print(f' context["data_interval_start"] : {context["data_interval_start"]}'
              f' context["data_interval_end"] : {context["data_interval_end"]}'
              f' kst_data_interval_end : {kst_data_interval_end}')

        target_day = kst_data_interval_end.strftime("%A")
        print(f"target_day : {target_day}")

        if target_day != "Sunday":
            return 'generate_token'
        else:
            return 'done_task'

    def generate_token(**context):
        hantoo_token_cls = GenerateHantooToken()
        access_token = hantoo_token_cls.generate_token()

        context["ti"].xcom_push(
            key="new_generated_token",
            value=access_token
        )


    def update_token_variable(**context):
        new_token = context["ti"].xcom_pull(
            key="new_generated_token",
            task_ids="generate_token"
        )

        hantoo_token_cls = GenerateHantooToken()
        hantoo_token_cls.update_token(new_token)  # variable update


    is_not_sunday_task = BranchPythonOperator(
        task_id="is_not_sunday",
        python_callable=is_not_sunday,
        provide_context=True
    )

    generate_token_task = PythonOperator(
        task_id="generate_token",
        python_callable=generate_token,
        provide_context=True
    )

    update_token_variable_task = PythonOperator(
        task_id="update_token_variable",
        python_callable=update_token_variable,
        provide_context=True
    )

    done_task = EmptyOperator(task_id="done_task", trigger_rule="none_failed")

    is_not_sunday_task >> generate_token_task
    is_not_sunday_task >> done_task

    generate_token_task >> update_token_variable_task >> done_task
