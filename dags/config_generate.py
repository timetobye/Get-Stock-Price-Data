import pendulum
import sys
sys.path.append('/opt/airflow/')

from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator

from utils.configuration_control import ConfigurationControl


"""
2023-10-31 기준 
- 한국투자증권에서 제공하는 API에는 토큰이 유효한 토큰인지 여부를 확인할 수 있는 API 는 제공하지 않은 상태
- 또한 토큰의 유효 기간이 24시간 이므로, 재발급 받는 것이 작업의 복잡성을 낮추는 것으로 판단
- 향후 API 가 유효한지 확인할 수 있는 환경이 갖추어 진다면, DAG 구성은 달라질 수 있음
- 한국 시장 조회를 위해서만 사용하고 있음
"""


with DAG(
    dag_id="Generate_API_Token",
    start_date=pendulum.datetime(2023, 10, 20, 17),
    schedule_interval='0 17 * * *',  # 한국 시간으로 새벽 2시 실행
    # default_args=default_args
    catchup=False,
    tags=["Config", "Key", "Token", "Mon - Sat"]

) as dag:
    config_control = ConfigurationControl()

    def is_not_sunday(**kwargs):
        data_interval_end = kwargs["data_interval_end"]  # type : pendulum.datetime
        kst_data_interval_end = data_interval_end.in_timezone('Asia/Seoul')

        print(f' kwargs["data_interval_start"] : {kwargs["data_interval_start"]}'
              f' kwargs["data_interval_end"] : {kwargs["data_interval_end"]}'
              f' kst_data_interval_end : {kst_data_interval_end}')

        target_day = kst_data_interval_end.strftime("%A")
        print(f"target_day : {target_day}")

        if target_day != "Sunday":
            return 'generate_token'
        else:
            return 'done_task'

    def generate_token(config_control, **kwargs):
        access_token = config_control.generate_token()
        kwargs["ti"].xcom_push(
            key="access_token",
            value=access_token
        )


    def update_token(config_control, **kwargs):
        access_token = kwargs["ti"].xcom_pull(
            key="access_token",
            task_ids="generate_token"
        )
        # config_control.access_token = access_token
        config_control.update_token_file(access_token)


    def generate_hash_key(config_control, **kwargs):
        hash_key = config_control.generate_hash_key()
        kwargs["ti"].xcom_push(
            key="hash_key",
            value=hash_key
        )


    def update_hash_key(config_control, **kwargs):
        hash_key = kwargs["ti"].xcom_pull(
            key="hash_key",
            task_ids="generate_hash_key"
        )
        config_control.update_hash_key_file(hash_key)


    is_not_sunday_task = BranchPythonOperator(
        task_id="is_not_sunday",
        python_callable=is_not_sunday,
        provide_context=True
    )

    generate_token_task = PythonOperator(
        task_id="generate_token",
        python_callable=generate_token,
        op_args=[config_control],
        provide_context=True
    )

    update_token_task = PythonOperator(
        task_id="update_token",
        python_callable=update_token,
        op_args=[config_control],
        provide_context=True
    )

    generate_hash_key_task = PythonOperator(
        task_id="generate_hask_key",
        python_callable=generate_hash_key,
        op_args=[config_control],
        provide_context=True
    )

    update_hash_key_task = PythonOperator(
        task_id="update_hask_key",
        python_callable=update_hash_key,
        op_args=[config_control],
        provide_context=True
    )

    done_task = EmptyOperator(task_id="done_task", trigger_rule="none_failed")

    is_not_sunday_task >> generate_token_task
    is_not_sunday_task >> done_task

    generate_token_task >> update_token_task >> generate_hash_key_task >> update_hash_key_task >> done_task
