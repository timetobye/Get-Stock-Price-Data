from airflow.providers.slack.operators.slack import SlackAPIPostOperator


class SlackAlert:
    def __init__(self, channel_type="main"):
        """
        slack은 기본적으로 main 채널을 바라봅니다. main 채널명은 variable에 기록합니다.
        """
        from airflow.models import Variable

        slack_json = Variable.get(key="slack", deserialize_json=True)
        self.slack_token = slack_json[channel_type]["access_token"]
        self.slack_channel_name = slack_json[channel_type]["channel_name"]

    def create_failure_alert(self, context):
        task_instance = context["ti"]
        alert = SlackAPIPostOperator(
            task_id='slack_failed',
            token=self.slack_token,
            channel=self.slack_channel_name,
            text=f"*Result* Failed :fire: \n"
                 f"*Dag*: {task_instance.dag_id} \n"
                 f"*Task*: {task_instance.task_id} \n"
                 f"*exec_date*: {context['ds']} \n"
                 f"*run_id*: {task_instance.run_id} \n"
                 f"*Log Url*: {task_instance.log_url}"
            )

        return alert.execute(context=context)

    def create_success_alert(self, context):
        task_instance = context["ti"]
        alert = SlackAPIPostOperator(
            task_id='slack_success',
            token=self.slack_token,
            channel=self.slack_channel_name,
            text=f"*Result* Success :checkered_flag: \n"
                 f"*Dag*: {task_instance.dag_id} \n"
                 f"*Task*: {task_instance.task_id} \n"
                 f"*exec_date*: {context['ds']} \n"
                 f"*run_id*: {task_instance.run_id} \n"
                 f"*Log Url*: {task_instance.log_url}"
            )

        return alert.execute(context=context)
