import json
import os
import time
from collections import defaultdict
from datetime import datetime, timedelta

import boto3


def send_slack_message(channel_id, message):
    from slack_sdk import WebClient
    from slack_sdk.errors import SlackApiError

    slack_bot_token = os.environ["slack_bot_token"]
    client = WebClient(token=slack_bot_token)
    print(f"message : {message}")
    try:
        # Call the chat.postMessage method using the WebClient
        response_result = client.chat_postMessage(
            channel=channel_id,
            attachments=message["attachments"],
        )
        print("post slack message")

    except SlackApiError as e:
        print(f"Error posting message: {e}")


def make_slack_message(slack_user_id, price_data_list, ticker):

    slack_message_format = {
        "attachments": [
            {
                "color": "#f2c744",
                "blocks": [
                    {
                        "type": "section",
                        "text": {
                            "type": "mrkdwn",
                            "text": f"<@{slack_user_id}> 님 요청하신 {ticker} 종목의 가격 정보 입니다.",
                        },
                    },
                ],
            }
        ]
    }
    for price_data in price_data_list:
        date, *price, volume = price_data

        price_template = {
            "type": "section",
            "fields": [
                {"type": "mrkdwn", "text": f"*날짜*: {date}"},
                {"type": "mrkdwn", "text": f"*Volume*: {volume}"},
                {"type": "mrkdwn", "text": f"*Open*: *$* {price[0]}"},
                {"type": "mrkdwn", "text": f"*Close*: *$* {price[1]}"},
                {"type": "mrkdwn", "text": f"*High*: *$* {price[2]}"},
                {"type": "mrkdwn", "text": f"*Low*: *$* {price[3]}"},
            ],
        }

        slack_message_format["attachments"][0]["blocks"].append(price_template)

    return slack_message_format


def lambda_handler(event, context):
    query_data = event["Records"][0]["body"]
    print(query_data, type(query_data))

    query_data = json.loads(query_data)

    slack_user_id = query_data.get("user_id", None)
    ticker = query_data.get("ticker", None)
    start_date = query_data.get("start_date", None)
    end_date = query_data.get("end_date", None)
    slack_response_url = query_data.get("slack_response_url", None)

    athena_client = boto3.client("athena")
    query = f"""
        <your Query>
    """

    response = athena_client.start_query_execution(
        QueryString=query,
        QueryExecutionContext={"Database": "<your database>"},
        ResultConfiguration={
            'OutputLocation': 's3://your-bucket-name/results/'
        },
    )

    query_execution_id = response["QueryExecutionId"]
    print(f"query_execution_id : {query_execution_id}")

    query_status = None
    start_time = time.time()

    while query_status not in ["SUCCEEDED", "FAILED", "CANCELLED"]:
        query_status = athena_client.get_query_execution(
            QueryExecutionId=query_execution_id
        )["QueryExecution"]["Status"]["State"]

        if time.time() - start_time >= 60:
            return {
                "statusCode": 400,
                "body": json.dumps("Fail : Query results sent to Slack."),
            }

    if query_status == "SUCCEEDED":
        result_location = athena_client.get_query_execution(
            QueryExecutionId=query_execution_id
        )["QueryExecution"]["ResultConfiguration"]["OutputLocation"]
        s3_client = boto3.client("s3")
        result = s3_client.get_object(
            Bucket=result_location.split("/")[2],
            Key="/".join(result_location.split("/")[3:]),
        )
        data_str = result["Body"].read().decode("utf-8")

    lines = data_str.strip().split("\n")
    data_list = [line.replace('"', "").split(",") for line in lines][1:]

    slack_message = make_slack_message(slack_user_id, data_list, ticker)
    send_slack_message(
        "your Slack ID", slack_message
    )

    return {"statusCode": 200, "body": json.dumps("Query results sent to Slack.")}
