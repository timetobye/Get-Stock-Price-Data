import json
import logging
import os
from datetime import datetime, timedelta

import boto3
import requests


def send_slack_message(channel_id, message):
    from slack_sdk import WebClient
    from slack_sdk.errors import SlackApiError

    slack_bot_token = os.environ["slack_bot_token"]
    client = WebClient(token=slack_bot_token)
    logger = logging.getLogger(__name__)

    try:
        # Call the chat.postMessage method using the WebClient
        response_result = client.chat_postMessage(channel=channel_id, text=message)
        logger.info(response_result)

    except SlackApiError as e:
        print(f"Error posting message: {e}")


def parse_slack_payload_body(payload_data):
    import base64
    from urllib.parse import parse_qs

    decoded_body = base64.b64decode(payload_data["body"]).decode("utf-8")
    parsed_body = parse_qs(decoded_body)

    return parsed_body


def send_message_to_sqs(message):
    queue_url = (
        "<your sqs url>"
    )
    sqs_client = boto3.client("sqs")
    sqs_client.send_message(QueueUrl=queue_url, MessageBody=json.dumps(message))


def lambda_handler(event, context):
    parsed_body = parse_slack_payload_body(event)

    # 파라미터에서 'user_id', 'response_url' 값을 추출
    slack_user_id = parsed_body.get("user_id", [None])[0]
    slack_response_url = parsed_body.get("response_url", [None])[0]

    # 파라미터에서 'text' 값을 추출
    event_text = parsed_body.get("text", [None])[0]
    ticker, *date_range = event_text.split()
    print(ticker, date_range)

    if len(date_range) == 2:
        start_date, end_date = date_range
    else:
        start_date = date_range[0]
        utc_now = datetime.now()
        seoul_timezone_offset = timedelta(hours=9)
        end_date = (utc_now + seoul_timezone_offset).strftime("%Y-%m-%d")

    ticker = str.upper(ticker)

    message = {
        "user_id": slack_user_id,
        "ticker": ticker,
        "start_date": start_date,
        "end_date": end_date,
        "slack_response_url": slack_response_url,
    }

    send_message_to_sqs(message)

    return {
        "response_type": "ephemeral",  # option : "in_channel"
        "text": f"<@{slack_user_id}> 님 조회 결과를 전달 드리겠습니다. 잠시만 기다려주세요.",
    }
