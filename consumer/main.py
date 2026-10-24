import json

import boto3
from secret_keys import SecretKeys

secret_keys = SecretKeys()
sqs_client = boto3.client("sqs", region_name=secret_keys.REGION_NAME)

ecs_client = boto3.client(
    "ecs",
    region_name=secret_keys.REGION_NAME
)

def poll_sqs():
    print("in poll sqs")
    while True:
        print("in poll sqs true")
        response = sqs_client.receive_message(
            QueueUrl=secret_keys.AWS_SQS_VIDEO_PROCESSING,
            MaxNumberOfMessages=1,
            WaitTimeSeconds=10
        )

        print(f"response: {response}")

        for message in response.get("Messages", []):
            message_body = json.loads(message.get("Body"))
            print(f"message_body: {message_body}")

            if "Service" in message_body and "Event" in message_body and message_body.get("Event") == "s3:TestEvent":
                sqs_client.delete_message(QueueUrl=secret_keys.AWS_SQS_VIDEO_PROCESSING, ReceiptHandle=message["ReceiptHandle"])
                continue

            if "Records" in message_body:
                s3_record = message_body["Records"][0]["s3"]
                bucket_name = s3_record["bucket"]["name"]
                s3_key = s3_record["object"]["key"]

                # spin up a docker container
                response = ecs_client.run_task(
                    cluster="arn:aws:ecs:ap-south-1:357980556561:cluster/TranscoderCluster",
                    launchType="FARGATE",
                    taskDefinition="arn:aws:ecs:ap-south-1:357980556561:task-definition/video-transcoder:4",
                    overrides={
                        "containerOverrides": [
                            {
                                "name": "video-transcoder",
                                "environment": [
                                    {"name": "S3_BUCKET", "value": bucket_name},
                                    {"name": "S3_KEY", "value": s3_key}
                                ]
                            }
                        ]
                    },
                    networkConfiguration={
                        "awsvpcConfiguration": {
                            "subnets": [
                                "subnet-039e1a6fcca5cedea",
                                "subnet-0b9aaeed305824695",
                                "subnet-0adc88f8c247fb7c8"
                            ],
                            "assignPublicIp": "ENABLED",
                            "securityGroups": ["sg-0c7fdf9b8c3185bb4"]
                        }
                    }
                )

                print(response)
                sqs_client.delete_message(
                    QueueUrl=secret_keys.AWS_SQS_VIDEO_PROCESSING,
                    ReceiptHandle=message["ReceiptHandle"]
                )

print("Starting")
poll_sqs()


