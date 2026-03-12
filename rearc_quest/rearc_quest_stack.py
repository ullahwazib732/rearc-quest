from aws_cdk import *
from aws_cdk import (
    aws_s3 as s3,
    aws_lambda as _lambda,
    aws_events as events,
    aws_events_targets as targets,
    aws_sqs as sqs,
    aws_s3_notifications as s3n,
    aws_lambda_event_sources as event_sources,
    aws_stepfunctions as sfn,
    aws_stepfunctions_tasks as tasks
)
from constructs import Construct

class RearcQuestStack(Stack):

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # -----------------------------
        # S3 bucket (versioned)
        # -----------------------------
        bucket = s3.Bucket(
            self,
            "DatasetBucket",
            versioned=True
        )

        # -----------------------------
        # SQS + DLQ
        # -----------------------------
        dlq = sqs.Queue(self, "AnalyticsDLQ", visibility_timeout=Duration.seconds(360))

        queue = sqs.Queue(
            self,
            "AnalyticsQueue",
            dead_letter_queue=sqs.DeadLetterQueue(
                max_receive_count=3,
                queue=dlq
            ),
            visibility_timeout=Duration.seconds(360)
        )

        # -----------------------------
        # Lambda Layer (am a super asyncio user so I need that!)
        # -----------------------------
        aioboto3_layer = _lambda.LayerVersion(
            self,
            "Aioboto3Layer",
            code=_lambda.Code.from_asset("lambda_layers/aioboto3"),
            compatible_runtimes=[_lambda.Runtime.PYTHON_3_11]
        )

        pandas_layer = _lambda.LayerVersion(
            self,
            "PandasLayer",
            code=_lambda.Code.from_asset("lambda_layers/pandas"),
            compatible_runtimes=[_lambda.Runtime.PYTHON_3_11]
        )

        loguru_layer = _lambda.LayerVersion(
            self,
            "LoguruLayer",
            code=_lambda.Code.from_asset("lambda_layers/loguru"),
            compatible_runtimes=[_lambda.Runtime.PYTHON_3_11]
        )

        bs4_layer = _lambda.LayerVersion(
            self,
            "BS4Layer",
            code=_lambda.Code.from_asset("lambda_layers/bs4"),
            compatible_runtimes=[_lambda.Runtime.PYTHON_3_11]
        )

        # -----------------------------
        # STEP 2 Lambda
        # -----------------------------
        step_2_lambda = _lambda.Function(
            self,
            "Step2Lambda",
            runtime=_lambda.Runtime.PYTHON_3_11,
            handler="step_2.handler",
            code=_lambda.Code.from_asset("lambda"),
            layers=[aioboto3_layer, pandas_layer, loguru_layer, bs4_layer],
            timeout=Duration.minutes(5),
            environment={
                "BUCKET_NAME": bucket.bucket_name,
                "BASE_URL": "https://honolulu-api.datausa.io/tesseract/",
                "FORMAT": "jsonrecords",
                "PARAM_CUBES": "acs_yg_total_population_1",
                "PARAM_DRILLDOWN": "Year,Nation",
                "PARAM_MEASURES": "Population",
                "EXECUTION_LEVEL": "aws",
            }
        )

        # -----------------------------
        # STE 1 Lambda
        # -----------------------------
        step_1_lambda = _lambda.Function(
            self,
            "Step1Lambda",
            runtime=_lambda.Runtime.PYTHON_3_11,
            handler="step_1.handler",
            code=_lambda.Code.from_asset("lambda"),
            layers=[aioboto3_layer, pandas_layer, loguru_layer, bs4_layer],
            timeout=Duration.minutes(5),
            environment={
                "BUCKET_NAME": bucket.bucket_name,
                "BASE_URL": "https://download.bls.gov/pub/time.series/pr/",
                "EXECUTION_LEVEL": "aws",
            }
        )

        # -----------------------------
        # STEP 3 Lambda
        # -----------------------------
        step_3_lambda = _lambda.Function(
            self,
            "AnalyticsLambda",
            runtime=_lambda.Runtime.PYTHON_3_11,
            handler="step_3.handler",
            code=_lambda.Code.from_asset("lambda"),
            layers=[aioboto3_layer, pandas_layer, loguru_layer, bs4_layer],
            timeout=Duration.minutes(5),
            environment={
                "BUCKET_NAME": bucket.bucket_name,
                "FILE_NAME_1": "pr.data.0.Current",
                "FILE_NAME_2": "step_2.json",
                "EXECUTION_LEVEL": "aws",
                "SERIES_ID" : "PRS30006032",
                "QUATER" : "Q01",
            }
        )

        bucket.grant_read_write(step_1_lambda)
        bucket.grant_read_write(step_2_lambda)
        bucket.grant_read(step_3_lambda)

        queue.grant_consume_messages(step_3_lambda)

        # -----------------------------
        # Step Function
        # -----------------------------
        bls_task = tasks.LambdaInvoke(
            self,
            "Sync BLS Dataset",
            lambda_function=step_1_lambda
        )

        api_task = tasks.LambdaInvoke(
            self,
            "Fetch Population API",
            lambda_function=step_2_lambda
        )

        definition = bls_task.next(api_task)

        state_machine = sfn.StateMachine(
            self,
            "DataPipelineStateMachine",
            definition=definition
        )

        # -----------------------------
        # Daily schedule
        # -----------------------------
        rule = events.Rule(
            self,
            "DailySchedule",
            schedule=events.Schedule.rate(Duration.days(1))
        )

        rule.add_target(
            targets.SfnStateMachine(state_machine)
        )

        # -----------------------------
        # S3 → SQS notification
        # -----------------------------
        bucket.add_event_notification(
            s3.EventType.OBJECT_CREATED,
            s3n.SqsDestination(queue),
            s3.NotificationKeyFilter(suffix=".json")
        )

        # -----------------------------
        # SQS → Analytics Lambda
        # -----------------------------
        step_3_lambda.add_event_source(
            event_sources.SqsEventSource(queue)
        )