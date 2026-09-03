# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0
#
# The material is intended for educational purposes and should not be deployed
# in production environments without additional security testing. This is
# sample code for non-production usage. You should work with your security and
# legal teams to meet your organizational security, regulatory, and compliance
# requirements before deployment.
"""CDK stack that provisions the Databricks DynamoDB read-through cache.

Resources created:
  * Amazon DynamoDB table        - cache store (on-demand, native TTL janitor).
  * AWS Secrets Manager secret   - holds the Databricks token (value set later).
  * AWS Lambda function (Python) - read-through cache and scheduled refresher.
  * Amazon EventBridge Scheduler - invokes the Lambda on a configurable cadence.
  * AWS IAM roles                - least-privilege roles for the Lambda and the
                                   scheduler.
  * Amazon CloudWatch alarms     - Lambda errors and throttles.
"""

from __future__ import annotations

import json
import re
from typing import Dict, Optional

import jsii
from aws_cdk import (
    Aspects,
    CfnOutput,
    CfnResource,
    Duration,
    IAspect,
    RemovalPolicy,
    Stack,
    aws_cloudwatch as cloudwatch,
    aws_dynamodb as dynamodb,
    aws_iam as iam,
    aws_lambda as _lambda,
    aws_logs as logs,
    aws_scheduler as scheduler,
    aws_secretsmanager as secrets,
)
from constructs import Construct, IConstruct


@jsii.implements(IAspect)
class ReadableLogicalIds:
    """Aspect that rewrites auto-generated CloudFormation logical IDs.

    By default the AWS CDK appends an 8-character hash to each logical ID (for
    example, ``CacheTableC1E6DF7E``) to guarantee global uniqueness. Construct
    paths in this stack are already unique, so this aspect replaces every
    resource's logical ID with a readable name derived from its construct path
    and drops the hash. Applying it as an aspect means it covers ALL resources
    in the stack, including the ones the CDK creates implicitly (such as the
    Lambda service role and its default policy).
    """

    def visit(self, node: IConstruct) -> None:
        if not isinstance(node, CfnResource):
            return
        # node.node.path looks like "StackId/CacheFunction/ServiceRole/Resource".
        segments = node.node.path.split("/")[1:]  # drop the stack id
        segments = [s for s in segments if s not in ("Resource", "Default")]
        readable = "".join(re.sub(r"[^A-Za-z0-9]+", "", s) for s in segments)
        if readable:
            node.override_logical_id(readable)


class DatabricksCacheStack(Stack):
    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        *,
        env_name: str,
        databricks_host: Optional[str],
        warehouse_id: Optional[str],
        freshness_window_seconds: int,
        refresh_rate_minutes: int,
        query_manifest: Dict[str, str],
        **kwargs,
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)

        if not databricks_host or not warehouse_id:
            raise ValueError(
                "Both 'databricksHost' and 'warehouseId' context values are "
                "required. Pass them in cdk.json or with -c flags. See README."
            )

        # Fail fast if the documentation placeholders are still in place. The
        # placeholders are committed in cdk.json on purpose (so the sample
        # carries no real workspace identifiers); a real deployment must
        # override them with your own values via cdk context, e.g.:
        #   cdk deploy -c databricksHost=https://dbc-xxxx.cloud.databricks.com \
        #              -c warehouseId=<your-id>
        # or a local, untracked cdk.context.json. Without this guard a
        # placeholder host deploys cleanly and only fails at runtime with a
        # confusing connection error.
        for label, value in (
            ("databricksHost", databricks_host),
            ("warehouseId", warehouse_id),
        ):
            if "<" in value or ">" in value or "your-" in value:
                raise ValueError(
                    f"'{label}' is still set to the placeholder value "
                    f"'{value}'. Override it with your real Databricks value "
                    f"via cdk context (-c {label}=...) or an untracked "
                    f"cdk.context.json before deploying. See README."
                )

        # Explicit physical names are set on every resource below using this
        # prefix. Without an explicit name, CloudFormation appends a random
        # suffix (for example, "...-CacheFunction-pSiPGCxfK0pa") to guarantee
        # uniqueness. Setting the name yourself produces clean, predictable
        # ARNs. Trade-off (applies to ALL named resources): renaming a resource
        # forces replacement, and the same stack cannot be deployed twice in
        # the same account and Region without a name collision, so the name
        # includes the environment to keep it unique per environment.
        name_prefix = construct_id  # e.g. "DatabricksCacheStack-dev"

        # ----------------------------------------------------------------- #
        # DynamoDB cache table
        # ----------------------------------------------------------------- #
        table = dynamodb.Table(
            self,
            "CacheTable",
            table_name=f"{name_prefix}-CacheTable",
            partition_key=dynamodb.Attribute(
                name="cache_key", type=dynamodb.AttributeType.STRING
            ),
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST,
            encryption=dynamodb.TableEncryption.AWS_MANAGED,  # KMS key in-account, CloudTrail-audited.
            time_to_live_attribute="ttl",  # Fallback option, not for correctness.
            point_in_time_recovery=True,
            removal_policy=(
                RemovalPolicy.DESTROY if env_name == "dev" else RemovalPolicy.RETAIN
            ),
        )

        # ----------------------------------------------------------------- #
        # Secrets Manager secret for the Databricks token
        # ----------------------------------------------------------------- #
        # Note: AWS Secrets Manager always appends a 6-character suffix to the
        # secret ARN (for example, ".../token-wMmzHy"). This is a Secrets
        # Manager behavior and cannot be removed; the secret *name* stays clean.
        databricks_secret = secrets.Secret(
            self,
            "DatabricksToken",
            description="Databricks personal access or service principal token",
            secret_name=f"databricks-cache/{env_name}/token",
        )

        # ----------------------------------------------------------------- #
        # Lambda function (read-through cache + refresher)
        # ----------------------------------------------------------------- #
        # Create the log group explicitly to avoid the deprecated log_retention
        # prop, which provisions an extra LogRetention custom resource (a
        # Node.js Lambda plus role and policy) with hashed logical IDs.
        log_group = logs.LogGroup(
            self,
            "CacheFunctionLogGroup",
            log_group_name=f"/aws/lambda/{name_prefix}-CacheFunction",
            retention=logs.RetentionDays.TWO_WEEKS,
            removal_policy=RemovalPolicy.DESTROY,
        )

        # Explicit execution role so it gets a clean name instead of the
        # auto-generated "...-CacheFunctionServiceRole-<suffix>". Rather than
        # attaching the broad AWSLambdaBasicExecutionRole managed policy, scope
        # CloudWatch Logs access to just this function's own log group (least
        # privilege). The log group is created explicitly above, so the role
        # does not need logs:CreateLogGroup at all.
        cache_fn_role = iam.Role(
            self,
            "CacheFunctionRole",
            role_name=f"{name_prefix}-CacheFunctionRole",
            assumed_by=iam.ServicePrincipal("lambda.amazonaws.com"),
        )
        log_group.grant_write(cache_fn_role)

        cache_fn = _lambda.Function(
            self,
            "CacheFunction",
            function_name=f"{name_prefix}-CacheFunction",
            role=cache_fn_role,
            runtime=_lambda.Runtime.PYTHON_3_13,
            handler="handler.lambda_handler",
            code=_lambda.Code.from_asset("lambda"),
            timeout=Duration.minutes(5),
            memory_size=256,
            architecture=_lambda.Architecture.ARM_64,
            log_group=log_group,
            environment={
                "CACHE_TABLE_NAME": table.table_name,
                "DATABRICKS_SECRET_ARN": databricks_secret.secret_arn,
                "DATABRICKS_HOST": databricks_host,
                "WAREHOUSE_ID": warehouse_id,
                "FRESHNESS_WINDOW_SECONDS": str(freshness_window_seconds),
                "QUERY_MANIFEST": json.dumps(query_manifest),
                "LOG_LEVEL": "INFO",
            },
        )

        # Least-privilege grants.
        table.grant_read_write_data(cache_fn)
        databricks_secret.grant_read(cache_fn)

        # ----------------------------------------------------------------- #
        # EventBridge Scheduler -> Lambda refresh on a cadence
        # ----------------------------------------------------------------- #
        scheduler_role = iam.Role(
            self,
            "SchedulerRole",
            role_name=f"{name_prefix}-SchedulerRole",
            assumed_by=iam.ServicePrincipal("scheduler.amazonaws.com"),
        )
        cache_fn.grant_invoke(scheduler_role)

        refresh_schedule = scheduler.CfnSchedule(
            self,
            "RefreshSchedule",
            name=f"{name_prefix}-RefreshSchedule",
            flexible_time_window=scheduler.CfnSchedule.FlexibleTimeWindowProperty(
                mode="OFF"
            ),
            schedule_expression=f"rate({refresh_rate_minutes} minutes)",
            target=scheduler.CfnSchedule.TargetProperty(
                arn=cache_fn.function_arn,
                role_arn=scheduler_role.role_arn,
                input=json.dumps({"action": "refresh"}),
                retry_policy=scheduler.CfnSchedule.RetryPolicyProperty(
                    maximum_retry_attempts=2
                ),
            ),
            description=f"Refresh Databricks cache entries ({env_name})",
        )

        # ----------------------------------------------------------------- #
        # CloudWatch alarms
        # ----------------------------------------------------------------- #
        errors_alarm = cache_fn.metric_errors(
            period=Duration.minutes(5)
        ).create_alarm(
            self,
            "CacheFunctionErrorsAlarm",
            alarm_name=f"{name_prefix}-CacheFunctionErrorsAlarm",
            evaluation_periods=1,
            threshold=1,
            alarm_description="Databricks cache Lambda reported errors",
            comparison_operator=(
                cloudwatch.ComparisonOperator.GREATER_THAN_OR_EQUAL_TO_THRESHOLD
            ),
            treat_missing_data=cloudwatch.TreatMissingData.NOT_BREACHING,
        )

        throttles_alarm = cache_fn.metric_throttles(
            period=Duration.minutes(5)
        ).create_alarm(
            self,
            "CacheFunctionThrottlesAlarm",
            alarm_name=f"{name_prefix}-CacheFunctionThrottlesAlarm",
            evaluation_periods=1,
            threshold=1,
            alarm_description="Databricks cache Lambda is being throttled",
            comparison_operator=(
                cloudwatch.ComparisonOperator.GREATER_THAN_OR_EQUAL_TO_THRESHOLD
            ),
            treat_missing_data=cloudwatch.TreatMissingData.NOT_BREACHING,
        )

        # ----------------------------------------------------------------- #
        # Outputs - one per AWS resource created by this stack, for easy
        # reference after deployment (cdk deploy prints these, and they appear
        # in the CloudFormation console "Outputs" tab).
        # ----------------------------------------------------------------- #
        # DynamoDB cache table
        CfnOutput(self, "CacheTableName", value=table.table_name)
        CfnOutput(self, "CacheTableArn", value=table.table_arn)

        # Secrets Manager secret (Databricks token)
        CfnOutput(self, "DatabricksSecretName", value=databricks_secret.secret_name)
        CfnOutput(self, "DatabricksSecretArn", value=databricks_secret.secret_arn)

        # Lambda function (read-through cache + refresher)
        CfnOutput(self, "CacheFunctionName", value=cache_fn.function_name)
        CfnOutput(self, "CacheFunctionArn", value=cache_fn.function_arn)

        # Lambda IAM execution role
        CfnOutput(self, "CacheFunctionRoleArn", value=cache_fn.role.role_arn)

        # CloudWatch log group
        CfnOutput(self, "CacheFunctionLogGroupName", value=log_group.log_group_name)
        CfnOutput(self, "CacheFunctionLogGroupArn", value=log_group.log_group_arn)

        # EventBridge Scheduler schedule and its IAM role
        CfnOutput(self, "RefreshScheduleArn", value=refresh_schedule.attr_arn)
        CfnOutput(self, "RefreshScheduleName", value=refresh_schedule.ref)
        CfnOutput(self, "SchedulerRoleArn", value=scheduler_role.role_arn)

        # CloudWatch alarms
        CfnOutput(self, "ErrorsAlarmArn", value=errors_alarm.alarm_arn)
        CfnOutput(self, "ThrottlesAlarmArn", value=throttles_alarm.alarm_arn)

        # ----------------------------------------------------------------- #
        # Readable logical IDs (drops the auto-generated hash suffix from every
        # resource in this stack, e.g. "CacheTable" instead of
        # "CacheTableC1E6DF7E").
        # ----------------------------------------------------------------- #
        Aspects.of(self).add(ReadableLogicalIds())
