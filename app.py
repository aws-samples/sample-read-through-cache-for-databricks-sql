#!/usr/bin/env python3
# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0
#
# The material is intended for educational purposes and should not be deployed
# in production environments without additional security testing. This is
# sample code for non-production usage. You should work with your security and
# legal teams to meet your organizational security, regulatory, and compliance
# requirements before deployment.
"""CDK application entry point for the Databricks DynamoDB read-through cache.

Configuration is supplied through CDK context (cdk.json or -c flags) so the same
code can be deployed to multiple environments without modification. See
README.md.
"""

import os

import aws_cdk as cdk

from databricks_cache.databricks_cache_stack import DatabricksCacheStack

app = cdk.App()

# Resolve configuration from CDK context with sensible fallbacks.
env_name = app.node.try_get_context("envName") or "dev"
databricks_host = app.node.try_get_context("databricksHost")
warehouse_id = app.node.try_get_context("warehouseId")
freshness_window_seconds = int(
    app.node.try_get_context("freshnessWindowSeconds") or 3600
)
refresh_rate_minutes = int(app.node.try_get_context("refreshRateMinutes") or 30)
query_manifest = app.node.try_get_context("queryManifest") or {}

DatabricksCacheStack(
    app,
    f"DatabricksCacheStack-{env_name}",
    env_name=env_name,
    databricks_host=databricks_host,
    warehouse_id=warehouse_id,
    freshness_window_seconds=freshness_window_seconds,
    refresh_rate_minutes=refresh_rate_minutes,
    query_manifest=query_manifest,
    env=cdk.Environment(
        account=os.environ.get("CDK_DEFAULT_ACCOUNT"),
        region=os.environ.get("CDK_DEFAULT_REGION"),
    ),
    description=(
        "Read-through cache for Databricks Serverless SQL Warehouse results "
        f"using AWS Lambda and Amazon DynamoDB ({env_name})."
    ),
)

app.synth()
