#!/usr/bin/env python3
# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0
#
# The material is intended for educational purposes and should not be deployed
# in production environments without additional security testing. This is
# sample code for non-production usage. You should work with your security and
# legal teams to meet your organizational security, regulatory, and compliance
# requirements before deployment.
"""Store the Databricks token in the Secrets Manager secret created by the stack.

Usage:
    python scripts/set_databricks_token.py --secret-arn <ARN> --token <TOKEN>
    python scripts/set_databricks_token.py --secret-arn <ARN>   # prompts securely

The token is never printed or logged.
"""

import argparse
import getpass
import json
import sys

import boto3


def main() -> int:
    parser = argparse.ArgumentParser(description="Set the Databricks token secret")
    parser.add_argument("--secret-arn", required=True, help="Secret ARN or name")
    parser.add_argument("--token", help="Databricks token (omit to be prompted)")
    parser.add_argument("--region", help="AWS Region (optional)")
    args = parser.parse_args()

    token = args.token or getpass.getpass("Databricks token: ")
    if not token:
        print("No token provided.", file=sys.stderr)
        return 1

    client = boto3.client("secretsmanager", region_name=args.region)
    client.put_secret_value(
        SecretId=args.secret_arn,
        SecretString=json.dumps({"token": token}),
    )
    print("Databricks token stored successfully.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
