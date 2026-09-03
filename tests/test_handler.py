# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0
#
# The material is intended for educational purposes and should not be deployed
# in production environments without additional security testing. This is
# sample code for non-production usage. You should work with your security and
# legal teams to meet your organizational security, regulatory, and compliance
# requirements before deployment.
"""Unit tests for the Lambda handler.

These tests use lightweight in-memory fakes for DynamoDB and Secrets Manager
and stub the Databricks call, so they run fully offline with no third-party
AWS-mocking dependency (no moto). Run them with:  pytest  (from the repository root).
"""

import importlib
import json
import os
import sys
import time

import pytest

# Make the lambda/ directory importable.
LAMBDA_DIR = os.path.join(os.path.dirname(__file__), "..", "lambda")
sys.path.insert(0, os.path.abspath(LAMBDA_DIR))

TABLE_NAME = "test-cache-table"
# Dummy Secrets Manager ARN built on the reserved documentation account ID. It is
# a resource identifier, never a credential, and nothing resolves it: the tests
# replace the Secrets Manager client with FakeSecrets. Bandit's B105 heuristic
# only matches the variable name, so the false positive is suppressed here.
SECRET_ARN = (
    "arn:aws:secretsmanager:us-east-1:123456789012:secret:test-databricks-token"  # nosec B105
)
REGION = "us-east-1"


class FakeTable:
    """Minimal in-memory stand-in for a boto3 DynamoDB Table.

    Implements only the operations the handler uses: ``get_item`` and
    ``put_item`` against a single ``cache_key`` partition key.
    """

    def __init__(self):
        self._store = {}

    def get_item(self, Key):
        item = self._store.get(Key["cache_key"])
        return {"Item": item} if item is not None else {}

    def put_item(self, Item):
        self._store[Item["cache_key"]] = dict(Item)


class FakeSecrets:
    """Minimal in-memory stand-in for a boto3 Secrets Manager client."""

    def __init__(self, secret_string):
        self._secret_string = secret_string

    def get_secret_value(self, SecretId):
        return {"SecretString": self._secret_string}


@pytest.fixture()
def handler(monkeypatch):
    # Configure the environment the handler reads at import time.
    monkeypatch.setenv("AWS_DEFAULT_REGION", REGION)
    monkeypatch.setenv("CACHE_TABLE_NAME", TABLE_NAME)
    monkeypatch.setenv("DATABRICKS_SECRET_ARN", SECRET_ARN)
    monkeypatch.setenv("DATABRICKS_HOST", "https://example.cloud.databricks.com")
    monkeypatch.setenv("WAREHOUSE_ID", "wh-123")
    monkeypatch.setenv("FRESHNESS_WINDOW_SECONDS", "3600")
    monkeypatch.setenv("QUERY_MANIFEST", json.dumps({"property-types": "SELECT 1"}))

    # Import the handler fresh so it picks up the env vars.
    if "handler" in sys.modules:
        del sys.modules["handler"]
    handler = importlib.import_module("handler")

    # Swap the boto3-backed clients for in-memory fakes. boto3 clients are
    # created at import but never called, since these fakes replace them before
    # any test exercises a code path that touches AWS.
    monkeypatch.setattr(handler, "_table", FakeTable())
    # The placeholder token below is only ever read back by FakeSecrets; nosec
    # suppresses Bandit B105, which matches on the "token" key name.
    monkeypatch.setattr(
        handler, "_secrets", FakeSecrets(json.dumps({"token": "fake-token"}))  # nosec B105
    )
    # Reset the module-level token cache for isolation.
    monkeypatch.setattr(handler, "_token_cache", (None, 0.0))
    return handler


def _fake_result():
    return {"columns": ["code", "name"], "rows": [["US", "United States"]], "row_count": 1}


def test_cache_miss_then_hit(handler, monkeypatch):
    calls = {"count": 0}

    def fake_execute(sql):
        calls["count"] += 1
        return _fake_result()

    monkeypatch.setattr(handler, "_execute_statement", fake_execute)

    # First call: cache miss -> hits Databricks.
    resp1 = handler.lambda_handler({"action": "read", "cache_key": "property-types"}, None)
    body1 = json.loads(resp1["body"])
    assert resp1["statusCode"] == 200
    assert body1["source"] == "databricks"
    assert body1["row_count"] == 1
    assert calls["count"] == 1

    # Second call: cache hit -> Databricks NOT called again.
    resp2 = handler.lambda_handler({"action": "read", "cache_key": "property-types"}, None)
    body2 = json.loads(resp2["body"])
    assert body2["source"] == "cache"
    assert calls["count"] == 1


def test_expired_item_triggers_refetch(handler, monkeypatch):
    monkeypatch.setattr(handler, "_execute_statement", lambda sql: _fake_result())

    # Seed an already-expired item directly into the fake table.
    handler._table.put_item(
        Item={
            "cache_key": "property-types",
            "payload": handler._encode_payload(_fake_result()),
            "expires_at": int(time.time()) - 10,
            "refreshed_at": int(time.time()) - 3610,
        }
    )

    resp = handler.lambda_handler({"action": "read", "cache_key": "property-types"}, None)
    body = json.loads(resp["body"])
    # Expired -> served from Databricks, then rewritten.
    assert body["source"] == "databricks"


def test_missing_cache_key_returns_400(handler):
    resp = handler.lambda_handler({"action": "read"}, None)
    assert resp["statusCode"] == 400


def test_unknown_key_returns_404(handler):
    resp = handler.lambda_handler({"action": "read", "cache_key": "does-not-exist"}, None)
    assert resp["statusCode"] == 404


def test_refresh_all_keys(handler, monkeypatch):
    monkeypatch.setattr(handler, "_execute_statement", lambda sql: _fake_result())

    resp = handler.lambda_handler({"action": "refresh"}, None)
    body = json.loads(resp["body"])
    assert resp["statusCode"] == 200
    assert "property-types" in body["refreshed"]
    assert body["errors"] == []


def test_payload_roundtrip(handler):
    original = _fake_result()
    encoded = handler._encode_payload(original)
    assert handler._decode_payload(encoded) == original
