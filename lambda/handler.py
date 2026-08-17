# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0
#
# The material is intended for educational purposes and should not be deployed
# in production environments without additional security testing. This is
# sample code for non-production usage. You should work with your security and
# legal teams to meet your organizational security, regulatory, and compliance
# requirements before deployment.
"""
AWS Lambda handler implementing a read-through cache for Databricks Serverless
SQL Warehouse query results, backed by Amazon DynamoDB.

Two invocation modes are supported:

1. Read path (synchronous, user-facing)
   - Invoked directly, via a Lambda function URL, or by another service.
   - Looks up the requested cache key in DynamoDB.
   - On a fresh hit (stored ``expires_at`` is in the future) the cached payload
     is returned immediately and Databricks is not contacted.
   - On a miss or an expired item, the underlying SQL is run against Databricks,
     the result is written back to DynamoDB with a new ``expires_at``, and the
     payload is returned.

2. Refresh path (asynchronous, scheduled)
   - Invoked by Amazon EventBridge Scheduler with ``{"action": "refresh"}``.
   - Re-runs the query (or queries) and overwrites the DynamoDB item(s) with a
     fresh payload and ``expires_at`` so user requests stay on the fast path.

Freshness is enforced by the handler on read (it compares ``expires_at`` to the
current time). DynamoDB native TTL is intentionally NOT relied upon for
correctness, because TTL deletion is eventual.

The handler uses only the AWS Lambda Python runtime built-ins (``boto3`` and the
standard library), so no third-party packages need to be bundled.
"""

from __future__ import annotations

import gzip
import json
import logging
import os
import time
import urllib.error
import urllib.request
from decimal import Decimal
from typing import Any, Dict, List, Optional, Tuple

import boto3

# --------------------------------------------------------------------------- #
# Configuration (from environment variables set by the CDK stack)
# --------------------------------------------------------------------------- #

LOG = logging.getLogger()
LOG.setLevel(os.environ.get("LOG_LEVEL", "INFO"))

CACHE_TABLE_NAME = os.environ["CACHE_TABLE_NAME"]
DATABRICKS_SECRET_ARN = os.environ["DATABRICKS_SECRET_ARN"]
DATABRICKS_HOST = os.environ["DATABRICKS_HOST"].rstrip("/")
WAREHOUSE_ID = os.environ["WAREHOUSE_ID"]
FRESHNESS_WINDOW_SECONDS = int(os.environ.get("FRESHNESS_WINDOW_SECONDS", "3600"))

# Mapping of cache key -> SQL statement, provided as a JSON string at deploy time.
# This deploy-time manifest is the ONLY source of SQL the function executes;
# callers select a query by cache_key and can never supply SQL themselves.
QUERY_MANIFEST: Dict[str, str] = json.loads(os.environ.get("QUERY_MANIFEST", "{}"))

# How long the secret value is cached in-memory before it is re-read (seconds).
SECRET_REFRESH_SECONDS = int(os.environ.get("SECRET_REFRESH_SECONDS", "300"))

# Databricks Statement Execution API tuning.
STATEMENT_WAIT_TIMEOUT = "30s"          # server-side synchronous wait
POLL_INTERVAL_SECONDS = 2               # client-side poll interval when async
MAX_POLL_ATTEMPTS = 20                  # bounded so we never hit the Lambda timeout
MAX_HTTP_RETRIES = 5                    # retries on 429 / 5xx

# --------------------------------------------------------------------------- #
# Reusable clients (initialized once per execution environment, outside the
# handler, so warm invocations reuse them).
# --------------------------------------------------------------------------- #

_dynamodb = boto3.resource("dynamodb")
_table = _dynamodb.Table(CACHE_TABLE_NAME)
_secrets = boto3.client("secretsmanager")

# In-memory token cache: (token, fetched_at_epoch)
_token_cache: Tuple[Optional[str], float] = (None, 0.0)


# --------------------------------------------------------------------------- #
# Credential handling
# NOTE: For production workloads, follow the Databricks recommended access best 
# practices as documented in "Authorize access to Databricks resources" 
# (https://docs.databricks.com/aws/en/dev-tools/auth/).
# --------------------------------------------------------------------------- #

def _get_databricks_token(force: bool = False) -> str:
    """Return the Databricks token, caching it in memory between invocations.

    The cached value is refreshed every ``SECRET_REFRESH_SECONDS`` so that
    Secrets Manager rotation is picked up without a redeploy. ``force=True``
    bypasses the cache (used after a 401/403 from Databricks).
    """
    global _token_cache
    token, fetched_at = _token_cache
    age = time.time() - fetched_at
    if token and not force and age < SECRET_REFRESH_SECONDS:
        return token

    resp = _secrets.get_secret_value(SecretId=DATABRICKS_SECRET_ARN)
    raw = resp.get("SecretString") or ""
    # Support either a plain token string or a JSON object {"token": "..."}.
    try:
        parsed = json.loads(raw)
        value = parsed.get("token") if isinstance(parsed, dict) else raw
    except json.JSONDecodeError:
        value = raw

    if not value:
        raise RuntimeError("Databricks token in Secrets Manager is empty")

    _token_cache = (value, time.time())
    return value


# --------------------------------------------------------------------------- #
# Databricks Statement Execution API
# --------------------------------------------------------------------------- #


class _NoRedirectHandler(urllib.request.HTTPRedirectHandler):
    """Refuse HTTP redirects on Databricks calls.

    Python's urllib replays request headers - including the ``Authorization``
    bearer token - to the redirect target, and (unlike ``requests``) does not
    strip ``Authorization`` on a cross-host redirect. Following a redirect to an
    unexpected host would therefore leak the Databricks token, so redirects are
    rejected outright instead of being followed.
    """

    def redirect_request(self, req, fp, code, msg, headers, newurl):
        raise urllib.error.HTTPError(
            req.full_url, code, f"Refusing redirect to {newurl}", headers, fp
        )


# Opener that performs HTTPS requests without following redirects.
_OPENER = urllib.request.build_opener(_NoRedirectHandler)


def _http_request(
    url: str,
    method: str,
    token: str,
    body: Optional[Dict[str, Any]] = None,
) -> Tuple[int, Dict[str, Any]]:
    """Perform an HTTPS request with exponential backoff on 429 / 5xx."""
    data = json.dumps(body).encode("utf-8") if body is not None else None
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
        "User-Agent": "databricks-dynamodb-cache/1.0",
    }

    backoff = 1.0
    last_status = 0
    for attempt in range(1, MAX_HTTP_RETRIES + 1):
        req = urllib.request.Request(url=url, data=data, headers=headers, method=method)
        try:
            with _OPENER.open(req, timeout=60) as resp:
                payload = json.loads(resp.read().decode("utf-8") or "{}")
                return resp.status, payload
        except urllib.error.HTTPError as exc:
            last_status = exc.code
            detail = exc.read().decode("utf-8", errors="replace")
            if exc.code in (429,) or 500 <= exc.code < 600:
                LOG.warning(
                    "Databricks HTTP %s (attempt %s/%s): %s",
                    exc.code, attempt, MAX_HTTP_RETRIES, detail,
                )
                time.sleep(backoff + (0.1 * attempt))  # jitter
                backoff = min(backoff * 2, 16.0)
                continue
            raise RuntimeError(f"Databricks API error {exc.code}: {detail}") from exc
        except urllib.error.URLError as exc:
            LOG.warning(
                "Databricks connection error (attempt %s/%s): %s",
                attempt, MAX_HTTP_RETRIES, exc,
            )
            time.sleep(backoff)
            backoff = min(backoff * 2, 16.0)

    raise RuntimeError(
        f"Databricks API request to {url} failed after "
        f"{MAX_HTTP_RETRIES} attempts (last status {last_status})"
    )


def _execute_statement(sql: str) -> Dict[str, Any]:
    """Run a SQL statement on the configured warehouse and return the result.

    Returns a dict: {"columns": [...], "rows": [[...], ...], "row_count": N}.
    """
    token = _get_databricks_token()
    submit_url = f"{DATABRICKS_HOST}/api/2.0/sql/statements"
    body = {
        "statement": sql,
        "warehouse_id": WAREHOUSE_ID,
        "wait_timeout": STATEMENT_WAIT_TIMEOUT,
        "on_wait_timeout": "CONTINUE",
        "disposition": "INLINE",
        "format": "JSON_ARRAY",
    }

    try:
        _status, result = _http_request(submit_url, "POST", token, body)
    except RuntimeError as exc:
        # On an auth failure, force a token refresh and retry once.
        if "401" in str(exc) or "403" in str(exc):
            token = _get_databricks_token(force=True)
            _status, result = _http_request(submit_url, "POST", token, body)
        else:
            raise

    statement_id = result.get("statement_id")
    state = (result.get("status") or {}).get("state")

    # Poll while the statement is still running.
    attempts = 0
    while state in ("PENDING", "RUNNING") and attempts < MAX_POLL_ATTEMPTS:
        time.sleep(POLL_INTERVAL_SECONDS)
        attempts += 1
        poll_url = f"{DATABRICKS_HOST}/api/2.0/sql/statements/{statement_id}"
        _status, result = _http_request(poll_url, "GET", token)
        state = (result.get("status") or {}).get("state")

    if state != "SUCCEEDED":
        status = result.get("status") or {}
        error = status.get("error") or {}
        raise RuntimeError(
            f"Databricks statement did not succeed (state={state}): "
            f"{error.get('message', 'no detail')}"
        )

    manifest = result.get("manifest") or {}
    schema = manifest.get("schema") or {}
    columns = [c.get("name") for c in schema.get("columns", [])]
    data = (result.get("result") or {}).get("data_array") or []
    return {"columns": columns, "rows": data, "row_count": len(data)}


# --------------------------------------------------------------------------- #
# DynamoDB cache access
# --------------------------------------------------------------------------- #

def _encode_payload(payload: Dict[str, Any]) -> str:
    """Serialize the payload to JSON, gzip-compress it, and hex-encode the
    result for storage (``gzip.compress(raw).hex()``)."""
    raw = json.dumps(payload, separators=(",", ":")).encode("utf-8")
    return gzip.compress(raw).hex()


def _decode_payload(blob: str) -> Dict[str, Any]:
    raw = gzip.decompress(bytes.fromhex(blob))
    return json.loads(raw.decode("utf-8"))


def _read_from_cache(cache_key: str) -> Optional[Dict[str, Any]]:
    resp = _table.get_item(Key={"cache_key": cache_key})
    item = resp.get("Item")
    if not item:
        return None
    expires_at = int(item.get("expires_at", 0))
    if expires_at <= int(time.time()):
        LOG.info("Cache EXPIRED for key=%s", cache_key)
        return None
    LOG.info("Cache HIT for key=%s", cache_key)
    return _decode_payload(item["payload"])


def _write_to_cache(cache_key: str, payload: Dict[str, Any]) -> int:
    now = int(time.time())
    expires_at = now + FRESHNESS_WINDOW_SECONDS
    item = {
        "cache_key": cache_key,
        "payload": _encode_payload(payload),
        "row_count": Decimal(str(payload.get("row_count", 0))),
        "refreshed_at": now,
        "expires_at": expires_at,
        # Optional janitor: native TTL well beyond the freshness window so
        # abandoned keys are eventually cleaned up. NOT used for correctness.
        "ttl": expires_at + (7 * 24 * 3600),
    }
    _table.put_item(Item=item)
    LOG.info("Cache WRITE for key=%s (expires_at=%s)", cache_key, expires_at)
    return expires_at


# --------------------------------------------------------------------------- #
# Request parsing
# --------------------------------------------------------------------------- #

def _resolve_sql(cache_key: str) -> str:
    """Resolve the SQL for a cache key from the deploy-time manifest.

    ``QUERY_MANIFEST`` is the only source of SQL the function executes. Callers
    select a pre-defined query by ``cache_key``; they cannot supply SQL in the
    event, so untrusted input can never run arbitrary statements against the
    warehouse.
    """
    if cache_key in QUERY_MANIFEST:
        return QUERY_MANIFEST[cache_key]
    raise KeyError(
        f"No SQL found for cache_key '{cache_key}'. Add it to the query manifest "
        f"(queryManifest in cdk.json) and redeploy."
    )


def _parse_event(event: Dict[str, Any]) -> Dict[str, Any]:
    """Normalize direct-invoke and Lambda function URL events."""
    # Lambda function URL / API Gateway style event.
    if isinstance(event, dict) and "rawQueryString" in event:
        params = event.get("queryStringParameters") or {}
        return {"action": "read", "cache_key": params.get("key")}

    if isinstance(event, dict) and "body" in event and event.get("body"):
        try:
            return json.loads(event["body"])
        except (json.JSONDecodeError, TypeError):
            pass

    return event if isinstance(event, dict) else {}


# --------------------------------------------------------------------------- #
# Handler
# --------------------------------------------------------------------------- #

def _read(cache_key: str) -> Dict[str, Any]:
    cached = _read_from_cache(cache_key)
    if cached is not None:
        return {"cache_key": cache_key, "source": "cache", **cached}

    sql = _resolve_sql(cache_key)
    result = _execute_statement(sql)
    _write_to_cache(cache_key, result)
    return {"cache_key": cache_key, "source": "databricks", **result}


def _refresh(cache_key: Optional[str]) -> Dict[str, Any]:
    keys: List[str] = [cache_key] if cache_key else list(QUERY_MANIFEST.keys())
    if not keys:
        raise KeyError("Refresh requested but the query manifest is empty")

    refreshed, errors = [], []
    for key in keys:
        try:
            sql = _resolve_sql(key)
            result = _execute_statement(sql)
            _write_to_cache(key, result)
            refreshed.append(key)
        except Exception as exc:  # noqa: BLE001 - record and continue
            LOG.exception("Refresh failed for key=%s", key)
            errors.append({"cache_key": key, "error": str(exc)})

    return {"action": "refresh", "refreshed": refreshed, "errors": errors}


def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    parsed = _parse_event(event)
    action = (parsed.get("action") or "read").lower()
    # Log only non-sensitive routing fields. The raw event is intentionally NOT
    # logged so that any caller-supplied SQL or row data stays out of the logs.
    LOG.info("Request action=%s cache_key=%s", action, parsed.get("cache_key"))

    try:
        if action == "refresh":
            body = _refresh(parsed.get("cache_key"))
            return _response(200, body)

        cache_key = parsed.get("cache_key")
        if not cache_key:
            return _response(400, {"error": "Missing 'cache_key' in request"})
        body = _read(cache_key)
        return _response(200, body)
    except KeyError as exc:
        return _response(404, {"error": str(exc)})
    except Exception:  # noqa: BLE001
        # Log full detail server-side, but return a generic message so internal
        # / backend error details are not disclosed to the caller.
        LOG.exception("Unhandled error")
        return _response(
            502, {"error": "Upstream query failed; see function logs for details."}
        )


def _response(status: int, body: Dict[str, Any]) -> Dict[str, Any]:
    """Shape a response usable both for direct invoke and a function URL."""
    return {
        "statusCode": status,
        "headers": {"Content-Type": "application/json"},
        "body": json.dumps(body, default=str),
    }
