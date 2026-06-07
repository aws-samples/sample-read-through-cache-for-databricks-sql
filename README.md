# Databricks Serverless SQL Read-Through Cache (AWS Lambda + DynamoDB)

This project reduces read latency for applications that consume static or
slowly changing data from a Databricks Serverless SQL Warehouse. It places an
AWS Lambda function and an Amazon DynamoDB table in front of the warehouse as a
read-through cache, with an Amazon EventBridge Scheduler schedule that refreshes
the cache on a configurable cadence so the warehouse can stay auto-stopped.

Infrastructure is defined with the AWS Cloud Development Kit (AWS CDK) v2 in
Python. The Lambda function is implemented in Python and uses only the runtime
built-ins (`boto3` and the standard library), so nothing needs to be bundled.

---

## This code is for non-production usage
The material is intended for educational purposes and should not be deployed in production environments without additional security testing. This is sample code for non-production usage. You should work with your security and legal teams to meet your organizational security, regulatory, and compliance requirements before deployment.
```

## Architecture

```
Client ──▶ AWS Lambda ──▶ Amazon DynamoDB (cache)
                │
                └────────▶ Databricks Statement Execution API ──▶ Serverless SQL Warehouse

Amazon EventBridge Scheduler ──▶ AWS Lambda (scheduled refresh)
AWS Secrets Manager ──▶ AWS Lambda (Databricks token)
```

Read path: Lambda returns a fresh DynamoDB item directly; on a miss or expiry it
queries Databricks, writes the result to DynamoDB with an `expires_at`
timestamp, and returns it. Freshness is enforced by the handler comparing
`expires_at` to the current time (DynamoDB native TTL is used only as a janitor,
not for correctness).

---

## Repository layout

```
.
├── app.py                              # CDK app entry point
├── cdk.json                            # CDK config + context defaults
├── requirements.txt                    # CDK (deploy-time) dependencies
├── requirements-dev.txt                # test dependencies
├── pytest.ini                          # test configuration
├── README.md                           # this file
├── BOM.csv                             # dependency license inventory
├── Notice.txt                          # third-party license texts and notices
├── .gitignore
├── databricks_cache/
│   ├── __init__.py
│   └── databricks_cache_stack.py       # the CDK stack
├── lambda/
│   └── handler.py                      # Lambda function code
├── payloads/                           # example Lambda invocation events
│   ├── read-country-codes.json
│   ├── read-host-list.json
│   └── refresh-all.json
├── scripts/
│   └── set_databricks_token.py         # helper to store the Databricks token
└── tests/
    ├── __init__.py
    └── test_handler.py                 # unit tests (in-memory fakes, offline)
```

---

## Prerequisites

Install these once, on any OS:

| Tool | Version | Notes |
|------|---------|-------|
| Python | 3.13.x or 3.14.x | Runs the CDK app and tests locally; the Lambda runtime is Python 3.13 |
| Node.js | 20 LTS or later | Required by the CDK Toolkit |
| AWS CDK Toolkit | v2 | `npm install -g aws-cdk` |
| AWS CLI | v2 | For credentials and ad hoc commands |

You also need:

- An AWS account and credentials configured locally (`aws configure` or AWS SSO).
- A Databricks workspace with a Serverless SQL Warehouse and its **warehouse ID**.
- A Databricks **personal access token** or **service principal OAuth token** with
  `CAN_USE` on the warehouse and `SELECT` on the queried tables.

Verify the toolchain:

```bash
python --version
node --version
cdk --version
aws --version
```

---

## Configuration

Configuration comes from CDK context. The optional keys below ship with
defaults in `cdk.json`. The two **required** keys — `databricksHost` and
`warehouseId` — are intentionally **not** committed to `cdk.json` so the repo
carries no real workspace identifiers. Supply them per deployment in one of two
ways:

- A local, untracked `cdk.context.json` (already excluded by `.gitignore`):

  ```json
  {
    "databricksHost": "https://dbc-xxxx.cloud.databricks.com",
    "warehouseId": "your-warehouse-id"
  }
  ```

- Or command-line flags on each `cdk` command:

  ```bash
  cdk deploy -c databricksHost=https://dbc-xxxx.cloud.databricks.com \
             -c warehouseId=your-warehouse-id
  ```

The stack fails fast at synth time if either value is missing or still a
placeholder. Edit the optional keys in `cdk.json` to tune behavior:

| Context key | Required | Source | Description |
|-------------|----------|--------|-------------|
| `databricksHost` | **yes** | `cdk.context.json` or `-c` | Workspace URL, e.g. `https://abc-123.cloud.databricks.com` |
| `warehouseId` | **yes** | `cdk.context.json` or `-c` | Serverless SQL Warehouse ID |
| `envName` | no (default `dev`) | `cdk.json` | Environment suffix used in resource names |
| `freshnessWindowSeconds` | no (default 3600) | `cdk.json` | How long a cached item is fresh |
| `refreshRateMinutes` | no (default 30) | `cdk.json` | EventBridge refresh cadence |
| `queryManifest` | yes (to cache) | `cdk.json` | Map of `cache_key` → SQL statement |

> Values in `cdk.json` take precedence over `cdk.context.json`, so the required
> keys are kept out of `cdk.json` to let your untracked file (or `-c` flags)
> provide them.

> Keep `refreshRateMinutes` shorter than `freshnessWindowSeconds / 60` so the
> scheduled refresh repopulates items before they expire.

---

## Deployment

Run all commands from the repository root. The steps are the same on every OS;
only the virtual-environment activation command differs.

```bash
pip install -r requirements.txt

python -m venv .venv            # use python3 if python is not on your PATH

# Activate the virtual environment:
#   Windows (PowerShell):  .\.venv\Scripts\Activate.ps1
#   Windows (cmd):         .venv\Scripts\activate.bat
#   macOS / Linux:         source .venv/bin/activate

cdk bootstrap # NOTE: once per AWS account/Region; skip if bootstrap is already done
cdk synth
cdk deploy
```

> If PowerShell blocks the activate script, run once (current user):
> `Set-ExecutionPolicy -Scope CurrentUser RemoteSigned`.

---

## Set the Databricks token (after first deploy)

The stack creates an empty Secrets Manager secret. Populate it with your token.
The deploy output prints `DatabricksSecretArn`.

### Helper script options

`scripts/set_databricks_token.py` accepts the following arguments:

| Argument | Required | Description |
|----------|----------|-------------|
| `--secret-arn` | **yes** | Secret ARN or name to write (use the `DatabricksSecretArn` from the deploy output) |
| `--token` | no | Databricks token supplied inline on the command line; omit it to be prompted securely |
| `--region` | no | AWS Region to target; defaults to the Region from your AWS configuration |

When `--token` is omitted, the script prompts for the token with `getpass` and
does not echo the entered value, so the token never appears on screen or in your
shell history.

Prompt securely (recommended) — no `--token`, so the token is not echoed:

```bash
python scripts/set_databricks_token.py --secret-arn <DatabricksSecretArn>
```

Supply the token inline and pin the Region:

```bash
python scripts/set_databricks_token.py --secret-arn <DatabricksSecretArn> \
  --token <YOUR_DATABRICKS_TOKEN> --region <aws-region>
```

Or with the AWS CLI:

```bash
aws secretsmanager put-secret-value \
  --secret-id <DatabricksSecretArn> \
  --secret-string "{\"token\":\"<YOUR_DATABRICKS_TOKEN>\"}"
```

The handler accepts either a JSON secret `{"token":"..."}` or a plain token
string.

---

## Test the deployment

Invoke the Lambda directly with the AWS CLI (replace `<CacheFunctionName>` with
the value from the deploy output). The read example below uses a `cache_key`
defined in the `queryManifest` (`country-codes`), so it runs without any extra
input.

```bash
aws lambda invoke \
  --function-name <CacheFunctionName> \
  --payload '{"action":"read","cache_key":"country-codes"}' \
  --cli-binary-format raw-in-base64-out \
  response.json
cat response.json
```

> On Windows PowerShell, replace the line-continuation `\` with a backtick
> (`` ` ``) and read the output with `Get-Content response.json`.

The first call returns `"source":"databricks"`; subsequent calls within the
freshness window return `"source":"cache"`.

### How a `cache_key` is resolved to SQL

The handler resolves the SQL for a `cache_key` solely from the deploy-time
`queryManifest` (from `cdk.json` `context`); see `_resolve_sql` in
`lambda/handler.py`. Callers cannot supply SQL in the event. If the `cache_key`
is not present in the manifest, the handler raises a `KeyError` and returns a
`404` response.

To add a new query, add a `cache_key` → SQL entry to `queryManifest` and
redeploy.

Trigger a manual refresh of all manifest keys:

```bash
aws lambda invoke --function-name <CacheFunctionName> \
  --payload '{"action":"refresh"}' \
  --cli-binary-format raw-in-base64-out response.json
```

---

## Example payloads

The `payloads/` directory contains ready-to-use invocation events you can
pass to `aws lambda invoke` with the `--payload file://<path>` form. Each file
holds a single JSON event for the handler.

| File | `action` | `cache_key` | Notes |
|------|----------|-------------|-------|
| `read-country-codes.json` | `read` | `country-codes` | In the `queryManifest`; runs as-is |
| `read-host-list.json` | `read` | `host-list` | In the `queryManifest`; runs as-is |
| `refresh-all.json` | `refresh` | — | Refreshes all `queryManifest` keys |

Use a payload file from the `payloads/` directory like this:

```bash
aws lambda invoke \
  --function-name <CacheFunctionName> \
  --payload file://payloads/read-country-codes.json \
  --cli-binary-format raw-in-base64-out \
  response.json
cat response.json
```

---

## Running the tests

The tests use lightweight in-memory fakes for DynamoDB and Secrets Manager and
stub the Databricks call, so they run fully offline with no AWS-mocking
dependency.

```bash
python -m venv .venv          # if not already created
# activate the venv (see deployment section for your OS)
pip install -r requirements-dev.txt
pytest
```

---

## Useful CDK commands

| Command | Description |
|---------|-------------|
| `cdk ls` | List stacks |
| `cdk synth` | Synthesize the CloudFormation template |
| `cdk diff` | Compare deployed stack with current state |
| `cdk deploy` | Deploy the stack |
| `cdk deploy -c envName=test -c databricksHost=... -c warehouseId=...` | Deploy with overrides |
| `cdk destroy` | Remove the stack |

---

## Cleanup

```bash
cdk destroy
```

In `dev` (`envName=dev`) the DynamoDB table is destroyed with the stack. In
other environments the table is retained by design; delete it manually if you no
longer need the cached data.

---

## Security

See [CONTRIBUTING](CONTRIBUTING.md#security-issue-notifications) for more information.

- The Databricks token is stored in AWS Secrets Manager and read at runtime.
  It is never placed in code, environment variables, or `cdk.json`.
- The Lambda execution role is granted only what it needs: read/write on the
  specific DynamoDB table, read on the single secret, and log writes scoped to
  its own CloudWatch log group (no broad AWS-managed policy).
- Callers cannot supply SQL. The function executes only the vetted queries
  defined in `queryManifest` at deploy time, selected by `cache_key`, so
  untrusted input can never run arbitrary SQL against the warehouse.
- Cached results are stored in DynamoDB with AWS-managed (KMS) encryption at
  rest. If your queries return personal data (the sample `host-list` query
  selects names, emails, and phone numbers), review retention and access before
  pointing the cache at real data.
- Databricks API calls reject HTTP redirects, so the bearer token is never
  replayed to an unexpected host.
- Prefer the secure prompt over `--token` when storing the token; an inline
  `--token` value can be captured in your shell history and process list.
- If you expose the function (for example with a Lambda function URL), require
  authentication (`AWS_IAM`). An open endpoint lets anyone read cached data and
  trigger refreshes, which is both a data-exposure and a cost (denial-of-wallet)
  risk.
- Do NOT commit `cdk.context.json`, `cdk.local.json`, or any file containing a
  real token. They are excluded by `.gitignore`.

---

## License

This library is licensed under the MIT-0 License. See the LICENSE file.

Third-party dependencies retain their own licenses; see `BOM.csv` and
`Notice.txt` in this directory for the dependency license inventory and
attributions.
