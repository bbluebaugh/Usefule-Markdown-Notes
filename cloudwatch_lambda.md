# 📋 Prerequisites & IAM Setup

Before linking the subscription, ensure your Lambda execution role has the necessary permissions to read CloudWatch logs and make outbound HTTP requests to your auditing API.
Your Lambda function's IAM policy needs this minimal block to interact with CloudWatch logs:
json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "logs:CreateLogGroup",
                "logs:CreateLogStream",
                "logs:PutLogEvents"
            ],
            "Resource": "arn:aws:logs:*:*:*"
        }
    ]
}
Use code with caution.

# 🐍 Production Python Lambda Script

Neo4j query log rows are space-delimited or tab-delimited strings containing crucial auditing metadata (Timestamp, Log Level, Component, Username, Protocol, Client IP, Database, Query Execution Time, and the raw Cypher statement).
This boilerplate logic safely unpacks the compressed AWS payload, loops through each log entry, abstracts the text, and forwards it to your external API:
```python
import base64
import json
import gzip
import urllib3

# Initialize connection pool outside the handler for connection reuse
http = urllib3.PoolManager()
AUDIT_API_URL = "https://yourcompany.com"

def lambda_handler(event, context):
    # 1. Decode and decompress the payload from CloudWatch
    try:
        cw_data = event['awslogs']['data']
        compressed_payload = base64.b64decode(cw_data)
        uncompressed_payload = gzip.decompress(compressed_payload)
        log_data = json.loads(uncompressed_payload)
    except Exception as e:
        print(f"Failed to unpack CloudWatch payload: {str(e)}")
        return {"statusCode": 400, "body": "Invalid payload format"}

    # 2. Iterate through rows inside the log block
    audit_payloads = []
    for log_event in log_data['logEvents']:
        log_line = log_event['message'].strip()
        
        # Skip internal system keep-alives or empty lines if necessary
        if not log_line:
            continue
            
        # Structure the message payload for your Auditing API
        audit_payloads.append({
            "source_host": log_data.get("logStream"),
            "log_group": log_data.get("logGroup"),
            "timestamp": log_event.get("timestamp"),  # Epoch timestamp in milliseconds
            "raw_query_log": log_line
        })

    # 3. Batch ship payloads to your API to minimize network overhead
    if audit_payloads:
        try:
            encoded_data = json.dumps({"events": audit_payloads}).encode('utf-8')
            response = http.request(
                'POST',
                AUDIT_API_URL,
                body=encoded_data,
                headers={
                    'Content-Type': 'application/json',
                    'Authorization': 'Bearer YOUR_API_AUDIT_TOKEN' # Replace with valid auth
                },
                timeout=5.0
            )
            print(f"Shipped {len(audit_payloads)} logs. API Response Status: {response.status}")
        except Exception as e:
            print(f"Network error pushing logs to Auditing API: {str(e)}")
            # Handle retries or dead-letter queuing here if data loss is unacceptable
            raise e

    return {"statusCode": 200, "body": f"Successfully audited {len(audit_payloads)} events"}
```
> Use code with caution.
# 🚀 Activating the CloudWatch Trigger (CLI Method)

While you can add this trigger via the AWS Console UI by selecting Add Trigger -> CloudWatch Logs inside Lambda, doing it via the AWS CLI or Infrastructure as Code avoids common permission edge cases.
To link your Lambda function to your existing neo4j-query-logs group, run these two commands:

**Step 1: Give CloudWatch permission to execute your Lambda function**
```bash
aws lambda add-permission \
    --function-name "your-audit-lambda-name" \
    --statement-id "cwl-access" \
    --action "lambda:InvokeFunction" \
    --principal "://amazonaws.com" \
    --source-arn "arn:aws:logs:REGION:ACCOUNT_ID:log-group:neo4j-query-logs:*"
```
> Use code with caution.
Step 2: Create the subscription filter
```bash
aws logs put-subscription-filter \
    --log-group-name "neo4j-query-logs" \
    --filter-name "ShipToAuditingAPI" \
    --filter-pattern "" \
    --destination-arn "arn:aws:lambda:REGION:ACCOUNT_ID:function:your-audit-lambda-name"
```
> Use code with caution.
*(Note: An empty filter pattern `""` means every single line written to query.log is securely intercepted and sent to the auditing scope without dropping entries).*
