#!/usr/bin/env python3
"""
Script: Upload sample DataFrame as CSV to AWS S3 using boto3
- Uses environment variables for credentials (GitHub Actions / .env / local)
- Configurable bucket name and object key via env vars
- Proper error handling and logging
- Safe: no hardcoded credentials
"""

import pandas as pd
import boto3
from io import StringIO
import os
import sys
import logging

# -------------------------------------------------------------------------
# Configure logging (visible in GitHub Actions logs)
# -------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

# merge changes test 2

# -------------------------------------------------------------------------
# Get AWS credentials from environment variables
# -------------------------------------------------------------------------
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_SESSION_TOKEN = os.getenv("AWS_SESSION_TOKEN")     # optional / can be None
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")       # default region

# Required credentials check
if not AWS_ACCESS_KEY_ID or not AWS_SECRET_ACCESS_KEY:
    logger.error("Missing required AWS credentials")
    logger.error("AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY must be set")
    sys.exit(1)

# -------------------------------------------------------------------------
# Get S3 target configuration (with sensible defaults)
# -------------------------------------------------------------------------
BUCKET_NAME = os.getenv("S3_BUCKET", "zeb-ds-poc-s3")
S3_KEY = os.getenv("S3_KEY", "gunaranjan_poc/git-test/sample_data.csv")

logger.info(f"Target: s3://{BUCKET_NAME}/{S3_KEY}")
logger.info(f"Region: {AWS_REGION}")

# -------------------------------------------------------------------------
# Sample data (you can replace this with real data loading logic)
# -------------------------------------------------------------------------
data = {
    'id': [1001, 1002, 1003, 1004],
    'name': ['Rajan', 'Priya', 'Arjun', 'Meera'],
    'city': ['Chennai', 'Coimbatore', 'Madurai', 'Salem'],
    'score': [89.5, 92.0, 78.5, 95.0],
    'active': [True, True, False, True],
    'registered': ['2025-01-15', '2025-02-03', '2024-11-20', '2025-03-10']
}

df = pd.DataFrame(data)

logger.info("DataFrame preview:")
logger.info("\n" + df.to_string(index=False))

# -------------------------------------------------------------------------
# Convert DataFrame to CSV in memory
# -------------------------------------------------------------------------
csv_buffer = StringIO()
df.to_csv(csv_buffer, index=False, encoding='utf-8')
csv_buffer.seek(0)

# -------------------------------------------------------------------------
# Create S3 client
# -------------------------------------------------------------------------
try:
    s3_client = boto3.client(
        's3',
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        aws_session_token=AWS_SESSION_TOKEN if AWS_SESSION_TOKEN else None,
        region_name=AWS_REGION
    )

    # -------------------------------------------------------------------------
    # Upload
    # -------------------------------------------------------------------------
    logger.info("Uploading CSV to S3...")
    
    s3_client.put_object(
        Bucket=BUCKET_NAME,
        Key=S3_KEY,
        Body=csv_buffer.getvalue(),
        ContentType='text/csv',
        # Optional: add server-side encryption if needed
        # ServerSideEncryption='AES256',
    )

    logger.info("Upload successful ✓")
    logger.info(f"Location: s3://{BUCKET_NAME}/{S3_KEY}")
    logger.info(f"Full URI: https://{BUCKET_NAME}.s3.{AWS_REGION}.amazonaws.com/{S3_KEY}")

except boto3.exceptions.S3UploadFailedError as e:
    logger.error("S3 upload failed", exc_info=True)
    sys.exit(1)
except Exception as e:
    logger.error("Unexpected error during S3 operation", exc_info=True)
    logger.error("Most common causes:")
    logger.error("  • Invalid/expired credentials")
    logger.error("  • No s3:PutObject permission on this bucket/prefix")
    logger.error("  • Bucket does not exist or wrong region")
    logger.error("  • Network/connectivity issue in CI")
    sys.exit(1)