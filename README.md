# Public Editor Consensus Algorithms and Article Publishing

This project is configured to run on AWS Lambda. It listens to an SQS queue
and processes one of these three messages:

 * request_consensus
 * publish_article
 * unpublish_article

The messages include lists of CSV files stored in S3 that the function retrieves for analysis.

Outputs are sent to S3 and a notification message is sent to the SQS queue provided in the
incoming message.

Sample messages are included in directory `test_data_persistent/request_*`.

To enable local testing without requiring AWS credentials, the sample data is stored in `test_output`.

To test:

```bash
cd consensus_and_scoring
PYTHONPATH=. pytest test2/test_consensus_locally.py
PYTHONPATH=. pytest test2/test_publish_article_locally.py
```

Example messages can be retrieved by TagWorks researchers with access to the Tag Batch model in admin.

If you have AWS credentials with permissions to read the S3 buckets in the messages,
you can retrieve the sample data with:

```bash
PYTHONPATH=. pytest test2/manual_test_consensus_with_S3_fetch.py
PYTHONPATH=. pytest test2/manual_test_publish_with_S3_fetch.py
```
