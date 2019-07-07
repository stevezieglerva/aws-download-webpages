import unittest
import time
import boto3
from lambda_function import *
import json



s3_event = {
  "Records": [
    {
      "eventVersion": "2.0",
      "eventSource": "aws:s3",
      "awsRegion": "us-east-1",
      "eventTime": "1970-01-01T00:00:00.000Z",
      "eventName": "ObjectCreated:Put",
      "userIdentity": {
        "principalId": "EXAMPLE"
      },
      "requestParameters": {
        "sourceIPAddress": "127.0.0.1"
      },
      "responseElements": {
        "x-amz-request-id": "EXAMPLE123456789",
        "x-amz-id-2": "EXAMPLE123/5678abcdefghijklambdaisawesome/mnopqrstuvwxyzABCDEFGH"
      },
      "s3": {
        "s3SchemaVersion": "1.0",
        "configurationId": "testConfigRule",
        "bucket": {
          "name": "example-bucket",
          "ownerIdentity": {
            "principalId": "EXAMPLE"
          },
          "arn": "arn:aws:s3:::svz-aws-download-webpages"
        },
        "object": {
          "key": "tests/input/url_list.txt",
          "size": 1024,
          "eTag": "0123456789abcdef0123456789abcdef",
          "sequencer": "0A1B2C3D4E5F678901"
        }
      }
    }
  ]
}

class IntegrationTests(unittest.TestCase):

	def test_lambda_handler__given_valid_inputs__runs_without_exceptions(self):
		# Act
		result = lambda_handler(s3_event, None)
		print(json.dumps(result, indent=3))
		
		# Assert
		self.assertEqual(result["files_found"], 1)
		self.assertEqual(result["urls_found"], 2)





if __name__ == '__main__':
	unittest.main()		


