import sys
import os
import traceback
import logging
import structlog
from urllib.parse import urlparse


def get_files_from_s3_lambda_event(event):
	"""Returns an dict of the bucket/keys for the S3 files listed in a Lambda S3 event"""
	files_found = {}

	if "Records" not in event:
		raise ValueError("Records key not in event")
	count = 0
	for record in event["Records"]:
		count = count + 1
		key = record["s3"]["object"]["key"]
		bucket_arn = record["s3"]["bucket"]["arn"]
		bucket_name = get_bucket_name_from_arn(bucket_arn)
		file_url = get_bucket_file_url(bucket_name, key)
		files_found[file_url] = {"bucket" : bucket_name, "key" : key}
	return files_found


def get_file_text_from_s3_file_urls(s3_file_url_dict, s3_boto):
	"""Returns the text of the S3 files based on their URLs"""
	file_texts = {}
	for s3_url, s3_ref in s3_file_url_dict.items():
		if "bucket" not in s3_ref:
			raise ValueError("bucket key not in s3_ref: " + str(s3_ref))
		if "key" not in s3_ref:
			raise ValueError("key key not in s3_ref: " + str(s3_ref))
		bucket_name = s3_ref["bucket"]
		key = s3_ref["key"]
		obj = s3_boto.Object(bucket_name, key)
		file_contents = obj.get()['Body'].read().decode('utf-8') 	
		file_texts[s3_url] = file_contents
	return file_texts


def get_bucket_file_url(bucket, key):
	"""Returns the a URL to an S3 object given a bucket name and"""
	#https://s3.amazonaws.com/link-checker/2018-05-27-235740.txt
	file_url = "https://s3.amazonaws.com/" + bucket + "/" + key
	return file_url


def get_bucket_name_from_arn(bucket_arn):
	"""Returns the bucket from an S3 ojbect arn"""
	bucket_name = bucket_arn.rsplit(":", 1)[-1]
	return bucket_name


def get_bucket_name_from_url(file_url):
	"""Returns the bucket name from an S3 ojbect URL"""
	parts = urlparse(file_url)
	paths = parts.path.split("/")
	return paths[1]

def get_key_from_url(file_url):
	"""Returns the key from an S3 ojbect URL"""	
	parts = urlparse(file_url)
	bucket_name = get_bucket_name_from_url(file_url)
	key = parts.path.replace("/" + bucket_name + "/", "")
	return key

def create_s3_text_file(bucket, key, file_text, s3_boto):
	file_text_binary = bytes(file_text, 'utf-8')
	object = s3_boto.Object(bucket, key)
	response = object.put(Body=file_text_binary)
	return response

