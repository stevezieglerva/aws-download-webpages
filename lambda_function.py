import boto3
import time
import datetime
import logging
import structlog
import os
import json
import sys
import requests
import bs4
import re
from S3TextFromLambdaEvent import *
from firehose_helpers import *


def lambda_handler(event, context):
	try:
		aws_request_id = ""
		if context is not None:
			aws_request_id = context.aws_request_id

		print("Started")
		if "text_logging" in os.environ:
			log = structlog.get_logger()
		else:
			log = setup_logging("aws-download-webpages", event, aws_request_id)

		if "async" in event:
			s3 = boto3.resource("s3")
			url = event["url"]
			res = download_page(url.strip())
			print(str(res.status_code) + "-" + url)
			result = {"processing_type" : "async download urls", "url" : url, "status_code" : res.status_code, "length" : len(res.text)}
			log.critical("processed url", result=result)
			filename = re.sub(r"[^a-zA-Z0-9-_]", "_", url) + ".html"
			create_s3_text_file("svz-aws-download-webpages", "output/" + filename, res.text, s3)
			stream_firehose_string("aws-download-webpage", "downloaded\t{}\t{}".format(url, res.status_code))

			return result
		else:
			file_data = get_urls_from_file_text(event)
			files_found = len(file_data)
			urls_found = 0
			for key, value in file_data.items():
				log.critical("read url text file", url_file=key)
				url_list_text = value 
				urls = url_list_text.split("\n")
				urls_found = len(urls)
				for url in urls:
					event["url"] = url
					invoke_self_async(event, context)
			print("Finished")
			result = {"processing_type" : "read_url_files", "files_found" : files_found, "urls_found" : urls_found}
			return result

	except Exception as e:
		print("Exception: "+ str(e))
		log.exception(e)
		raise(e)

	return {"msg" : "Success"}


def setup_logging(lambda_name, lambda_event, aws_request_id):
	logging.basicConfig(
		format="%(message)s",
		stream=sys.stdout,
		level=logging.INFO
	)
	structlog.configure(
		processors=[
			structlog.stdlib.filter_by_level,
			structlog.stdlib.add_logger_name,
			structlog.stdlib.add_log_level,
			structlog.stdlib.PositionalArgumentsFormatter(),
			structlog.processors.TimeStamper(fmt="iso"),
			structlog.processors.StackInfoRenderer(),
			structlog.processors.format_exc_info,
			structlog.processors.UnicodeDecoder(),
			structlog.processors.JSONRenderer()
		],
		context_class=dict,
		logger_factory=structlog.stdlib.LoggerFactory(),
		wrapper_class=structlog.stdlib.BoundLogger,
		cache_logger_on_first_use=True,
	)
	log = structlog.get_logger()
	log = log.bind(aws_request_id=aws_request_id)
	log = log.bind(lambda_name=lambda_name)
	log.critical("started", input_events=json.dumps(lambda_event, indent=3))

	return log
	

def get_urls_from_file_text(event):
	s3 = boto3.resource("s3")
	s3_files = get_files_from_s3_lambda_event(event)
	file_text_data = get_file_text_from_s3_file_urls(s3_files, s3)
	return file_text_data


def download_page(url):
	res = requests.get(url, allow_redirects=True, timeout=10)
	return res


def strip_html(html):
	return ""


def invoke_self_async(event, context):
	log = structlog.get_logger()
	event["async"] = True
	log.warning("invoke_self_async", context=context)
	boto3.client("lambda").invoke(
		FunctionName="aws-download-webpage",
		InvocationType='Event',
		Payload=bytes(json.dumps(event), "utf-8"))
	stream_firehose_string("aws-download-webpage", "async\t{}".format(event["url"]))




