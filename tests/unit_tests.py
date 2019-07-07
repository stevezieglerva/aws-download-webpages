import unittest
import time
import boto3
from lambda_function import *
import json


class TestMethods(unittest.TestCase):
	def test_sample_test(self):
		# Arrange
		self.assertEqual(1, 1)


if __name__ == '__main__':
	unittest.main()		


