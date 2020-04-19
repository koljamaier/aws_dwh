import boto3
import os

boto = boto3.Session()
glue = boto.client("glue")


def lambda_handler(event, context):
    """"This lambda handler triggers a glue crawler"""

    crawler_name = os.environ["crawlerName"]


    if crawler_name:
        try:
            glue.start_crawler(Name=crawler_name)
        except Exception as e:
            print(e)
            print('Error starting crawler')
            raise e
    else:
        raise ValueError("crawlerName must be defined")

