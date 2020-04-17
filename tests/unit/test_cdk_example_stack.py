import json
import pytest

from aws_cdk import core
from cdk_example.cdk_example_stack import CdkExampleStack


def get_template():
    app = core.App()
    CdkExampleStack(app, "cdk-example")
    return json.dumps(app.synth().get_stack("cdk-example").template)


def test_sqs_queue_created():
    assert("AWS::SQS::Queue" in get_template())


def test_sns_topic_created():
    assert("AWS::SNS::Topic" in get_template())
