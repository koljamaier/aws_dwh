#!/usr/bin/env python3

from aws_cdk import core

from cdk_example.cdk_example_stack import CdkExampleStack
from cdk_example.emr_stack import EmrTestStack


app = core.App()
# CdkExampleStack(app, "cdk-example", env={'region': 'us-west-2'})
EmrTestStack(app, "cdk-example", env={'region': 'us-west-2'})

app.synth()
