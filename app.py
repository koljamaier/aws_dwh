#!/usr/bin/env python3

from aws_cdk import core
from aws_dwh.emr_stack import UdacityCapstoneStack


app = core.App()
UdacityCapstoneStack(app, "capstone-stack", env={'region': 'us-west-2'})

app.synth()
