#!/usr/bin/env python3

import os

from aws_cdk import App, Environment, Tags
from stacks.lgsf_stack import LgsfStack

valid_environments = ("development",)

app_wide_context = {}
if dc_env := os.environ.get("DC_ENVIRONMENT"):
    app_wide_context["dc-environment"] = dc_env

app = App(context=app_wide_context)

env = Environment(account=os.getenv("CDK_DEFAULT_ACCOUNT"), region="eu-west-2")

# Set the DC Environment early on. This is important to be able to conditionally
# change the stack configurations
dc_environment = app.node.try_get_context("dc-environment") or None
assert dc_environment in valid_environments, (
    f"context `dc-environment` must be one of {valid_environments}"
)

# Create the LGSF stack
LgsfStack(
    app,
    f"LGSF-{dc_environment.title()}",
    env=env,
    description=f"LGSF Local Government Scraper Framework - {dc_environment.title()}",
)

Tags.of(app).add("dc-product", "LGSF")
Tags.of(app).add("dc-environment", dc_environment)

app.synth()
