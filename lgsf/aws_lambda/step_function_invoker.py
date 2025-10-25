"""
Utility for invoking AWS Step Functions from local commands.

This module provides functionality to trigger Step Function executions
from the local CLI, allowing scrapers to be run on AWS infrastructure
with the same interface as local execution.

The Step Function ARN is auto-discovered using multiple strategies:
1. Deterministic name based on CDK convention
2. Pattern matching across all state machines
3. CloudFormation stack outputs
"""

import json
import os
from datetime import datetime
from typing import Optional


class StepFunctionInvoker:
    """
    Invokes AWS Step Functions for scraper orchestration.

    This class handles the interaction with AWS Step Functions API
    to trigger scraper executions remotely. It auto-discovers the
    Step Function ARN using multiple fallback strategies.
    """

    def __init__(self, state_machine_arn: Optional[str] = None):
        """
        Initialize the Step Function invoker.

        Args:
            state_machine_arn: Optional ARN of the Step Function. If not provided,
                             will be auto-discovered based on environment.
        """
        self._state_machine_arn = state_machine_arn
        self._client = None
        self._sts_client = None

    @property
    def state_machine_arn(self) -> str:
        """
        Get the Step Function ARN, auto-discovering it if not explicitly set.

        Returns:
            str: The Step Function ARN

        Raises:
            ValueError: If ARN cannot be determined
        """
        if self._state_machine_arn:
            return self._state_machine_arn

        # Auto-discover the ARN
        self._state_machine_arn = self._discover_step_function_arn()
        return self._state_machine_arn

    def _discover_step_function_arn(self) -> str:
        """
        Auto-discover the Step Function ARN using multiple strategies.

        Tries in order:
        1. Deterministic name: lgsf-{environment}-stepfn-scraper-orchestration
        2. Pattern matching: Find state machines matching the naming pattern
        3. CloudFormation stack outputs (if available)

        Returns:
            str: The discovered Step Function ARN

        Raises:
            ValueError: If ARN cannot be discovered
        """
        # Get environment from DC_ENVIRONMENT or default to production
        environment = os.environ.get("DC_ENVIRONMENT", "production")

        # Get AWS account ID and region
        account_id = self._get_account_id()
        region = self._get_region()

        # Strategy 1: Try deterministic name (for newly deployed stacks)
        state_machine_name = f"lgsf-{environment}-stepfn-scraper-orchestration"
        arn = f"arn:aws:states:{region}:{account_id}:stateMachine:{state_machine_name}"

        try:
            self.client.describe_state_machine(stateMachineArn=arn)
            return arn
        except self.client.exceptions.StateMachineDoesNotExist:
            pass  # Try next strategy
        except Exception:
            pass  # Try next strategy

        # Strategy 2: List all state machines and find by pattern
        try:
            arn = self._find_state_machine_by_pattern(environment)
            if arn:
                return arn
        except Exception:
            pass  # Try next strategy

        # Strategy 3: Try to get from CloudFormation stack outputs
        try:
            arn = self._find_state_machine_from_cfn(environment)
            if arn:
                return arn
        except Exception:
            pass

        # All strategies failed
        raise ValueError(
            f"Could not find Step Function for environment '{environment}' in {region}.\n"
            f"Tried:\n"
            f"  1. Deterministic name: {state_machine_name}\n"
            f"  2. Pattern matching in list_state_machines\n"
            f"  3. CloudFormation stack outputs\n\n"
            f"Make sure the CDK stack is deployed for environment '{environment}'.\n"
            f"Deploy with: cd cdk && cdk deploy --context dc-environment={environment}"
        )

    def _find_state_machine_by_pattern(self, environment: str) -> Optional[str]:
        """
        Find state machine by listing all and matching on name pattern.

        Looks for state machines with names containing:
        - 'lgsf' (case insensitive)
        - 'scraper' OR 'orchestration' (case insensitive)

        Args:
            environment: The environment name

        Returns:
            str: ARN if found, None otherwise
        """
        try:
            # List all state machines (with pagination)
            state_machines = []
            next_token = None

            while True:
                if next_token:
                    response = self.client.list_state_machines(nextToken=next_token)
                else:
                    response = self.client.list_state_machines()

                state_machines.extend(response.get("stateMachines", []))
                next_token = response.get("nextToken")

                if not next_token:
                    break

            # Find matching state machines
            candidates = []
            for sm in state_machines:
                name = sm["name"].lower()

                # Must contain 'lgsf'
                if "lgsf" not in name:
                    continue

                # Should contain 'scraper' or 'orchestration'
                if "scraper" not in name and "orchestration" not in name:
                    continue

                # Preferably contains the environment name
                # But don't require it in case of naming differences
                candidates.append(
                    {
                        "sm": sm,
                        "has_env": environment.lower() in name,
                        "has_stepfn": "stepfn" in name,
                    }
                )

            if not candidates:
                return None

            # Sort candidates by preference:
            # 1. Has environment name
            # 2. Has 'stepfn' in name
            candidates.sort(key=lambda x: (x["has_env"], x["has_stepfn"]), reverse=True)

            return candidates[0]["sm"]["stateMachineArn"]

        except Exception:
            return None

    def _find_state_machine_from_cfn(self, environment: str) -> Optional[str]:
        """
        Try to find the state machine ARN from CloudFormation stack outputs.

        Args:
            environment: The environment name

        Returns:
            str: ARN if found, None otherwise
        """
        try:
            import boto3

            cfn = boto3.client("cloudformation")

            # Try common stack naming patterns
            stack_names = [
                f"LGSFStack-{environment}",
                f"lgsf-{environment}",
                f"LGSF-{environment}",
            ]

            for stack_name in stack_names:
                try:
                    response = cfn.describe_stacks(StackName=stack_name)
                    stacks = response.get("Stacks", [])

                    if stacks:
                        outputs = stacks[0].get("Outputs", [])

                        # Look for output with 'StateMachine' or 'StepFunction' in key
                        for output in outputs:
                            key = output.get("OutputKey", "")
                            if (
                                "statemachine" in key.lower()
                                or "stepfunction" in key.lower()
                            ):
                                if (
                                    "scraper" in key.lower()
                                    or "orchestration" in key.lower()
                                ):
                                    return output.get("OutputValue")
                except Exception:
                    continue

            return None

        except Exception:
            return None

    def get_s3_report_by_key(self, bucket: str, key: str) -> Optional[dict]:
        """
        Fetch the run report from S3 using the exact S3 key.

        Args:
            bucket: S3 bucket name containing reports
            key: The exact S3 key (path) to the report

        Returns:
            dict: Report data if found, None otherwise
        """
        try:
            import boto3

            s3 = boto3.client("s3")

            # Fetch the report directly using the exact key
            report_response = s3.get_object(Bucket=bucket, Key=key)
            report_data = json.loads(report_response["Body"].read())
            return report_data

        except Exception as e:
            # Report doesn't exist yet or other error
            return None

    def _get_account_id(self) -> str:
        """
        Get the AWS account ID for the current credentials.

        Returns:
            str: AWS account ID
        """
        if not self._sts_client:
            try:
                import boto3
            except ImportError:
                raise ImportError(
                    "boto3 is required for AWS Step Function invocation. "
                    "Install it with: pip install boto3"
                )
            self._sts_client = boto3.client("sts")

        return self._sts_client.get_caller_identity()["Account"]

    def _get_region(self) -> str:
        """
        Get the AWS region from session or environment.

        Returns:
            str: AWS region
        """
        # Try environment variable first
        region = os.environ.get("AWS_REGION") or os.environ.get("AWS_DEFAULT_REGION")

        if not region:
            # Fall back to boto3 session default
            try:
                import boto3

                session = boto3.session.Session()
                region = session.region_name
            except ImportError:
                raise ImportError(
                    "boto3 is required for AWS Step Function invocation. "
                    "Install it with: pip install boto3"
                )

        if not region:
            raise ValueError(
                "Could not determine AWS region. Set AWS_REGION or AWS_DEFAULT_REGION "
                "environment variable, or configure AWS CLI with 'aws configure'."
            )

        return region

    @property
    def client(self):
        """Lazy load boto3 client only when needed."""
        if self._client is None:
            try:
                import boto3
            except ImportError:
                raise ImportError(
                    "boto3 is required for AWS Step Function invocation. "
                    "Install it with: pip install boto3"
                )
            self._client = boto3.client("stepfunctions")
        return self._client

    def invoke(
        self,
        council_ids: Optional[str] = None,
        all_councils: bool = False,
        execution_name: Optional[str] = None,
    ) -> dict:
        """
        Start a Step Function execution.

        Args:
            council_ids: Comma-separated council IDs to scrape (e.g., "ABE,XYZ")
            all_councils: Whether to run all councils
            execution_name: Optional custom name for the execution

        Returns:
            dict: Response from Step Functions API containing execution ARN and start date

        Raises:
            ValueError: If neither council_ids nor all_councils is specified
        """
        if not council_ids and not all_councils:
            raise ValueError(
                "Either council_ids or all_councils must be specified for AWS execution"
            )

        # Build the input payload for the Step Function
        input_payload = {}

        if council_ids:
            input_payload["council_ids"] = council_ids

        if all_councils:
            input_payload["all_councils"] = True

        # Generate execution name if not provided
        if not execution_name:
            timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
            if council_ids:
                # Use first council ID in name for single council runs
                first_council = council_ids.split(",")[0]
                execution_name = f"scraper-{first_council}-{timestamp}"
            else:
                execution_name = f"scraper-all-{timestamp}"

        # Start the execution
        response = self.client.start_execution(
            stateMachineArn=self.state_machine_arn,
            name=execution_name,
            input=json.dumps(input_payload),
        )

        return {
            "execution_arn": response["executionArn"],
            "start_date": response["startDate"].isoformat(),
            "execution_name": execution_name,
            "input": input_payload,
            "state_machine_arn": self.state_machine_arn,
        }

    def get_execution_status(self, execution_arn: str) -> dict:
        """
        Get the status of a Step Function execution.

        Args:
            execution_arn: The ARN of the execution to check

        Returns:
            dict: Execution details including status, start/stop times, and output
        """
        response = self.client.describe_execution(executionArn=execution_arn)

        result = {
            "execution_arn": response["executionArn"],
            "status": response["status"],
            "start_date": response["startDate"].isoformat(),
            "name": response.get("name"),
        }

        if "stopDate" in response:
            result["stop_date"] = response["stopDate"].isoformat()

        if "output" in response:
            result["output"] = json.loads(response["output"])

        if "error" in response:
            result["error"] = response["error"]

        if "cause" in response:
            result["cause"] = response["cause"]

        return result

    def wait_for_execution(self, execution_arn: str, poll_interval: int = 5) -> dict:
        """
        Wait for a Step Function execution to complete, polling for status.

        Args:
            execution_arn: The ARN of the execution to wait for
            poll_interval: How often to poll for status (in seconds)

        Returns:
            dict: Final execution status

        Yields:
            dict: Status updates during execution
        """
        import time

        while True:
            status = self.get_execution_status(execution_arn)

            # Yield current status
            yield status

            # Check if execution is complete
            if status["status"] in ["SUCCEEDED", "FAILED", "TIMED_OUT", "ABORTED"]:
                return status

            # Wait before next poll
            time.sleep(poll_interval)

    def get_s3_report(self, bucket: str, execution_id: str) -> Optional[dict]:
        """
        Fetch the run report from S3 for a specific execution.

        Args:
            bucket: S3 bucket name containing reports
            execution_id: The execution ID (short ID from execution ARN)

        Returns:
            dict: Report data if found, None otherwise
        """
        try:
            import boto3

            s3 = boto3.client("s3")

            # List objects in run-reports/ prefix
            response = s3.list_objects_v2(Bucket=bucket, Prefix="run-reports/")

            if "Contents" not in response:
                return None

            # Find the report matching this execution ID
            for obj in response["Contents"]:
                key = obj["Key"]
                # Report format: run-reports/run-report-{timestamp}-{execution_id}.json
                if execution_id[:8] in key:  # Match first 8 chars of execution ID
                    # Fetch the report
                    report_response = s3.get_object(Bucket=bucket, Key=key)
                    report_data = json.loads(report_response["Body"].read())
                    return report_data

            return None

        except Exception:
            return None
