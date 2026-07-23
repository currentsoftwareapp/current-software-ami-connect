import os

from dotenv import load_dotenv

AWS_PROFILE_ENV_VAR_NAME = "AMI_CONNECT__AWS_PROFILE"
AWS_REGION_ENV_VAR_NAME = "AMI_CONNECT__AWS_REGION"

DEFAULT_AWS_REGION = "us-west-2"


# Environment variables from .env file are used by these functions
# .env is the preferred method for configuring those variables during local dev
load_dotenv()


def set_global_aws_profile(aws_profile: str = None):
    """
    Sets the AWS profile to use for the current process. This is used by boto3 to
    determine which AWS credentials to use during local development.

    In production, AWS access is provided via the EC2 instance role.
    """
    if aws_profile is None and AWS_PROFILE_ENV_VAR_NAME not in os.environ:
        raise ValueError(
            f"No AWS profile specified. Set {AWS_PROFILE_ENV_VAR_NAME} in your .env file (copy .env.example to get started)."
        )
    # Overwrite with the provided profile if specified
    if aws_profile is not None:
        os.environ[AWS_PROFILE_ENV_VAR_NAME] = aws_profile
    set_global_aws_region()


def set_global_aws_region(aws_region: str = None):
    """
    Sets the AWS region to use for the current process. This is used by boto3 to
    determine which AWS region to use during local development.

    In production, AWS access is provided via the EC2 instance role.
    """
    if aws_region is not None:
        os.environ[AWS_REGION_ENV_VAR_NAME] = aws_region
    elif AWS_REGION_ENV_VAR_NAME not in os.environ:
        os.environ[AWS_REGION_ENV_VAR_NAME] = DEFAULT_AWS_REGION


def get_global_aws_profile() -> str | None:
    """
    Returns the AWS profile to use for the current process, or None if not set.
    """
    return os.environ.get(AWS_PROFILE_ENV_VAR_NAME)


def get_global_aws_region() -> str | None:
    """
    Returns the AWS region to use for the current process, or None if not set.
    """
    return os.environ.get(AWS_REGION_ENV_VAR_NAME, DEFAULT_AWS_REGION)


def get_global_airflow_site_url() -> str | None:
    """
    Returns the Airflow site URL to use for the current process, or None if not set.
    """
    return os.environ.get("AMI_CONNECT__AIRFLOW_SITE_URL")
