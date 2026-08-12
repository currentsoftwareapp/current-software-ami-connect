terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 6.18"
    }
    tls = {
      source  = "hashicorp/tls"
      version = "~> 4.0"
    }
  }

  required_version = ">= 1.4.0"
}

provider "aws" {
  region  = var.aws_region
  profile = var.aws_profile

  default_tags {
    tags = {
      Name    = var.ami_connect_tag
      Project = var.ami_connect_tag
    }
  }
}

# Route 53 query logging requires the CloudWatch Logs log group to live in
# us-east-1, regardless of where the rest of the infrastructure runs.
provider "aws" {
  alias   = "us_east_1"
  region  = "us-east-1"
  profile = var.aws_profile

  default_tags {
    tags = {
      Name    = var.ami_connect_tag
      Project = var.ami_connect_tag
    }
  }
}

provider "tls" {}
