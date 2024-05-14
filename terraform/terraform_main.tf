terraform {
  required_providers {
    aws={
        source = "hashicorp/aws"
        version = "~> 5.0"
    }
  }
  backend "s3" {
    bucket = "blackwater-state"
    key = "state-bucket/terraform.tfstate"
    region = "eu-west-2"
  }
}

provider "aws" {
  region = "eu-west-2"
  access_key = local.envs["aws_access_key_id"]
  secret_key = local.envs["aws_secret_access_key"]
}

data "aws_caller_identity" "current" {
  
}

data "aws_region" "current" {
  
}