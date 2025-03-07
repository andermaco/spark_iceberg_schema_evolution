import pytest
import os


def test_aws_config_creation(aws_config):
    assert aws_config.glue_database == "test_db"
    assert aws_config.glue_table == "test_table"
    assert aws_config.s3_path == "s3://test-bucket/test-path"
    assert aws_config.iam_role == "arn:aws:iam::123456789012:role/test-role"
    assert aws_config.workgroup == "test_workgroup"
    assert aws_config.workgroup_s3_path == "s3://test-workgroup"
    assert aws_config.region == "eu-west-1"
