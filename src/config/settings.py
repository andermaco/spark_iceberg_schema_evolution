import os
from dataclasses import dataclass


@dataclass(frozen=True)
class AWSConfig:
    """AWS configuration settings."""
    print(os.environ.get("AWS_S3_PATH"))
    s3_path: str = os.getenv("AWS_S3_PATH")
    glue_database: str = os.getenv("GLUE_DATABASE")
    glue_table: str = os.getenv("GLUE_TABLE")
    iam_role: str = os.getenv("AWS_IAM_ROLE")
    workgroup: str = os.getenv("WORKGROUP")
    workgroup_s3_path: str = os.getenv("WORKGROUP_S3_PATH")
    region: str = os.getenv("AWS_REGION")

    def __post_init__(self):
        self.validate()

    def validate(self) -> None:
        """
        Validate that all required settings are set.
        """
        missing = [
            field for field, value in self.__dict__.items()
            if value is None or value == ""
        ]
        if missing:
            raise ValueError(
                f"Missing required AWS configuration: {', '.join(missing)}"
            )


# Create a singleton instance
# aws_config = AWSConfig()
