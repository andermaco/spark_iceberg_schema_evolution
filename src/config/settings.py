import os
import boto3
from dataclasses import dataclass
from typing import Optional

def singleton(class_):
    instances = {}
    
    def get_instance(*args, **kwargs):
        if class_ not in instances:
            instances[class_] = class_(*args, **kwargs)
        return instances[class_]
    
    return get_instance

# @singleton pattern and @dataclass pattern conflict with each other
# so we need to not use @dataclass

# @dataclass(frozen=False)
@singleton
class AWSConfig:
    """AWS configuration settings."""

    def __init__(self):
        self._s3_path: str = os.getenv("AWS_S3_PATH")
        self._glue_database: str = os.getenv("GLUE_DATABASE")
        self._glue_table: str = os.getenv("GLUE_TABLE")
        self._iam_role: str = os.getenv("AWS_IAM_ROLE")
        self._workgroup: str = os.getenv("WORKGROUP")
        self._workgroup_s3_path: str = os.getenv("WORKGROUP_S3_PATH")
        self._region: str = os.getenv("AWS_REGION", "us-east-1")  # Default region
        self._catalog_id: str = os.getenv("AWS_CATALOG_ID")
        
        # Initialize boto3 session
        self._session: Optional[boto3.Session] = None
        self._init_boto3_session()
        
        # self.validate()
    
    def _init_boto3_session(self) -> None:
        """Initialize boto3 session with credentials."""
        try:
            self._session = boto3.Session(
                region_name=self._region
            )
            # Test the session by getting caller identity
            sts = self._session.client('sts')
            sts.get_caller_identity()
        except Exception as e:
            raise RuntimeError(f"Failed to initialize AWS session: {str(e)}")

    
    def __post_init__(self):
        self.validate()

    def validate(self) -> None:
        """
        Validate that all required settings are set.
        """
        missing = [
            field for field, value in self.__dict__.items()
            if value is None or value == "" and not field.startswith('_')
        ]
        if missing:
            raise ValueError(
                f"Missing required AWS configuration: {', '.join(missing)}"
            )

    @property
    def s3_path(self) -> str:
        return self._s3_path
    
    @s3_path.setter
    def s3_path(self, value):
        self._s3_path = value

    @property
    def glue_database(self) -> str:
        return self._glue_database
    
    @glue_database.setter
    def glue_database(self, value):
        self._glue_database = value

    @property
    def glue_table(self) -> str:
        return self._glue_table
    
    @glue_table.setter
    def glue_table(self, value):
        self._glue_table = value

    @property
    def iam_role(self) -> str:
        return self._iam_role
    
    @iam_role.setter
    def iam_role(self, value):
        self._iam_role = value

    @property
    def workgroup(self) -> str:
        return self._workgroup
    
    @workgroup.setter
    def workgroup(self, value):
        self._workgroup = value

    @property
    def workgroup_s3_path(self) -> str:
        return self._workgroup_s3_path
    
    @workgroup_s3_path.setter
    def workgroup_s3_path(self, value):
        self._workgroup_s3_path = value

    @property
    def region(self) -> str:
        return self._region
    
    @region.setter
    def region(self, value):
        self._region = value

    @property
    def catalog_id(self) -> str:
        return self._catalog_id
    
    @catalog_id.setter
    def catalog_id(self, value):
        self._catalog_id = value

    @property
    def session(self) -> boto3.Session:
        return self._session

# # Usage example:
# def example_usage():
#     # Both variables will reference the same instance
#     config1 = AWSConfig()
#     config2 = AWSConfig()   
#     print(config1 == config2)  # Output: True

# if __name__ == "__main__":
#     example_usage()
