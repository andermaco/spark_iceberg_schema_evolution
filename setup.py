from setuptools import setup, find_packages

setup(
    name="schema_evolution",
    version="0.1.0",
    packages=find_packages(),
    install_requires=[        
        "python~=3.10",
        "pyspark~=3.3.0",
        "pandas~=1.3.2",
        "boto3~=1.36.3",
        "awswrangler~=3.11.0",
        "faker~=30.8.1"
    ]
)