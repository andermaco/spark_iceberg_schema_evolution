from setuptools import setup, find_packages

setup(
    name="schema_evolution",
    version="0.1.0",
    packages=find_packages(),
    install_requires=[        
        "python~=3.10",
        "pyspark==3.5.4",
        "pandas~=1.3.2",
        "boto3~=1.36.3",
        "awswrangler~=3.11.0",
        "faker~=30.8.1"
    ],
    extras_require={
        'test': [
            'pytest>=7.0.0',
            'pytest-cov>=4.0.0',
            'pytest-mock>=3.10.0',
        ],
    },
)