#!/usr/bin/env python3

from setuptools import setup

setup(
    version=0.1,
    name='aws-s3-cat',
    description='Streamer aws s3',
    author='Celogeek',
    author_email='me@celogeek.com',
    url='https://github.com/celogeek/aws-s3-cat',
    packages=["aws_s3_cat"],
    test_suite='nose.collector',
    entry_points={
        'console_scripts': [
            'aws-s3-cat=aws_s3_cat.__main__:main'
        ]
    }
)
