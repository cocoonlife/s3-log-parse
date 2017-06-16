from setuptools import setup, find_packages
import os

# allow setup.py to be run from any path
os.chdir(os.path.normpath(os.path.join(os.path.abspath(__file__), os.pardir)))

setup(
    name='s3-log-parse',
    version="0.1.0",
    description="Tool for procesing AWS S3 access logs",
    url="https://github.com/cocoonlife/s3-log-parse",
    author="Rob Clarke",
    author_email='rob.clarke@cocoon.life',
    packages=find_packages(),
    entry_points={
        'console_scripts': [
            's3logparse=s3logparse.cli:main',
        ]
    },
    classifiers=[
        'Programming Language :: Python',
        'Development Status :: 4 - Beta'
    ],
    setup_requires=['nose>=1.0', 'flake8>=3.3.0'],
    install_requires=['pytz'],
    test_suite="tests",
)
