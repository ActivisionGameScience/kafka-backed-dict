#!/usr/bin/env python

from setuptools import setup, find_packages
import os

#from pip.req import parse_requirements
#import pip
#requirements = [
#    str(req.req) for req in parse_requirements('requirements.txt', session=pip.download.PipSession())
#]

setup(name='kafka-backed-dict',
      version=os.getenv('VERSION'),
      description="A persistent key-value store (in-mem or rocksdb) backed by kafka",
      author='Spencer Stirling',
      packages=[
          'kafka_backed_dict',
      ],
      include_package_data=True,
      install_requires=[
          'confluent-kafka',
      ],
     )
