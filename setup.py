# -*- coding: utf-8 -*-
import os
from setuptools import setup


def read(filename):
    return open(os.path.join(os.path.dirname(__file__), filename)).read()


setup(
    name='cabbage',
    version='0.5',
    description='asyncio-based AMQP client and server for RPC.',
    packages=['cabbage'],
    install_requires=read('requirements.txt'),
    long_description=read('README.md'),
    classifiers=[
        'Development Status :: 4 - Beta',
        'Programming Language :: Python :: 3.6',
        'Topic :: Utilities',
    ],
)
