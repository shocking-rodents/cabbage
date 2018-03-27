# -*- coding: utf-8 -*-
import os
from setuptools import setup


def read(filename):
    return open(os.path.join(os.path.dirname(__file__), filename)).read()


def get_version():
    """Get version from the package without actually importing it."""
    init = read('cabbage/__init__.py')
    for line in init.split('\n'):
        if line.startswith('__version__'):
            return eval(line.split('=')[1])


setup(
    name='cabbage',
    version=get_version(),
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
