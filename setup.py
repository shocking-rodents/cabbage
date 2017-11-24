import os
from setuptools import setup, find_packages


def read(fname):
    return open(os.path.join(os.path.dirname(__file__), fname)).read()

setup(
    name='cabbage',
    version="0.1",
    description='AMQP client and server for RPC.',
    packages=find_packages(),
    install_requires=read('requirements.txt'),
    long_description=read('README.md'),
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Topic :: Utilities",
    ],
)
