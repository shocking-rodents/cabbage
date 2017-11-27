import os
from setuptools import setup


def read(fname):
    return open(os.path.join(os.path.dirname(__file__), fname)).read()

setup(
    name='cabbage',
    version='0.1',
    description='AMQP client and server for RPC.',
    packages=['cabbage'],
    install_requires=read('requirements.txt'),
    long_description=read('README.md'),
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Programming Language :: Python :: 3.6',
        'Topic :: Utilities',
    ],
)
