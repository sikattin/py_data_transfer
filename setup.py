# -*- coding: utf-8 -*-


from setuptools import setup, find_packages


with open('README.rst') as f:
    readme = f.read()

with open('LICENSE') as f:
    license = f.read()

setup(
    name='datatransfer',
    version='1.0',
    description='the data transfering module to use scp.',
    long_description=None,
    author='shikano takeki',
    author_email='shikano.takeki@nexon.co.jp',
    url=None,
    license='MIT',
    packages=find_packages(exclude=('docs',))
)

