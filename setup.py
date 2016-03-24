#!/usr/bin/env python3

from setuptools import setup

setup(name='postgres_kernel',
	version='0.1.0',
	description='PostgreSQL kernel for Jupyter/IPython',
	url='http://github.com/mreithub/postgres_kernel',
	author='Manuel Reithuber',
	author_email='manuel@reithuber.net',
	license='MIT',
	packages=['postgres_kernel'],
	zip_safe=False)
