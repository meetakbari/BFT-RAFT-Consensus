#!/usr/bin/env python

from sys import version_info as version
from setuptools import setup, find_packages

exclude = ['docs', 'tests']

if version.major < 3 or (version.major == 3 and version.minor < 4):
    exclude += ['raft.server']
    entry_points = {}
else:
    entry_points = {'console_scripts': ['raftd=raft.server.main:run']}

setup(name='bft-raft',
      version='1.0',
      description='Implementation of BFT-RAFT Consensus in Python',
      author='Simone Accascina of simonacca/raft',
      url='https://github.com/simonacca/raft',
      keywords='distributed consensus raft',
      # packages = ['raft.client'],
      packages=find_packages(exclude=exclude),
      install_requires=['msgpack-python'],
      entry_points=entry_points,
      )
