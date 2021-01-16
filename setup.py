#!/usr/bin/env python
from setuptools import setup
from os.path import dirname, join

import market_maker


here = dirname(__file__)


setup(name='TomBot',
      version=market_maker.__version__,
      description='Market making bot for BitMEX API',
      url='https://github.com/ifer21/TomBot',
      long_description=open(join(here, 'README.md')).read(),
      long_description_content_type='text/markdown',
      author='Samuel Reed',
      author_email='',
      install_requires=[
          'requests',
          'websocket-client',
          'future'
      ],
      packages=['market_maker', 'market_maker.auth', 'market_maker.utils', 'market_maker.ws'],
      entry_points={
          'console_scripts': ['tombot = market_maker:run']
      }
      )
