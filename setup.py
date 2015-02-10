#!/usr/bin/env python
# -*- coding: utf-8 -*-

try:
    import sys
    reload(sys).setdefaultencoding("UTF-8")
except:
    pass

try:
    from setuptools import setup, find_packages
except ImportError:
    print 'Please install or upgrade setuptools or pip to continue.'
    sys.exit(1)

setup(name='gnss_analysis',
      description='Software-in-the-loop testing for libswiftnav RTK filters',
      version='0.22',
      author='Swift Navigation',
      author_email='ian@swiftnav.com',
      maintainer='Ian Horn',
      maintainer_email='ian@swiftnav.com',
      # url='https://github.com/imh/gnss-analysis',
      keywords='',
      classifiers=['Intended Audience :: Developers',
                   'Intended Audience :: Science/Research',
                   'Operating System :: POSIX :: Linux',
                   'Operating System :: MacOS :: MacOS X',
                   'Programming Language :: Python',
                   'Topic :: Scientific/Engineering :: Interface Engine/Protocol Translator',
                   'Topic :: Software Development :: Libraries :: Python Modules',
                   'Programming Language :: Python :: 2.7'
                   ],
      packages=find_packages(),
      platforms="Linux,Windows,Mac",
      py_modules=['sbp_log_analysis'],
      use_2to3=False,
      zip_safe=False)
