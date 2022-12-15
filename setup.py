#!/usr/bin/env python
import sys
from setuptools import setup
from setuptools.command.test import test as TestCommand

requires = ['docopt', 'delnone', 'future', 'safeoutput>=2.0', 'filetype==1.0.6', "S3Lib>=1.6.0", 'tqdm']

test_requires = ['tox', 'pytest==4.6.8', 'tabulate']

setup(
    name='farmfs',
    version='0.8.6',
    author='Andrew Thomson',
    author_email='athomsonguy@gmail.com',
    packages=['farmfs'],
    install_requires = requires,
    entry_points = {
      'console_scripts': [
        'farmfs = farmfs.ui:ui_main',
        'farmdbg = farmfs.ui:dbg_main',
        ],
    },
    url='http://github.com/andrewguy9/farmfs',
    license='MIT',
    description='tool which de-duplicates files in a filesystem by checksum.',
    long_description_content_type='text/markdown',
    long_description=open('README.md').read(),
    scripts=['bin/snap.sh'],
    classifiers=[
        'Development Status :: 4 - Beta',
        'License :: OSI Approved :: MIT License',
        'Topic :: System :: Filesystems',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 2.7',
    ],
)
