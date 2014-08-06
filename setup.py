from setuptools import setup

setup(name='farmfs',
      version='0.1',
      description='tool which de-duplicates files in a filesystem by checksum.',
      url='http://github.com/andrewguy9/farmfs',
      author='andrew thomson',
      author_email='athomsonguy@gmail.com',
      license='MIT',
      packages=['farmfs'],
      scripts=['bin/farmfs'],
      zip_safe=False)
