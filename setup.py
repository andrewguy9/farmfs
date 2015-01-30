from setuptools import setup

setup(name='farmfs',
      version='0.1',
      description='tool which de-duplicates files in a filesystem by checksum.',
      url='http://github.com/andrewguy9/farmfs',
      author='andrew thomson',
      author_email='athomsonguy@gmail.com',
      license='MIT',
      packages=['farmfs'],
      install_requires = ['prototypes'],
      entry_points = {
        'console_scripts': [
          'farmfs = farmfs.farmui:main',
          'farmdbg = farmfs.farmdbg:main',
          ],
      },
      zip_safe=False)
