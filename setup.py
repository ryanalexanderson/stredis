from setuptools import setup

setup(name='stredis',
      version='0.1',
      description='Function to stream between stdin, redis streams, and stdout',
      url='http://github.com/ryanalexanderson/stredis',
      author='Ryan Anderson',
      author_email='ryanalexanderson@yahoo.ca',
      license='MIT',
      packages=['stredis'],
      install_requires=['redis',],
      scripts=['stredis'],
      zip_safe=False)