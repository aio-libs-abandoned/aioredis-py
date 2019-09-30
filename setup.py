import re
import os.path
import sys
import platform
from setuptools import setup, find_packages


install_requires = ['async-timeout']
if platform.python_implementation() == 'CPython':
    install_requires.append('hiredis')

PY_VER = sys.version_info

if PY_VER < (3, 5):
    raise RuntimeError("aioredis doesn't support Python version prior 3.5")


def read(*parts):
    with open(os.path.join(*parts), 'rt') as f:
        return f.read().strip()


def read_version():
    regexp = re.compile(r"^__version__\W*=\W*'([\d.abrc]+)'")
    init_py = os.path.join(os.path.dirname(__file__),
                           'aioredis', '__init__.py')
    with open(init_py) as f:
        for line in f:
            match = regexp.match(line)
            if match is not None:
                return match.group(1)
        raise RuntimeError('Cannot find version in {}'.format(init_py))


classifiers = [
    'License :: OSI Approved :: MIT License',
    'Development Status :: 4 - Beta',
    'Programming Language :: Python',
    'Programming Language :: Python :: 3',
    'Programming Language :: Python :: 3.5',
    'Programming Language :: Python :: 3.6',
    'Programming Language :: Python :: 3.7',
    'Programming Language :: Python :: 3 :: Only',
    'Operating System :: POSIX',
    'Environment :: Web Environment',
    'Intended Audience :: Developers',
    'Topic :: Software Development',
    'Topic :: Software Development :: Libraries',
    'Framework :: AsyncIO',
]

setup(name='aioredis',
      version=read_version(),
      description=("asyncio (PEP 3156) Redis support"),
      long_description="\n\n".join((read('README.rst'), read('CHANGES.txt'))),
      classifiers=classifiers,
      platforms=["POSIX"],
      author="Alexey Popravka",
      author_email="alexey.popravka@horsedevel.com",
      url="https://github.com/aio-libs/aioredis",
      license="MIT",
      packages=find_packages(exclude=["tests"]),
      install_requires=install_requires,
      include_package_data=True,
      )
