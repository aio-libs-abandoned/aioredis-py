import os.path
import re

from setuptools import find_packages, setup


def read(*parts):
    with open(os.path.join(*parts)) as f:
        return f.read().strip()


def read_version():
    regexp = re.compile(r"^__version__\W*=\W*\"([\d.abrc]+)\"")
    init_py = os.path.join(os.path.dirname(__file__), "aioredis", "__init__.py")
    with open(init_py) as f:
        for line in f:
            match = regexp.match(line)
            if match is not None:
                return match.group(1)
        raise RuntimeError(f"Cannot find version in {init_py}")


classifiers = [
    "License :: OSI Approved :: MIT License",
    "Development Status :: 4 - Beta",
    "Programming Language :: Python",
    "Programming Language :: Python :: 3",
    "Programming Language :: Python :: 3.6",
    "Programming Language :: Python :: 3.7",
    "Programming Language :: Python :: 3 :: Only",
    "Operating System :: POSIX",
    "Environment :: Web Environment",
    "Intended Audience :: Developers",
    "Topic :: Software Development",
    "Topic :: Software Development :: Libraries",
    "Framework :: AsyncIO",
]

setup(
    name="aioredis",
    version=read_version(),
    description="asyncio (PEP 3156) Redis support",
    long_description="\n\n".join((read("README.md"), read("CHANGELOG.md"))),
    long_description_content_type="text/markdown",
    classifiers=classifiers,
    platforms=["POSIX"],
    url="https://github.com/aio-libs/aioredis",
    license="MIT",
    packages=find_packages(exclude=["tests"]),
    install_requires=[
        "async-timeout",
        "typing-extensions",
    ],
    extras_require={
        "hiredis": 'hiredis>=1.0; implementation_name=="cpython"',
    },
    package_data={"aioredis": ["py.typed"]},
    python_requires=">=3.6",
    include_package_data=True,
)
