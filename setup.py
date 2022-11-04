from setuptools import find_packages, setup

NAME = "feast-teradata"
REQUIRES_PYTHON = ">=3.7.0"

setup(
    name=NAME,
    version="0.0.1",
    long_description=open("README.md").read(),
    long_description_content_type="text/markdown",
    python_requires=REQUIRES_PYTHON,
    packages=find_packages(include=["feast_teradata"]),
    install_requires=["feast==0.26.0"],
    license="Apache",
)
