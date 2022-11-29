from setuptools import find_packages, setup

with open('requirements.txt') as f:
    required = f.read().splitlines()

setup(
    name="feast-teradata",
    version="0.0.1",
    long_description=open("README.md").read(),
    long_description_content_type="text/markdown",
    python_requires=">=3.7",
    packages=find_packages(include=["feast_teradata"]),
    install_requires=required,
    tests_require=['pytest==6.2.4'],
    license="Teradata",
)
