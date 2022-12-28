from setuptools import find_packages, setup

with open('requirements.txt') as f:
    required = f.read().splitlines()

setup(
    name="feast-teradata",
    version="0.1",
    author="Teradata",
    long_description=open("README.md").read(),
    long_description_content_type="text/markdown",
    python_requires=">=3.7",
    packages=find_packages(exclude=('tests',)),
    install_requires=required,
    tests_require=['pytest==6.2.4'],
    license_files=['LICENSE.txt', 'LICENSE-3RD-PARTY.txt'],
    entry_points={'console_scripts': [
        'create_repo = feast_teradata.cli:main',
    ]},
)
