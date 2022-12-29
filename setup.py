from setuptools import find_packages, setup

with open('requirements.txt') as f:
    required = f.read().splitlines()

with open("README.md") as f:
    long_description = f.read()

setup(
    name="feast-teradata",
    version="1.0.0",
    author="Teradata Corporation",
    author_email="developers@teradata.com",
    url="https://github.com/Teradata/feast-teradata",
    long_description=long_description,
    long_description_content_type="text/markdown",
    python_requires=">=3.7",
    packages=find_packages(exclude=('tests',)),
    package_data={
        'feast_teradata.cli': [
            'template/teradata/**',
            'template/teradata/**/**',
        ],
    },
    install_requires=required,
    tests_require=['pytest==6.2.4'],
    license_files=['LICENSE', 'LICENSE-3RD-PARTY.txt'],
    entry_points={'console_scripts': [
        'feast-td=feast_teradata.cli.cli:cli',
    ]},
)
