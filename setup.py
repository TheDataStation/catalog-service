from setuptools import setup, find_packages
import io
import os

VERSION = "0.1"


def get_long_description():
    with io.open(
            os.path.join(os.path.dirname(os.path.abspath(__file__)), "README.md"),
            encoding="utf8",
    ) as fp:
        return fp.read()


setup(
    name="ds-catalog-service",
    description="Catalog Service Implementation Supporting Data Stations".title(),
    long_description=get_long_description(),
    long_description_content_type="text/markdown",
    author="Florents Tselai",
    version=VERSION,
    license="MIT License",
    packages=find_packages(exclude=["tests", "tests.*"]),
    install_requires=open('requirements.txt', 'r').read(),
    setup_requires=["pytest-runner"],
    extras_require={
        "testing": ["pytest"]
    },
    entry_points="""
        [console_scripts]
        ds-catalog-service=ds_catalog_service.cli:cli
    """,
    url="https://github.com/TheDataStation/catalog-service",
    project_urls={
        "Documentation": "https://github.com/TheDataStation/catalog-service",
        "Changelog": "https://github.com/TheDataStation/catalog-service/",
        "Source code": "https://github.com/TheDataStation/catalog-service",
        "Issues": "https://github.com/TheDataStation/catalog-service/issues",
        "CI": "https://github.com/TheDataStation/catalog-service/actions",
    },
    python_requires=">=3.6.0",
    classifiers=[
        "Intended Audience :: Developers",
        "Intended Audience :: Science/Research",
        "Intended Audience :: End Users/Desktop",
        "Topic :: Database"
    ],
)
