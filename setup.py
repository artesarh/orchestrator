from setuptools import find_packages, setup

setup(
    name="reporting_pipeline",
    packages=find_packages(exclude=["reporting_pipeline_tests"]),
    install_requires=[
        "dagster",
        "dagster-webserver",
        "requests",
        "azure-storage-blob",
        "croniter",
        "python-dateutil"
    ],
    extras_require={"dev": ["dagster-webserver", "pytest"]},
)
