from setuptools import find_packages, setup

setup(
    name="news8a841d781095471c",
    version="0.1.8",
    packages=find_packages(),
    install_requires=[
        'aiohttp>=3.8.1',  # For asynchronous HTTP requests
        'python-dateutil>=2.8.2',  # For parsing dates
        'requests>=2.26.0',  # For fetching the JSON feed
        'tldextract>=3.1.0'
    ],
    extras_require={"dev": ["pytest", "pytest-cov", "pytest-asyncio"]},
)
