import setuptools
from setuptools import setup

dependencies = [
    "aiohttp",
    "django",
    "azure-storage-blob",
    "azure-storage-file",
    "azure-storage-queue",
    "azure-storage-common"
]

packages = [
    package
    for package in setuptools.PEP420PackageFinder().find()
]

setup (
    name='pyblob',
    version='0.1',
    description='Python Blob For Microsoft Azure Blob Interface',
    url='https://github.com/li195111/pyblob',
    author='Yue Li',
    author_email='green07111@gmail.com',
    install_requirements=dependencies,
    lincense='MIT',
    packages=packages,
    zip_safe=False,
    keywords=[
        'Blob',
        'Azure Storage Blob',
        'Azure Storage',
        'Azure'],
    classifiers=[
        'Development Status :: 5 - Production/Stable',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 3.7'
    ],
)
