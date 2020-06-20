
from setuptools import find_packages, setup



try:
    with open("requirements.txt", "r") as file:
        requirement = file.readline()
except FileNotFoundError:
    requirement = []

setup(
    name="src",
    version="0.1.0",
    author="Azan",
    author_email="azan.khanyari@gmail.com",
    description="AS_IXP",
    packages=find_packages(exclude=("tests",)),
    install_requires=requirement,
    classifiers=[
        "Development Status :: 2 - Pre-Alpha",
        "Programming Language :: Python :: 3.7",
    ],
)
