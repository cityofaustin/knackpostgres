from setuptools import setup

setup(
    author="John Clary",
    author_email="john.clary@austintexas.gov",
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        # Pick your license as you wish (should match "license" above)
        "License :: Public Domain",
        "Programming Language :: Python :: 3",
    ],
    description="Converts Knack applications to PosthgeSQL.",
    install_requires=["knackpy", "requests"],
    keywords="knack api postgresql sql python",
    license="Public Domain",
    name="knackpostgres",
    packages=["knackpostgres"],
    url="http://github.com/cityofaustin/knack-postgres",
    version="0.0.1",
)
