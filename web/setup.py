import io

from setuptools import find_packages
from setuptools import setup

with io.open("README.md", "rt", encoding="utf8") as f:
    readme = f.read()

setup(
    name="sfmta",
    version="1.0.0",
    url="http://discoverdataengineering.live/sfmta",
    maintainer="Robert",
    maintainer_email="engr@discoverdataengineering.live",
    description="Web application to access SFMTA vehicle location data.",
    long_description=readme,
    packages=find_packages(),
    include_package_data=True,
    zip_safe=False,
    install_requires=["flask"],
    extras_require={"test": ["pytest", "coverage"]},
)
