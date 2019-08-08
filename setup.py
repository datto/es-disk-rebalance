
from setuptools import setup

setup(
	name="es_rebalance",
	version="0.1",
	description="Disk-based rebalancing tool for Elasticsearch",
	url="",
	author="Alex Parrill",
	author_email="aparrill@datto.com",
	license="",
	packages=["es_rebalance"],
	zip_safe=False,
	entry_points={
		"console_scripts": ["es-rebalance=es_rebalance.__main__:main"]
	},
	install_requires=[
		"elasticsearch6<7.0.0,>=6.0.0"
	]
)
