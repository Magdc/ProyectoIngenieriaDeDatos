"""Paquete que Dataflow instala en sus workers (`--setup_file src/setup.py`).

Solo lleva el código propio que usa el pipeline: `streaming` y `common`
(`common` lo necesita el Lote 2.2 para la anonimización). Apache Beam ya viene
en la imagen del worker; aquí solo se suman las dependencias que no trae.
Lo usa `scripts/dataflow_job.sh start` (ARCHITECTURE.md §3.13).
"""

import setuptools

setuptools.setup(
    name="ta-streaming",
    version="2.1.0",
    description="TrendAnalyzer · pipeline de streaming (Dataflow)",
    packages=setuptools.find_packages(
        include=["streaming", "streaming.*", "common", "common.*"],
        exclude=["*.tests", "*.tests.*"],
    ),
    package_data={"common": ["*.yaml"]},
    python_requires=">=3.11",
    install_requires=[
        "jsonschema==4.26.0",
        "referencing==0.37.0",
        "PyYAML==6.0.3",
    ],
)
