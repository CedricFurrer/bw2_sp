# bw2_sp
The "bw2_sp" repository is a collection of useful functions to work with SimaPro data in Brightway.

The repository provides several modules that contain functions for different purposes:
- [lci.py](lci.py) contains functions to import and harmonize life cycle inventory data from either SimaPro or XML (ecospold2) data

Checkout the [notebook folder](notebook/) for specific use cases.

## Dependencies
This repository depends on the Brightway v2 package and contains functions to facilitate the work with SimaPro LCI and LCIA data. The package is not yet compatible with Brightway2.5, but will so in the future.
The package works with the following packages:
- bw2io: _v0.8.12_
- bw2data: _v3.6.6_
- bw2calc: _v1.8.2_
- bw2analyzer: _v0.10_
- sentence-transformers: _v2.7.0_
