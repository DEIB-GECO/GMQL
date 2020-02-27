GenoMetric Query Language (GMQL) Engine
=======================================

[![Join the chat at https://gitter.im/DEIB-GECO/GMQL](https://badges.gitter.im/DEIB-GECO/GMQL.svg)](https://gitter.im/DEIB-GECO/GMQL?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)
[![Build status](https://travis-ci.org/DEIB-GECO/GMQL.svg?branch=master)](https://travis-ci.org/DEIB-GECO)
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/it.polimi.genomics/gmql/badge.svg)](https://maven-badges.herokuapp.com/maven-central/it.polimi.genomics/gmql)



A GMQL script is expressed as a sequence of GMQL operations with the following structure:
```
<dataset> = operation(<parameters>) <datasets>
```
where each dataset stands for a [Genomic Data Model (GDM)](docs/GDM_DS_Structure.md) dataset. Operations are either unary (with one input dataset), or binary (with two input datasets), and construct one result dataset.

For Quick Start please refer to:
> [Installation Guide](https://github.com/DEIB-GECO/GMQL_Package/)

For detailed GMQL language documentation:
> [GMQL Language Commands and documentation.](http://www.bioinformatics.deib.polimi.it/genomic_computing/GMQL/doc/GMQL_V2_manual.pdf)


For a look on GDMS architecture:
> [Engine architecture and deployments.](docs/gmql_architecture.md)


For programatical importing of GDMS kernel JARs in Scala applications and programatically scripting GMQL in Scala: 
> [Scripting GMQL programatically.](docs/GMQL_APIs.md)

For more information about GDMS repository architecture and repository manager:
> [Repository Manager](GMQL-Repository/README.md)


GDMS repository is based on a dataset notion, for more information about the data module and GDM dataset architecture:
> [GDM DataSet architecture.](docs/GDM_DS_Structure.md)

Shell API is provided for GDMS repository, to list datasets, add, delete, alter datasets in GDMS repository:
> [Repository Manager shell API](docs/SHELL_API.md)

The first step in the installation is to understand the engine configurations, currently we have two sets of configurations. One set of configurations for the repository and the other for the executor.
> [Engine Configurations.](docs/Configurations.md)

