
GenoMetric Query Language (GMQL) Engine
=======================================

A GMQL script is expressed as a sequence of GMQL operations with the following structure:
```
<dataset> = operation(<parameters>) <datasets>
```
where each dataset stands for a [Genomic Data Model (GDM)](docs/GDM_DS_Structure.md) dataset. Operations are either unary (with one input dataset), or binary (with two input datasets), and construct one result dataset.

###[GMQL Language Commands and documentation.](http://www.bioinformatics.deib.polimi.it/genomic_computing/GMQL/doc/GMQL_V2_manual.pdf)

###[Engine architecture and deployments.](docs/gmql_architecture.md)

###[Scripting GMQL programatically.](docs/GMQL_APIs.md)

###[Repository Manager](GMQL-Repository/README.md)

###[GDM DataSet architecture.](docs/GDM_DS_Structure.md)

###[Repository Manager shell API](docs/SHELL_API.md)

###[Engine Configurations.](docs/Configutations.md)
