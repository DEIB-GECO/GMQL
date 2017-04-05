# Repository
Data are stored in the repository in a dataset represenation. GMQL repository stores hetrogeneous datasets, with several datasets having different schema formats. Each dataset contains both the samples and their related descriptive data as a metadata associated with each sample (metadata can be the cancer type, cell type, antibody target, or lab information).
## Data Set structure
Dataset in GMQL repository are structured according to the definition of [Genomic Data Model](http://www.sciencedirect.com/science/article/pii/S1046202316303012)(GDM). for more infornation about dataset structure, see [GDM dataset structure](../docs/GDM_DS_Structure.md).
## GMQL Reposiotry Manager
Files are stored in the repository in their original format. Managing the repository includes: 
* Adding new dataset.
* Deleting dataset
* Modifing dataset
  * Add sample.
  * Delete sample.
* Query a dataset
* Copy dataset

## Shell API
Go to [shell api](../docs/SHELL_API.md)
