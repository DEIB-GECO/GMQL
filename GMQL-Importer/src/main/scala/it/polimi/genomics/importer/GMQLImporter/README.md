## GMQLImporter base components

For the basic idea of downloading, transforming and loading data into the GMQLRepository, here are the core components that make this possible.

###Sources and their Datasets representation inside GMQLImporter

####[GMQLSource](https://github.com/akaitoua/GMQL/blob/master/GMQL-Importer/src/main/scala/it/polimi/genomics/importer/GMQLImporter/GMQLSource.scala)

Data structure that represents the "source entity" inside the GMQLImporter project.

A source entity is a data provider as ENCODE, TCGA2BED or potentially any other.

A set of sources with its datasets is provided into an XML file that follows an specified schema.

The generalization of a source contains:

  * Sufficient and necessary information to recognize (name) and locate it remotely (url) and localy (outputFolder).
  
  * Selection of the methods for data downloading (downloader) and transformation (transformer).
  
  * Settings for GMQL data importation (gmqlUser, download_enabled, transform_enabled, load_enabled).
  
  * Set of datasets provided by the source that are of interest for the importation process (datasets).
  
  * Extra information that the downloading or transformation process can need to work correctly with the source (parameters).

|Attribute name |Type|Description|
|----------------|----|-----------|
|name|String|Is the source name, i.e. ENCODE|
|url|String|Source data root location i.e. https://www.encodeproject.org/metadata/|
|outputFolder|String|Working folder for the source|
|downloader|String|Package and name of the GMQLDownloader to be used for this source i.e. Defaults.FTPDownloader|
|transformer|String|Package and name of the GMQLTransformer to be used for this source i.e ENCODEImporter.ENCODETransformer|
|gmqlUser|String|User for whom datasets are i.e. public|
|downloadEnabled|Boolean|Indicates whether download or not the source's datasets.|
|transformEnabled|Boolean|Indicates whether transform or not the source's datasets.|
|loadEnabled|Boolean|Indicates whether load or not the source's datasets into GMQLRepository.|
|datasets|Seq[GMQLDataset]|Datasets to be downloaded/transformed/loaded|
|parameters|Seq[(String,String)]|Extra data that maybe is needed for GMQLDownloader or GMQLTransformer|

####[GMQLDataset](https://github.com/akaitoua/GMQL/blob/master/GMQL-Importer/src/main/scala/it/polimi/genomics/importer/GMQLImporter/GMQLDataset.scala)

GMQLDataset is a data structure that represents the "dataset entity" inside the GMQLImporter project. 

Datasets belong to a source and represent typically a set with specific data format.

The generalization of a dataset contains:

  * Local path to locate it (outputFolder).

  * Sufficient information to locate the dataset schema (schema) and to know if is remote or local (schemaLocation).

  * Settings for GMQL data importation (download_enabled, transform_enabled, load_enabled).

  * Necessary information for the download and transform process to identify the dataset inside the source (parameters).

|Attribute name |Type|Description|
----------------|----|-----------|
|outputFolder|String|Working folder for the source|
|schema|Boolean|.schema file location with the schema for importing the dataset into the GMQLRepository.|
|schemaLocation|Boolean|indicates if the schema is on local or remote location.|
|downloadEnabled|Boolean|Indicates whether download or not the dataset.|
|transformEnabled|Boolean|Indicates whether transform or not the dataset.|
|loadEnabled|Boolean|Indicates whether load or not the dataset into GMQLRepository.|
|parameters|Seq[(String,String)]|Extra data that maybe is needed for GMQLDownloader or GMQLTransformer|

###Basic operations for GMQLRepository creation

####[GMQLDownloader (Trait)](https://github.com/akaitoua/GMQL/blob/master/GMQL-Importer/src/main/scala/it/polimi/genomics/importer/GMQLImporter/GMQLDownloader.scala)

A GMQLDownloader is a responsible for data downloading from a source. Its role is to understand how the datasets are located inside the source and by using the information provided in the source and its datasets, and download every dataset into separated directories inside the source directory.

As there are diverse kinds of sources and data downloading techniques and protocols, GMQLDownloader is a trait that allows many objects to perform the download method. 

To keep the data up to date, GMQLDownloader keeps track of what has been done and what has to be done by GMQLTransformer using a download log. Any implementation of it has to follow the download logging protocol.

|_GMQLDownloader_ (Trait)|
|------------------------|
|download(source:GMQLSource): Unit|

####[GMQLTransformer (Trait)](https://github.com/akaitoua/GMQL/blob/master/GMQL-Importer/src/main/scala/it/polimi/genomics/importer/GMQLImporter/GMQLTransformer.scala)

A GMQLTransformer is a responsible for dataset transformation into GDM dataset. By reading the download log, it understands what data is ready to be transformed or what has already been transformed. Also among its responsibility is to organize the dataset and set it ready to be imported into GMQLRepository by checking that inside each dataset directory is a transformations sub directory that contains pairs of files (data and metadata) for each dataset element, and the schema file for the whole dataset. 

As there are diverse kinds of sources and data transformation techniques, different GMQLTransformer implementations are used. 

To keep the data up to date, GMQLTransformer keeps track of what has been done and what has to be done by GMQLLoader using a transformation log. Any implementation of it has to follow the transformation logging protocol.

|_GMQLTransformer_ (Trait)|
|------------------------|
|transform(source:GMQLSource): Unit|
|organize(source:GMQLSource): Unit|

####[GMQLLoader](https://github.com/akaitoua/GMQL/blob/master/GMQL-Importer/src/main/scala/it/polimi/genomics/importer/GMQLImporter/GMQLLoader.scala)

GMQLLoader is the responsible for dataset loading into GMQLRepository. By reading the transformation log, it understands what data is ready to be loaded or what has already been loaded. 

There is just one way to load data into the GMQLRepository, so GMQLLoader is a single unit that tries to load the previously transformed and organized data.

|GMQLLoader|
|------------------------|
|loadIntoGMQL(source:GMQLSource): Unit|

###Interaction between GMQLImporter base classes


<p align="center">![Alt text](https://dl.dropboxusercontent.com/u/24829944/GMQL%20Git/img/base%20classes%20interaction.png "Base classes interaction UML")
