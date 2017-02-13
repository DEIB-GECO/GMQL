# Linux Shell API 

Syntax: ___RepositoryManager $COMMAND___.  

Allowed Commands are :

* ___RegisterUser___ 

  To register the current user to the GMQL repository.

* ___UnRegisterUser___ 

  To unregister the current user from GMQL repository. <br />
  All the datasets, history, User private schemata and user results will be deleted. <br />
  The Samples on the Local File System will not be deleted.<br />

* ___CREATEDS DS_NAME SCHEMA_URL SAMPLES_PATH___ 
		
    __DS_NAME__ is the new dataset name

    __SCHEMA_URL__ is the path to the schema file

        - The schema can be a keyword { BED | BEDGRAPH | NARROWPEAK | BROADPEAK }

    __SAMPLES_PATH__ is the path to the samples, can be one of the following formats:

        - Samples urls comma separated: /to/the/path/sample1.bed,/to/the/path/sample2.bed 

        - Samples folder Path: /to/the/path/samplesFolder/ 
          in this case all the samples with an associated metadata will be added to the dataset.

    **TIP:** each sample file must be associated with a metadata file. The meta data file name should have the full name of the sample ended with ".meta" extension, for example: { sample.bed sample.bed.meta }

* ___DELETEDS DS_NAME___ 

  To Delete a dataset named DS_NAME

* ___ADDSAMPLE DS_NAME SAMPLE_URL___ 

  __DS_NAME__ the dataset name (It has to be already added in the system). 

  __SAMPLE_URL__ is the path to the sample. No need to add the metadata Path since it must be in the same folder.
	For example: /to/the/path/sample.bed

* ___DELETESAMPLE DS_NAME SAMPLE_URL___ 

  Delete one sample form the dataset named __DS_NAME__

  __SAMPLE_URL__ must be identical to what { LIST DS_NAME } command prints. 

* ___LIST ALL|DS_NAME___

  ALL to print all the datasets for the current user and the __public__ user. 

  __DS_NAME__ to print the samples of this dataset

* ___CopyDSToLocal DS_NAME LOCAL_DIRECTORY___ 

  This command copy all the samples of __DS_NAME__ to local folder. <br />
  The samples will be copied with its metadata. <br />
  __LOCAL_DIRECTORY__ is the full path to the local location. <br />

**INFO:** For more information read the GMQL shell commands document.
