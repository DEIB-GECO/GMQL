# Repository Manager

## RepositoryManager $COMMAND

Allowed Commands are :

### RegisterUser

To register the current user to the GMQL repository.

### UnRegisterUser

To unregister the current user from GMQL repository. <br />
All the datasets, history, User private schemata and user results will be deleted. <br />
The Samples on the Local File System will not be deleted.<br />

### CREATEDS DS_NAME SCHEMA_URL SAMPLES_PATH 
		
DS_NAME is the new dataset name

SCHEMA_URL is the path to the schema file

- The schema can be a keyword { BED | BEDGRAPH | NARROWPEAK | BROADPEAK }

SAMPLES_PATH is the path to the samples, can be one of the following formats:

- Samples urls separated by a comma with no spaces in between:
	
	/to/the/path/sample1.bed,/to/the/path/sample2.bed 

- Samples folder Path: /to/the/path/samplesFolder/ 
	
	in this case all the samples with an associated metadata will be added to the dataset.

**TIP:** each sample file must be associated with a metadata file. The meta data file name should have the full name of the sample ended with ".meta" extension, for example: { sample.bed sample.bed.meta }

### DELETEDS DS_NAME 

To Delete a dataset named DS_NAME

### ADDSAMPLE DS_NAME SAMPLE_URL 

DS_NAME the dataset name (It has to be already added in the system). 

SAMPLE_URL is the path to the sample. No need to add the metadata Path since it must be in the same folder.
	For example: /to/the/path/sample.bed

### DELETESAMPLE DS_NAME SAMPLE_URL 

Delete one sample form the dataset named DS_NAME 

SAMPLE_URL must be identical to what { LIST DS_NAME } command prints. 

### LIST ALL|DS_NAME

ALL to print all the datasets for the current user and the public user. 

DS_NAME to print the samples of this dataset

### CopyDSToLocal DS_NAME LOCAL_DIRECTORY 

This command copy all the samples of DS_NAME to local folder. <br />
The samples will be copied with its metadata. <br />
LOCAL_DIRECTORY is the full path to the local location. <br />

**INFO:** For more information read the GMQL shell commands document.
