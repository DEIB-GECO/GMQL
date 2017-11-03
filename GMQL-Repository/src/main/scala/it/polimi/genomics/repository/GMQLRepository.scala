package it.polimi.genomics.repository

import java.io.InputStream

import it.polimi.genomics.core.DataStructures.IRDataSet
import it.polimi.genomics.core.GDMSUserClass.GDMSUserClass
import it.polimi.genomics.core._
import it.polimi.genomics.repository.GMQLExceptions._

/**
  * Created by Abdulrahman Kaitoua on 11/04/16.
  */
trait GMQLRepository {

  /**
    *
    *  Import Dataset into GMQL from Local file system.
    *
    * @param dataSetName  String of the dataset name.
    * @param userName String of the user name.
    * @param Samples List of GMQL samples [[ GMQLSample]].
    * @param schemaPath String of the path to the xml file of the dataset schema.
    * @throws GMQLNotValidDatasetNameException
    * @throws GMQLUserNotFound
    * @throws java.lang.Exception
    */
  @throws(classOf[GMQLDSException])
  @deprecated
  def importDs(dataSetName:String, userName:String, Samples:java.util.List[GMQLSample], schemaPath:String)



  /**
    *
    *  Import Dataset into GMQL from Local file system.
    *
    * @param dataSetName  String of the dataset name.
    * @param userName String of the user name.
    * @param userClass GDMSUserClass
    * @param Samples List of GMQL samples [[ GMQLSample]].
    * @param schemaPath String of the path to the xml file of the dataset schema.
    * @throws GMQLNotValidDatasetNameException
    * @throws GMQLUserNotFound
    * @throws java.lang.Exception
    */
  @throws(classOf[GMQLDSException])
  @throws(classOf[GMQLDSExceedsQuota])
  def importDs(dataSetName:String, userName:String, userClass: GDMSUserClass, Samples:java.util.List[GMQLSample], schemaPath:String)


  /**
    *
    * Add a new dataset to GMQL repository, this dataset is usually a result of a script execution.
    *
    * @param dataSet Intermediate Representation (IRDataSet) of the dataset, contains the dataset name and schema.
    * @param userName
    * @param Samples
    * @param GMQLScriptPaht
    * @throws it.polimi.genomics.repository.GMQLExceptions.GMQLDSNotFound
    * @throws it.polimi.genomics.repository.GMQLExceptions.GMQLUserNotFound
    * @throws it.polimi.genomics.repository.GMQLExceptions.GMQLSampleNotFound
    */
  @throws(classOf[GMQLDSException])
  def createDs(dataSet:IRDataSet, userName:String, Samples:java.util.List[GMQLSample], GMQLScriptPaht:String = "ROOT_DS",schemaType:GMQLSchemaFormat.Value=GMQLSchemaFormat.TAB, schemaCoordinateSystem:GMQLSchemaCoordinateSystem.Value=GMQLSchemaCoordinateSystem.ZeroBased)


  /**
    *
    *  Delete data set from the repository
    *
    * @param userName Intermediate Representation (IRDataSet) of the dataset, contains the dataset name and schema.
    * @throws GMQLDSNotFound
    * @throws GMQLDSException
    * @throws GMQLUserNotFound
    */
  @throws(classOf[GMQLDSException])
  def deleteDS(dataSetName:String, userName:String)


  /**
    *
    *   Add [[ GMQLSample]]  to dataset [[ IRDataSet]] in the repository
    *
    * @param dataSet Intermediate Representation (IRDataSet) of the dataset, contains the dataset name and schema.
    * @param Sample
    * @throws GMQLDSNotFound
    * @throws GMQLDSException
    * @throws GMQLUserNotFound
    * @throws GMQLSampleNotFound
    */
  @throws(classOf[GMQLDSException])
  def addSampleToDS(dataSet:String, userName:String, Sample:GMQLSample, userClass: GDMSUserClass = GDMSUserClass.PUBLIC)

  /**
    *
    *  Delete [[ GMQLSample]] from a dataset [[ IRDataSet]]
    *
    * @param dataSet Intermediate Representation (IRDataSet) of the dataset, contains the dataset name and schema.
    * @param Sample
    * @throws GMQLDSNotFound
    * @throws GMQLDSException
    * @throws GMQLUserNotFound
    * @throws GMQLSampleNotFound
    */
  @throws(classOf[GMQLDSException])
  def deleteSampleFromDS(dataSet:String, userName:String, Sample:GMQLSample)

  /**
    *
    *  List all the [[ IRDataSet]] dataset of the user in the repository
    *
    * @param userName [[ String]] of the user name
    * @throws GMQLDSException
    * @throws GMQLUserNotFound
    */
  @throws(classOf[GMQLDSException])
  def listAllDSs(userName:String): java.util.List[IRDataSet]

  /**
    *
    *   List all the samples [[ GMQLSample]] of specific dataset in the repository
    *
    * @param dataSetName
    * @param userName
    * @throws GMQLDSException
    * @return
    */
  @throws(classOf[GMQLDSException])
  def listDSSamples(dataSetName:String, userName:String):java.util.List[GMQLSample]

  /**
    *
    * List the result [[ GMQLSample]] and the schema of an execution of GMQL script
    *
    * @param dataSetName [[ String]] of the dataset name
    * @throws GMQLDSException
    * @return
    */
  @throws(classOf[GMQLDSException])
  def listResultDSSamples(dataSetName:String, userName:String):(java.util.List[GMQLSample],java.util.List[GMQLSchemaField])

  /**
    * Copy data set from GMQL repository to local folder,
    * dataset includes; Schema file, script file, samples files, and metadata files
    *
    * @param dataSet Intermediate Representation (IRDataSet) of the dataset, contains the dataset name and schema.
    * @throws GMQLDSException
    */
  @throws(classOf[GMQLDSException])
  def exportDsToLocal(dataSet:String, userName:String,localDir:String)

  /**
    *
    *  Verify the existance of a dataset in the reposiotry under a specific username
    *
    * @param dataSet [[ IRDataSet]] of the dataset
    * @param userName  [[ String]] of the user name
    * @throws GMQLDSException
    * @return
    */
  @throws(classOf[GMQLDSException])
  def DSExists(dataSet:String, userName:String): Boolean

  /**
    *
    *  Verify the existance of a dataset in the public dataset
    *
    * @param dataSet [[ IRDataSet]] of the dataset
    * @throws GMQLDSException
    * @return
    */
  @throws(classOf[GMQLDSException])
  def DSExistsInPublic( dataSet:String): Boolean

  /**
    *
    *   return the schema of the dataset
    *
    * @param schemaPath [[ String]] of the path to the schema xml
    * @return
    */
  def readSchemaFile(schemaPath:String):  GMQLSchema

  /**
    *  get the dataset schema
    *
    * @param datasetName String of the dataset name
    * @param userName String of the username, the owner of the dataset
    * @return [[ GMQLSchema]] as the schema of the dataset
    */
  def getSchema(datasetName:String, userName:String): GMQLSchema

  /**
    *  return the schema file as stream.
    * @param datasetName String of the dataset name
    * @param userName String of the username, the owner of the dataset
    * @return [[InputStream]] for the schema file
    */
  def getSchemaStream(datasetName:String, userName:String): InputStream


  /**
    *
    * Get the metadata of a Dataset
    *
    * @param dataSet [[ IRDataSet]], dataset identifier
    * @param userName [[ String]] of the user name
    * @return
    */
  def getMeta(dataSet: String,userName:String):String

  /**
    *
    *  Return a [[ String]] of the meta data of specific sample
    *
    * @param dataSet [[ IRDataSet]], dataset identifier
    * @param userName [[ String]] of the user name
    * @param sample
    * @return
    */
  def getSampleMeta(dataSet: String, userName:String, sample: GMQLSample):String

  /**
    *
    *  Return a list of {@ink GNQLSample} after searching the meta data with a query
    *
    * @param dataSet [[ IRDataSet]], dataset identifier
    * @param userName [[ String]] of the user name
    * @param query  [[ String]] of the query
    * @return
    */
  def searchMeta(dataSet: String, userName:String, query:String): java.util.List[GMQLSample]

  /**
    *
    *  Register user in the repository
    *
    * @param userName [[ String]] of the user name
    * @return
    */
  def registerUser(userName:String): Boolean

  /**
    *
    *  Delete a user from the repsository
    *
    * @param userName [[ String]] of the user name
    * @return
    */
  def unregisterUser(userName:String): Boolean

  /**
    * Return the location of the dataset, Local, HDFS, remote
    *
    * @param dataSet String of the dataset name
    * @param userName String of the name of the owner of the dataset
    * @return The Location as either LOCAL, HDFS, or REMOTE
    */
  def getDSLocation(dataSet:String, userName:String): (RepositoryType.Value,DatasetOrigin.Value)


  /**
    * change the dataset name to a new name
    *
    * @param datasetName old dataset name as a String
    * @param newDSName new dataset name as a String
    */
  def changeDSName(datasetName:String, userName:String, newDSName:String)

  /**
    * send streams of the sample and its meta data.
    *
    * @param dataSetName dataset name as a string
    * @param userName the owner of the dataset as a String
    * @param sampleName The sample name, which is the file name with out the full path as a String
    * @return Two Streams, one for the sample and the other for the metadata
    */
  def sampleStreams(dataSetName: String, userName: String, sampleName: String): (InputStream, InputStream)

  /**
    *  Return a stream of the script file.
    *
    * @param dataSetName dataset name of the requested script
    * @param userName the owner of the dataset and the script
    * @return [[InputStream]] as the script string file.
    */
  def getScriptStream(dataSetName: String, userName: String): InputStream

  /**
    *  Return a stream of the vocabulary file.
    *
    * @param dataSetName dataset name of the requested script
    * @param userName the owner of the dataset and the script
    * @return [[InputStream]] as the script string file.
    */
  def getVocabularyStream(dataSetName: String, userName: String): InputStream

  /**
    * Return a stram of the dataset.xml file
    * @param datasetName
    * @param userName
    * @return
    */
  def getDsInfoStream(datasetName: String, userName: String): InputStream


  // Dataset Meta and Profiling information

  /**
    * Returns the metadata associated to a dataset, e.g:
    *   Source => Politecnico di Milano
    *   Type => GDM
    *   Creation Date => 21 Mar 2011
    *   Creation Time => 00:18:56
    *   Size => "50.12 MB"
    *
    * @param datasetName dataset name as a string
    * @param userName   the owner of the dataset
    * @return a Map[String, String] containing property_name => value
    */
  def getDatasetMeta(datasetName: String, userName: String): Map[String, String]


  /**
    * Set an entry on dataset metadata
    *
    * @param datasetName
    * @param userName
    * @param metaEntries , a map of key => values entries
    */
  def setDatasetMeta(datasetName: String, userName: String, metaEntries:Map[String,String])


  /**
    * Returns profiling information concerning the whole dataset, e.g.:
    *   Number of samples => 15
    *   Number of regions => 31209
    *   Average region length => 123.12
    *
    * @param datasetName dataset name as a string
    * @param userName   the owner of the dataset
    * @return a Map[String, String] containing property_name => value
    */
  def getDatasetProfile(datasetName: String, userName: String): Map[String, String]


  /**
    * Returns profiling information concerning a specific sample of the dataset, e.g.:
    *
    *   Number of samples => 15
    *   Number of regions => 31209
    *   Average region length => 123.12
    *
    * @param datasetName dataset name as a string
    * @param sampleName name of the sample (index 1 .. N)
    * @param userName the owner of the dataset
    */
  def getSampleProfile(datasetName: String, sampleName: String, userName: String): Map[String, String]


  // User Class management

  /**
    * Returns information about the user disk quota usage
    * @param userName
    * @param userClass
    * @return (occupied, available) in KBs , available = remaining
    */
  def getUserQuotaInfo(userName: String, userClass: GDMSUserClass): (Float, Float)

  /**
    * Boolean value: true if user quota is exceeded
    * @param username
    * @param userClass
    * @return
    */
  def isUserQuotaExceeded(username: String, userClass: GDMSUserClass): Boolean
}

