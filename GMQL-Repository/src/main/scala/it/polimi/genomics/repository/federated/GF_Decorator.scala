package it.polimi.genomics.repository.federated

import java.io.InputStream
import java.util

import it.polimi.genomics.core.DataStructures.{GMQLInstance, IRDataSet, Instance, LOCAL_INSTANCE}
import it.polimi.genomics.core.GDMSUserClass.GDMSUserClass
import it.polimi.genomics.core.{GMQLSchema, GMQLSchemaCoordinateSystem, GMQLSchemaField, GMQLSchemaFormat}
import it.polimi.genomics.repository.{DatasetOrigin, GMQLRepository, GMQLSample, RepositoryType}

import scala.util.{Failure, Success, Try}


class GF_Decorator (val repository : GMQLRepository) extends GMQLRepository {

  val api = new GF_Communication()

  /**
    *
    * Import Dataset into GMQL from Local file system.
    *
    * @param dataSetName String of the dataset name.
    * @param userName    String of the user name.
    * @param userClass   GDMSUserClass
    * @param Samples     List of GMQL samples [[ GMQLSample]].
    * @param schemaPath  String of the path to the xml file of the dataset schema.
    * @throws GMQLNotValidDatasetNameException
    * @throws GMQLUserNotFound
    * @throws java.lang.Exception
    */
  override def importDs(dataSetName: String, userName: String, userClass: GDMSUserClass, Samples: util.List[GMQLSample], schemaPath: String): Unit =
    repository.importDs(dataSetName: String, userName, userClass, Samples, schemaPath)

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
  override def createDs(dataSet: IRDataSet, userName: String, Samples: util.List[GMQLSample], GMQLScriptPaht: String, schemaType: GMQLSchemaFormat.Value, schemaCoordinateSystem: GMQLSchemaCoordinateSystem.Value, dsmeta: Map[String, String]): Unit =
    repository.createDs(dataSet, userName, Samples, GMQLScriptPaht, schemaType, schemaCoordinateSystem, dsmeta)

  /**
    *
    * Delete data set from the repository
    *
    * @param userName Intermediate Representation (IRDataSet) of the dataset, contains the dataset name and schema.
    * @throws GMQLDSNotFound
    * @throws GMQLDSException
    * @throws GMQLUserNotFound
    */
  override def deleteDS(dataSetName: String, userName: String): Unit =
    repository.deleteDS(dataSetName, userName)

  /**
    *
    * Add [[ GMQLSample]]  to dataset [[ IRDataSet]] in the repository
    *
    * @param dataSet Intermediate Representation (IRDataSet) of the dataset, contains the dataset name and schema.
    * @param Sample
    * @throws GMQLDSNotFound
    * @throws GMQLDSException
    * @throws GMQLUserNotFound
    * @throws GMQLSampleNotFound
    */
  override def addSampleToDS(dataSet: String, userName: String, Sample: GMQLSample, userClass: GDMSUserClass): Unit =
    repository.addSampleToDS(dataSet, userName, Sample, userClass)

  /**
    *
    * Delete [[ GMQLSample]] from a dataset [[ IRDataSet]]
    *
    * @param dataSet Intermediate Representation (IRDataSet) of the dataset, contains the dataset name and schema.
    * @param Sample
    * @throws GMQLDSNotFound
    * @throws GMQLDSException
    * @throws GMQLUserNotFound
    * @throws GMQLSampleNotFound
    */
  override def deleteSampleFromDS(dataSet: String, userName: String, Sample: GMQLSample): Unit =
    repository.deleteSampleFromDS(dataSet, userName, Sample)

  /**
    *
    * List all the [[ IRDataSet]] dataset of the user in the repository
    *
    * @param userName [[ String]] of the user name
    * @throws GMQLDSException
    * @throws GMQLUserNotFound
    */
  override def listAllDSs(userName: String): util.List[IRDataSet] =

    if (userName=="federated")
     api.listDatasets()
    else
      repository.listAllDSs(userName)

  /**
    *
    * List all the samples [[ GMQLSample]] of specific dataset in the repository
    *
    * @param dataSetName
    * @param userName
    * @throws GMQLDSException
    * @return
    */
  override def listDSSamples(dataSetName: String, userName: String): util.List[GMQLSample] =

   if (userName=="federated")
     api.getSamples(dataSetName)
   else
     repository.listDSSamples(dataSetName, userName)

  /**
    *
    * List the result [[ GMQLSample]] and the schema of an execution of GMQL script
    *
    * @param dataSetName [[ String]] of the dataset name
    * @throws GMQLDSException
    * @return
    */
  override def listResultDSSamples(dataSetName: String, userName: String): (util.List[GMQLSample], util.List[GMQLSchemaField]) =
    repository.listResultDSSamples(dataSetName, userName)

  /**
    * Copy data set from GMQL repository to local folder,
    * dataset includes; Schema file, script file, samples files, and metadata files
    *
    * @param dataSet Intermediate Representation (IRDataSet) of the dataset, contains the dataset name and schema.
    * @throws GMQLDSException
    */
  override def exportDsToLocal(dataSet: String, userName: String, localDir: String): Unit =
    repository.exportDsToLocal(dataSet, userName, localDir)

  /**
    *
    * Verify the existance of a dataset in the reposiotry under a specific username
    *
    * @param dataSet  [[ IRDataSet]] of the dataset
    * @param userName [[ String]] of the user name
    * @throws GMQLDSException
    * @return
    */
  override def DSExists(dataSet: String, userName: String, location: Option[GMQLInstance]): Boolean ={
    location match {
      case Some(LOCAL_INSTANCE) | None => repository.DSExists(dataSet, userName,Some(LOCAL_INSTANCE))
      case Some(Instance(name)) => Try(api.getDataset(dataSet)) match {
        case Success(v) =>
          v.locations.exists(_.id == name)
        case Failure(_) =>
          false
      }
    }
  }


  /**
    *
    * Verify the existance of a dataset in the public dataset
    *
    * @param dataSet [[ IRDataSet]] of the dataset
    * @throws GMQLDSException
    * @return
    */
  override def DSExistsInPublic(dataSet: String): Boolean =
    repository.DSExistsInPublic(dataSet)

  /**
    *
    * return the schema of the dataset
    *
    * @param schemaPath [[ String]] of the path to the schema xml
    * @return
    */
  override def readSchemaFile(schemaPath: String): GMQLSchema =
    repository.readSchemaFile(schemaPath)

  /**
    * get the dataset schema
    *
    * @param datasetName String of the dataset name
    * @param userName    String of the username, the owner of the dataset
    * @return [[ GMQLSchema]] as the schema of the dataset
    */
  override def getSchema(datasetName: String, userName: String): GMQLSchema =

    if (userName == "federated")
      api.getSchema(datasetName)
    else
      repository.getSchema(datasetName, userName)

  /**
    * return the schema file as stream.
    *
    * @param datasetName String of the dataset name
    * @param userName    String of the username, the owner of the dataset
    * @return [[InputStream]] for the schema file
    */
  override def getSchemaStream(datasetName: String, userName: String): InputStream =
    repository.getSchemaStream(datasetName, userName)

  /**
    *
    * Get the metadata of a Dataset
    *
    * @param dataSet  [[ IRDataSet]], dataset identifier
    * @param userName [[ String]] of the user name
    * @return
    */
  override def getMeta(dataSet: String, userName: String): String =

    if (userName=="federated")
      api.getMeta(dataSet)
    else
      repository.getMeta(dataSet, userName)

  /**
    *
    * Return a [[ String]] of the meta data of specific sample
    *
    * @param dataSet  [[ IRDataSet]], dataset identifier
    * @param userName [[ String]] of the user name
    * @param sample
    * @return
    */
  override def getSampleMeta(dataSet: String, userName: String, sample: GMQLSample): String =

    if (userName=="federated")
      api.getSampleMeta(dataSet, sample.name)
    else
      repository.getSampleMeta(dataSet, userName, sample)

  /**
    *
    * Return a list of {@ink GNQLSample} after searching the meta data with a query
    *
    * @param dataSet  [[ IRDataSet]], dataset identifier
    * @param userName [[ String]] of the user name
    * @param query    [[ String]] of the query
    * @return
    */
  override def searchMeta(dataSet: String, userName: String, query: String): util.List[GMQLSample] =
    searchMeta(dataSet, userName, query)

  /**
    *
    * Register user in the repository
    *
    * @param userName [[ String]] of the user name
    * @return
    */
  override def registerUser(userName: String): Boolean =
    repository.registerUser(userName)

  /**
    *
    * Delete a user from the repsository
    *
    * @param userName [[ String]] of the user name
    * @return
    */
  override def unregisterUser(userName: String): Boolean =
    repository.unregisterUser(userName)

  /**
    * Return the location of the dataset, Local, HDFS, remote
    *
    * @param dataSet  String of the dataset name
    * @param userName String of the name of the owner of the dataset
    * @return The Location as either LOCAL, HDFS, or REMOTE
    */
  override def getDSLocation(dataSet: String, userName: String): (RepositoryType.Value, DatasetOrigin.Value) =
    repository.getDSLocation(dataSet, userName)

  /**
    * change the dataset name to a new name
    *
    * @param datasetName old dataset name as a String
    * @param newDSName   new dataset name as a String
    */
  override def changeDSName(datasetName: String, userName: String, newDSName: String): Unit =
    repository.changeDSName(datasetName, userName, newDSName)

  /**
    * send streams of the sample and its meta data.
    *
    * @param dataSetName dataset name as a string
    * @param userName    the owner of the dataset as a String
    * @param sampleName  The sample name, which is the file name with out the full path as a String
    * @return Two Streams, one for the sample and the other for the metadata
    */
  override def sampleStreams(dataSetName: String, userName: String, sampleName: String): (InputStream, InputStream) =
    repository.sampleStreams(dataSetName, userName, sampleName)

  /**
    * Return a stream of the script file.
    *
    * @param dataSetName dataset name of the requested script
    * @param userName    the owner of the dataset and the script
    * @return [[InputStream]] as the script string file.
    */
  override def getScriptStream(dataSetName: String, userName: String): InputStream =
    repository.getScriptStream(dataSetName, userName)

  /**
    * Return a stream of the vocabulary file.
    *
    * @param dataSetName dataset name of the requested script
    * @param userName    the owner of the dataset and the script
    * @return [[InputStream]] as the script string file.
    */
  override def getVocabularyStream(dataSetName: String, userName: String): InputStream =
    repository.getVocabularyStream(dataSetName, userName)

  /**
    * Return a stram of the dataset.xml file
    *
    * @param datasetName
    * @param userName
    * @return
    */
  override def getDsInfoStream(datasetName: String, userName: String): InputStream =
    repository.getDsInfoStream(datasetName, userName)

  /**
    * Returns the metadata associated to a dataset, e.g:
    * Source => Politecnico di Milano
    * Type => GDM
    * Creation Date => 21 Mar 2011
    * Creation Time => 00:18:56
    * Size => "50.12 MB"
    *
    * @param datasetName dataset name as a string
    * @param userName    the owner of the dataset
    * @return a Map[String, String] containing property_name => value
    */
  override def getDatasetMeta(datasetName: String, userName: String): Map[String, String] =

    if (userName == "federated")
     api.getDatasetMeta(datasetName)
    else
      repository.getDatasetMeta(datasetName, userName)

  /**
    * Set an entry on dataset metadata
    *
    * @param datasetName
    * @param userName
    * @param metaEntries , a map of key => values entries
    */
  override def setDatasetMeta(datasetName: String, userName: String, metaEntries: Map[String, String]): Unit =
    repository.setDatasetMeta(datasetName, userName, metaEntries)

  /**
    * Returns profiling information concerning the whole dataset, e.g.:
    * Number of samples => 15
    * Number of regions => 31209
    * Average region length => 123.12
    *
    * @param datasetName dataset name as a string
    * @param userName    the owner of the dataset
    * @return a Map[String, String] containing property_name => value
    */
  override def getDatasetProfile(datasetName: String, userName: String): Map[String, String] =

    if (userName=="federated")
      api.getDasetProfile(datasetName)
    else
      repository.getDatasetProfile(datasetName, userName)




  /**
    * Returns profiling information concerning a specific sample of the dataset, e.g.:
    *
    * Number of samples => 15
    * Number of regions => 31209
    * Average region length => 123.12
    *
    * @param datasetName dataset name as a string
    * @param sampleName  name of the sample (index 1 .. N)
    * @param userName    the owner of the dataset
    */
  override def getSampleProfile(datasetName: String, sampleName: String, userName: String): Map[String, String] =

    if (userName=="federated")
      api.getSampleProfile(datasetName,sampleName)
    else
      repository.getSampleProfile(datasetName, sampleName, userName)


  /**
    * Returns information about the user disk quota usage
    *
    * @param userName
    * @param userClass
    * @return (occupied, user_quota) in KBs
    */
  override def getUserQuotaInfo(userName: String, userClass: GDMSUserClass): (Long, Long) =
    repository.getUserQuotaInfo(userName, userClass)

  /**
    * Boolean value: true if user quota is exceeded
    *
    * @param username
    * @param userClass
    * @return
    */
  override def isUserQuotaExceeded(username: String, userClass: GDMSUserClass): Boolean =
    repository.isUserQuotaExceeded(username, userClass)

  /**
    * Save a serialized dag to the dag folder for the specified user
    *
    * @param userName      [[String]] the username
    * @param serializedDag [[String]] the serialized dag
    * @param fileName      [[String]] the file name of the dag in the folder
    * @return the resulting location
    **/
  override def saveDagQuery(userName: String, serializedDag: String, fileName: String): String =
    repository.saveDagQuery(userName, serializedDag, fileName)

  override def getInfoStream(dataSetName: String, userName: String): InputStream =
    repository.getInfoStream(dataSetName, userName)
}