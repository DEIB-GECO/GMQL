package it.polimi.genomics.repository

import java.util

import it.polimi.genomics.core.DataStructures.IRDataSet
import it.polimi.genomics.core.ParsingType
import it.polimi.genomics.core.ParsingType._
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
    * @param Samples List of GMQL samples {@link GMQLSample}.
    * @param schemaPath String of the path to the xml file of the dataset schema.
    * @throws GMQLNotValidDatasetNameException
    * @throws GMQLUserNotFound
    * @throws java.lang.Exception
    */
  @throws(classOf[GMQLDSException])
  def importDs(dataSetName:String, userName:String, Samples:java.util.List[GMQLSample], schemaPath:String)


  /**
    *
    * Add a new dataset to GMQL repository, this dataset is usually a result of a script execution.
    *
    * @param dataSet Intermediate Representation (IRDataSet) of the dataset, contains the dataset name and schema.
    * @param Schema
    * @param Samples
    * @param GMQLScriptPaht
    * @throws it.polimi.genomics.repository.GMQLExceptions.GMQLDSNotFound
    * @throws it.polimi.genomics.repository.GMQLExceptions.GMQLUserNotFound
    * @throws it.polimi.genomics.repository.GMQLExceptions.GMQLSampleNotFound
    */
  @throws(classOf[GMQLDSException])
  def createDs(dataSet:IRDataSet, userName:String, Samples:java.util.List[GMQLSample], GMQLScriptPaht:String = "ROOT_DS",schemaType:GMQLSchemaTypes.Value=GMQLSchemaTypes.Delimited)


  /**
    *
    *  Delete data set from the repository
    *
    * @param dataSet Intermediate Representation (IRDataSet) of the dataset, contains the dataset name and schema.
    * @throws GMQLDSNotFound
    * @throws GMQLDSException
    * @throws GMQLUserNotFound
    */
  @throws(classOf[GMQLDSException])
  def DeleteDS(dataSetName:String, userName:String)


  /**
    *
    *   Add {@link GMQLSample}  to dataset {@link IRDataSet} in the repository
    *
    * @param dataSet Intermediate Representation (IRDataSet) of the dataset, contains the dataset name and schema.
    * @param Sample
    * @throws GMQLDSNotFound
    * @throws GMQLDSException
    * @throws GMQLUserNotFound
    * @throws GMQLSampleNotFound
    */
  @throws(classOf[GMQLDSException])
  def AddSampleToDS(dataSet:String, userName:String, Sample:GMQLSample)

  /**
    *
    *  Delete {@link GMQLSample} from a dataset {@link IRDataSet}
    *
    * @param dataSet Intermediate Representation (IRDataSet) of the dataset, contains the dataset name and schema.
    * @param Sample
    * @throws GMQLDSNotFound
    * @throws GMQLDSException
    * @throws GMQLUserNotFound
    * @throws GMQLSampleNotFound
    */
  @throws(classOf[GMQLDSException])
  def DeleteSampleFromDS(dataSet:String, userName:String, Sample:GMQLSample)

  /**
    *
    *  List all the {@link IRDataSet} dataset of the user in the repository
    *
    * @param userName {@link String} of the user name
    * @throws GMQLDSException
    * @throws GMQLUserNotFound
    */
  @throws(classOf[GMQLDSException])
  def ListAllDSs(userName:String): java.util.List[IRDataSet]

  /**
    *
    *   List all the samples {@link GMQLSample} of specific dataset in the repository
    *
    * @param dataSet
    * @throws GMQLDSException
    * @return
    */
  @throws(classOf[GMQLDSException])
  def ListDSSamples(dataSetName:String, userName:String):java.util.List[GMQLSample]

  /**
    *
    * List the result {@link GMQLSample} and the schema of an execution of GMQL script
    *
    * @param dataSetName {@link String} of the dataset name
    * @throws GMQLDSException
    * @return
    */
  @throws(classOf[GMQLDSException])
  def ListResultDSSamples(dataSetName:String, userName:String):(java.util.List[GMQLSample],java.util.List[(String,PARSING_TYPE)])

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
    * @param dataSet {@link IRDataSet} of the dataset
    * @param userName  {@link String} of the user name
    * @throws GMQLDSException
    * @return
    */
  @throws(classOf[GMQLDSException])
  def DSExists(dataSet:String, userName:String): Boolean

  /**
    *
    *  Verify the existance of a dataset in the public dataset
    *
    * @param dataSet {@link IRDataSet} of the dataset
    * @throws GMQLDSException
    * @return
    */
  @throws(classOf[GMQLDSException])
  def DSExistsInPublic( dataSet:String): Boolean

  /**
    *
    *  return the statistics (profiling ) of the dataset
    *
    * @param dataSet Intermediate Representation (IRDataSet) of the dataset, contains the dataset name and schema.
    * @return
    */
  def getDSStatistics(dataSet:String, userName:String):GMQLStatistics


  /**
    *
    *   return the schema of the dataset
    *
    * @param schemaPath {@link String} of the path to the schema xml
    * @return
    */
  def readSchemaFile(schemaPath:String): util.List[(String, ParsingType.Value)]

  /**
    *
    * Get the metadata of a Dataset
    *
    * @param dataSet {@link IRDataSet}, dataset identifier
    * @param userName {@link String} of the user name
    * @return
    */
  def getMeta(dataSet: String,userName:String):String

  /**
    *
    *  Return a {@link String} of the meta data of specific sample
    *
    * @param dataSet {@link IRDataSet}, dataset identifier
    * @param userName {@link String} of the user name
    * @param sample
    * @return
    */
  def getSampleMeta(dataSet: String, userName:String, sample: GMQLSample):String

  /**
    *
    *  Return a list of {@ink GNQLSample} after searching the meta data with a query
    *
    * @param dataSet {@link IRDataSet}, dataset identifier
    * @param userName {@link String} of the user name
    * @param query  {@link String} of the query
    * @return
    */
  def searchMeta(dataSet: String, userName:String, query:String): java.util.List[GMQLSample]

  /**
    *
    *  Register user in the repository
    *
    * @param userName {@link String} of the user name
    * @return
    */
  def registerUser(userName:String): Boolean

  /**
    *
    *  Delete a user from the repsository
    *
    * @param userName {@link String} of the user name
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

}


