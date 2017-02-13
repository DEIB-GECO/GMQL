package it.polimi.genomics.repository

import java.util

import it.polimi.genomics.core.DataStructures.IRDataSet
import it.polimi.genomics.core.ParsingType
import it.polimi.genomics.core.ParsingType._
import it.polimi.genomics.repository.GMQLExceptions.{GMQLDSException, GMQLDSNotFound, GMQLSampleNotFound, GMQLUserNotFound}

/**
  * Created by Abdulrahman Kaitoua on 11/04/16.
  */
trait GMQLRepository {

  /**
    *
    * DO NOT Forget to check the existance ot the datasetname before creating the dataset
    *
    * @param dataSet
    * @param Schema
    * @param Samples
    * @param GMQLScriptPaht
    * @throws GMQLDSNotFound
    * @throws GMQLUserNotFound
    * @throws GMQLSampleNotFound
    */
  @throws(classOf[GMQLDSException])
  def importDs(dataSetName:String, userName:String, Samples:java.util.List[GMQLSample], schemaPath:String)


  /**
    *
    * DO NOT Forget to check the existance ot the datasetname before creating the dataset
    *
    * @param dataSet
    * @param Schema
    * @param Samples
    * @param GMQLScriptPaht
    * @throws GMQLDSNotFound
    * @throws GMQLUserNotFound
    * @throws GMQLSampleNotFound
    */
  @throws(classOf[GMQLDSException])
  def createDs(dataSet:IRDataSet, userName:String, Samples:java.util.List[GMQLSample], GMQLScriptPaht:String = "ROOT_DS",schemaType:GMQLSchemaTypes.Value=GMQLSchemaTypes.Delimited)


  /**
    *
    * @param dataSet
    * @throws GMQLDSNotFound
    * @throws GMQLDSException
    * @throws GMQLUserNotFound
    */
  @throws(classOf[GMQLDSException])
  def DeleteDS(dataSetName:String, userName:String)


  /**
    *
    * @param dataSet
    * @param Sample
    * @throws GMQLDSNotFound
    * @throws GMQLDSException
    * @throws GMQLUserNotFound
    * @throws GMQLSampleNotFound
    */
  @throws(classOf[GMQLDSException])
  def AddSampleToDS(dataSet:IRDataSet, userName:String, Sample:GMQLSample)

  /**
    *
    * @param dataSet
    * @param Sample
    * @throws GMQLDSNotFound
    * @throws GMQLDSException
    * @throws GMQLUserNotFound
    * @throws GMQLSampleNotFound
    */
  @throws(classOf[GMQLDSException])
  def DeleteSampleFromDS(dataSet:IRDataSet, userName:String, Sample:GMQLSample)

  /**
    *
    * @param userName
    * @throws GMQLDSException
    * @throws GMQLUserNotFound
    */
  @throws(classOf[GMQLDSException])
  def ListAllDSs(userName:String): java.util.List[IRDataSet]

  /**
    *
    * @param dataSet
    * @throws GMQLDSException
    * @return
    */
  @throws(classOf[GMQLDSException])
  def ListDSSamples(dataSetName:String, userName:String):java.util.List[GMQLSample]

  /**
    *
    * @param dataSetName
    * @throws GMQLDSException
    * @return
    */
  @throws(classOf[GMQLDSException])
  def ListResultDSSamples(dataSetName:String, userName:String):(java.util.List[GMQLSample],java.util.List[(String,PARSING_TYPE)])

  /**
    *DO NOT Forget to check the existance ot the dataset name before copying the dataset
    *
    * @param dataSet
    * @throws GMQLDSException
    */
  @throws(classOf[GMQLDSException])
  def exportDsToLocal(dataSet:String, userName:String,localDir:String)

  /**
    *
    * @param dataSet
    * @throws GMQLDSException
    * @return
    */
  @throws(classOf[GMQLDSException])
  def DSExists(dataSet:IRDataSet, userName:String): Boolean

  /**
    *
    * @param dataSet
    * @throws GMQLDSException
    * @return
    */
  @throws(classOf[GMQLDSException])
  def DSExistsInPublic( dataSet:IRDataSet): Boolean

  /**
    *
    * @param dataSet
    * @return
    */
  def getDSStatistics(dataSet:IRDataSet, userName:String):GMQLStatistics


  /**
    *
    * @param dataSet
    * @return
    */
  def readSchemaFile(schemaPath:String): util.List[(String, ParsingType.Value)]

  /**
    *
    * @param dataSet
    * @return
    */
  def getMeta(dataSet: IRDataSet,userName:String):String

  /**
    *
    * @param dataSet
    * @param sample
    * @return
    */
  def getSampleMeta(dataSet: IRDataSet, userName:String, sample: GMQLSample):String

  /**
    *
    * @param dataSet
    * @param query
    * @return
    */
  def searchMeta(dataSet: IRDataSet, userName:String, query:String): java.util.List[GMQLSample]

  /**
    *
    * @param userName
    * @return
    */
  def registerUser(userName:String): Boolean

  /**
    *
    * @param userName
    * @return
    */
  def unregisterUser(userName:String): Boolean

}


