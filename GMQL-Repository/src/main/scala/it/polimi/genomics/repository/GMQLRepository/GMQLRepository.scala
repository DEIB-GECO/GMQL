package it.polimi.genomics.repository.GMQLRepository

import it.polimi.genomics.core.DataStructures.IRDataSet
import it.polimi.genomics.core.ParsingType._

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
    * @throws it.polimi.genomics.repository.GMQLRepository.GMQLDSNotFound
    * @throws it.polimi.genomics.repository.GMQLRepository.GMQLUserNotFound
    * @throws it.polimi.genomics.repository.GMQLRepository.GMQLSampleNotFound
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
    * @throws it.polimi.genomics.repository.GMQLRepository.GMQLDSNotFound
    * @throws it.polimi.genomics.repository.GMQLRepository.GMQLUserNotFound
    * @throws it.polimi.genomics.repository.GMQLRepository.GMQLSampleNotFound
    */
  @throws(classOf[GMQLDSException])
  def createDs(dataSet:IRDataSet, userName:String, Samples:java.util.List[GMQLSample], GMQLScriptPaht:String = "ROOT_DS",schemaType:GMQLSchemaTypes.Value=GMQLSchemaTypes.Delimited)


  /**
    *
    * @param dataSet
    * @throws it.polimi.genomics.repository.GMQLRepository.GMQLDSNotFound
    * @throws it.polimi.genomics.repository.GMQLRepository.GMQLDSException
    * @throws it.polimi.genomics.repository.GMQLRepository.GMQLUserNotFound
    */
  @throws(classOf[GMQLDSException])
  def DeleteDS(dataSetName:String, userName:String)


  /**
    *
    * @param dataSet
    * @param Sample
    * @throws it.polimi.genomics.repository.GMQLRepository.GMQLDSNotFound
    * @throws it.polimi.genomics.repository.GMQLRepository.GMQLDSException
    * @throws it.polimi.genomics.repository.GMQLRepository.GMQLUserNotFound
    * @throws it.polimi.genomics.repository.GMQLRepository.GMQLSampleNotFound
    */
  @throws(classOf[GMQLDSException])
  def AddSampleToDS(dataSet:IRDataSet, userName:String, Sample:GMQLSample)

  /**
    *
    * @param dataSet
    * @param Sample
    * @throws it.polimi.genomics.repository.GMQLRepository.GMQLDSNotFound
    * @throws it.polimi.genomics.repository.GMQLRepository.GMQLDSException
    * @throws it.polimi.genomics.repository.GMQLRepository.GMQLUserNotFound
    * @throws it.polimi.genomics.repository.GMQLRepository.GMQLSampleNotFound
    */
  @throws(classOf[GMQLDSException])
  def DeleteSampleFromDS(dataSet:IRDataSet, userName:String, Sample:GMQLSample)

  /**
    *
    * @param userName
    * @throws it.polimi.genomics.repository.GMQLRepository.GMQLDSException
    * @throws it.polimi.genomics.repository.GMQLRepository.GMQLUserNotFound
    */
  @throws(classOf[GMQLDSException])
  def ListAllDSs(userName:String): java.util.List[IRDataSet]

  /**
    *
    * @param dataSet
    * @throws it.polimi.genomics.repository.GMQLRepository.GMQLDSException
    * @return
    */
  @throws(classOf[GMQLDSException])
  def ListDSSamples(dataSetName:String, userName:String):java.util.List[GMQLSample]

  /**
    *
    * @param dataSetName
    * @throws it.polimi.genomics.repository.GMQLRepository.GMQLDSException
    * @return
    */
  @throws(classOf[GMQLDSException])
  def ListResultDSSamples(dataSetName:String, userName:String):java.util.List[GMQLSample]

  /**
    *DO NOT Forget to check the existance ot the dataset name before copying the dataset
    *
    * @param dataSet
    * @throws it.polimi.genomics.repository.GMQLRepository.GMQLDSException
    */
  @throws(classOf[GMQLDSException])
  def exportDsToLocal(dataSet:String, userName:String,localDir:String)

  /**
    *
    * @param dataSet
    * @throws it.polimi.genomics.repository.GMQLRepository.GMQLDSException
    * @return
    */
  @throws(classOf[GMQLDSException])
  def DSExists(dataSet:IRDataSet, userName:String): Boolean

  /**
    *
    * @param dataSet
    * @throws it.polimi.genomics.repository.GMQLRepository.GMQLDSException
    * @return
    */
  @throws(classOf[GMQLDSException])
  def DSExistsInPublic( dataSet:IRDataSet, userName:String): Boolean

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
  def getSchema(dataSetName: String, userName:String): java.util.List[(String,PARSING_TYPE)]

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

}


