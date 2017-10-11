package it.polimi.genomics.repository.FSRepository

import java.io._
import java.util

import it.polimi.genomics.core.GDMSUserClass.GDMSUserClass
import it.polimi.genomics.core.GMQLSchemaField
import it.polimi.genomics.repository.FSRepository.datasets.GMQLDataSetXML
import it.polimi.genomics.repository.GMQLExceptions.{GMQLDSException, GMQLNotValidDatasetNameException, GMQLSampleNotFound, GMQLUserNotFound}
import it.polimi.genomics.repository.{Utilities => General_Utilities, _}
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConverters._
/**
  * Created by abdulrahman on 12/04/16.
  */
class LFSRepository extends GMQLRepository with XMLDataSetRepository{
  private final val logger: Logger = LoggerFactory.getLogger(this.getClass)
  General_Utilities()

  /**
    *     * Import Dataset into GMQL from Local file system.
    *
    * @param dataSetName  String of the dataset name.
    * @param userName String of the user name.
    * @param Samples List of GMQL samples [[ GMQLSample]].
    * @param schemaPath String of the path to the xml file of the dataset schema.
    * @throws GMQLNotValidDatasetNameException
    * @throws GMQLUserNotFound
    * @throws java.lang.Exception
    */
  @throws(classOf[GMQLNotValidDatasetNameException])
  @throws(classOf[GMQLUserNotFound])
  @throws(classOf[Exception])
  override def importDs(dataSetName: String, userName: String, Samples: java.util.List[GMQLSample], schemaPath: String): Unit = {

    //Check if the dataset schema is valid otherwise return an exception
    if (FS_Utilities.validate(schemaPath)) {
      super.importDs(dataSetName: String, userName: String, Samples: java.util.List[GMQLSample], schemaPath: String)
    } else {
      logger.warn("The dataset schema does not confirm the schema style (XSD)")
      throw new Exception("Schema error")
    }
  }




  /**
    *
    * @param dataSet Intermediate Representation (IRDataSet) of the dataset, contains the dataset name and schema.
    * @return
    */
  override def getDSStatistics(dataSet: String, userName: String): GMQLStatistics = ???

  /**
    * Copy data set from GMQL repository to local folder,
    * dataset includes; Schema file, script file, samples files, and metadata files
    *
    * @param dataSetName
    * @throws GMQLDSException
    */
  override def exportDsToLocal(dataSetName: String, userName: String, localDir:String): Unit = {
    import java.io.{File, FileInputStream, FileOutputStream}

    // export the schema and the script files
    super.exportDsToLocal(dataSetName, userName, localDir)

    val gMQLDataSetXML = new GMQLDataSetXML(dataSetName, userName).loadDS()
    val dest = new File(localDir)

    //copy samples/meta files to local file system
    gMQLDataSetXML.samples.map { x =>
      val src = new File(x.name)
      val srcMeta = new File(x.meta)

      new FileOutputStream(dest + "/" + src.getName) getChannel() transferFrom(
        new FileInputStream(src) getChannel, 0, Long.MaxValue)

      new FileOutputStream(dest + "/" + srcMeta.getName) getChannel() transferFrom(
        new FileInputStream(srcMeta) getChannel, 0, Long.MaxValue)
    }

  }

  /**
    * This method is used only by GMQL engine to list the samples of a generated dataset.
    * List result dataset samples method is used before creating a new dataset from a result dataset.
    * @param dataSetName
    * @throws GMQLDSException
    * @return
    */
  @deprecated
  override def listResultDSSamples(dataSetName:String, userName: String): (java.util.List[GMQLSample],java.util.List[GMQLSchemaField]) = {
    val dsPath = General_Utilities().getRegionDir(userName) + dataSetName
    val samples = new java.io.File(dsPath).listFiles(
      new FileFilter() {
        @Override def accept(pathname: java.io.File) = !pathname.getName.startsWith("_") && !pathname.getName.startsWith(".") && !pathname.getName.endsWith(".meta");
      }
    ).map(x=> new GMQLSample(x.getPath)).toList.asJava

    val schema = readSchemaFile(dsPath+ "/test.schema")
    (samples,schema.fields.asJava)
  }


  /**
    *
    * @param dataSet Intermediate Representation (IRDataSet) of the dataset, contains the dataset name and schema.
    * @param query
    * @return
    */
  override def searchMeta(dataSet: String, userName: String, query: String): java.util.List[GMQLSample] = ???

  /**
    *
    * @param dataSetName dataset name as a string
    * @param userName the owner of the dataset as a String
    * @param sampleName The sample name, which is the file name with out the full path as a String
    * @return Two Streams, one for the sample and the other for the metadata
    */
  override def sampleStreams(dataSetName: String, userName: String, sampleName: String): (InputStream, InputStream) = {
    val gMQLDataSetXML = new GMQLDataSetXML(dataSetName, userName).loadDS()

    val sampleOption = gMQLDataSetXML.samples.find(_.name.split("\\.").head.endsWith(sampleName))
    sampleOption match {
      case Some(sample) =>
        val dsLocation = getDSLocation(dataSetName,userName)
        val dir = if(dsLocation._1.equals(RepositoryType.LOCAL) && dsLocation._2.equals(DatasetOrigin.GENERATED))
          General_Utilities().getRegionDir(userName)
        else {
          ""
        }
        val pathRegion = new File(dir +sample.name)
        val pathMeta = new File(dir +sample.meta)

        //check region file exists
        if (!pathRegion.exists()) {
          logger.error("The Dataset sample Url is not found: " + sample.name)
          throw new GMQLSampleNotFound
        }
        //check meta file exists
        if (!pathMeta.exists()) {
          logger.error("The Dataset sample Url is not found: " + sample.meta)
          throw new GMQLSampleNotFound
        }
        (new FileInputStream(pathRegion), new FileInputStream(pathMeta))
      case None =>
        logger.error("The Dataset sample Url is not found in the xml: ")
        throw new GMQLSampleNotFound
    }
  }

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
  override def importDs(dataSetName: String, userName: String, userClass: GDMSUserClass, Samples: util.List[GMQLSample], schemaPath: String): Unit = {
    importDs(dataSetName, userName, Samples, schemaPath)
  }

  /**
    * Return a stram of the dataset.xml file
    *
    * @param datasetName
    * @param userName
    * @return
    */
  override def getDsInfoStream(datasetName: String, userName: String): InputStream = ???

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
  override def getDatasetMeta(datasetName: String, userName: String): Map[String, String] = {

    var res = Map[String,String]()
    res += ("Source" -> "Wellington")
    res += ("Type" -> "Wellington")
    res += ("Creation Date" -> "Wellington")
    res += ("Creation Time" -> "Wellington")
    res += ("Size" -> "50.12 MB")

    res

  }

  /**
    * Set an entry on dataset metadata
    *
    * @param datasetName
    * @param userName
    * @param key
    * @param value
    */
  override def setDatasetMeta(datasetName: String, userName: String, key: String, value: String): Unit = ???

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
  override def getDatasetProfile(datasetName: String, userName: String): Map[String, String] = {

    var res = Map[String,String]()
    res += ("Number of samples" -> "15")
    res += ("Number of regions" -> "31209")
    res += ("Average region length" -> "123.12")

    res
  }

  /**
    * Returns profiling information concerning a specific sample of the dataset, e.g.:
    *
    * Number of samples => 15
    * Number of regions => 31209
    * Average region length => 123.12
    *
    * @param datasetName dataset name as a string
    * @param sampleId    id of the sample (index 1 .. N)
    * @param usernName   the owner of the dataset
    */
  override def getSampleProfile(datasetName: String, sampleId: Long, usernName: String): Unit = {

    var res = Map[String,String]()
    res += ("Number of samples" -> "15")
    res += ("Number of regions" -> "31209")
    res += ("Average region length" -> "123.12")

    res
  }

  /**
    * Returns information about the user disk quota usage
    *
    * @param userName
    * @param userClass
    * @return (occupied, available) in KBs
    */
  override def getUserQuotaInfo(userName: String, userClass: GDMSUserClass): (Float, Float) = {

    (500000,1000000)
  }

  /**
    * Boolean value: true if user quota is exceeded
    *
    * @param username
    * @param userClass
    * @return
    */
  override def isUserQuotaExceeded(username: String, userClass: GDMSUserClass): Boolean = {
    false
  }
}