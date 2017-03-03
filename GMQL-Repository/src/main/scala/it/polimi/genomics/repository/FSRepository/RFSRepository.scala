package it.polimi.genomics.repository.FSRepository

import java.io.{File, FilenameFilter, InputStream}
import java.nio.file.Files

import it.polimi.genomics.core.DataStructures.IRDataSet
import it.polimi.genomics.core.ParsingType
import it.polimi.genomics.core.ParsingType.PARSING_TYPE
import it.polimi.genomics.repository.{Utilities => General_Utilities}
import it.polimi.genomics.repository.FSRepository.datasets.GMQLDataSetXML
import it.polimi.genomics.repository.GMQLExceptions._
import it.polimi.genomics.repository._
import it.polimi.genomics.wsc.Knox.KnoxClient
import org.apache.hadoop.fs.{FileSystem, Path}
import org.slf4j.LoggerFactory

import scala.concurrent.Await
import scala.xml.XML
import scala.collection.JavaConverters._

/**
  * Created by abdulrahman on 12/04/16.
  */
class RFSRepository extends GMQLRepository with XMLDataSetRepository {
  private final val logger = LoggerFactory.getLogger(this.getClass)
  General_Utilities()

  /**
    *
    * @param dataSet Intermediate Representation (IRDataSet) of the dataset, contains the dataset name and schema.
    * @param Samples List of GMQL samples [[ GMQLSample]].
    * @param userName String of the user who own this dataset
    * @param GMQLScriptPath String that describe the path to the script text file on the local file system
    * @param schemaType The schema type as one of the [[ GMQLSchemaTypes]]
    * @throws GMQLNotValidDatasetNameException
    * @throws GMQLUserNotFound
    */
  @throws(classOf[GMQLNotValidDatasetNameException])
  @throws(classOf[GMQLUserNotFound])
  override def createDs(dataSet: IRDataSet, userName: String, Samples: java.util.List[GMQLSample], GMQLScriptPath: String, schemaType: GMQLSchemaTypes.Value): Unit = {

    val tempDir = General_Utilities().getTempDir( userName) + "/" + dataSet.position + "_/"
    val tempDirectory = new File(tempDir)
    tempDirectory.mkdirs()

    //download meta data files from remote server to local temporary directory to be able to build the meta descriptor of the dataset
    val samples = Samples.asScala.map { x =>
      val metaFile = tempDir + new File(x.meta).getName
      KnoxClient.downloadFile(General_Utilities().getHDFSRegionDir(userName) + x.meta, new File(metaFile))
      new GMQLSample(x.name, metaFile, x.ID)
    }.toList

    //create DS as a set of XML files in the local repository, this will read the meta files from the temp directory.
    super.createDs(dataSet, userName, Samples, GMQLScriptPath,schemaType)

    //clean temp from the meta files.
    tempDirectory.delete()
  }

  /**
    *
    * DO NOT Forget to check the existance ot the datasetname before creating the dataset
    *
    * @param dataSet Intermediate Representation (IRDataSet) of the dataset, contains the dataset name and schema.
    * @param Schema
    * @param Samples List of GMQL samples [[ GMQLSample]].
    * @param GMQLScriptPaht
    * @throws GMQLDSNotFound
    * @throws GMQLUserNotFound
    * @throws GMQLSampleNotFound
    */
  @throws(classOf[GMQLNotValidDatasetNameException])
  @throws(classOf[GMQLUserNotFound])
  override def importDs(dataSetName: String, userName: String, Samples: java.util.List[GMQLSample], schemaPath: String): Unit = {
    if (FS_Utilities.validate(schemaPath)) {
      // Import the dataset schema and Script files to the local folder
      super.importDs(dataSetName: String, userName: String, Samples: java.util.List[GMQLSample], schemaPath: String)

      //move data using KNOX to the remote Cluster.
      Samples.asScala.map { x =>
        KnoxClient.mkdirs(General_Utilities().HDFSRepoDir + (new File(x.name).getParent))
        KnoxClient.uploadFile(x.name, General_Utilities().HDFSRepoDir + x.name)
        KnoxClient.uploadFile(x.name + ".meta", General_Utilities().HDFSRepoDir + x.name + ".meta")
      }
    } else {
      logger.info("The dataset schema does not confirm the schema style (XSD)")
    }
  }

  /**
    *
    * @param dataSet Intermediate Representation (IRDataSet) of the dataset, contains the dataset name and schema.
    * @param Sample GMQL sample [[ GMQLSample]].
    * @throws GMQLDSNotFound
    * @throws GMQLDSException
    * @throws GMQLUserNotFound
    * @throws GMQLSampleNotFound
    */
  @throws(classOf[GMQLDSException])
  override def addSampleToDS(dataSet: String, userName: String, Sample: GMQLSample) = ???

  /**
    *
    * @param dataSet Intermediate Representation (IRDataSet) of the dataset, contains the dataset name and schema.
    * @throws GMQLDSNotFound
    * @throws GMQLDSException
    * @throws GMQLUserNotFound
    */
  @throws(classOf[GMQLDSException])
  override def deleteDS(dataSetName: String, userName: String): Unit = {
    val dataset = new GMQLDataSetXML(dataSetName, userName).loadDS()
    val dfsDir = General_Utilities().getHDFSRegionDir(userName)
    dataset.samples.map { x => KnoxClient.delDIR(dfsDir + x.name); KnoxClient.delDIR(dfsDir + x.meta) }

    dataset.Delete()
  }

  /**
    *
    * @param dataSet Intermediate Representation (IRDataSet) of the dataset, contains the dataset name and schema.
    * @param Sample GMQL sample [[ GMQLSample]].
    * @throws GMQLDSNotFound
    * @throws GMQLDSException
    * @throws GMQLUserNotFound
    * @throws GMQLSampleNotFound
    */
  override def deleteSampleFromDS(dataSet: String, userName: String, sample: GMQLSample): Unit = ???

  /**
    *
    * @param dataSet Intermediate Representation (IRDataSet) of the dataset, contains the dataset name and schema.
    * @throws GMQLDSException
    * @return
    */
  override def listResultDSSamples(dataSetName: String, userName: String):(java.util.List[GMQLSample],java.util.List[GMQLSchemaField])= {
    import scala.concurrent.duration._
    val dsPath = General_Utilities().getHDFSRegionDir(userName) + dataSetName
    //User Knox client to connect to the remote cluster to list all the files under the result directory
    val contents = Await.result(KnoxClient.listFiles(dsPath), 10.second)

    //Search only for the files in the list not directories
    val files = contents flatMap { x => if (x._2.equals("FILE")) Some(x._1) else None }

    //filter the listed files to include only the region files that has a corresponding meta files.
    val samples = files.flatMap(x => if (x.endsWith("meta") || x.endsWith("schema") || x.endsWith("_SUCCESS")) None else Some(new GMQLSample(dataSetName + x, dataSetName + x + ".meta"))).toList.asJava

    KnoxClient.downloadFile(dsPath + "/test.schema",new File(General_Utilities().getTempDir() +dataSetName+".schema"))
    val schema = readSchemaFile(General_Utilities().getTempDir() + dataSetName +".schema")

    (samples,schema.fields.asJava)
  }


  /**
    *
    * @param dataSet Intermediate Representation (IRDataSet) of the dataset, contains the dataset name and schema.
    * @return
    */
  override def getDSStatistics(dataSet: String, userName: String): GMQLStatistics = ???


  /**
    * Export data from the remote cluster to local folder.
    * Exported Data include the schema, the sampels, meta files, and script text file.
    *
    * @param dataSet Intermediate Representation (IRDataSet) of the dataset, contains the dataset name and schema.
    * @throws GMQLDSException
    */
  override def exportDsToLocal(dataSetName: String, userName: String, localDir: String): Unit = {

    // export the schema and the script files
    super.exportDsToLocal(dataSetName, userName, localDir)

    val gMQLDataSetXML = new GMQLDataSetXML(dataSetName, userName).loadDS()
    val dest = new File(localDir)

    //Move data from remote cluster to local file system. Samples and its corresponding meta files.
    gMQLDataSetXML.samples.map { x =>
      val HDFSRegionFolder = General_Utilities().getHDFSRegionDir(userName)
      logger.info("Downloading file: "+ HDFSRegionFolder+ x.name, localDir + "/" + new File(x.name).getName)
      KnoxClient.downloadFile(HDFSRegionFolder+ x.name, new File(localDir + "/" + new File(x.name).getName))
      KnoxClient.downloadFile(HDFSRegionFolder + x.name + ".meta", new File(localDir + "/" + new File(x.name).getName + ".meta"))
    }
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
    * THIS FUNCTION IMPLEMENTATION IS NOT TESTED FOR REMOTE
    * NEEDS TO BE REWRITEN
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
        val pathRegion = new Path(sample.name)
        val pathMeta = new Path(sample.meta)

        val conf = FS_Utilities.gethdfsConfiguration
        val fs = FileSystem.get(conf)
        //check region file exists
        if (!fs.exists(pathRegion)) {
          logger.error("The Dataset sample Url is not found: " + sample.name)
          throw new GMQLSampleNotFound
        }
        //check meta file exists
        if (!fs.exists(pathMeta)) {
          logger.error("The Dataset sample Url is not found: " + sample.meta)
          throw new GMQLSampleNotFound
        }
        (fs.open(pathRegion), fs.open(pathMeta))
      case None =>
        logger.error("The Dataset sample Url is not found in the xml: ")
        throw new GMQLSampleNotFound
    }
  }
}
