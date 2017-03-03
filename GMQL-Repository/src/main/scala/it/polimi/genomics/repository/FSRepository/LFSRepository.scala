package it.polimi.genomics.repository.FSRepository

import java.io._

import it.polimi.genomics.core.DataStructures.IRDataSet
import it.polimi.genomics.core.ParsingType
import it.polimi.genomics.core.ParsingType.PARSING_TYPE
import it.polimi.genomics.repository.{Utilities => General_Utilities}
import it.polimi.genomics.repository._
import it.polimi.genomics.repository.FSRepository.datasets.GMQLDataSetXML
import it.polimi.genomics.repository.GMQLExceptions.{GMQLDSException, GMQLNotValidDatasetNameException, GMQLSampleNotFound, GMQLUserNotFound}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.slf4j.LoggerFactory

import scala.xml.XML
import scala.collection.JavaConverters._
/**
  * Created by abdulrahman on 12/04/16.
  */
class LFSRepository extends GMQLRepository with XMLDataSetRepository{
  private final val logger = LoggerFactory.getLogger(this.getClass)
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
    * @param dataSet Intermediate Representation (IRDataSet) of the dataset, contains the dataset name and schema.
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
    * @param dataSet Intermediate Representation (IRDataSet) of the dataset, contains the dataset name and schema.
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
        val pathRegion = new File(sample.name)
        val pathMeta = new File(sample.meta)

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
}
