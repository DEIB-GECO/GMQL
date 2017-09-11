package it.polimi.genomics.repository.FSRepository

import java.io._
import java.text.SimpleDateFormat
import java.util.Date

import it.polimi.genomics.core.DataStructures.IRDataSet
import it.polimi.genomics.core.{GMQLSchemaField, GMQLSchemaFormat}
import it.polimi.genomics.repository
import it.polimi.genomics.repository.FSRepository.datasets.GMQLDataSetXML
import it.polimi.genomics.repository.GMQLExceptions.{GMQLDSException, GMQLDSNotFound, GMQLSampleNotFound, GMQLUserNotFound}
import it.polimi.genomics.repository.{GMQLRepository, GMQLSample, GMQLStatistics, Utilities => General_Utilities}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._
/**
  * Created by abdulrahman on 12/04/16.
  */
class DFSRepository extends GMQLRepository with XMLDataSetRepository{
  private final val logger = LoggerFactory.getLogger(this.getClass)
    repository.Utilities()

  /**
    * Add a new dataset to GMQL repository, this dataset is usually a result of a script execution.
    *
    * @param dataSet Intermediate Representation (IRDataSet) of the dataset, contains the dataset name and schema.
    * @param userName The user name in which this dataSet belongs to.
    * @param Samples List of GMQL samples [[ GMQLSample]].
    * @param GMQLScriptPath The path to the script text file that generated this data set.
    * @param schemaType One of GMQL schema types as shown in [[ GMQLSchemaFormat]]
    */
  override def createDs(dataSet:IRDataSet, userName: String, Samples: java.util.List[GMQLSample], GMQLScriptPath: String,schemaType:GMQLSchemaFormat.Value): Unit = {
    //Create Temp folder to place the meta files temporarly in Local file system
    val tmpFolderName = General_Utilities().getTempDir(userName)+dataSet.position+"_/"
    val tmpFolder = new File(tmpFolderName)
    tmpFolder.mkdirs()

    //copy all the meta data from HDFS to Local file system
    val samples = Samples.asScala.map{x=>
      val metaFile= tmpFolderName + x.name+".meta"
      val sourceHDFSMetaFile = General_Utilities().getHDFSRegionDir(userName) + x.name+".meta"
      FS_Utilities.copyfiletoLocal(sourceHDFSMetaFile, metaFile)
      new GMQLSample(name = x.name,meta = metaFile )
    }.toList.asJava

    //create DS as a set of XML files in the local repository
    super.createDs(dataSet, userName, samples, GMQLScriptPath, schemaType)

    //clean the temp Directory
    tmpFolder.delete()
  }

  /**
    *     * Import Dataset into GMQL from Local file system.
    *
    * @param dataSetName String of the dataset name.
    * @param Samples List of GMQL samples [[ GMQLSample]].
    * @param userName String of the user name to add this dataset to.
    * @param schemaPath String of the path to the xml file of the dataset's schema.
    */
  override def importDs(dataSetName: String, userName: String, Samples: java.util.List[GMQLSample], schemaPath: String): Unit = {
    if (FS_Utilities.validate(schemaPath)) {
      val date = new SimpleDateFormat("yyyyMMdd_HHmmss").format(new Date());
      val samples = Samples.asScala.map(x=> GMQLSample(x.ID,dataSetName+"_"+date+ "/"+new File(x.name).getName,dataSetName +"_"+date+ "/"+new File(x.meta).getName) ).asJava
      // Import the dataset schema and Script files to the local folder
      super.importDs(dataSetName: String, userName: String, samples ,schemaPath)

      // Copy sample and Meta data from HDFS to the local folder
      samples.asScala.map{x =>
        val HDFSfile = General_Utilities().getHDFSDSRegionDir(userName,dataSetName+"_"+date) +  new File(x.name).getName
        FS_Utilities.copyfiletoHDFS(x.name, HDFSfile)
      }

      FS_Utilities.copyfiletoHDFS(General_Utilities().getSchemaDir(userName)+dataSetName+".schema",
        General_Utilities().getHDFSRegionDir(userName)+ new Path(samples.get(0).name).getParent.toString+ "/test.schema"
      )
    } else {
      logger.info("The dataset schema does not confirm the schema style (XSD)")
    }
  }

  /**
    *  Add sample to Data Set,
    *  TODO: i did not finish it since The web interface does not use it for the moment
    *
    * @param dataSet Intermediate Representation (IRDataSet) of the dataset, contains the dataset name and schema.
    * @param Sample GMQL sample [[ GMQLSample]].
    * @param userName String of the user name.
    */
  override def addSampleToDS(dataSet: String, userName: String, Sample: GMQLSample): Unit = ???

  /**
    * Delete Data Set from the repository, Delete XML files from local File system and delete the samples and meta files from HDFS.
    *
    * @param dataSet Intermediate Representation (IRDataSet) of the dataset, contains the dataset name and schema.
    * @throws GMQLDSNotFound
    * @throws GMQLDSException
    * @throws GMQLUserNotFound
    */
  override def deleteDS(dataSetName:String, userName:String): Unit = {
    val dataset = new GMQLDataSetXML(dataSetName,userName).loadDS()

    //Delete files from HDFS
    val conf = FS_Utilities.gethdfsConfiguration()
    val fs = FileSystem.get(conf)
    val hdfspath = conf.get("fs.defaultFS") + General_Utilities().getHDFSRegionDir(userName)
    dataset.samples.map{x=>
      fs.delete(new Path(hdfspath+x.name),true);
      fs.delete(new Path(hdfspath+x.meta),true)
    }

    //Delete dataset XML files
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
  override def deleteSampleFromDS(dataSet:String, userName: String, Sample:GMQLSample): Unit = ???

  /**
    *   List of the samples that was generated from a GMQL script execution
    *   and the schema for these samples
    * @param dataSet
    * @throws GMQLDSException
    * @return
    */
  override def listResultDSSamples(dataSetName:String, userName: String): (java.util.List[GMQLSample],java.util.List[GMQLSchemaField]) = {
    val conf = FS_Utilities.gethdfsConfiguration()
    val fs = FileSystem.get(conf);
    val dsPath = conf.get("fs.defaultFS") +repository.Utilities().getHDFSRegionDir(userName) + dataSetName
    val samples = fs.listStatus(new Path(dsPath))
        .flatMap(x => {
          if (fs.exists(new Path(x.getPath.toString+".meta")) ) {
            Some(new GMQLSample(dataSetName+x.getPath.getName))
          } else
            None
        }).toList.asJava;
    val schema =
      readSchemaFile(dsPath + "/test.schema")
    (samples,schema.fields.asJava)
  }


  /**
    *
    * @param dataSet Intermediate Representation (IRDataSet) of the dataset, contains the dataset name and schema.
    * @return
    */
  override def getDSStatistics(dataSet: String, userName: String): GMQLStatistics = ???

  /**
    *  export the dataset from Hadoop Distributed File system to Local File system
    *
    * @param dataSetName String of the dataset name
    * @param userName String of the owner of the dataset
    * @param localDir  String of the local directory path
    */
  override def exportDsToLocal(dataSetName: String, userName: String, localDir:String): Unit = {

    // export the schema and the script files
    super.exportDsToLocal(dataSetName, userName, localDir)

    val gMQLDataSetXML = new GMQLDataSetXML(dataSetName, userName).loadDS()
    val dest = new File(localDir)
    dest.mkdir()

    //copy samples/meta files to local file system
    gMQLDataSetXML.samples.map { x =>
      FS_Utilities.copyfiletoLocal(repository.Utilities().getHDFSRegionDir(userName) + x.name, localDir + "/" + new File(x.name).getName)
    }

  }

  /**
    *   Register user in the repository.
    *
    * @param userName String of the user name.
    * @return
    */
  override def registerUser(userName: String): Boolean = {
    val dir = General_Utilities().getHDFSRegionDir(userName)
    var creationMessage = if(FS_Utilities.createDFSDir(dir)) "\t Created..." else "\t Not created..."
    logger.info( dir + creationMessage)

    // create a folder also for the serialized dags
    val dag_dir = General_Utilities().getHDFSDagQueryDir(userName, create = false)
    creationMessage = if(FS_Utilities.createDFSDir(dag_dir)) "\t Dag folder created..." else "\t Dag folder not created..."
    logger.info( dag_dir + creationMessage)
    super.registerUser(userName)
  }

  /**
    *   Delete a user from the repository.
    *
    * @param userName String of the user name.
    * @return
    */
  override def unregisterUser(userName: String): Boolean = {
    logger.info(s"HDFS Folders Deletion for user $userName...")

    logger.info( General_Utilities().getHDFSRegionDir( userName )+"\t Status:" +
      (if (FS_Utilities.deleteDFSDir(General_Utilities().getHDFSRegionDir( userName ))) "Deleted." else "Error"))

    super.unregisterUser(userName)
  }


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
        logger.debug(s"Get stream of: $userName.$dataSetName->$sampleName")
        val pathRegion = new Path(General_Utilities().getHDFSRegionDir(userName) + sample.name)
        val pathMeta = new Path(General_Utilities().getHDFSRegionDir(userName) + sample.meta)

        val conf = FS_Utilities.gethdfsConfiguration()
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

  /**
    * Save a serialized dag to the dag folder for the specified user
    *
    * @param userName      [[String]] the username
    * @param serializedDag [[String]] the serialized dag
    * @return the resulting location
    **/
  override def saveDagQuery(userName: String, serializedDag: String, fileName: String): String = {
    val queryPath = General_Utilities().getHDFSDagQueryDir(userName)
    val conf = FS_Utilities.gethdfsConfiguration()
    val fs = FileSystem.get(conf)
    val resultPath = queryPath + "/" + fileName
    val outStream = fs.create(new Path(resultPath))
    serializedDag.map(x => outStream.write(x))
    outStream.close()
    resultPath
  }
}
