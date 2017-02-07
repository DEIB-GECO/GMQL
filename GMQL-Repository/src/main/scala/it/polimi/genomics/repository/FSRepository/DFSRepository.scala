package it.polimi.genomics.repository.FSRepository

import java.io.{File, FileInputStream, FileOutputStream, FilenameFilter}

import com.google.common.io.Files
import it.polimi.genomics.core.DataStructures.IRDataSet
import it.polimi.genomics.core.ParsingType
import it.polimi.genomics.core.ParsingType._
import it.polimi.genomics.repository
import it.polimi.genomics.repository.FSRepository.datasets.GMQLDataSetXML
import it.polimi.genomics.repository.GMQLExceptions.{GMQLDSException, GMQLDSNotFound, GMQLSampleNotFound, GMQLUserNotFound}
import it.polimi.genomics.repository.{ GMQLRepository, GMQLSample, GMQLSchemaTypes, GMQLStatistics, Utilities => General_Utilities}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.slf4j.LoggerFactory

import scala.xml.XML
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
    * @param Samples List of GMQL samples {@link GMQLSample}.
    * @param GMQLScriptPath The path to the script text file that generated this data set.
    * @param schemaType One of GMQL schema types as shown in {@link GMQLSchemaTypes}
    */
  override def createDs(dataSet:IRDataSet, userName: String, Samples: java.util.List[GMQLSample], GMQLScriptPath: String,schemaType:GMQLSchemaTypes.Value): Unit = {
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
    * @param Samples List of GMQL samples {@link GMQLSample}.
    * @param userName String of the user name to add this dataset to.
    * @param schemaPath String of the path to the xml file of the dataset's schema.
    */
  override def importDs(dataSetName: String, userName: String, Samples: java.util.List[GMQLSample], schemaPath: String): Unit = {
    if (FS_Utilities.validate(schemaPath)) {
      // Import the dataset schema and Script files to the local folder
      super.importDs(dataSetName: String, userName: String, Samples: java.util.List[GMQLSample], schemaPath: String)

      // Copy sample and Meta data from HDFS to the local folder
      Samples.asScala.map(x => FS_Utilities.copyfiletoHDFS(x.name, General_Utilities().getHDFSRegionDir(userName) + x.name))

      FS_Utilities.copyfiletoHDFS(General_Utilities().getSchemaDir(userName)+dataSetName+".schema",
        General_Utilities().getHDFSRegionDir(userName)+ new Path(Samples.get(0).name).getParent.toString+ "/test.schema"
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
    * @param Sample GMQL sample {@link GMQLSample}.
    * @userName String of the user name.
    */
  override def AddSampleToDS(dataSet: IRDataSet, userName: String, Sample: GMQLSample): Unit = ???

  /**
    * Delete Data Set from the repository, Delete XML files from local File system and delete the samples and meta files from HDFS.
    *
    * @param dataSet Intermediate Representation (IRDataSet) of the dataset, contains the dataset name and schema.
    * @throws GMQLDSNotFound
    * @throws GMQLDSException
    * @throws GMQLUserNotFound
    */
  override def DeleteDS(dataSetName:String, userName:String): Unit = {
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
    * @param Sample GMQL sample {@link GMQLSample}.
    * @throws GMQLDSNotFound
    * @throws GMQLDSException
    * @throws GMQLUserNotFound
    * @throws GMQLSampleNotFound
    */
  override def DeleteSampleFromDS(dataSet:IRDataSet, userName: String, Sample:GMQLSample): Unit = ???

  /**
    *   List of the samples that was generated from a GMQL script execution
    *   and the schema for these samples
    * @param dataSet
    * @throws GMQLDSException
    * @return
    */
  override def ListResultDSSamples(dataSetName:String , userName: String): (java.util.List[GMQLSample],java.util.List[(String,PARSING_TYPE)]) = {
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
    val schema = readSchemaFile(dsPath + "/test.schema")
    (samples,schema)
  }
  /**
    *
    * @param dataSet Intermediate Representation (IRDataSet) of the dataset, contains the dataset name and schema.
    * @return
    */
  override def getDSStatistics(dataSet: IRDataSet, userName: String): GMQLStatistics = ???

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
    logger.info(General_Utilities().getHDFSRegionDir(userName)
      + FS_Utilities.createDFSDir(General_Utilities().getHDFSRegionDir(userName)))
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
}
