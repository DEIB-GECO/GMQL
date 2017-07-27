package it.polimi.genomics.repository.FSRepository

import java.io._

import it.polimi.genomics.core.DataStructures.IRDataSet
import it.polimi.genomics.core.ParsingType.PARSING_TYPE
import it.polimi.genomics.core.{GMQLSchema, GMQLSchemaField, GMQLSchemaFormat, ParsingType}
import it.polimi.genomics.repository.FSRepository.datasets.GMQLDataSetXML
import it.polimi.genomics.repository.GMQLExceptions._
import it.polimi.genomics.repository.{DatasetOrigin, GMQLRepository, GMQLSample, RepositoryType, Utilities => General_Utilities}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConverters._
import scala.xml.XML

/**
  * Created by abdulrahman on 16/01/2017.
  */
trait XMLDataSetRepository extends GMQLRepository{
  private final val logger: Logger = LoggerFactory.getLogger(this.getClass)
  General_Utilities()
  /**
    *
    *
    *
    * @param dataSet Intermediate Representation (IRDataSet) of the dataset, contains the dataset name and schema.
    * @param Schema
    * @param Samples
    * @param GMQLScriptPaht
    * @throws GMQLDSNotFound
    * @throws GMQLUserNotFound
    * @throws GMQLSampleNotFound
    */
  def createDs(dataSet:IRDataSet, userName: String = General_Utilities().USERNAME, Samples: java.util.List[GMQLSample], GMQLScriptPath: String,schemaType:GMQLSchemaFormat.Value): Unit = {
    // Check the dataset name, return if the dataset is already used in
    // the repository of the this user or the public repository.
    if (DSExists(dataSet.position, userName)) {
      logger.warn(s"The dataset (${dataSet.position})  is already registered")
      throw new GMQLNotValidDatasetNameException(s"The dataset name (${dataSet.position}) is already registered")
    }

    val samples: List[GMQLSample] = Samples.asScala.map{ x=>if (x.meta.equals("nothing.meta")) new GMQLSample(x.name, x.name+".meta",x.ID) else x}.toList
    //create DS descriptive file of the Data set
    val gMQLDataSetXML = new GMQLDataSetXML(dataSet,userName,samples,GMQLScriptPath, schemaType,"GENERATED_"+General_Utilities().MODE )
    gMQLDataSetXML.Create()
  }

  /**
    *
    *
    *
    * @param dataSet Intermediate Representation (IRDataSet) of the dataset, contains the dataset name and schema.
    * @param Schema
    * @param Samples
    * @param GMQLScriptPaht
    * @throws GMQLDSNotFound
    * @throws GMQLUserNotFound
    * @throws GMQLSampleNotFound
    */
  @throws(classOf[GMQLNotValidDatasetNameException])
  override def importDs(dataSetName:String, userName: String = General_Utilities().USERNAME, Samples: java.util.List[GMQLSample], schemaPath:String): Unit = {
    //    Files.copy((new File(schemaPath)),(new File(GMQLRepository.FS_Utilities.RepoDir + userName + "/schema/" + dataSetName + ".schema")))

    // Check the dataset name, return if the dataset is already used in
    // the repository of the this user or the public repository.
    if (DSExists(dataSetName, userName)) {
      logger.warn("The dataset name is already registered")
      throw new GMQLNotValidDatasetNameException(s"The dataset name ($dataSetName) is already registered")
    }

      val xmlFile = XML.load(schemaPath)
      val schemaFields = (xmlFile \\ "field")
      val schemaType = (xmlFile \\ "gmqlSchema").head.attribute("type").get.head.text
      val schema = schemaFields.map { x => (x.text.trim, ParsingType.attType(x.attribute("type").get.head.text)) }.toList.asJava
      val dataSet = new IRDataSet(dataSetName, schema)
      val gMQLDataSetXML = new GMQLDataSetXML(dataSet, userName, Samples.asScala.toList, GMQLSchemaFormat.getType(schemaType), "IMPORTED_"+General_Utilities().MODE  )
      gMQLDataSetXML.Create()
  }

  /**
    *
    *   Add sample to dataset in the repository
    *
    * @param dataSet Intermediate Representation (IRDataSet) of the dataset, contains the dataset name and schema.
    * @param Sample
    * @throws GMQLDSNotFound
    * @throws GMQLDSException
    * @throws GMQLUserNotFound
    * @throws GMQLSampleNotFound
    */
  @throws(classOf[GMQLDSException])
  override def addSampleToDS(dataSet: String, userName: String = General_Utilities().USERNAME, Sample: GMQLSample) ={
    val ds = new GMQLDataSetXML(dataSet,userName).loadDS()
    ds.addSample(Sample)
  }

  /**
    *
    * @param dataSet Intermediate Representation (IRDataSet) of the dataset, contains the dataset name and schema.
    * @param sample
    * @return
    */
  @throws(classOf[GMQLSampleNotFound])
  override def getSampleMeta(dataSet: String, userName: String = General_Utilities().USERNAME, sample: GMQLSample): String = {
    val ds = new GMQLDataSetXML(dataSet,userName).loadDS()
    ds.getMeta(sample)
  }

  /**
    *
    * @param dataSet Intermediate Representation (IRDataSet) of the dataset, contains the dataset name and schema.
    * @return
    */
  override def getMeta(dataSet: String,userName:String = General_Utilities().USERNAME): String = {
    new GMQLDataSetXML(dataSet,userName).getMeta()
  }

  /**
    *
    * @param userName
    * @throws GMQLDSException
    * @throws GMQLUserNotFound
    */
  override def listAllDSs(userName: String = General_Utilities().USERNAME): java.util.List[IRDataSet] = {
    val dSs = new File(General_Utilities().getDataSetsDir(userName)).listFiles(new FilenameFilter() {
      def accept(dir: File, name: String): Boolean = {
        return name.endsWith(".xml")
      }
    })
    dSs.map(x=>new GMQLDataSetXML(new IRDataSet(x.getName().subSequence(0, x.getName().length() - 4).toString(),List[(String,PARSING_TYPE)]().asJava),userName).loadDS().dataSet).toList.asJava
  }

  /**
    *
    * @param dataSet Intermediate Representation (IRDataSet) of the dataset, contains the dataset name and schema.
    * @throws GMQLDSException
    * @return
    */
  override def DSExists(dataSet: String, userName: String = General_Utilities().USERNAME): Boolean = {
    new GMQLDataSetXML(dataSet,userName).exists()  || new GMQLDataSetXML(dataSet,"public").exists()
  }

  /**
    *
    * @param dataSet Intermediate Representation (IRDataSet) of the dataset, contains the dataset name and schema.
    * @throws GMQLDSException
    * @return
    */
  override def DSExistsInPublic(dataSet: String): Boolean = {
    new GMQLDataSetXML(dataSet,"public").exists()
  }

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
  override def deleteDS(dataSetName:String, userName:String = General_Utilities().USERNAME): Unit = {
    new GMQLDataSetXML(dataSetName,userName).loadDS().Delete()
  }

  /**
    *
    * @param dataSet Intermediate Representation (IRDataSet) of the dataset, contains the dataset name and schema.
    * @param Sample
    * @throws GMQLDSNotFound
    * @throws GMQLDSException
    * @throws GMQLUserNotFound
    * @throws GMQLSampleNotFound
    */
  override def deleteSampleFromDS(dataSet:String, userName: String = General_Utilities().USERNAME, sample:GMQLSample): Unit = {
    new GMQLDataSetXML(dataSet,userName).loadDS().delSample(sample)
  }

  /**
    *
    * @param dataSet
    * @throws GMQLDSException
    * @return
    */
  override def listDSSamples(dataSetName:String, userName: String = General_Utilities().USERNAME): java.util.List[GMQLSample] ={
    new GMQLDataSetXML(dataSetName,userName).loadDS().samples.asJava
  }

  /**
    * DO NOT Forget to check the existance ot the dataset name before copying the dataset
    *
    * @param dataSet Intermediate Representation (IRDataSet) of the dataset, contains the dataset name and schema.
    * @throws GMQLDSException
    */
  @throws(classOf[GMQLNotValidDatasetNameException])
  override def exportDsToLocal(dataSetName: String, userName: String = General_Utilities().USERNAME, localDir:String): Unit = {

    // Check the dataset name, return if the dataset is already used in
    // the repository of the this user or the public repository.
    if (!DSExists(dataSetName, userName)) {
      logger.warn("The dataset name is not found..")
      throw new GMQLNotValidDatasetNameException(s"The dataset name ($dataSetName) is Not found in the repository")
    }

    val gMQLDataSetXML = new GMQLDataSetXML(dataSetName, userName).loadDS()
    val dest = new File(localDir)
    dest.mkdir()

    val srcSchema = new File(gMQLDataSetXML.schemaDir)
    if (srcSchema.exists()) {
      val schemaOS = new FileOutputStream(dest + "/" + srcSchema.getName)
      schemaOS getChannel() transferFrom(
        new FileInputStream(srcSchema) getChannel, 0, Long.MaxValue)
    }

    val srcScript = new File (gMQLDataSetXML.GMQLScriptUrl)
    if(srcScript.exists()) {
      val scriptOS = new FileOutputStream(dest + "/" + srcScript.getName)
      scriptOS getChannel() transferFrom(
        new FileInputStream(srcScript) getChannel, 0, Long.MaxValue)
      scriptOS.close()
    }
  }


  /**
    *
    * @param dataSet Intermediate Representation (IRDataSet) of the dataset, contains the dataset name and schema.
    * @param query
    * @return
    */
  override def searchMeta(dataSet: String, userName: String = General_Utilities().USERNAME, query: String): java.util.List[GMQLSample] = ???

  override def registerUser(userName:String): Boolean ={
        val indexes = new File(General_Utilities().getIndexDir( userName ))
        val datasets = new File(General_Utilities().getDataSetsDir( userName ))
        val metadata = new File(General_Utilities().getMetaDir( userName ))
        val schema = new File(General_Utilities().getSchemaDir( userName ))
        val queries = new File(General_Utilities().getScriptsDir( userName ))
        // create also the folder for dags
        val dags = new File(General_Utilities().getDagQueryDir( userName, create = false ))
        try{
          logger.info( General_Utilities().getIndexDir( userName ) + (if (indexes.mkdirs) "\tCreated" else "\tfailed"))
          logger.info( General_Utilities().getDataSetsDir( userName ) + (if (datasets.mkdir) "\tCreated" else "\tfailed"))
          logger.info( General_Utilities().getMetaDir( userName ) + (if (metadata.mkdir) "\tCreated" else "\tfailed"))
          logger.info( General_Utilities().getSchemaDir( userName ) + (if (schema.mkdir) "\tCreated" else "\tfailed"))
          logger.info( General_Utilities().getScriptsDir( userName ) + (if (queries.mkdir) "\tCreated" else "\tfailed"))
          // logging for the cration of dags folder
          logger.info( General_Utilities().getDagQueryDir( userName, create = false ) + (if (dags.mkdir) "\tCreated" else "\tfailed"))
          true
        }
    catch {
      case ex:Throwable => false
    }
  }

  /**
    *
    * @param userName [[ String]] of the user name
    * @return
    */
  override def unregisterUser(userName: String = General_Utilities().USERNAME): Boolean = {
    try {
      FS_Utilities.deleterecursive(new File(General_Utilities().getUserDir(userName)))
      new File(General_Utilities().getUserDir(userName)).delete
      true
    } catch {
      case ioe: Throwable => false
    }
  }


  def readSchemaFile(schemaPath:String): GMQLSchema = {
    val conf = new Configuration();
    val path = new Path(schemaPath);
    val fs = FileSystem.get(path.toUri(), conf);
    val gtfFields = List("seqname","start","end","strand")
    val tabFields = List("chr","left","right","strand")
    val xmlFile = XML.load(fs.open(path))
    val cc = (xmlFile \\ "field")
    val schemaList = cc.flatMap{ x => if(gtfFields.contains(x.text.trim)||tabFields.contains(x.text.trim)) None else Some(new GMQLSchemaField(x.text.trim, ParsingType.attType(x.attribute("type").get.head.text)))}.toList
    val schemaType = GMQLSchemaFormat.getType((xmlFile \\ "gmqlSchema" \ "@type").text)
    val schemaname = (xmlFile \\ "gmqlSchemaCollection" \ "@name").text
    new GMQLSchema(schemaname,schemaType, schemaList)
  }

  /**
    *
    * @param dataSet String of the dataset name
    * @param userName String of the name of the owner of the dataset
    * @return The Location as either LOCAL, HDFS, or REMOTE
    */
  override def getDSLocation(dataSet: String, userName: String): (RepositoryType.Value, DatasetOrigin.Value) = {
    val LOCAL = ".*(LOCAL)".r
    val HDFS = ".*(HDFS)".r
    val REMOTE = ".*(REMOTE)".r

    val GENERATED = "(GENERATED).*".r
    val IMPORTED = "(IMPORTED).*".r
    val repo = new GMQLDataSetXML(dataSet,userName).loadDS().Repo
    val location = repo match {
      case LOCAL(repo) => RepositoryType.LOCAL
      case HDFS(repo)=> RepositoryType.HDFS
      case REMOTE(repo) => RepositoryType.REMOTE
      case _ => //throw new RuntimeException("Note supported Repository Type :");
        RepositoryType.HDFS
    }

    val ds_origin = repo match {
      case GENERATED(r) => DatasetOrigin.GENERATED
      case IMPORTED(r) =>DatasetOrigin.IMPORTED
      case _ => //throw new RuntimeException("Note supported dataset origin ");
        DatasetOrigin.IMPORTED
    }

    (location,ds_origin)

  }

  override def changeDSName(datasetName: String, userName:String, newDSName: String): Unit = {
    // Check the dataset name, return if the dataset is already used in
    // the repository of the this user or the public repository.
    if (!DSExists(datasetName, userName)) {
      logger.warn("The dataset name is Not found")
      throw new GMQLNotValidDatasetNameException(s"The dataset name ($datasetName) is not found.")
    }

    // Check the dataset name, return if the dataset is already used in
    // the repository of the this user or the public repository.
    if (DSExists(newDSName, userName)) {
      logger.warn("The dataset name is already registered")
      throw new GMQLNotValidDatasetNameException(s"The dataset name ($newDSName) is already registered.")
    }

    val gMQLDataSetXML = new GMQLDataSetXML(datasetName, userName).loadDS()
    gMQLDataSetXML.changeDSName(newDSName)

  }

  /**
    *
    * @param datasetName String of the dataset name
    * @param userName String of the username, the owner of the dataset
    *     */
  override def getSchema(datasetName: String, userName: String): GMQLSchema = {
    val schemaPath = new File(General_Utilities().getSchemaDir( userName ) + datasetName + ".schema")
    val xmlFile = XML.loadFile(schemaPath)
    val cc = (xmlFile \\ "field")
    val schemaList = cc.map{ x =>
      val schemaFN = x.text.trim
      val schemaType = if(schemaFN.toUpperCase().equals("STOP") || schemaFN.toUpperCase().equals("RIGHT") || schemaFN.toUpperCase().equals("END") || schemaFN.toUpperCase().equals("START") || schemaFN.toUpperCase().equals("LEFT")) ParsingType.LONG
      else ParsingType.attType(x.attribute("type").get.head.text)
      new GMQLSchemaField(schemaFN, schemaType)
    }.toList
    val schemaType = GMQLSchemaFormat.getType((xmlFile \\ "gmqlSchema" \ "@type").text)
    val schemaname = (xmlFile \\ "gmqlSchemaCollection" \ "@name").text
    new GMQLSchema(schemaname,schemaType, schemaList)
  }

  /**
    *
    * @param datasetName String of the dataset name
    * @param userName String of the username, the owner of the dataset
    */
  override def getSchemaStream(datasetName: String, userName: String): InputStream = {
    val schemaPath = General_Utilities().getSchemaDir( userName ) + datasetName + ".schema"
    new FileInputStream(schemaPath)
  }

  override def getScriptStream(dataSetName: String, userName: String): InputStream = {
    val scriptPath = General_Utilities().getScriptsDir( userName ) + dataSetName + ".gmql"
    new FileInputStream(scriptPath)
  }
}
