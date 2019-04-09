package it.polimi.genomics.repository.FSRepository

import java.io._
import java.nio.file.{Files, Paths}
import java.text.SimpleDateFormat
import java.util.Date

import it.polimi.genomics.core.DataStructures.{GMQLInstance, IRDataSet, Instance}
import it.polimi.genomics.core.GDMSUserClass._
import it.polimi.genomics.core.ParsingType.PARSING_TYPE
import it.polimi.genomics.core.exception.UserExceedsQuota
import it.polimi.genomics.core.{GDMSUserClass, _}
import it.polimi.genomics.repository.FSRepository.FS_Utilities.checkDsName
import it.polimi.genomics.repository.FSRepository.datasets.GMQLDataSetXML
import it.polimi.genomics.repository.GMQLExceptions._
import it.polimi.genomics.repository.{DatasetOrigin, GMQLRepository, GMQLSample, RepositoryType, Utilities => General_Utilities}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConverters._
import scala.util.Try
import scala.xml.{Elem, Node, NodeSeq, XML}


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
    * @param Samples
    * @param GMQLScriptPath
    * @throws GMQLDSNotFound
    * @throws GMQLUserNotFound
    * @throws GMQLSampleNotFound
    */
  def createDs(dataSet:IRDataSet, userName: String = General_Utilities().USERNAME, Samples: java.util.List[GMQLSample], GMQLScriptPath: String,schemaType:GMQLSchemaFormat.Value, schemaCoordinateSystem: GMQLSchemaCoordinateSystem.Value, dsmeta: Map[String, String]): Unit = {
    // Check the dataset name, return if the dataset is already used in
    // the repository of the this user or the public repository.
    if (DSExists(dataSet.position, userName)) {
      logger.warn(s"The dataset (${dataSet.position})  is already registered")
      throw new GMQLNotValidDatasetNameException(s"The dataset name (${dataSet.position}) is already registered")
    }

    // Store dataset info file
    setDatasetMeta(dataSet.position,userName,dsmeta)

    val samples: List[GMQLSample] = Samples.asScala.map{ x=>if (x.meta.equals("nothing.meta")) new GMQLSample(x.name, x.name+".meta",x.ID) else x}.toList
    //create DS descriptive file of the Data set
    val gMQLDataSetXML = new GMQLDataSetXML(dataSet,userName,samples,GMQLScriptPath, schemaType, schemaCoordinateSystem, "GENERATED_"+General_Utilities().MODE )
    gMQLDataSetXML.Create()
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
    * @throws UserExceedsQuota
    * @throws java.lang.Exception
    */
  @throws(classOf[GMQLNotValidDatasetNameException])
  override def importDs(dataSetName: String, userName: String, userClass: GDMSUserClass = GDMSUserClass.PUBLIC, Samples: java.util.List[GMQLSample], schemaPath: String): Unit = {
    checkDsName(dataSetName)

    if( isUserQuotaExceeded(userName, userClass) ) {
      throw new UserExceedsQuota()
    }

    // Files.copy((new File(schemaPath)),(new File(GMQLRepository.FS_Utilities.RepoDir + userName + "/schema/" + dataSetName + ".schema")))

    // Check the dataset name, return if the dataset is already used in
    // the repository of the this user or the public repository.
    if (DSExists(dataSetName, userName)) {
      logger.warn("The dataset name is already registered")
      throw new GMQLNotValidDatasetNameException(s"The dataset name ($dataSetName) is already registered")
    }

    val xmlFile = XML.load(schemaPath)
    val schemaFields = (xmlFile \\ "field")
    val schemaType = (xmlFile \\ "gmqlSchema").head.attribute("type").get.head.text
    val coordinateSystemAttribute = (xmlFile \\ "gmqlSchema").head.attribute("coordinate_system")
    val schemaCoordinateSystem = if (coordinateSystemAttribute.isDefined) coordinateSystemAttribute.get.head.text else "none"
    val schema = schemaFields.map { x => (x.text.trim, attType(x.attribute("type").get.head.text)) }.toList.asJava
    val dataSet = new IRDataSet(dataSetName, schema)
    val gMQLDataSetXML = new GMQLDataSetXML(dataSet, userName, Samples.asScala.toList, GMQLSchemaFormat.getType(schemaType), GMQLSchemaCoordinateSystem.getType(schemaCoordinateSystem), "IMPORTED_"+General_Utilities().MODE  )
    gMQLDataSetXML.Create()


    // Set upload date in dataset meta
    val date =  new SimpleDateFormat("yyyy/MM/dd HH:mm:ss").format(new Date());
    setDatasetMeta(dataSetName, userName, Map("Upload date" -> date))
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
  @deprecated
  override def addSampleToDS(dataSet: String, userName: String = General_Utilities().USERNAME, Sample: GMQLSample, userClass: GDMSUserClass = GDMSUserClass.PUBLIC) ={

    val exceeded  = General_Utilities().getRepository().isUserQuotaExceeded(userName, userClass)

    if( exceeded ) {
      throw new UserExceedsQuota()
    }


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
    * @param dataSet Intermediate Representation (IRDataSet) of the dataset, contains the dataset name and schema.
    * @return
    */
  override def getMetaIterator(dataSet: String,userName:String = General_Utilities().USERNAME):  Iterator[String] = {
    new GMQLDataSetXML(dataSet,userName).getMetaIterator()
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
  override def DSExists(dataSet: String, userName: String = General_Utilities().USERNAME, location: Option[GMQLInstance]): Boolean = {
    new GMQLDataSetXML(dataSet,userName).exists()  ||
    //getOrElse there is no public user
      Try( new GMQLDataSetXML(dataSet, "public").exists()).getOrElse(false)
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
    * @param dataSetName Intermediate Representation (IRDataSet) of the dataset, contains the dataset name and schema.
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
    * @param sample
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
    * @param dataSetName
    * @throws GMQLDSException
    * @return
    */
  override def listDSSamples(dataSetName:String, userName: String = General_Utilities().USERNAME): java.util.List[GMQLSample] ={
    new GMQLDataSetXML(dataSetName,userName).loadDS().samples.asJava
  }

  /**
    * DO NOT Forget to check the existance ot the dataset name before copying the dataset
    *
    * @param dataSetName Intermediate Representation (IRDataSet) of the dataset, contains the dataset name and schema.
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
    val profiles = new File(General_Utilities().getProfileDir( userName ))
    val dsmeta = new File(General_Utilities().getDSMetaDir( userName ))
    // create also the folder for dags
    val dags = new File(General_Utilities().getDagQueryDir( userName, create = false ))
    try{
      logger.info( General_Utilities().getIndexDir( userName ) + (if (indexes.mkdirs) "\tCreated" else "\tfailed"))
      logger.info( General_Utilities().getDataSetsDir( userName ) + (if (datasets.mkdir) "\tCreated" else "\tfailed"))
      logger.info( General_Utilities().getMetaDir( userName ) + (if (metadata.mkdir) "\tCreated" else "\tfailed"))
      logger.info( General_Utilities().getSchemaDir( userName ) + (if (schema.mkdir) "\tCreated" else "\tfailed"))
      logger.info( General_Utilities().getScriptsDir( userName ) + (if (queries.mkdir) "\tCreated" else "\tfailed"))
      logger.info( General_Utilities().getProfileDir( userName ) + (if (profiles.mkdir) "\tCreated" else "\tfailed"))
      logger.info( General_Utilities().getDSMetaDir( userName ) + (if (dsmeta.mkdir) "\tCreated" else "\tfailed"))
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
    val schemaList = cc.flatMap{ x => if(gtfFields.contains(x.text.trim)||tabFields.contains(x.text.trim)) None else Some(new GMQLSchemaField(x.text.trim, attType(x.attribute("type").get.head.text)))}.toList
    val schemaType = GMQLSchemaFormat.getType((xmlFile \\ "gmqlSchema" \ "@type").text)
    val schemaCoordinateSystem = GMQLSchemaCoordinateSystem.getType((xmlFile \\ "gmqlSchema" \ "@coordinate_system").text)
    val schemaname = (xmlFile \\ "gmqlSchemaCollection" \ "@name").text
    new GMQLSchema(schemaname,schemaType, schemaCoordinateSystem, schemaList)
  }


  def attType(x: String): ParsingType.Value = x.toUpperCase match {
    case "STRING" => ParsingType.STRING
    case "CHAR" => ParsingType.CHAR
    case "CHARACTAR" => ParsingType.CHAR
    case "LONG" => ParsingType.LONG
    case "INTEGER" => ParsingType.INTEGER
    case "INT" => ParsingType.INTEGER
    case "BOOLEAN" => ParsingType.STRING
    case "BOOL" => ParsingType.STRING
    case _ => ParsingType.DOUBLE
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
      else attType(x.attribute("type").get.head.text)
      new GMQLSchemaField(schemaFN, schemaType)
    }.toList
    val schemaType = GMQLSchemaFormat.getType((xmlFile \\ "gmqlSchema" \ "@type").text)
    val schemaCoordinateSystem = GMQLSchemaCoordinateSystem.getType((xmlFile \\ "gmqlSchema" \ "@coordinate_system").text)
    val schemaname = (xmlFile \\ "gmqlSchemaCollection" \ "@name").text
    new GMQLSchema(schemaname,schemaType, schemaCoordinateSystem, schemaList)
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

  /**
    *  Return a stream of the vocabulary file.
    *
    * @param dataSetName dataset name of the requested script
    * @param userName the owner of the dataset and the script
    * @return [[InputStream]] as the script string file.
    */
  override def getVocabularyStream(dataSetName: String, userName: String): InputStream = {
    new ByteArrayInputStream("N/A".getBytes)
  }


  // Dataset Meta and Profiles
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

    val filename = General_Utilities().getDSMetaDir(userName)+"/"+datasetName+".dsmeta"

    if (Files.exists(Paths.get(filename))) {
      val xml = XML.loadFile(filename);
      (xml \\ "dataset" \ "property").map(x=>(x.attribute("name").get.text, x.text)).toMap
    } else {
      Map()
    }

  }

  /**
    * Set an entry on dataset metadata
    *
    * @param datasetName
    * @param userName
    * @param metaEntries , a map of key => values entries
    */
  override def setDatasetMeta(datasetName: String, userName: String, metaEntries:Map[String,String]): Unit = {

    val conf = new Configuration()
    val fs = FileSystem.get(conf)

    val xmlFile = General_Utilities().getDSMetaDir(userName)+"/"+datasetName+".dsmeta"

    // Append Child to XML element
    def addChild(n: Node, newChild: Node) = n match {
      case Elem(prefix, label, attribs, scope, child @ _*) =>
        Elem(prefix, label, attribs, scope, child ++ newChild : _*)
    }

    var meta = <dataset></dataset>

    // If the meta file already exists, update that one
    if (fs.exists(new Path(xmlFile))) {
      meta = XML.loadFile(xmlFile)
    }

    for( entry <- metaEntries ) {
      val property = <property name={entry._1}>{entry._2}</property>
      meta = addChild(meta, property)
    }

    storeDsMeta(meta.toString, userName,datasetName)

  }

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

    val filename = General_Utilities().getProfileDir(userName)+"/"+datasetName+".profile"

    if (Files.exists(Paths.get(filename))) {
      val xml = XML.loadFile(filename);
      (xml \\ "dataset" \ "feature").map(x=>(x.attribute("name").get.text, x.text)).toMap
    } else {
      Map("Info" -> "Dataset profile not available.")
    }

  }

  /**
    * Returns profiling information concerning a specific sample of the dataset, e.g.:
    *
    * Number of samples => 15
    * Number of regions => 31209
    * Average region length => 123.12
    *
    * @param datasetName dataset name as a string
    * @param sampleName  name of the sample (no format), e.g. S_00001
    * @param userName   the owner of the dataset
    */
  override def getSampleProfile(datasetName: String, sampleName: String, userName: String): Map[String, String] = {

    val filename = General_Utilities().getProfileDir(userName)+"/"+datasetName+".profile"

    if (Files.exists(Paths.get(filename))) {
      val xml = XML.loadFile(filename)
      val sampleNode: NodeSeq = (xml \\ "dataset" \\ "sample").filter(_.attribute("name").get.text.split("\\.").head == sampleName)
      val profile = (sampleNode \\ "feature").map(x=> {
        ( x.attribute("name").get.text , x.text)
      }).toMap

      if(profile.isEmpty) {
        Map("Info" -> "Sample profile not available.")
      } else {
        profile
      }


    } else {
      Map("Info" -> "Sample profile not available.")
    }

  }

  /**
    * Boolean value: true if user quota is exceeded
    *
    * @param username
    * @param userClass
    * @return
    */
  override def isUserQuotaExceeded(username: String, userClass: GDMSUserClass): Boolean = {
    val info = getUserQuotaInfo(username, userClass)
    return info._1  > info._2
  }

  /**
    * Returns information about the user disk quota usage
    * @param userName
    * @param userClass
    * @return (occupied, user_quota) in KBs
    */
  override def getUserQuotaInfo(userName: String, userClass: GDMSUserClass): (Long, Long) = {

    val userDatasets = listAllDSs(userName).asScala.toList
    val dSMetaDir    = General_Utilities().getDSMetaDir(userName)
    val values = userDatasets.map(x=> {

      val file = dSMetaDir+"/"+x.position+".dsmeta"

      try {
        val dsXML = XML.loadFile(file)
        val sizeEls = (dsXML \\ "dataset" \ "property").filter(x => (x.attribute("name").get.text == "Size"))

        if (!sizeEls.isEmpty) {
          sizeEls.head.text.split(" ").head.replace(",",".").toFloat
        } else {
          0
        }
      } catch {
        case e: FileNotFoundException => {
          0
        }
      }

    })

    var size = 0L // final size

    if( !values.isEmpty ) {
      size = values.reduce((a,b)=> a+b) * 1000 toLong // in KB
    }

    val user_quota = General_Utilities().getUserQuota(userClass)

    (size,user_quota)
  }

  def storeDsMeta(meta:String, userName:String, dsname:String) = {

    val conf = new Configuration()
    val fs = FileSystem.get(conf)

    try {
      val output = fs.create(new Path( General_Utilities().getDSMetaDir(userName)+"/"+dsname+".dsmeta"))
      val os = new java.io.BufferedOutputStream(output)
      os.write(meta.getBytes("UTF-8"))
      os.close()

    } catch {
      case e: Throwable => {
        logger.error(e.getMessage)
        e.printStackTrace()
      }
    }

  }

  override def getInfoStream(dataSetName: String, userName: String): InputStream = {
    val list = (getDatasetMeta(dataSetName, userName) ++ getDatasetProfile(dataSetName, userName)).toList.sorted
    val resultString = list.map(x => x._1+ "\t" + x._2 ).mkString("\n")
    new ByteArrayInputStream(resultString.getBytes)
  }



}
