package it.polimi.genomics.repository.FSRepository.datasets

import java.beans.Transient
import java.io._
import java.nio.charset.Charset
import java.nio.file.{Files, Path, Paths}

import it.polimi.genomics.core.DataStructures.IRDataSet
import it.polimi.genomics.core.ParsingType._
import it.polimi.genomics.repository.FSRepository.FS_Utilities
//import it.polimi.genomics.repository.FSRepository.Indexing.LuceneIndex
import it.polimi.genomics.repository.GMQLExceptions.{GMQLDSNotFound, GMQLNotValidDatasetNameException, GMQLSampleNotFound, GMQLUserNotFound}
import it.polimi.genomics.repository._
import org.slf4j.LoggerFactory

import scala.xml.XML
import scala.collection.JavaConverters._
/**
  * Created by abdulrahman Kaitoua on 12/04/16.
  */

/**
  *
  * @param dataSet
  */
case class GMQLDataSetXML(val dataSet: IRDataSet) {
  val logger = LoggerFactory.getLogger(this.getClass)
  type InternalSchema = List[(String, PARSING_TYPE)]

  var schema: InternalSchema = dataSet.schema.asScala.toList
  var GMQLScriptUrl: String = ""
  var samples = List[GMQLSample]()
  var DSname = dataSet.position
  var userName = "temp"
  val LOCAL = "GENERATED"
  var Repo = LOCAL

  def schemaDir: String = Utilities().RepoDir + this.userName + "/schema/" + DSname + ".schema";
  var schemaType: GMQLSchemaTypes.Value = GMQLSchemaTypes.Delimited
  Utilities()

  /**
    *   Constructor with parameters to construct a {@link GMQLDataSetXML} object
    *
    * @param dataset  {@link IRDataSet} variable that contains the name of the dataset and the schema
    * @param username the owner of the dataset
    * @param repo String field for the XML file, to describe the repository which this dataset is generated for
    * @param fields List of {@link GMQLSample}, holds the samples URIs and meta data URIs.
    * @throws GMQLUserNotFound
    */
  @throws(classOf[GMQLNotValidDatasetNameException])
  @throws(classOf[GMQLUserNotFound])
  def this(dataset: IRDataSet, username: String, repo:String, fields: List[GMQLSample] ) {
    this(dataset)
    checkuser(username);
    var i = -1;
    this.samples = fields.map(x => new GMQLSample(x.name,x.meta, if (x.ID == null) {
      i = i + 1;
      i.toString
    } else x.ID))
    this.userName = username
    this.Repo = repo
  }

  /**
    *  Constructor with parameters to construct a {@link GMQLDataSetXML} object
    *
    * @param dataset {@link IRDataSet} variable that contains the name of the dataset and the schema
    * @param username The owner of the dataset
    * @throws GMQLUserNotFound
    */
  @throws(classOf[GMQLUserNotFound])
  def this(dataset: IRDataSet, username: String) {
    this(dataset)
    checkuser(username);
    this.userName = username
  }

  /**
    *  Constructor with parameters to construct a {@link GMQLDataSetXML} object
    *
    * @param datasetName String that describe the dataset name
    * @param username The owner of the dataset
    * @throws GMQLUserNotFound
    */
  @throws(classOf[GMQLUserNotFound])
  def this(datasetName: String, username: String) {
    this(new IRDataSet(datasetName, List[(String,PARSING_TYPE)]().asJava))
    checkuser(username);
    this.userName = username
  }

  /**
    *  Constructor with parameters to construct a {@link GMQLDataSetXML} object
    *
    * @param dataset {@link IRDataSet} variable that contains the name of the dataset and the schema
    * @param username the owner of the dataset
    * @param fields List of {@link GMQLSample}, holds the samples URIs and meta data URIs.
    * @param GMQLCodeUrl String as a URI to the location of GMQL script
    * @param repo String field for the XML file, to describe the repository which this dataset is generated for
    * @throws GMQLUserNotFound
    */
  @throws(classOf[GMQLUserNotFound])
  @throws(classOf[GMQLNotValidDatasetNameException])
  def this(dataset: IRDataSet, username: String, fields: List[GMQLSample], GMQLCodeUrl: String,repo:String) {
    this(dataset, username,repo , fields)
    this.GMQLScriptUrl = GMQLCodeUrl
  }

  /**
    *  Constructor with parameters to construct a {@link GMQLDataSetXML} object
    *
    * @param dataset {@link IRDataSet} variable that contains the name of the dataset and the schema
    * @param username the owner of the dataset
    * @param fields List of {@link GMQLSample}, holds the samples URIs and meta data URIs.
    * @param GMQLCodeUrl  String as a URI to the location of GMQL script
    * @param schemaType {@link GMQLSchemaTypes} that shows the type of the schema
    * @param repo String field for the XML file, to describe the repository which this dataset is generated for
    */
  @throws(classOf[GMQLNotValidDatasetNameException])
  def this(dataset: IRDataSet, username: String, fields: List[GMQLSample], GMQLCodeUrl: String, schemaType: GMQLSchemaTypes.Value,repo:String ) {
    this(dataset, username, fields, GMQLCodeUrl,repo )
    this.schemaType = schemaType
  }

  /**
    *  Constructor with parameters to construct a {@link GMQLDataSetXML} object
    *
    * @param dataset {@link IRDataSet} variable that contains the name of the dataset and the schema
    * @param username the owner of the dataset
    * @param fields List of {@link GMQLSample}, holds the samples URIs and meta data URIs.
    * @param schemaType {@link GMQLSchemaTypes} that shows the type of the schema
    * @param repo String field for the XML file, to describe the repository which this dataset is generated for
    */
  @throws(classOf[GMQLNotValidDatasetNameException])
  def this(dataset: IRDataSet, username: String, fields: List[GMQLSample], schemaType: GMQLSchemaTypes.Value,repo:String ) {
    this(dataset, username, repo, fields )
    this.schemaType = schemaType
  }

  /**
    * Set the data set name
    * @param DSname
    */
  def setDSName(DSname:String): Unit ={
    this.DSname = DSname
  }

  /**
    *  Find if the Dataset exists in the repository
    *
    * @throws GMQLUserNotFound exception that the user was not found as registered user
    * @return True, if exsists
    */
  @throws(classOf[GMQLUserNotFound])
  def exists(): Boolean = {
    val dataSetDir: File = new File(Utilities().getDataSetsDir(userName))
    if (!dataSetDir.exists) {
      logger.warn("This user is not registered yet.." + userName)
      throw new GMQLUserNotFound()
    }

    val datasets: Array[File] = dataSetDir.listFiles
    for (dataset <- datasets) {
      if (dataset.getName == this.DSname.trim + ".xml") {
        return true
      }
    }
    return false
  }

  /**
    * Load GMQL dataset in memory, This is needed after every construction of dataset that is already in repository
    * Where each dataset is identified by only the username and the dataset name.
    * We did not add it to the constructor so we can prepare the object and call create or load object if the dataset is already in repository.
    * Loading includes, loading the schema, loading the script path, loading the samples URIs.
    *
    *
    * @throws GMQLDSNotFound
    * @return
    */
  @throws(classOf[GMQLDSNotFound])
  def loadDS(): GMQLDataSetXML = {
    //Loading DataSet XML
    if (exists()) {
      val DSXMLfile = Utilities().getDataSetsDir(userName) + this.DSname + ".xml"
      val dsXML = XML.loadFile(DSXMLfile);
      val cc = (dsXML \\ "url")
      this.DSname = (dsXML \\ "dataset").head.attribute("name").get.head.text
      this.Repo = (dsXML \\ "dataset").head.attribute("execType").get.head.text
      this.userName = (dsXML \\ "dataset").head.attribute("username").get.head.text
      samples = cc.map(x => new GMQLSample(x.text.trim,x.text.trim+".meta",x.attribute("id").get.head.text)).toList
//      this.schemaDir = (dsXML \\ "dataset").head.attribute("schemaDir").get.head.text

      try{
      this.GMQLScriptUrl = (dsXML \\ "dataset").head.attribute("script").get.head.text
      }catch{
        case ex:Exception => logger.debug("Generating script is not found")
      }
      // Loading schema
      val schemaFields = (XML.loadFile(this.schemaDir) \\ "field")
      import it.polimi.genomics.repository.FSRepository._
      this.schema = schemaFields.map(x => (x.text.trim, FS_Utilities.attType(x.attribute("type").get.head.text))).toList
      this
    } else throw new GMQLDSNotFound()

  }

  /**
    *   Create a dataset.
    *   Includes constructing a single meta file, storing the schema file,
    *   and storing the dataset XML with the samples URIs
    * @return
    */
  def Create(): Boolean = {
    try {
      logger.info("Start Creating ( " + this.DSname + " )Dataset")
      logger.info("Building the MetaData..")
      buildmetaDataWithIndex()
      logger.info("Building the MetaData..\tDone.")
      storeXML(generateSchemaXML(this.schema), Utilities().getSchemaDir( this.userName)  + DSname + ".schema")
      logger.info("Schema file has been stored ..")
      storeXML(generateDSXML(), Utilities().getDataSetsDir( this.userName ) + DSname + ".xml")
      logger.info("Dataset ( " + this.DSname + " ) => Created..")
      true
    }
    catch {
      case pce: Exception => {
        logger.error(pce.getMessage, pce)
        logger.error("Some error occured :) Rolling back the operation...")
        this.Delete
        false
      }
    }
  }

  /**
    *  Generate the dataset XML file
    *
    * @return
    */
  private def generateDSXML(): String = {

    val GMQLDSxml =
    //http://www.bioinformatics.deib.polimi.it/GMQL/
      "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>\n" +
        "<DataSets xmlns=\"http://gmql.orchestrator.com/datasets\">\n" +
        "<dataset name=\"" + DSname + "\" schemaDir=\"" + this.schemaDir + "\" execType=\""+Repo+"\" username=\"" + this.userName + "\" script=\""+this.GMQLScriptUrl+"\">\n" +
        this.samples.map(x => "           <url id=\"" + x.ID + "\">" + x.name + "</url>").mkString("\n") +
        "\n</dataset>\n" +
        "</DataSets>"

    GMQLDSxml
  }

  /**
    * store XML file
    * @param xmlString XML contents as a {@link String}
    * @param location {@link String} to the location of the file
    * @return
    */
  private def storeXML(xmlString: String, location: String) = {
    new PrintWriter(location) {
      write(xmlString);
      close
    }
  }

  /**
    *  Generate the schema XML
    *
    * @param schema
    * @return
    */
  private def generateSchemaXML(schema: InternalSchema): String = {
    val schemaPart = if(Repo.startsWith("GENERATED")){
      if (schemaType.equals(GMQLSchemaTypes.GTF)) {
        Some(
          "           <field type=\"STRING\">seqname</field>\n" +
          "           <field type=\"STRING\">source</field>\n" +
          "           <field type=\"STRING\">feature</field>\n" +
          "           <field type=\"LONG\">start</field>\n" +
          "           <field type=\"LONG\">end</field>\n" +
          "           <field type=\"DOUBLE\">score</field>\n" +
          "           <field type=\"CHAR\">strand</field>\n" +
          "           <field type=\"STRING\">frame</field>\n")

      } else {
        Some(
          "           <field type=\"STRING\">chr</field>\n" +
          "           <field type=\"LONG\">left</field>\n" +
          "           <field type=\"LONG\">right</field>\n" +
          "           <field type=\"CHAR\">strand</field>\n")
      }
    }else{
        None
      }

    val schemaHeader =
    //http://www.bioinformatics.deib.polimi.it/GMQL/
      "<?xml version='1.0' encoding='UTF-8'?>\n" +
        "<gmqlSchemaCollection name=\"" + DSname + "\" xmlns=\"http://genomic.elet.polimi.it/entities\">\n" +
    "\t<gmqlSchema type=\""+schemaType.toString+"\">\n"+
        schemaPart.getOrElse("") +
        schema.flatMap { x =>
          if (schemaType.equals(GMQLSchemaTypes.GTF) && x._1.toLowerCase() == "score") None
          else Some("           <field type=\"" + x._2.toString + "\">" + x._1 + "</field>")}.mkString("\n") + "\n" +
        "\t</gmqlSchema>\n" +
        "</gmqlSchemaCollection>"

      schemaHeader
  }

  /**
    *
    */
  private def buildmetaDataWithIndex() {
    val writer: PrintWriter = new PrintWriter(Utilities().getMetaDir(this.userName) + this.DSname + ".meta", "UTF-8")
    var line: String = null
    for (sample <- samples) {
      try {
        val file: Path = Paths.get(sample.meta)
        val reader: BufferedReader = Files.newBufferedReader(file, Charset.defaultCharset)
          //   Lucene indexing
          //   Disabled since we are not using the index for now.
//        if (url.ID.toInt == 0) {
//          LuceneIndex.buildIndex(FS_Utilities().RepoDir + this.userName + "/indexes/" + this.DSname + "/",
//            url.meta, url.ID.toInt, true, false);
//        } else {
//          LuceneIndex.addSampletoIndex(FS_Utilities().RepoDir + this.userName + "/indexes/" + this.DSname + "/",
//            url.meta, url.ID.toInt, false);
//        }
        while ( {
          line = reader.readLine;
          line
        } != null)
          writer.println(sample.ID + "\t" + line)

        reader.close
      } catch {
        case _:Throwable => logger.error("Meta file is not found .. " + sample.name + ".meta \tCheck the schema URL.. ")
      }
    }
    writer.close

    logger.info("Meta of" + DSname + " data set is Built... ")
  }

  /**
    *  Get the
    * @return
    */
  def getIndexURI: String = Utilities().getIndexDir(this.userName ) + this.DSname

  /**
    *  Delete the data set from the local repository
    */
  def Delete() = {
    val index: File = new File(Utilities().getIndexDir( this.userName ) + this.DSname)
    val meta: File = new File(Utilities().getMetaDir( this.userName) + this.DSname + ".meta")
    val dataset: File = new File(Utilities().getDataSetsDir( this.userName) + this.DSname + ".xml")
    val schema: File = new File(Utilities().getSchemaDir( this.userName ) + this.DSname + ".schema")
    val regions: File = new File(Utilities().getRegionDir( this.userName ) + this.DSname)
    var result: File = null

    if (dataset.exists) {
      dataset.delete
      logger.info(this.DSname + " dataset file deleted ..")
    }
    else logger.info(this.DSname + ", dataset is not found..\n")

    if (schema.exists) {
      schema.delete
      logger.info(this.DSname + " Schema is deleted ..")
    }
    else logger.info(this.DSname + ", schema is not found..\n")

    if (regions.exists) {
      FS_Utilities.deleteFromLocalFSRecursive(regions)
      logger.info(this.DSname + " regions is deleted .. " + regions.toString)
    }
    else logger.warn(this.DSname + ", regions is not found in { regions } folder..\n")

    if (meta.exists) {
      meta.delete
      logger.info(this.DSname + " meta Data is deleted ..")
    }
    else logger.warn(this.DSname + ", meta is not found..\n")

    if (index.exists) {
      FS_Utilities.deleteFromLocalFSRecursive(index)
      index.delete
      logger.info(this.DSname + " index is also deleted ..")
    }
    else logger.warn(this.DSname + ", index is not found..\n")

    logger.info("All files and folders related to " + this.DSname + " are now deleted\n" + "\t except the user local original files..")
  }

  /**
    *
    * @param sample {@link GMQLSample} to add to the dataset
    * @throws GMQLSampleNotFound
    * @throws java.io.IOException
    * @return
    */
  @throws(classOf[GMQLSampleNotFound])
  @throws(classOf[IOException])
  def addSample(sample: GMQLSample): GMQLDataSetXML = {
    val id: Int = if (samples.size > 0 && sample.ID == null) samples(samples.size - 1).ID.toInt + 1 else sample.ID.toInt

    val meta: File = new File(sample.meta)
    if (!meta.exists) {
      logger.error("Metadata not found for Sample : " + sample.name);
    }
    //
    //    if (!(new File(sample.name).exists())) throw new GMQLSampleNotFound()

    samples = samples :+ (new GMQLSample(sample.name, sample.meta,id.toString))

    var reader: BufferedReader = null
    var line: String = null
    val out: PrintWriter = new PrintWriter(new BufferedWriter(new FileWriter(Utilities().getMetaDir( this.userName ) + this.DSname + ".meta", true)))
    try {
      val file: Path = Paths.get(sample.meta)
      if (Files.exists(file) && Files.isReadable(file)) {
//        LuceneIndex.addSampletoIndex(Utilities().RepoDir + this.userName + "/indexes/" + this.DSname + "/", sample.meta, id, false)
        reader = Files.newBufferedReader(file, Charset.defaultCharset)
        while ( {
          line = reader.readLine;
          line
        } != null) {
          out.println(id + "\t" + line)
        }
        reader.close
      }
      logger.info("Sample Added...")
    }
    catch {
      case e: IOException => {
        logger.error(e.getMessage, e)
        delSample(sample)
      }
    } finally {
      if (out != null) out.close()
    }
    submitChangesToXML()
    this
  }

  /**
    *  Delete DS xml file and store the new one.
    *
    */
  private def submitChangesToXML(): Unit = {
    new File(Utilities().getDataSetsDir( this.userName )+  DSname + ".xml").delete()
    this.storeXML(generateDSXML(), Utilities().getDataSetsDir(this.userName ) + DSname + ".xml")
  }

  def changeDSName(newDSName:String): Unit = {
    new File(Utilities().getDataSetsDir( this.userName )+  DSname + ".xml").delete()
    new File(Utilities().getSchemaDir( this.userName )+  DSname + ".schema").delete()
    new File(Utilities().getMetaDir( this.userName )+  DSname + ".meta")
      .renameTo(new File(Utilities().getMetaDir( this.userName )+newDSName+".meta"))

    setDSName(newDSName)
    this.storeXML(generateDSXML(), Utilities().getDataSetsDir(this.userName ) + newDSName + ".xml")
    this.storeXML(generateSchemaXML(this.schema), Utilities().getSchemaDir( this.userName)  + DSname + ".schema")
  }
  /**
    *  Delete sample from the dataset
    *
    * @param sample {@link GMQLSample} to add to the dataset
    * @return
    */
  def delSample(sample: GMQLSample): Int = {
    val deletedid: Int = checkSampleInDataSet(sample, true)
    if (deletedid != 0) {
      try {
        val writer: PrintWriter = new PrintWriter(Utilities().getMetaDir( this.userName )+ this.DSname + ".meta.tmp", "UTF-8")
        val file: Path = Paths.get(Utilities().getMetaDir( this.userName ) + this.DSname + ".meta")
        if (Files.exists(file) && Files.isReadable(file)) {
//          LuceneIndex.deleteIndexedSamplebyURL(Utilities().RepoDir + this.userName + "/indexes/" + this.DSname + "/", sample.meta )
          val reader: BufferedReader = Files.newBufferedReader(file, Charset.defaultCharset)
          var line: String = null
          while ( {
            line = reader.readLine;
            line
          } != null) {
            val str: Array[String] = line.split("\t")
            if (!(str(0).toInt == deletedid))
              writer.println(line)
          }
          reader.close
        }
        else {
          logger.error("Sample ( " + sample.name + " ) is not found")
        }
        writer.close
        val f1: File = new File(Utilities().getMetaDir( this.userName ) + this.DSname + ".meta")
        val f: File = new File(Utilities().getMetaDir( this.userName ) + this.DSname + ".meta.tmp")
        f.renameTo(f1)
      }
      catch {
        case ex: Exception => {
          logger.error(ex.getMessage, ex)
        }
      }
    }
    else {
      logger.warn("The Sample does not exists in this data set. Please check the Sample name ")
    }
    submitChangesToXML()
    return deletedid
  }

  /**
    *
    *   Get the meta data of specific sample in a dataset.
    *
    * @param sample {@link GMQLSample} to add to the dataset
    * @throws GMQLSampleNotFound
    * @return
    */
  @throws(classOf[GMQLSampleNotFound])
  def getMeta(sample: GMQLSample): String = {
    val s = samples.filter(x => x.name == sample.name)
    if (!s.isEmpty) {
      val reader: BufferedReader = Files.newBufferedReader(Paths.get(Utilities().getMetaDir( userName ) + DSname + ".meta"), Charset.defaultCharset)
      var line: String = ""
      val st = new StringBuilder();
      while ( {
        line = reader.readLine;
        line
      } != null) {
        val str: Array[String] = line.split("\t")
        if (str(0) == s(0).ID)
          st.append(str.tail.mkString("\t")).append("\n")
      }
      reader.close
      st.toString()
    } else throw new GMQLSampleNotFound()
  }

  /**
    *
    * Return the ID of the sample in the XML of the dataset
    *
    * @param sample {@link GMQLSample} to add to the dataset
    * @param delete  Boolean of Wether to delete the sample from the dataset or keep it
    * @return
    */
  def checkSampleInDataSet(sample: GMQLSample, delete: Boolean): Int = {
    var found: Boolean = false
    var del: (GMQLSample, Int) = (new GMQLSample(), 0)
    try {
      del = this.samples.zipWithIndex.filter(field => field._1.name.trim == sample.name.trim).head

      if (delete && found) {
        logger.info("Deleted sample = " + del._1)
        samples.drop(del._2)
      }
    }
    catch {
      case pce: NumberFormatException => {
        logger.error(pce.getMessage, pce)
      }
    }

    return del._1.ID.toInt
  }

  def getMeta(): String = {
    scala.io.Source.fromFile(Utilities().getMetaDir(userName ) + DSname + ".meta").mkString
  }

  /**
    *  Check if the user is registered in the repository
    *
    * @param username the owner of the dataset
    * @throws GMQLUserNotFound
    * @return
    */
  @throws(classOf[GMQLUserNotFound])
  def checkuser(username: String): Boolean = {
    val dataSetDir: File = new File(Utilities().RepoDir + username)
    if (!dataSetDir.exists) {
      logger.debug("This user may not be registered .." + username)
      throw new GMQLUserNotFound()
    }
    true
  }
}