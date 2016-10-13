package it.polimi.genomics.repository.FSRepository.datasets

import java.beans.Transient
import java.io._
import java.nio.charset.Charset
import java.nio.file.StandardCopyOption._
import java.nio.file.{Files, Paths, Path}
import javax.xml.bind.annotation._

import it.polimi.genomics.core.DataStructures.IRDataSet
import it.polimi.genomics.core.ParsingType
import it.polimi.genomics.core.ParsingType._
import it.polimi.genomics.repository.FSRepository.Indexing.LuceneIndex
import it.polimi.genomics.repository.GMQLRepository._
import org.slf4j.LoggerFactory

import scala.xml.XML
import scala.collection.JavaConverters._
/**
  * Created by abdulrahman on 12/04/16.
  */

class GMQLDataSetXML(val dataSet: IRDataSet) {
  val logger = LoggerFactory.getLogger(this.getClass)
  type InternalSchema = List[(String, PARSING_TYPE)]

  var schema: InternalSchema = dataSet.schema.asScala.toList
  var GMQLScriptUrl: String = ""
  var samples = List[GMQLSample]()
  var DSname = dataSet.position
  var userName = "temp"
  val LOCAL = "GENERATED_LOCAL"
  var Repo = LOCAL

  def schemaDir: String = Utilities.RepoDir + this.userName + "/schema/" + DSname + ".schema";
  var schemaType: GMQLSchemaTypes.Value = GMQLSchemaTypes.Delimited
  Utilities.apply()

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

  @throws(classOf[GMQLUserNotFound])
  def this(dataset: IRDataSet, username: String) {
    this(dataset)
    checkuser(username);
    this.userName = username
  }

  @throws(classOf[GMQLUserNotFound])
  def this(datasetName: String, username: String) {
    this(new IRDataSet(datasetName, List[(String,PARSING_TYPE)]().asJava))
    checkuser(username);
    this.userName = username
  }

  @throws(classOf[GMQLUserNotFound])
  @throws(classOf[GMQLNotValidDatasetNameException])
  def this(dataset: IRDataSet, username: String, fields: List[GMQLSample], GMQLCodeUrl: String,repo:String) {
    this(dataset, username,repo , fields)
    this.GMQLScriptUrl = GMQLCodeUrl
  }

  @throws(classOf[GMQLNotValidDatasetNameException])
  def this(dataset: IRDataSet, username: String, fields: List[GMQLSample], GMQLCodeUrl: String, schemaType: GMQLSchemaTypes.Value,repo:String ) {
    this(dataset, username, fields, GMQLCodeUrl,repo )
    this.schemaType = schemaType
  }

  @throws(classOf[GMQLNotValidDatasetNameException])
  def this(dataset: IRDataSet, username: String, fields: List[GMQLSample], schemaType: GMQLSchemaTypes.Value,repo:String ) {
    this(dataset, username, repo, fields )
    this.schemaType = schemaType
  }

  @throws(classOf[GMQLUserNotFound])
  def exists(): Boolean = {
    val dataSetDir: File = new File(Utilities.RepoDir + userName + "/datasets/")
    if (!dataSetDir.exists) {
      logger.warn("This user is not be registered yet.." + userName)
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

  @throws(classOf[GMQLDSNotFound])
  def loadDS(): GMQLDataSetXML = {
    //Loading DataSet XML
    if (exists()) {
      val DSXMLfile = Utilities.RepoDir + userName + "/datasets/" + this.DSname + ".xml"
      val dsXML = XML.loadFile(DSXMLfile);
      val cc = (dsXML \\ "url")
      this.DSname = (dsXML \\ "dataset").head.attribute("name").get.head.text
      this.userName = (dsXML \\ "dataset").head.attribute("username").get.head.text
      samples = cc.map(x => new GMQLSample(x.text.trim,x.text.trim+".meta",x.attribute("id").get.head.text)).toList

      try{
      this.GMQLScriptUrl = (dsXML \\ "dataset").head.attribute("script").get.head.text
      }catch{
        case ex:Exception => logger.debug(ex.getMessage)
      }
      // Loading schema
      println(this.schemaDir)
      val schemaFields = (XML.loadFile(this.schemaDir) \\ "field")
      this.schema = schemaFields.map(x => (x.text.trim, it.polimi.genomics.repository.FSRepository.Utilities.attType(x.attribute("type").get.head.text))).toList
      this
    } else throw new GMQLDSNotFound()

  }

  def Create(): Boolean = {
    try {
      logger.info("Start Creating ( " + this.DSname + " )Dataset")
      logger.info("Building the MetaData..")
      buildmetaDataWithIndex()
      logger.info("Building the MetaData..\tDone.")
      storeXML(generateSchemaXML(this.schema), Utilities.RepoDir + this.userName + "/schema/" + DSname + ".schema")
      logger.info("Schema file has been stored ..")
      storeXML(generateDSXML(), Utilities.RepoDir + this.userName + "/datasets/" + DSname + ".xml")
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

  def generateDSXML(): String = {

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

  def storeXML(xmlString: String, location: String) = {
    new PrintWriter(location) {
      write(xmlString);
      close
    }
  }

  def generateSchemaXML(schema: InternalSchema): String = {
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
    "<gmqlSchema type=\""+schemaType.toString+"\">\n"+
        schemaPart.getOrElse("") +
        schema.flatMap { x =>
          if (schemaType.equals(GMQLSchemaTypes.GTF) && x._1.toLowerCase() == "score") None
          else Some("           <field type=\"" + x._2.toString + "\">" + x._1 + "</field>")}.mkString("\n") + "\n" +
        "</gmqlSchema>\n" +
        "</gmqlSchemaCollection>"

      schemaHeader
  }

  private def buildmetaDataWithIndex() {
    val writer: PrintWriter = new PrintWriter(Utilities.RepoDir + this.userName + "/metadata/" + this.DSname + ".meta", "UTF-8")
    var line: String = null
    for (url <- samples) {
      try {
//        println(url.meta)
        val file: Path = Paths.get(url.meta)
        val reader: BufferedReader = Files.newBufferedReader(file, Charset.defaultCharset)
//        if (url.ID.toInt == 0) {
//          LuceneIndex.buildIndex(Utilities.RepoDir + this.userName + "/indexes/" + this.DSname + "/",
//            url.meta, url.ID.toInt, true, false);
//        } else {
//          LuceneIndex.addSampletoIndex(Utilities.RepoDir + this.userName + "/indexes/" + this.DSname + "/",
//            url.meta, url.ID.toInt, false);
//        }
        while ( {
          line = reader.readLine;
          line
        } != null)
          writer.println(url.ID + "\t" + line)

        reader.close
      } catch {
        case _:Throwable => logger.error("Meta file is not found .. " + url.name + ".meta \tCheck the schema URL.. ")
      }
    }
    writer.close

    logger.info("Meta of" + DSname + " data set is Built... ")
  }

  def getIndexURI: String = Utilities.RepoDir + this.userName + "/indexes/" + this.DSname

  def Delete() = {
    val index: File = new File(Utilities.RepoDir + this.userName + "/indexes/" + this.DSname)
    val meta: File = new File(Utilities.RepoDir + this.userName + "/metadata/" + this.DSname + ".meta")
    val dataset: File = new File(Utilities.RepoDir + this.userName + "/datasets/" + this.DSname + ".xml")
    val schema: File = new File(Utilities.RepoDir + this.userName + "/schema/" + this.DSname + ".schema")
    val regions: File = new File(Utilities.RepoDir + this.userName + "/regions/" + this.DSname)
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
      Utilities.deleteFromLocalFSRecursive(regions)
      logger.info(this.DSname + " regions is deleted .. " + regions.toString)
    }
    else logger.warn(this.DSname + ", regions is not found in { regions } folder..\n")

    if (meta.exists) {
      meta.delete
      logger.info(this.DSname + " meta Data is deleted ..")
    }
    else logger.warn(this.DSname + ", meta is not found..\n")

    if (index.exists) {
      Utilities.deleteFromLocalFSRecursive(index)
      index.delete
      logger.info(this.DSname + " index is also deleted ..")
    }
    else logger.warn(this.DSname + ", index is not found..\n")

    logger.info("All files and folders related to " + this.DSname + " are now deleted\n" + "\t except the user local original files..")
  }

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
    val out: PrintWriter = new PrintWriter(new BufferedWriter(new FileWriter(Utilities.RepoDir + this.userName + "/metadata/" + this.DSname + ".meta", true)))
    try {
      val file: Path = Paths.get(sample.meta)
      if (Files.exists(file) && Files.isReadable(file)) {
        LuceneIndex.addSampletoIndex(Utilities.RepoDir + this.userName + "/indexes/" + this.DSname + "/", sample.meta, id, false)
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

  private def submitChangesToXML(): Unit = {
    new File(Utilities.RepoDir + this.userName + "/datasets/" + DSname + ".xml").delete()
    this.storeXML(generateDSXML(), Utilities.RepoDir + this.userName + "/datasets/" + DSname + ".xml")
  }

  def delSample(sample: GMQLSample): Int = {
    val deletedid: Int = checkSampleInDataSet(sample, true)
    if (deletedid != 0) {
      try {
        val writer: PrintWriter = new PrintWriter(Utilities.RepoDir + this.userName + "/metadata/" + this.DSname + ".meta.tmp", "UTF-8")
        val file: Path = Paths.get(Utilities.RepoDir + this.userName + "/metadata/" + this.DSname + ".meta")
        if (Files.exists(file) && Files.isReadable(file)) {
          LuceneIndex.deleteIndexedSamplebyURL(Utilities.RepoDir + this.userName + "/indexes/" + this.DSname + "/", sample.meta )
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
        val f1: File = new File(Utilities.RepoDir + this.userName + "/metadata/" + this.DSname + ".meta")
        val f: File = new File(Utilities.RepoDir + this.userName + "/metadata/" + this.DSname + ".meta.tmp")
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

  @throws(classOf[GMQLSampleNotFound])
  def getMeta(sample: GMQLSample): String = {
    val s = samples.filter(x => x.name == sample.name)
    if (!s.isEmpty) {
      val reader: BufferedReader = Files.newBufferedReader(Paths.get(Utilities.RepoDir + userName + "/metadata/" + DSname + ".meta"), Charset.defaultCharset)
      var line: String = ""
      val st = new StringBuilder();
      while ( {
        line = reader.readLine;
        line
      } != null) {
        val str: Array[String] = line.split("\t")
        if (!(str(0) == s(0).ID))
          st.append(str.tail.mkString("\t"))
      }
      reader.close
      st.toString()
    } else throw new GMQLSampleNotFound()
  }

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
    scala.io.Source.fromFile(Utilities.RepoDir + userName + "/metadata/" + DSname + ".meta").mkString
  }

  @throws(classOf[GMQLUserNotFound])
  def checkuser(username: String): Boolean = {
    val dataSetDir: File = new File(Utilities.RepoDir + username)
    if (!dataSetDir.exists) {
      logger.debug("This user may not be registered .." + username)
      throw new GMQLUserNotFound()
    }
    true
  }
}