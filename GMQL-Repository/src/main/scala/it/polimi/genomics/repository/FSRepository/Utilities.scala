package it.polimi.genomics.repository.FSRepository

import java.nio.file.{FileSystems, Paths}

import it.polimi.genomics.core.ParsingType
import it.polimi.genomics.repository.GMQLRepository.{GMQLNotValidDatasetNameException, GMQLSample, GMQLSchemaTypes}
import java.io._

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.slf4j.LoggerFactory
import javax.xml.transform.stream.StreamSource
import javax.xml.validation.Schema
import javax.xml.validation.SchemaFactory
import javax.xml.validation.{Validator => JValidator}

import it.polimi.genomics.repository.GMQLRepository.Utilities._
import org.xml.sax.SAXException

/**
  * Created by abdulrahman on 12/04/16.
  */
object Utilities {
  val logger = LoggerFactory.getLogger(this.getClass)
  val CoreConfigurationFiles: String = System.getenv("HADOOP_CONF_DIR")+"/core-site.xml"
  val HDFSConfigurationFiles: String = System.getenv("HADOOP_CONF_DIR")+"/hdfs-site.xml"

  def getType(schemaType:String): GMQLSchemaTypes.Value ={
    schemaType. toLowerCase() match {
      case "gtf" => GMQLSchemaTypes.GTF
      case "del" => GMQLSchemaTypes.Delimited
      case "vcf" => GMQLSchemaTypes.VCF
      case _ => GMQLSchemaTypes.Delimited
    }
  }
  def validate(xmlFile: String): Boolean = {
    val xsdFile =  GMQLHOME+"/conf/gmqlSchemaCollection.xsd"

    try {
      val schemaLang = "http://www.w3.org/2001/XMLSchema"
      val factory = SchemaFactory.newInstance(schemaLang)
      val schema = factory.newSchema(new StreamSource(xsdFile))
      val validator = schema.newValidator()
      validator.validate(new StreamSource(xmlFile))
    } catch {
      case ex: SAXException => logger.debug("XSD validate SAXException", ex);  throw ex
      case ex: Exception => ex.printStackTrace()
    }
    true
  }

  @throws(classOf[GMQLNotValidDatasetNameException])
  def dirFiles( URL: String) {
    var fields = List[GMQLSample]()
    var i: Int = 0
    if ((new File(URL)).isDirectory) {
      val files: Array[File] = (new File(URL)).listFiles(new FilenameFilter() {
        def accept(dir: File, name: String): Boolean = {
          if (name.endsWith(".meta")) return false
          val f: File = new File(dir.toString + "/" + name + ".meta")
          logger.info(dir.toString + "/" + name + " => has meta file - " + f.exists)
          return f.exists
        }
      })
      if (files.length == 0) {
        logger.warn("The dataSet is empty.. \n\tCheck the files extensions. (i.e. sample.bed/sample.bed.meta)")
      }
      for (file <- files)
        fields = fields :+ new GMQLSample(file.getAbsolutePath, {i += 1; i.toString})
    }
    else {
      val url: Array[String] = URL.split(",")
      for (u <- url)
        fields =  fields :+ new GMQLSample(u, {i += 1; i.toString})
    }
  }

  def gethdfsConfiguration(): Configuration = {
    val conf = new Configuration();

    conf.addResource(new org.apache.hadoop.fs.Path(CoreConfigurationFiles))
    conf.addResource(new org.apache.hadoop.fs.Path(HDFSConfigurationFiles))
    conf.set("fs.hdfs.impl", classOf[org.apache.hadoop.hdfs.DistributedFileSystem].getName)
    conf.set("fs.file.impl", classOf[org.apache.hadoop.fs.LocalFileSystem].getName)
    conf
  }

  @throws(classOf[IOException])
  def copyfiletoLocal(sourceHDFSUrl: String, distLocalUrl: String) :Boolean= {
    val conf = gethdfsConfiguration
    val fs = FileSystem.get(conf)
    if (!fs.exists(new Path(sourceHDFSUrl))) {
      logger.error("The Dataset sample Url is not found: " + sourceHDFSUrl)
      return false
    }
    if (fs.exists(new Path(sourceHDFSUrl + ".meta"))) {
      fs.copyToLocalFile(new Path(sourceHDFSUrl + ".meta"), new Path(distLocalUrl + ".meta"))
    }
    println("source:", sourceHDFSUrl)
    println("Distination",distLocalUrl)
    fs.copyToLocalFile(new Path(sourceHDFSUrl), new Path(distLocalUrl))
    val dist: File = new File(distLocalUrl)
    true
  }

  @throws(classOf[IOException])
  def copyfiletoHDFS(sourceUrl: String, distUrl: String)= {
    val conf = gethdfsConfiguration
    val fs = FileSystem.get(conf)
    if (!fs.exists(new Path(Paths.get(distUrl).getParent.toString))) {
      fs.mkdirs(new Path(Paths.get(distUrl).getParent.toString))
    }

    fs.copyFromLocalFile(new Path(sourceUrl), new Path(distUrl))
    if ((new File(sourceUrl + ".meta")).exists) {
      fs.copyFromLocalFile(new Path(sourceUrl + ".meta"), new Path(distUrl + ".meta"))
    }
    println("source:", sourceUrl)
    println("Distination",distUrl)
  }

  def attType(x: String) = x.toUpperCase match {
    case "STRING" => ParsingType.STRING
    case "CHAR" => ParsingType.STRING
    case "CHARACTAR" => ParsingType.STRING
    case _ => ParsingType.DOUBLE
  }
}
