package it.polimi.genomics.repository.FSRepository

import java.nio.file.{FileSystems, Paths}

import it.polimi.genomics.core.{GMQLSchemaFormat, ParsingType}
import it.polimi.genomics.repository.{GMQLSample, Utilities => General_Utilities}
import java.io._

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, LocalFileSystem, Path}
import org.slf4j.{Logger, LoggerFactory}
import javax.xml.transform.stream.StreamSource
import javax.xml.validation.{Schema, SchemaFactory, Validator}

import it.polimi.genomics.repository.GMQLExceptions.GMQLNotValidDatasetNameException
import org.apache.hadoop.hdfs.DistributedFileSystem
import org.xml.sax.SAXException

/**
  * Static methods that are used in the FileSystem repositories.
  *
  * Created by abdulrahman on 12/04/16.
  */
object FS_Utilities {
  val logger: Logger = LoggerFactory.getLogger(this.getClass)

  val conf: Configuration = FS_Utilities.gethdfsConfiguration()
  val fs: FileSystem = FileSystem.get(conf)
  conf.get("fs.defaultFS")

  /**
    *
    * validate the xml file of the schema using XSD template
    *
    * @param xmlFile [[ String]] of the path to the schema file to be checked
    * @return
    */
  def validate(xmlFile: String): Boolean = {
    val xsdFile =  General_Utilities().getConfDir() + "/gmqlSchemaCollection.xsd"

    try {
      val schemaLang: String = "http://www.w3.org/2001/XMLSchema"
      val factory: SchemaFactory = SchemaFactory.newInstance(schemaLang)
      val schema: Schema = factory.newSchema(new StreamSource(xsdFile))
      val validator: Validator = schema.newValidator()
      validator.validate(new StreamSource(xmlFile))
    } catch {
      case ex: SAXException => logger.debug("XSD validate SAXException", ex);  throw ex
      case ex: Exception => ex.printStackTrace()
    }
    true
  }

  /**
    *
    *  Give a list of [[ GMQLSample]] from a directory
    *
    * @param URL [[ String]] of the path to the directory to check the samples inside.
    * @throws it.polimi.genomics.repository.GMQLExceptions.GMQLNotValidDatasetNameException
    */
  @deprecated
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

  /**
    *
    *   Return Hadoop Configuration File by reading Hadoop Core and HDFS config files
    *
    * @return [[ Configuration]] Hadoop configurations
    */
  def gethdfsConfiguration(): Configuration = {
    val conf = new Configuration();

    conf.addResource(new org.apache.hadoop.fs.Path(General_Utilities().CoreConfigurationFiles))
    conf.addResource(new org.apache.hadoop.fs.Path(General_Utilities().HDFSConfigurationFiles))
    conf.set("fs.hdfs.impl", classOf[org.apache.hadoop.hdfs.DistributedFileSystem].getName)
    conf.set("fs.file.impl", classOf[org.apache.hadoop.fs.LocalFileSystem].getName)
    conf
  }

  /**
    *
    *  Return Hadoop Distributed File System handle
    *
    * @return [[ FileSystem]]
    */
  def getFileSystem: FileSystem = {
    val fs:FileSystem = null
    try {
      val fs = FileSystem.get(gethdfsConfiguration)
    }
    catch{case e: IOException => e.printStackTrace()}
    fs
  }

  /**
    *
    *   Copy Files from HDFS to local
    *   This will check if the file has meta file and if found, it will copy the meta file too
    *
    * @param sourceHDFSUrl
    * @param distLocalUrl
    * @throws java.io.IOException
    * @return
    */
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
    logger.debug("source:", sourceHDFSUrl)
    logger.debug("Distination",distLocalUrl)
    fs.copyToLocalFile(new Path(sourceHDFSUrl), new Path(distLocalUrl))
    val dist: File = new File(distLocalUrl)
    true
  }

  /**
    *
    *   Copy File from Local file system to Hadoop File system
    *   TODO: make this function copy files in parallel using hadoop distcp command
    *
    * @param sourceUrl
    * @param distUrl
    * @throws java.io.IOException
    */
  @throws(classOf[IOException])
  def copyfiletoHDFS(sourceUrl: String, distUrl: String)= {
    val conf = gethdfsConfiguration
    val fs = FileSystem.get(conf)
    if (!fs.exists(new Path(Paths.get(distUrl).getParent.toString))) {
      logger.info(s"Folder ($distUrl) not found, creating directory..")
      fs.mkdirs(new Path(Paths.get(distUrl).getParent.toString))
    }else {
      logger.debug(s"Directory ($distUrl) is found in DFS")
    }
    logger.debug(s"source: $sourceUrl")
    logger.debug(s"Distination $distUrl")

    fs.copyFromLocalFile(new Path(sourceUrl), new Path(distUrl))
    if ((new File(sourceUrl + ".meta")).exists) {
      fs.copyFromLocalFile(new Path(sourceUrl + ".meta"), new Path(distUrl + ".meta"))
    }

  }

  /**
    *
    *   Delete sample from HDFS
    *   If the sample has meta file, the meta file will be deleted too
    *
    * @param url
    * @return
    * @throws IOException
    */
  @throws[IOException]
  def deleteDFSDir(url: String): Boolean = {
    def conf = gethdfsConfiguration
    def fs = FileSystem.get(conf)
    fs.delete(new Path(url), true)
    if (fs.exists(new Path(url + ".meta"))) fs.delete(new Path(url + ".meta"), true)
    true
  }

  /**
    *
    *   Delete file from local file system recuresivly
    *
    * @param inputfile
    */
  def deleterecursive(inputfile: File) {
    val files = inputfile.listFiles
    for (file <- files) {
      if (file.isDirectory) deleterecursive(file)
      logger.info("Folder" + file.getPath + ", Status: " + (if (file.delete) "Deleted." else "Error"))
    }
  }

  /**
    *
    *   Create directiry in HDFS
    *
    * @param url [[ String]] of the Directory location
    * @throws [[IOException]]
    * @return
    */
  @throws[IOException]
  def createDFSDir(url: String):Boolean = {
    val conf = new Configuration
    conf.addResource(new Path(General_Utilities().CoreConfigurationFiles))
    conf.addResource(new Path(General_Utilities().HDFSConfigurationFiles))
    conf.set("fs.hdfs.impl", classOf[DistributedFileSystem].getName)
    conf.set("fs.file.impl", classOf[LocalFileSystem].getName)
    val fs = FileSystem.get(conf)
    if (fs.mkdirs(new Path(url)))  true else false
  }

  /**
    *  Delete files from the local file system
    *
    * @param dir
    */
    def deleteFromLocalFSRecursive(dir: File) {
      var files: Array[File] = null
      if (dir.isDirectory) {
        files = dir.listFiles
        if (!(files == null)) {
          for (file <- files) {
            deleteFromLocalFSRecursive(file)
          }
        }
        dir.delete
      }
      else dir.delete
    }

  /**
    *
    *  Get the [[ GMQLSchemaFormat]] of a specific String
    *
    * @param schemaType String of the schema type
    * @return
    */
  def getType(schemaType:String): GMQLSchemaFormat.Value ={
    schemaType. toLowerCase() match {
      case "gtf" => GMQLSchemaFormat.GTF
      case "del" => GMQLSchemaFormat.TAB
      case "vcf" => GMQLSchemaFormat.VCF
      case _ => GMQLSchemaFormat.TAB
    }
  }

  /**
    *
    * Get the data type correspondance type in GMQL
    *
    * @param x [[ String]] of the data type
    * @return
    */
  def attType(x: String): ParsingType.Value = x.toUpperCase match {
    case "STRING" => ParsingType.STRING
    case "CHAR" => ParsingType.STRING
    case "CHARACTAR" => ParsingType.STRING
    case "LONG" => ParsingType.DOUBLE
    case "INTEGER" => ParsingType.DOUBLE
    case "INT" => ParsingType.DOUBLE
    case _ => ParsingType.DOUBLE
  }

}
