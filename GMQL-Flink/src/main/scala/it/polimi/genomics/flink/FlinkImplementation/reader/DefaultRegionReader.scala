package it.polimi.genomics.flink.FlinkImplementation.reader

import java.io.{File, FileNotFoundException, IOException}
import java.nio.charset.Charset
import java.nio.file.Paths
import javax.xml.bind.JAXBException

import it.polimi.genomics.core.DataStructures.IRDataSet
import it.polimi.genomics.core.DataTypes.FlinkRegionType
import it.polimi.genomics.core.ParsingType.PARSING_TYPE
import it.polimi.genomics.core.exception.ParsingException
import org.apache.hadoop.fs.{FileSystem, Path, PathFilter}
//import it.polimi.genomics.repository.{Utilities => General_Utilities}
//import it.polimi.genomics.repository.FSRepository.{LFSRepository, Utilities => FSR_Utilities}
import org.apache.flink.api.common.io.DelimitedInputFormat
import org.apache.flink.configuration.Configuration
import org.apache.flink.core.fs.FileInputSplit
import org.apache.flink.hadoop.shaded.com.google.common.base.Charsets
import org.apache.flink.hadoop.shaded.com.google.common.hash.Hashing
import org.slf4j.LoggerFactory


/** The default reader for local file system files. */
class DefaultRegionReader(parser : ((Long,String)) => FlinkRegionType)(files : List[String]) extends DelimitedInputFormat[FlinkRegionType] {

  private final val serialVersionUID  = 1L
  private var id : Long = 0L
  private final val CARRIAGE_RETURN : Byte = '\r'
  private final val NEW_LINE : Byte = '\n'
  private var charsetName : String = "UTF-8"


  final val logger = LoggerFactory.getLogger(this.getClass)

  override def configure(parameters : Configuration)={
    super.configure(parameters)


    if (charsetName == null || !Charset.isSupported(charsetName)) {
      throw new RuntimeException("Unsupported charset: " + charsetName)
    }
  }

  def getCharsetName() : String =  {
    return charsetName;
  }

  def setCharsetName(charsetName : String) {
    if (charsetName == null) {
      throw new IllegalArgumentException("Charset must not be null.");
    }
    this.charsetName = charsetName;
  }

  override def createInputSplits(minNumSplits : Int) = {
    val conf = new  org.apache.hadoop.conf.Configuration();
    val path = new org.apache.hadoop.fs.Path(files.head);
    val fs = FileSystem.get(path.toUri(), conf);
    files.flatMap((f) => {
      val file = new Path(f)

      if(fs.isDirectory(file)){
        logger.debug("File " + f + " is a directory")
//        file.listFiles(DataSetFilter).flatMap((subFile) => {
//          logger.debug("File " + subFile.toString + " is a single file")
//          super.setFilePath(subFile.toString)
//          super.createInputSplits(minNumSplits)
//        })
        fs.listStatus(file).flatMap { x =>
          super.setFilePath(x.getPath.toString)
          super.createInputSplits(minNumSplits)
        }

      } else {

//        if(repository.DSExists( ds)){
//          val username = if(repository.DSExistsInPublic(ds)) "public" else General_Utilities().USERNAME
//          logger.debug("File " + f + " is a repository placeholder")
//          // repository
//          val list =
//            try {
//              import scala.collection.JavaConverters._
//              repository.ListDSSamples(f,username).asScala.map(d =>
//                if (General_Utilities().MODE.equals(General_Utilities().HDFS)) {
//                  val hdfs = FSR_Utilities.gethdfsConfiguration().get("fs.defaultFS")
//                  hdfs + General_Utilities().getHDFSRegionDir(username) + d.name
//                }
//                else { d.name}
//              )
//            } catch {
//              case ex: JAXBException => logger.error("DataSet is corrupted"); List[String]()
//              case ex: FileNotFoundException => logger.error("DataSet is not Found"); List[String]()
//            }
//
//          list
//            .flatMap((subFile) => {
//              logger.debug("File " + subFile.toString + " is a single file")
//              super.setFilePath(subFile.toString)
//              super.createInputSplits(minNumSplits)
//            })
//
//        } else
        {
          logger.debug("File " + f + " is a single file")
          // single file
          super.setFilePath(f)
          super.createInputSplits(minNumSplits)
        }
      }
    }).toArray
  }

  /**
   * It creates a novel ID starting from the read file path
   * @param split
   * @throws java.io.IOException
   */
  @throws(classOf[IOException])
  override def open(split : FileInputSplit) = {
    super.open (split)
    val uri = split.getPath.toString
    val filePath2hash = uri.substring(uri.indexOf(":")+1,uri.size).replaceAll("/","")
    id = Hashing.md5().hashString(filePath2hash , Charsets.UTF_8).asLong()
  }

  /** Parse a single line of a text file. The file is assumed to be tab separated.
   *
   * @return a tuple where the first element is the ID (computed from the file path) and the second is the line text.
   */
  override def readRecord(reusable : (FlinkRegionType), bytes : Array[Byte], offset : Int, numBytes : Int) : (FlinkRegionType) = {
    (parser(id, new String(bytes.slice(offset,offset+numBytes), Charset.forName(charsetName))))
  }


  override def nextRecord(record : FlinkRegionType) : FlinkRegionType = {
    try{
      super.nextRecord(record)
    } catch {
       case e : ParsingException => {
         logger.warn("----------------- Region Data format error in the tuple: " + e.getMessage)
         return null;
       }
    }
  }

}