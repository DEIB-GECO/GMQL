package it.polimi.genomics.flink.FlinkImplementation.reader

import java.io._
import java.nio.charset.Charset

import it.polimi.genomics.core.DataTypes.FlinkMetaType
import it.polimi.genomics.core.exception.ParsingException
import it.polimi.genomics.repository.util.Utilities
import org.apache.flink.api.common.io.DelimitedInputFormat
import org.apache.flink.configuration.Configuration
import org.apache.flink.core.fs.FileInputSplit
import org.apache.flink.hadoop.shaded.com.google.common.base.Charsets
import org.apache.flink.hadoop.shaded.com.google.common.hash.Hashing
import org.apache.hadoop.fs.PathFilter
import org.slf4j.LoggerFactory


/** The default reader for local file system files. */
class TestingMetaReader(parser : ((Long,String)) => FlinkMetaType)(files : List[String]) extends DelimitedInputFormat[FlinkMetaType] {

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

    def openFile(file : String) : Seq[FileInputSplit]= {
      try {
        super.setFilePath(file)
        super.createInputSplits(minNumSplits)
      } catch {
        case e : FileNotFoundException => {
          logger.warn("Meta Data file not found: " + e.getMessage + " GMQL will try creating a default one.")
          try {
            createFakeMetaFile(file)
            super.setFilePath(file)
            super.createInputSplits(minNumSplits)
          } catch {
            case e : Throwable => {
              logger.warn("GMQL failed creating such file no meta-data will be loaded for that sample: " + e.getMessage)
              List()
            }
          }
        }
      }
    }

    val paths : List[String] = files

    paths.flatMap((f) => {
      val fs = Utilities.getInstance().getFileSystem
      if(new File(f).isDirectory){
        logger.debug("File : " + f + " is a directory")
        new File(f).listFiles(DataSetFilter).flatMap((subFile) => {
          logger.debug("File : " + subFile + ".meta is a single file")
          openFile(subFile + ".meta")
        })
      } else if (fs.exists(new org.apache.hadoop.fs.Path(f))) {
        logger.info("here")
        fs.listStatus(new org.apache.hadoop.fs.Path(f), new PathFilter {
          override def accept(path: org.apache.hadoop.fs.Path): Boolean = !path.getName.endsWith(".meta")
        }).flatMap { x =>
          logger.debug("File : " + x.getPath.toString + ".meta is a single file")
          openFile(x.getPath.toString + ".meta")
        }
      }else {
        logger.debug("File : " + f + ".meta is a single file")
        openFile(f + ".meta")
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
    //TODO hasher


    val uri = split.getPath.toString
    val filePath2hash = uri.substring(uri.indexOf(":")+1, uri.size - 5).replaceAll("/","")//.lastIndexOf("."))
//    logger.info(split.getPath.toString+"\t"+filePath2hash )
    id = Hashing.md5().hashString(filePath2hash, Charsets.UTF_8).asLong()
  }

  /** Parse a single line of a text file. The file is assumed to be tab separated.
    *
    * @return a tuple where the first element is the ID (computed from the file path) and the second is the line text.
    */
  override def readRecord(reusable : FlinkMetaType, bytes : Array[Byte], offset : Int, numBytes : Int) : FlinkMetaType = {
    val line = new String(bytes.slice(offset,offset+numBytes), Charset.forName(charsetName))
    parser(id, line)
  }

  @throws(classOf[ArrayIndexOutOfBoundsException])
  override def nextRecord(record : FlinkMetaType) : FlinkMetaType = {
    try{
      val res = super.nextRecord(record)
      res
    } catch {
      case e : ParsingException => {
        logger.warn("----------------- Meta Data format error in the tuple: " + e.getMessage)
        return null
      }
    }
  }

  def createFakeMetaFile(filePath : String) = {
    val fileName = filePath.substring(filePath.lastIndexOf('/') + 1)
    val out = new BufferedWriter(new FileWriter(new File(filePath)));
    val s = new StringBuilder
    s.append("GMQL_automatic_filename")
    s.append("\t")
    s.append(fileName)
    s.append("\n")
    s.append("GMQL_type")
    s.append("\t")
    s.append("GMQL generated meta-file, original was missing")
    out.write(s.toString())
    out.flush()
    out.close()
  }

}
