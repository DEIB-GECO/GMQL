package it.polimi.genomics.spark.implementation.loaders

//
//
// Author: Abdulrahman Kaitoua
//
//

import com.google.common.hash._
import it.polimi.genomics.core.DataStructures.RegionCondition.RegionCondition
import it.polimi.genomics.core.DataTypes.{GRECORD, MetaType}
import it.polimi.genomics.core.{GRecordKey, GValue}
import it.polimi.genomics.core.exception.ParsingException
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataInputStream, FileSystem, Path}
import org.apache.hadoop.io._
import org.apache.hadoop.io.compress.{CompressionCodec, CompressionCodecFactory}
import org.apache.hadoop.mapreduce.lib.input._
import org.apache.hadoop.mapreduce.{InputSplit, RecordReader, TaskAttemptContext}
import org.apache.hadoop.util.LineReader
import org.apache.spark.SparkContext
import org.apache.spark.rdd._
import org.slf4j.{Logger, LoggerFactory}

/**
  * Loader to enable combining
  */
object Loaders {

  private val defaultCombineSize: Int = 64
  private val defaultCombineDelim: String = "\n"
  private final val logger: Logger = LoggerFactory.getLogger(this.getClass)

  /**
    *
    * @param sc [[SparkContext]] for reading the path
    * @param path [[String]] as the input path
    */
  class Context(val sc: SparkContext, val path: String) {
    val conf = new Configuration()
    conf.set("textinputformat.record.delimiter", defaultCombineDelim)
    conf.set("mapreduce.input.fileinputformat.input.dir.recursive", "true")
    conf.set("mapred.input.dir", path)
    conf.setLong("mapred.max.split.size", defaultCombineSize*1024*1024)

    /**
      *
      *
      * @param size [[Long]] as the size of the split
      * @return [[Context]] instance
      */
    def setSplitSize(size: Long) = {
      conf.setLong("mapred.max.split.size", size*1024*1024)
      this
    }

    /**
      * delimerter to be considered in reading the records, default is "newline"
      *
      * @param delim [[String]] as the new delimiter
      * @return [[Context]] instance
      */
    def setRecordDelim(delim: String) = {
      conf.set("textinputformat.record.delimiter", delim)
      this
    }

    /**
      *  Load meta data using a parser function.
      *
      * @param parser parser function as (([[Long]],[[String]]))=>[[Option]] of [[MetaType]]
      * @param checkConsistency: if true throws a [[ParsingException]] in case of consistency problems
      * @return [[RDD]] of the metadata [[MetaType]]
      */
    def LoadMetaCombineFiles(parser:((Long,String))=>Option[MetaType], checkConsistency: Boolean): RDD[MetaType] = {
      sc
        .newAPIHadoopRDD(conf, classOf[CombineTextFileWithPathInputFormat], classOf[Long], classOf[Text])
        .flatMap(x =>  {
          if (checkConsistency) {
            parser(x._1,x._2.toString)
          } else {
            try {
              parser(x._1, x._2.toString)
            } catch {
              case e: ParsingException => logger.warn(e.getMessage); None;
            }
          }
        })

    }

    def LoadMetaCombineFiles(parser:((Long,String))=>Option[MetaType]): RDD[MetaType]  = LoadMetaCombineFiles(parser, false)

    /**
      *
      * Load regions data using a parser function.
      *
      * @param parser parser function as (([[Long]], [[String]])) => [[Option]][GRECORD]
      * @param lineFilter  line filter function as (([[RegionCondition]], [[GRECORD]]) => [[Boolean]]) to filter lines while loading
      * @param regionPredicate [[Option]] of [[RegionCondition]]
      * @param checkConsistency: if true throws a [[ParsingException]] in case of consistency problems
      * @return [[RDD]] of [[GRECORD]]
      */
    def LoadRegionsCombineFiles(parser: ((Long, String)) => Option[GRECORD], lineFilter: ((RegionCondition, GRECORD) => Boolean), regionPredicate: Option[RegionCondition], checkConsistency: Boolean): RDD[GRECORD] = {
      val rdd = sc
        .newAPIHadoopRDD(conf, classOf[CombineTextFileWithPathInputFormat], classOf[Long], classOf[Text])
      val rddPartitioned =
      //        if (rdd.partitions.size < 20)
      //        rdd.repartition(40)
      //      else
        rdd
      rddPartitioned.flatMap { x =>

        var  gRecord: Option[(GRecordKey, Array[GValue])] = None

        if (checkConsistency) {
           gRecord = parser(x._1, x._2.toString);
        } else {
          try {
            gRecord = parser(x._1, x._2.toString)
          } catch {
            case e: ParsingException => logger.warn(e.getMessage); None;
          }
        }

        gRecord match {
          case Some(reg) => if (regionPredicate.isDefined) {
            if (lineFilter(regionPredicate.get, reg)) gRecord else None
          } else gRecord
          case None => None
        }
      }
    }

    def LoadRegionsCombineFiles(parser: ((Long, String)) => Option[GRECORD],
                                lineFilter: ((RegionCondition, GRECORD) => Boolean),
                                regionPredicate: Option[RegionCondition]): RDD[GRECORD] =
      LoadRegionsCombineFiles(parser, lineFilter, regionPredicate, false)

    /**
      * Load regions data using a parser function.
      *
      * @param parser A parser function as (([[Long]], [[String]])) => [[Option]][GRECORD]
      * @param checkConsistency: if true throws a [[ParsingException]] in case of consistency problems
      * @return [[RDD]] of [[GRECORD]]
      */
    def LoadRegionsCombineFiles(parser:((Long,String)) =>Option[GRECORD], checkConsistency: Boolean): RDD[GRECORD] = {
      val rdd = sc.newAPIHadoopRDD(conf, classOf[CombineTextFileWithPathInputFormat], classOf[Long], classOf[Text])
      //.repartition(20)
      val rddPartitioned =
      //        if(rdd.partitions.size<20)
      //          rdd.repartition(40)
      //        else
        rdd

      rddPartitioned.flatMap(x => {
        if (checkConsistency) {
          parser(x._1, x._2.toString)
        } else {
          try {
            parser(x._1, x._2.toString)
          } catch {
            case e: ParsingException => logger.warn(e.getMessage); None;
          }
        }

      })
    }

    def LoadRegionsCombineFiles( parser:((Long,String)) => Option[GRECORD] ) : RDD[GRECORD] = LoadRegionsCombineFiles(parser , false)
  }

  /**
    *  Combine files into a bigger partition,
    *  this reduce the number of partitions when the files sizes are small (less then 64 MB).
    */
  private class CombineTextFileWithPathInputFormat extends CombineFileInputFormat[Long, Text] {
    override def createRecordReader(
                                     split: InputSplit,
                                     context: TaskAttemptContext): RecordReader[Long, Text] =
      new CombineFileRecordReader(split.asInstanceOf[CombineFileSplit], context, classOf[CombineTextFileWithPathRecordReader])
  }

  /**
    *
    * @param split Hadoop file system split, [[CombineFileSplit]]
    * @param context hadoop taask context, [[TaskAttemptContext]]
    * @param index index as [[Integer]]
    */
  private class CombineTextFileWithPathRecordReader(
                                                     split: CombineFileSplit,
                                                     context: TaskAttemptContext,
                                                     index: Integer) extends CombineMetaRecordReader[Long](split, context, index) {

    /**
      *
      * Hash the file name and generate an id for the sample.
      *
      * @param split Hadoop file system split, [[CombineFileSplit]]
      * @param index index as [[Integer]]
      * @return [[Long]] as the file ID
      */
    override def generateKey(split: CombineFileSplit, index: Integer): Long = {
      val uri = split.getPath(index).getName
      val uriExt =uri.substring(uri.lastIndexOf(".")+1,uri.size)
      val URLNoMeta = if(!uriExt.equals("meta"))uri.substring(0,uri.size ) else  uri.substring(0,uri.lastIndexOf("."))
      Hashing.md5().newHasher().putString(URLNoMeta.replaceAll("/",""),java.nio.charset.StandardCharsets.UTF_8).hash().asLong()
    }
  }

  /**
    * Combine file record, each line in meta file is concedered a record.
    *
    * @param split Hadoop file system split, [[CombineFileSplit]]
    * @param context  hadoop taask context, [[TaskAttemptContext]]
    * @param index as [[Integer]]
    * @tparam K the type of the record, usually here it is [[Text]]
    */
  private abstract class CombineMetaRecordReader[K](
                                                     split: CombineFileSplit,
                                                     context: TaskAttemptContext,
                                                     index: Integer) extends RecordReader[K, Text] {

    val conf: Configuration = context.getConfiguration
    val path: Path = split.getPath(index)
    val fs: FileSystem = path.getFileSystem(conf)
    val codec: Option[CompressionCodec] = Option(new CompressionCodecFactory(conf).getCodec(path))

    val start: Long = split.getOffset(index)
    val length: Long = if(codec.isEmpty) split.getLength(index) else Long.MaxValue
    val end: Long = start + length

    val fd: FSDataInputStream = fs.open(path)
    if(start > 0) fd.seek(start)

    val fileIn = codec match {
      case Some(codec) => codec.createInputStream(fd)
      case None => fd
    }

    var reader: LineReader = new LineReader(fileIn)
    var pos: Long = start

    def generateKey(split: CombineFileSplit, index: Integer): K

    protected val key: K = generateKey(split, index)
    protected val value: Text = new Text

    override def initialize(split: InputSplit, ctx: TaskAttemptContext) {}

    override def nextKeyValue(): Boolean = {
      if (pos < end) {
        val newSize = reader.readLine(value)
        pos += newSize
        newSize != 0
      } else {
        false
      }
    }

    override def close(): Unit = if (reader != null) { reader.close(); reader = null }
    override def getCurrentKey: K = key
    override def getCurrentValue: Text = value
    override def getProgress: Float = if (start == end) 0.0f else math.min(1.0f, (pos - start).toFloat / (end - start))
  }

  /**
    * Set [[SparkContext]] and the input directory path as a [[String]]
    *
    * @param sc  [[SparkContext]] to read the input files.
    * @param path input directory path as a [[String]]
    * @return
    */
  def forPath(sc: SparkContext, path: String) = {
    new Context(sc, path)
  }

  implicit class SparkContextFunctions(val self: SparkContext) extends AnyVal {

    def forPath(path: String): Loaders.Context = Loaders.forPath(self, path)
  }
}
