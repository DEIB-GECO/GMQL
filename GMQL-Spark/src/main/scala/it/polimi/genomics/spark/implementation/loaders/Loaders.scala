package it.polimi.genomics.spark.implementation.loaders

//
//
// Author: Abdulrahman Kaitoua
//
//

import com.google.common.hash._
import it.polimi.genomics.core.DataStructures.RegionCondition.RegionCondition
import it.polimi.genomics.core.{GValue}
import it.polimi.genomics.core.DataTypes.{GRECORD, MetaType}
import it.polimi.genomics.core.GRecordKey
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io._
import org.apache.hadoop.io.compress.CompressionCodecFactory
import org.apache.hadoop.mapreduce.lib.input._
import org.apache.hadoop.mapreduce.{InputSplit, RecordReader, TaskAttemptContext}
import org.apache.hadoop.util.LineReader
import org.apache.spark.SparkContext
import org.apache.spark.rdd._
import org.slf4j.LoggerFactory

object Loaders {

  private val defaultCombineSize = 64
  private val defaultCombineDelim = "\n"
  private final val logger = LoggerFactory.getLogger(this.getClass)
  class Context(val sc: SparkContext, val path: String) {
    val conf = new Configuration()
    conf.set("textinputformat.record.delimiter", defaultCombineDelim)
    conf.set("mapreduce.input.fileinputformat.input.dir.recursive", "true")
    conf.set("mapred.input.dir", path)
    conf.setLong("mapred.max.split.size", defaultCombineSize*1024*1024)

    def setSplitSize(size: Long) = {
      conf.setLong("mapred.max.split.size", size*1024*1024)
      this
    }

    def setRecordDelim(delim: String) = {
      conf.set("textinputformat.record.delimiter", delim)
      this
    }

    def LoadMetaCombineFiles(parser:((Long,String))=>Option[MetaType]): RDD[MetaType] = {
      sc
        .newAPIHadoopRDD(conf, classOf[CombineTextFileWithPathInputFormat], classOf[Long], classOf[Text])
        .flatMap(x =>  parser(x._1,x._2.toString))

    }

    def LoadRegionsCombineFiles(parser:((Long,String))=>Option[GRECORD],lineFilter:((RegionCondition,GRECORD)=> Boolean),regionPredicate:Option[RegionCondition]): RDD[( GRecordKey,Array[GValue])] = {
      sc
        .newAPIHadoopRDD(conf, classOf[CombineTextFileWithPathInputFormat], classOf[Long], classOf[Text])
        .flatMap{x => val gRecord = parser(x._1,x._2.toString);
        gRecord match{
          case Some(reg) => if (regionPredicate.isDefined) {if (lineFilter(regionPredicate.get,reg)) gRecord else None} else gRecord
          case None => None
        }
      }
    }

    def LoadRegionsCombineFiles(parser:((Long,String))=>Option[GRECORD]): RDD[GRECORD] = {
      val rdd = sc.newAPIHadoopRDD(conf, classOf[CombineTextFileWithPathInputFormat], classOf[Long], classOf[Text])
      //.repartition(20)
      val rddPartitioned =
        if(rdd.partitions.size<8)
          rdd//.repartition(8)
        else
          rdd

        rddPartitioned.flatMap(x => parser(x._1, x._2.toString))
    }
  }

  private class CombineTextFileWithPathInputFormat extends CombineFileInputFormat[Long, Text] {
    override def createRecordReader(
                                     split: InputSplit,
                                     context: TaskAttemptContext): RecordReader[Long, Text] =
      new CombineFileRecordReader(split.asInstanceOf[CombineFileSplit], context, classOf[CombineTextFileWithPathRecordReader])
  }

  private class CombineTextFileWithPathRecordReader(
                                                     split: CombineFileSplit,
                                                     context: TaskAttemptContext,
                                                     index: Integer) extends CombineMetaRecordReader[Long](split, context, index) {

    override def generateKey(split: CombineFileSplit, index: Integer) = {
      val uri = split.getPath(index).toString
      val uriExt =uri.substring(uri.lastIndexOf(".")+1,uri.size)
      val URLNoMeta = if(!uriExt.equals("meta"))uri.substring(uri.indexOf(":")+1,uri.size ) else  uri.substring(uri.indexOf(":")+1,uri.lastIndexOf("."))
//      logger.info ("LOADER: "+URLNoMeta.replaceAll("/","")+"\t"+ Hashing.md5().newHasher().putString(URLNoMeta.replaceAll("/",""),java.nio.charset.StandardCharsets.UTF_8).hash().asLong())
      //println("\n\n\n"+URLNoMeta+ "\n\n\n")
      //println(Hashing.md5().newHasher().putString(URLNoMeta,java.nio.charset.StandardCharsets.UTF_8).hash().asLong())
      //println(Hashing.md5().hashString(URLNoMeta, Charsets.UTF_8).asLong())
      Hashing.md5().newHasher().putString(URLNoMeta.replaceAll("/",""),java.nio.charset.StandardCharsets.UTF_8).hash().asLong()
      //id = Hashing.md5().hashString((split.getPath.getName.toString).toString, Charsets.UTF_8).asLong()
    }
  }

  private abstract class CombineMetaRecordReader[K](
                                                     split: CombineFileSplit,
                                                     context: TaskAttemptContext,
                                                     index: Integer) extends RecordReader[K, Text] {

    val conf = context.getConfiguration
    val path = split.getPath(index)
    val fs = path.getFileSystem(conf)
    val codec = Option(new CompressionCodecFactory(conf).getCodec(path))

    val start = split.getOffset(index)
    val length = if(codec.isEmpty) split.getLength(index) else Long.MaxValue
    val end = start + length

    val fd = fs.open(path)
    if(start > 0) fd.seek(start)

    val fileIn = codec match {
      case Some(codec) => codec.createInputStream(fd)
      case None => fd
    }

    var reader = new LineReader(fileIn)
    var pos = start

    def generateKey(split: CombineFileSplit, index: Integer): K

    protected val key = generateKey(split, index)
    protected val value = new Text

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

  def forPath(sc: SparkContext, path: String) = {
    new Context(sc, path)
  }

  implicit class SparkContextFunctions(val self: SparkContext) extends AnyVal {

    def forPath(path: String): Loaders.Context = Loaders.forPath(self, path)
  }
}
