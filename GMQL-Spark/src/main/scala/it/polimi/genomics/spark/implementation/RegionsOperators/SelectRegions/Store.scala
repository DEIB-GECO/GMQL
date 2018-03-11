package it.polimi.genomics.spark.implementation.RegionsOperators.SelectRegions

import it.polimi.genomics.core.DataStructures.{MetaOperator, RegionOperator}
import it.polimi.genomics.core.DataTypes.GRECORD
import it.polimi.genomics.core.ParsingType.PARSING_TYPE
import it.polimi.genomics.core.exception.SelectFormatException
import it.polimi.genomics.core.{GMQLSchemaCoordinateSystem, GRecordKey, GValue}
import it.polimi.genomics.spark.implementation.GMQLSparkExecutor
import it.polimi.genomics.spark.implementation.loaders.writeMultiOutputFiles
import it.polimi.genomics.spark.implementation.loaders.writeMultiOutputFiles.RDDMultipleTextOutputFormat
import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkContext}
import org.slf4j.LoggerFactory

import scala.collection.Map

object Store {

  def sampleNames(meta: RDD[(Long, (String, String))], extension: String, sc: SparkContext): Map[Long, String] = {
    val outSample = s"S_%05d." + extension
    val ids: Array[Long] = meta.keys.distinct().collect().sorted
    val newIDS: Map[Long, String] = ids.zipWithIndex.map(s => (s._1, outSample.format(s._2))).toMap
    val bc = sc.broadcast(newIDS)
    val res = bc.value
    bc.unpersist()
    res
  }
}

abstract class Store(val extension: String) extends Serializable{
  private final val logger = LoggerFactory.getLogger(this.getClass)
  final val ENCODING = "UTF-8"


  @throws[SelectFormatException]
  def apply(executor: GMQLSparkExecutor, path: String, value: RegionOperator, associatedMeta: MetaOperator, schema: List[(String, PARSING_TYPE)], coordinateSystem: GMQLSchemaCoordinateSystem.Value, sc: SparkContext): RDD[GRECORD] = {
    val regions = executor.implement_rd(value, sc)
    val meta: RDD[(Long, (String, String))] = executor.implement_md(associatedMeta, sc)

    val MetaOutputPath = path + "/meta/"
    val RegionOutputPath = path + "/exp/"

    logger.debug(MetaOutputPath)
    logger.debug(RegionOutputPath)
    logger.debug(regions.toDebugString)
    logger.debug(meta.toDebugString)

    val names = Store.sampleNames(meta, extension, sc)


    val regionsPartitioner = new HashPartitioner(names.size)

    val startOffset = if (coordinateSystem == GMQLSchemaCoordinateSystem.OneBased) 1 else 0 //start: 0-based -> 1-based

    val keyedRDD =
      regions
        .filter(x => names.contains(x._1._1))
        //.sortBy(s=>s._1) //disabled sorting
        .map ( x => (names(x._1._1), toLine(x, startOffset)))
        .partitionBy(regionsPartitioner)

    keyedRDD.saveAsHadoopFile(RegionOutputPath, classOf[String], classOf[String], classOf[RDDMultipleTextOutputFormat])

    val metaKeyValue = meta.sortBy(x => (x._1, x._2)).map(x => (names(x._1) + ".meta", x._2._1 + "\t" + x._2._2)).partitionBy(regionsPartitioner)
    metaKeyValue.saveAsHadoopFile(MetaOutputPath, classOf[String], classOf[String], classOf[RDDMultipleTextOutputFormat])
    writeMultiOutputFiles.fixOutputMetaLocation(MetaOutputPath)

    regions
  }


  def toLine(record: GRECORD, startOffset: Int): String
}
