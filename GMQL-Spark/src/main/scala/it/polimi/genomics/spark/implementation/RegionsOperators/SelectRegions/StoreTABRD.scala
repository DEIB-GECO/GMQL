package it.polimi.genomics.spark.implementation.RegionsOperators.SelectRegions

import it.polimi.genomics.core.DataStructures.{MetaOperator, RegionOperator}
import it.polimi.genomics.core.DataTypes.GRECORD
import it.polimi.genomics.core.GMQLSchemaCoordinateSystem
import it.polimi.genomics.core.ParsingType.PARSING_TYPE
import it.polimi.genomics.core.exception.SelectFormatException
import it.polimi.genomics.spark.implementation.GMQLSparkExecutor

import it.polimi.genomics.spark.implementation.loaders.writeMultiOutputFiles
import it.polimi.genomics.spark.implementation.loaders.writeMultiOutputFiles.RDDMultipleTextOutputFormat
import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkContext}
import org.slf4j.LoggerFactory

import scala.collection.Map

/**
  * Created by abdulrahman kaitoua on 25/05/15.
  */
object StoreTABRD {
  private final val logger = LoggerFactory.getLogger(StoreTABRD.getClass)
//  private final val ENCODING = "UTF-8"

  @throws[SelectFormatException]
  def apply(executor: GMQLSparkExecutor, path: String, value: RegionOperator, associatedMeta: MetaOperator, schema: List[(String, PARSING_TYPE)], coordinateSystem: GMQLSchemaCoordinateSystem.Value, sc: SparkContext): RDD[GRECORD] = {
    val regions = executor.implement_rd(value, sc)
    val meta = executor.implement_md(associatedMeta, sc)

    val MetaOutputPath = path + "/meta/"
    val RegionOutputPath = path + "/exp/"

    logger.debug(MetaOutputPath)
    logger.debug(RegionOutputPath)
    logger.debug(regions.toDebugString)
    logger.debug(meta.toDebugString)



    val outSample = "S"

    val Ids = meta.keys.distinct()
    val newIDS: Map[Long, String] = Ids.zipWithIndex().map(s => (s._1, s"${outSample}_%05d.gdm".format(s._2))).collectAsMap()
    val newIDSbroad = sc.broadcast(newIDS)

    val regionsPartitioner = new HashPartitioner(Ids.count.toInt)
    val offset = if (coordinateSystem == GMQLSchemaCoordinateSystem.OneBased) 1 else 0 //start: 0-based -> 1-based
    val keyedRDD =
      regions //.sortBy(s=>s._1) //disabled sorting
        .map { x =>
        val newStart = x._1._3 + offset

        val stringBuilder = new StringBuilder()
        stringBuilder
          .append(x._1._2)
          .append("\t")
          .append(newStart)
          .append("\t")
          .append(x._1._4)
          .append("\t")
          .append(x._1._5)
        x._2.foreach{stringBuilder.append("\t").append(_)}


        (
          newIDSbroad.value.getOrElse(x._1._1, s"ONLY_REGION_${x._1._1}.gdm"),
          stringBuilder.toString()
        )
      }.partitionBy(regionsPartitioner)

    keyedRDD.saveAsHadoopFile(RegionOutputPath, classOf[String], classOf[String], classOf[RDDMultipleTextOutputFormat])

    val metaKeyValue = meta.sortBy(x => (x._1, x._2)).map { x =>
      (
        newIDSbroad.value.get(x._1) + ".meta",
        new StringBuilder().append(x._2._1).append("\t").append(x._2._2).toString()
      )
    }.partitionBy(regionsPartitioner)

    metaKeyValue.saveAsHadoopFile(MetaOutputPath, classOf[String], classOf[String], classOf[RDDMultipleTextOutputFormat])
    writeMultiOutputFiles.fixOutputMetaLocation(MetaOutputPath)

    regions
  }
}
