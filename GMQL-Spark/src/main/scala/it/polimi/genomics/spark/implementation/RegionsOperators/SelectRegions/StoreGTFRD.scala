package it.polimi.genomics.spark.implementation.RegionsOperators.SelectRegions

import it.polimi.genomics.core.DataStructures.{MetaOperator, RegionOperator}
import it.polimi.genomics.core.DataTypes.GRECORD
import it.polimi.genomics.core.GMQLSchemaFormat
import it.polimi.genomics.core.ParsingType.PARSING_TYPE
import it.polimi.genomics.core.exception.SelectFormatException
import it.polimi.genomics.spark.implementation.GMQLSparkExecutor
import it.polimi.genomics.spark.implementation.loaders.writeMultiOutputFiles
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.{HashPartitioner, SparkContext}
import org.apache.spark.rdd.RDD
import org.slf4j.LoggerFactory

import scala.collection.Map

/**
  * Created by abdulrahman kaitoua on 25/05/15.
  */
object StoreGTFRD {
  private final val logger = LoggerFactory.getLogger(StoreGTFRD.getClass);
  private final val ENCODING = "UTF-8"

  @throws[SelectFormatException]
  def apply(executor: GMQLSparkExecutor, path: String, value: RegionOperator, associatedMeta:MetaOperator, schema : List[(String, PARSING_TYPE)], sc: SparkContext): RDD[GRECORD] = {
    val regions = executor.implement_rd(value, sc)
    val meta = executor.implement_md(associatedMeta,sc)

    val conf = new Configuration();
    val dfsPath = new org.apache.hadoop.fs.Path(path);
    val fs = FileSystem.get(dfsPath.toUri(), conf);

    val MetaOutputPath = path + "/meta/"
    val RegionOutputPath = path + "/exp/"

    logger.debug(MetaOutputPath)
    logger.debug(RegionOutputPath)
    logger.debug(regions.toDebugString)
    logger.debug(meta.toDebugString)

    val outputFolderName= try{
      new Path(path).getName
    }
    catch{
      case _:Throwable => path
    }

    val outSample = "S"

    val Ids = meta.keys.distinct()
    val newIDS: Map[Long, Long] = Ids.zipWithIndex().collectAsMap()
    val newIDSbroad = sc.broadcast(newIDS)

    val regionsPartitioner = new HashPartitioner(Ids.count.toInt)

    val keyedRDD = {
      val jobname = outputFolderName
      val score = schema.zipWithIndex.filter(x => x._1._1.toLowerCase().equals("score"))
      val source = schema.zipWithIndex.filter(x => x._1._1.toLowerCase().equals("source"))
      val feature = schema.zipWithIndex.filter(x => x._1._1.toLowerCase().equals("feature"))
      val frame = schema.zipWithIndex.filter(x => x._1._1.toLowerCase().equals("frame"))
      val scoreIndex = if (score.size > 0) score.head._2 else -1
      val sourceIndex = if (source.size > 0) source.head._2 else -1
      val featureIndex = if (feature.size > 0) feature.head._2 else -1
      val frameIndex = if (frame.size > 0) frame.head._2 else -1

      regions.map { x =>

        val values = schema.zip(x._2).flatMap { s =>
          if (s._1._1.equals("score")||s._1._1.equals("source")||s._1._1.equals("feature")||s._1._1.equals("frame")) None
          else Some(s._1._1 + " \"" + s._2 + "\";")
        }.mkString(" ")

        (outSample + "_" + "%05d".format(newIDSbroad.value.get(x._1._1).getOrElse(x._1._1)) + ".gtf",
          x._1._2 //chrom
            + "\t" + {if(sourceIndex >=0) x._2(sourceIndex).toString else "GMQL" }//variable name
            + "\t" + {if (featureIndex >=0) x._2(featureIndex) else  "Region"}
            + "\t" + x._1._3 + "\t" + x._1._4 + "\t" //start , stop
            + {
            if (scoreIndex >= 0) x._2(scoreIndex) else "0.0"
          } //score
            + "\t" + (if (x._1._5.equals('*')) '.' else x._1._5) + "\t" //strand
            + {if (frameIndex >=0) x._2(frameIndex) else  "."} //frame
            + "\t" + values
        )
      }.partitionBy(regionsPartitioner)
        .mapPartitions(x=>x.toList.sortBy{s=> val data = s._2.split("\t"); (data(0),data(3).toLong,data(4).toLong)}.iterator)
    }

    writeMultiOutputFiles.saveAsMultipleTextFiles(keyedRDD, RegionOutputPath)

    val metaKeyValue = {
      meta.map(x => (outSample+"_"+ "%05d".format(newIDSbroad.value.get(x._1).get) + ".gtf.meta", x._2._1 + "\t" + x._2._2)).repartition(1).sortBy(x=>(x._1,x._2))
    }
    writeMultiOutputFiles.saveAsMultipleTextFiles(metaKeyValue, MetaOutputPath)

    writeMultiOutputFiles.fixOutputMetaLocation(MetaOutputPath)

    regions
  }
}