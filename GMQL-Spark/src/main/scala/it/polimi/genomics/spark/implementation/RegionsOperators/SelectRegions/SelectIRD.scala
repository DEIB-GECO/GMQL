package it.polimi.genomics.spark.implementation.RegionsOperators

import java.nio.charset.StandardCharsets

import com.google.common.hash.Hashing
import it.polimi.genomics.core.DataStructures.MetaOperator
import it.polimi.genomics.core.DataStructures.RegionCondition.RegionCondition
import it.polimi.genomics.core.DataTypes._
import it.polimi.genomics.core.Debug.EPDAG
import it.polimi.genomics.core.GMQLLoader
import it.polimi.genomics.core.exception.SelectFormatException
import it.polimi.genomics.spark.implementation.GMQLSparkExecutor
//import it.polimi.genomics.repository.{Utilities => General_Utilities}
//import it.polimi.genomics.repository.FSRepository.{LFSRepository, Utilities => FSR_Utilities}
import org.apache.hadoop.fs.{FileSystem, Path, PathFilter}
//import org.apache.lucene.store.FSDirectory
import it.polimi.genomics.spark.implementation.loaders.Loaders._
import org.apache.hadoop.conf.Configuration
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.slf4j.LoggerFactory

/**
  * Created by Abdulrahman Kaitoua on 25/05/15.
  */
object SelectIRD {

  private final val logger = LoggerFactory.getLogger(this.getClass);
  var executor: GMQLSparkExecutor = null

  @throws[SelectFormatException]
  def apply(executor: GMQLSparkExecutor, regionCondition: Option[RegionCondition], filteredMeta: Option[MetaOperator], loader: GMQLLoader[Any, Any, Any, Any], URIs: List[String], repo: Option[String], sc: SparkContext): (Float, RDD[GRECORD]) = {
    PredicateRD.executor = executor
    val optimized_reg_cond = if (regionCondition.isDefined) Some(PredicateRD.optimizeConditionTree(regionCondition.get, false, filteredMeta, sc))
    else {
      None
    }
    logger.info("----------------SelectIRD ")

    val conf = new Configuration();
    val path = new org.apache.hadoop.fs.Path(URIs.head);
    val fs = FileSystem.get(path.toUri(), conf);

    val metaIdList = executor.implement_md(filteredMeta.get, sc)._2.keys.distinct.collect

    val startTime: Float = EPDAG.getCurrentTime


    val files: List[String] =

      URIs.flatMap { dirInput =>
        val uri = new Path(dirInput)
        if (fs.isDirectory(uri)) {
          fs.listStatus(new Path(dirInput), new PathFilter {
            override def accept(path: Path): Boolean = fs.exists(new Path(path.toString + ".meta"))
          }).map(x => x.getPath.toString).toList;
        } else if (fs.exists(uri)) List(dirInput)
        else None
      }


    val inputURIs = files.map { x =>
      val uri = x //.substring(x.indexOf(":") + 1, x.size).replaceAll("/", "");
      Hashing.md5().newHasher().putString(new Path(uri).getName, StandardCharsets.UTF_8).hash().asLong() -> x
    }.toMap




    val selectedURIs = metaIdList.map(x => inputURIs.get(x).get)

    def parser(x: (Long, String)) = loader.asInstanceOf[GMQLLoader[(Long, String), Option[GRECORD], (Long, String), Option[MetaType]]].region_parser(x)

    if (selectedURIs.size > 0) {
      val res = sc forPath (selectedURIs.mkString(",")) LoadRegionsCombineFiles(parser, PredicateRD.applyRegionSelect, optimized_reg_cond) cache;
      (startTime,res)
    } else {
      logger.warn("One input select is empty..")
      (startTime, sc.emptyRDD[GRECORD])
    }
  }
}
