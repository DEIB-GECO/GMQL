package it.polimi.genomics.spark.implementation.RegionsOperators

import java.io.{FileNotFoundException, File}
import java.nio.charset.StandardCharsets
import java.nio.file.Paths
import javax.xml.bind.JAXBException

import com.google.common.hash.Hashing
import it.polimi.genomics.core.DataStructures.RegionCondition.{MetaAccessor, REG_OP, RegionCondition}
import it.polimi.genomics.core.DataStructures.{MetaOperator, RegionCondition, RegionOperator}
import it.polimi.genomics.core.DataTypes._
import it.polimi.genomics.core.GMQLLoader
import it.polimi.genomics.core.exception.SelectFormatException
import it.polimi.genomics.repository.datasets.GMQLDataSetCollection
import it.polimi.genomics.repository.util.Utilities
import it.polimi.genomics.spark.implementation.GMQLSparkExecutor
import it.polimi.genomics.spark.implementation.loaders.{CustomParser, ANNParser, BedParser}
import org.apache.hadoop.fs.{PathFilter, Path}
import org.apache.lucene.store.FSDirectory
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import it.polimi.genomics.spark.implementation.loaders.Loaders._
import org.slf4j.LoggerFactory
import scala.collection.JavaConverters._
import scala.collection.mutable

/**
  * Created by Abdulrahman Kaitoua on 25/05/15.
  */
object SelectIRD {

  private final val logger = LoggerFactory.getLogger(this.getClass);
  var executor: GMQLSparkExecutor = null

  @throws[SelectFormatException]
  def apply(executor: GMQLSparkExecutor, regionCondition: Option[RegionCondition], filteredMeta: Option[MetaOperator], loader: GMQLLoader[Any, Any, Any, Any], URIs: List[String], repo: Option[String], sc: SparkContext): RDD[GRECORD] = {
    PredicateRD.executor = executor
    val optimized_reg_cond = if (regionCondition.isDefined) Some(PredicateRD.optimizeConditionTree(regionCondition.get, false, filteredMeta, sc))
    else {
      None
    }
    logger.info("----------------SelectIRD ")
    //    println("SelectRD, The dataset name : "+URIs(0))
    val files: List[String] = if (URIs.size == 1 && Utilities.getInstance().checkDSNameinRepo(Utilities.USERNAME, URIs(0))) {
      val username = if (Utilities.getInstance().checkDSNameinPublic(URIs(0))) "public" else Utilities.USERNAME
      var GMQLDSCol = new GMQLDataSetCollection();
      try {
        //          println("xml file : " + Utilities.getInstance().RepoDir + Utilities.USERNAME + "/datasets/" + URIs(0) + ".xml")
        GMQLDSCol = GMQLDSCol.parseGMQLDataSetCollection(Paths.get(Utilities.getInstance().RepoDir + username + "/datasets/" + URIs(0) + ".xml"));
        val dataset = GMQLDSCol.getDataSetList.get(0)
        dataset.getURLs.asScala.map { d =>
          //          println("inside SelectIRD", Utilities.getInstance().MODE)
          if (Utilities.getInstance().MODE.equals(Utilities.HDFS)) {
            val hdfs = Utilities.getInstance().gethdfsConfiguration().get("fs.defaultFS")
            //                        println("IRD hdfs file: " + hdfs.substring(0,hdfs.size) + Utilities.getInstance().HDFSRepoDir + Utilities.USERNAME + "/regions" + d.geturl)
            hdfs.substring(0, hdfs.size - 1) + Utilities.getInstance().HDFSRepoDir + username + "/regions" + d.geturl
          } else d.geturl
        }.toList
      } catch {
        case ex: JAXBException => logger.error("DataSet is corrupted"); List[String]()
        case ex: FileNotFoundException => logger.error("DataSet is not Found"); List[String]()
        case e: Exception => logger.error(e.getMessage); List[String]()
      }
    }
    else URIs.flatMap { dirInput =>
      val fs = Utilities.getInstance().getFileSystem
      if (new java.io.File(dirInput).isDirectory)
        new java.io.File(dirInput).listFiles.filter(!_.getName.endsWith(".meta")).map(x => x.getPath)
      else if (fs.exists(new Path(dirInput)) && URIs.size == 1) {
        fs.listStatus(new Path(dirInput), new PathFilter {
          override def accept(path: Path): Boolean = fs.exists(new Path(path.toString+".meta"))
        }).map(x => x.getPath.toString).toList;
      } else List(dirInput)
    }

    val inputURIs = files.map { x =>
      val uri = x.substring(x.indexOf(":") + 1, x.size).replaceAll("/", "");
      Hashing.md5().newHasher().putString(uri, StandardCharsets.UTF_8).hash().asLong() -> x
    }.toMap
        inputURIs.foreach(x=>logger.debug("HDFS File : "+x._1+"\t"+Hashing.md5().newHasher().putString(x._2.substring(x._2.indexOf(":")+1,x._2.size ).replaceAll("/",""),StandardCharsets.UTF_8).hash().asLong()+"\t"+x._2.substring(x._2.indexOf(":")+1,x._2.size ).replaceAll("/","")))
    val metaIdList = executor.implement_md(filteredMeta.get, sc).keys.distinct.collect
        metaIdList.foreach(x=> logger.debug("ID: "+ x))
    val selectedURIs = metaIdList.map(x => inputURIs.get(x).get)

    if (repo.isDefined) {
      //      logger.info ("Schema file ("+repo.get+")")
      import it.polimi.genomics.spark.implementation.loaders.CustomParser
      val parserObj = (new CustomParser).setSchema(repo.get)
      def parser(x: (Long, String)) = parserObj.region_parser(x)
      if(selectedURIs.size>0)
      sc forPath (selectedURIs.mkString(",")) LoadRegionsCombineFiles(parser, PredicateRD.applyRegionSelect, optimized_reg_cond) cache
      else {
        logger.warn("One input select is empty..")
        sc.emptyRDD[GRECORD]
      }
    } else {
      def parser(x: (Long, String)) = loader.asInstanceOf[GMQLLoader[(Long, String), Option[GRECORD], (Long, String), Option[MetaType]]].region_parser(x)
      if(selectedURIs.size>0)
        sc forPath (selectedURIs.mkString(",")) LoadRegionsCombineFiles(parser, PredicateRD.applyRegionSelect, optimized_reg_cond) cache
      else {
        logger.warn("One input select is empty..")
        sc.emptyRDD[GRECORD]
      }
    }
  }
}