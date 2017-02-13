package it.polimi.genomics.spark.implementation.MetaOperators.SelectMeta

/**
  * Created by Abdulrahman Kaitoua on 02/06/15.
  * Email: abdulrahman.kaitoua@polimi.it
  *
  */

import java.io.{File}
import java.nio.charset.StandardCharsets

import com.google.common.hash._
import it.polimi.genomics.core.DataStructures.MetadataCondition._
import it.polimi.genomics.core.DataTypes._
import it.polimi.genomics.core.exception.SelectFormatException
import it.polimi.genomics.core.{DataTypes, GMQLLoader}
import it.polimi.genomics.spark.implementation.GMQLSparkExecutor
import it.polimi.genomics.spark.implementation.MetaOperators.SelectMeta.SelectMD.metaSelection
import it.polimi.genomics.spark.implementation.loaders.Loaders._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path, PathFilter}
//import org.apache.lucene.store.{Directory}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.slf4j.LoggerFactory

object SelectIMDWithNoIndex {
  private final val logger = LoggerFactory.getLogger(this.getClass);
  var sparkContext: SparkContext = null


  def apply(executor: GMQLSparkExecutor, metaCondition: MetadataCondition, URIs: List[String], loader: GMQLLoader[Any, Any, Any, Any], sc: SparkContext): RDD[MetaType] = {
    logger.info("----------------SELECTIMDWithNoIndex  executing..")
    sparkContext = sc
    logger.debug("root", URIs(0))
    var indexDir: String = null;
    var indexing = false //          indexing = true  // true to ENABLE INDEXING SEARCH
    //          MetaSelectionIndex.index = FSDirectory.open(new File(dataset.getIndexURI))  // to ENABLE INDEXING SEARCH

    val conf = new Configuration();
    val path = new org.apache.hadoop.fs.Path(URIs.head);
    val fs = FileSystem.get(path.toUri(), conf);

    //check if there is a directory index, if not create index in memory
    val files =
//      try {
        URIs.flatMap { dirInput =>
          val file = new Path(dirInput)
          if (fs.isDirectory(file))
          {
            fs.listStatus(new Path(dirInput), new PathFilter {
              override def accept(path: Path): Boolean = {
//                logger.debug(path.toString + ".meta : " + fs.exists(new Path(path.toString + ".meta")))
                fs.exists(new Path(path.toString + ".meta"))
              }
            }).map(x => x.getPath.toString).toList
          }
          else
            List(dirInput)
        }
        //        MetaSelectionIndex.index = MetaSelectionIndex.buildIndex(res.map(x=>x+".meta").toArray)
//    } catch {
//      case e: Exception => {
//        indexing = false
//        logger.error(e.getMessage)
//        val res = URIs.flatMap { dirInput =>
//          if (new java.io.File(dirInput).isDirectory && URIs.size == 1)
//            new java.io.File(dirInput).listFiles.filter(!_.getName.endsWith(".meta")).map(x => x.getPath)
//          else if (new java.io.File(dirInput).isDirectory) None
//          else List(dirInput)
//        }
//        //        MetaSelectionIndex.index = MetaSelectionIndex.buildIndex(res.map(x=>x+".meta").toArray)
//        res
//      }
//    }

    // lazly read meta files for operations like greater than and less than, Cache the read for another predicates
    def parser(x: (Long, String)) = loader.asInstanceOf[GMQLLoader[(Long, String), Option[DataTypes.GRECORD], (Long, String), Option[DataTypes.MetaType]]].meta_parser(x)

    val input = sc forPath (files.map(x => x + ".meta").mkString(",")) LoadMetaCombineFiles (parser) cache

    val ids = sc.broadcast(if (indexing) MetaSelectionIndex.applyMetaSelect(metaCondition, input).collect else metaSelection.applyMetaSelect(metaCondition, input).collect)

    logger.info(new File(URIs(0)).getName + " Selected: " + ids.value.size)
    input.flatMap(x => if (ids.value.contains(x._1)) Some(x) else None).cache()
  }

  object MetaSelectionIndex extends MetaSelection {
    var username = ""
//    var index: Directory = null

    override def applyContainAttribute(name: String, input: RDD[MetaType]): RDD[ID] = {
      val query = name + "_*"

      val searchRes = searchIndex(query/*, index*/)
      if (searchRes.isDefined)
        sparkContext.parallelize(searchRes.get.split(",").map(x => Hashing.md5().newHasher().putString(x.replaceAll("/", ""), StandardCharsets.UTF_8).hash().asLong()))
      else
        sparkContext.emptyRDD[ID]
    }

    @throws[SelectFormatException]
    override def applyMetaPredicateEQ(predicate: Predicate, input: RDD[MetaType]): RDD[ID] = {
      val query = predicate.attribute_name + "_" + predicate.value

      val searchRes = searchIndex(query/*, index*/)
      if (searchRes.isDefined) {
        sparkContext.parallelize {
          searchRes.get.split(",").flatMap { x => /*println ("eq",x,getURI(x),Hashing.md5().newHasher().putString(getURI(x),StandardCharsets.UTF_8).hash().asLong());*/
            if (!x.isEmpty) Some(Hashing.md5().newHasher().putString(getURI(x).replaceAll("/", ""), StandardCharsets.UTF_8).hash().asLong()) else None
          }
        }
      } else
        sparkContext.emptyRDD[ID]
    }

    def getURI(uri: String) = {
      if (/*General_Utilities().MODE == General_Utilities().HDFS && !*/uri.startsWith("hdfs")) {
//        val hdfsuri = Utilities.gethdfsConfiguration().get("fs.defaultFS") + General_Utilities().getHDFSRegionDir(username) + uri
        /*hdfs*/uri.substring(/*hdfs*/uri.indexOf(":") + 1, /*hdfs*/uri.size)
      } else uri
    }

    @throws[SelectFormatException]
    override def applyMetaPredicateNOTEQ(predicate: Predicate, input: RDD[MetaType]): RDD[ID] = {
      val query = predicate.attribute_name + "* AND NOT " + predicate.attribute_name + "_" + predicate.value

      val searchRes = searchIndex(query/*, index*/)
      if (searchRes.isDefined)
        sparkContext.parallelize(searchRes.get.split(",").map(x => Hashing.md5().newHasher().putString(getURI(x).replaceAll("/", ""), StandardCharsets.UTF_8).hash().asLong()))
      else
        sparkContext.emptyRDD[ID]
    }

    /**
      * Build Index directory of the input files
      * First we have to scan all the files and build in memory index (we can have the index on HDD already built)
      *
      * @param paths array of URIs to the meta files locations (with the file name)
      * @return Directory Index of the meta files
      */
//    def buildIndex(uri: Array[String]): Directory = {
//      var dir = buildInMemIndex(new File(uri(0)), 1, null, false);
//      if (uri.length > 1)
//        for (url <- uri.slice(1, uri.length)) {
//          dir = buildInMemIndex(new File(url), 1, dir, false);
//        }
//      dir
//    }

    def searchIndex(query: String/*, dir: Directory*/): Option[String] = {
//      val search = new SearchIndex("/home/abdulrahman/gmql_repository/", "/user/",
//        "abdulrahman", "LOCAL", "");
      // when it is local that means do not consider any of the input directories except the file meta dir
      // LuceneIndex.printIndex(dir)
      val res = null//search.SearchLuceneIndex(query.replace(' ', '_').replaceAll("\\W+", "_"), "abdulrahman", dir)
      if (res == null)
        None;
      else
        Some(res);
    }
  }

}

