//package it.polimi.genomics.spark.implementation.MetaOperators.SelectMeta
//
///**
// * Created by Abdulrahman Kaitoua on 02/06/15.
// * Email: abdulrahman.kaitoua@polimi.it
// *
// */
//import java.io.{File, FileNotFoundException}
//import java.nio.charset.StandardCharsets
//import java.nio.file.Paths
//import javax.xml.bind.JAXBException
//
//import com.google.common.hash._
//import it.polimi.genomics.core.DataStructures.IRDataSet
//import it.polimi.genomics.core.DataStructures.MetadataCondition._
//import it.polimi.genomics.core.DataTypes._
//import it.polimi.genomics.core.ParsingType.PARSING_TYPE
//import it.polimi.genomics.core.exception.SelectFormatException
//import it.polimi.genomics.core.{DataTypes, GMQLLoader}
//import it.polimi.genomics.repository.FSRepository.Indexing.LuceneIndex._
//import it.polimi.genomics.repository.FSRepository.Indexing.SearchIndex
//import it.polimi.genomics.repository.FSRepository.LFSRepository
//import it.polimi.genomics.repository.{Utilities => General_Utilities}
//import it.polimi.genomics.repository.FSRepository.{ Utilities => FSR_Utilities}
//import it.polimi.genomics.spark.implementation.GMQLSparkExecutor
//import it.polimi.genomics.spark.implementation.loaders.Loaders._
//import org.apache.lucene.store.{Directory, FSDirectory}
//import org.apache.spark.SparkContext
//import org.apache.spark.rdd.RDD
//import org.slf4j.LoggerFactory
//
//import scala.collection.JavaConverters._
//object SelectIMD {
//  private final val logger = LoggerFactory.getLogger(SelectIMD.getClass);
//  var sparkContext :SparkContext = null
//  def apply(executor : GMQLSparkExecutor, metaCondition: MetadataCondition, URIs: List[String], loader : GMQLLoader[Any, Any, Any, Any], sc : SparkContext) : RDD[MetaType] = {
//
//    logger.info("----------------SELECTIMD executing..")
//    sparkContext = sc
//
//    var indexDir:String = null;
//    MetaSelectionIndex.username = General_Utilities().USERNAME
//    val repo = new LFSRepository()
//    val ds = IRDataSet(URIs(0),List[(String,PARSING_TYPE)]().asJava)
//    //check if there is a directory index, if not create index in memory
//    val files = try{
//      if(URIs.size == 1 && repo.DSExists(ds, General_Utilities().USERNAME)){
//        val username = if(repo.DSExistsInPublic(ds)) "public" else General_Utilities().USERNAME
//        MetaSelectionIndex.username = username
//        try {
//          val samples = repo.ListDSSamples( URIs(0),username);
//          MetaSelectionIndex.index = FSDirectory.open(new File(General_Utilities().getIndexDir(username)+URIs(0)))
//          samples.asScala.map { d =>
//          if (General_Utilities().MODE.equals(General_Utilities().HDFS)) {
//            val hdfs = FSR_Utilities.gethdfsConfiguration().get("fs.defaultFS")
//            hdfs.substring(0,hdfs.size) + General_Utilities().getHDFSRegionDir(username) + d.name
//          } else d.name
//        }
//        }catch {
//          case ex: JAXBException => logger.error("DataSet is corrupted"); List[String]()
//          case ex: FileNotFoundException => logger.error("DataSet is not Found"); List[String]()
//          case e:Exception=> logger.error(e.getMessage); List[String]()
//        }
//      } else {
//
//        val res = URIs.flatMap{dirInput =>
//          if(new java.io.File(dirInput).isDirectory&&URIs.size == 1)
//            new java.io.File(dirInput).listFiles.filter(!_.getName.endsWith(".meta")).map(x=>x.getPath)
//          else if(new java.io.File(dirInput).isDirectory) None
//          else List(dirInput)
//        }
//        MetaSelectionIndex.index = MetaSelectionIndex.buildIndex(res.map(x=>x+".meta").toArray)
//        res
//      }}catch {
//      case e:Exception => {
//        logger.error(e.getMessage)
//        val res = URIs.flatMap{dirInput =>
//          if(new java.io.File(dirInput).isDirectory&&URIs.size == 1)
//            new java.io.File(dirInput).listFiles.filter(!_.getName.endsWith(".meta")).map(x=>x.getPath)
//          else if(new java.io.File(dirInput).isDirectory) None
//          else List(dirInput)
//        }
//        MetaSelectionIndex.index = MetaSelectionIndex.buildIndex(res.map(x=>x+".meta").toArray)
//        res
//      }
//    }
//
//    // lazly read meta files for operations like greater than and less than, Cache the read for another predicates
//    def parser(x: (Long, String)) = loader.asInstanceOf[GMQLLoader[(Long, String),Option[DataTypes.GRECORD], (Long, String), Option[DataTypes.MetaType]]].meta_parser(x)
//    //println(files.map(x=>x+".meta").mkString(","))
//    val input = sc forPath(files.map(x=>x+".meta").mkString(",")) LoadMetaCombineFiles (parser) cache
//
//    // join the result of the selection with the input
//    val ids = sc.broadcast(MetaSelectionIndex.applyMetaSelect(metaCondition, input).collect)
//   // println(ids.value.size)
//    //ids.value.foreach(x=>println ("IDs: ",x ))
//    input.flatMap(x=>if(ids.value.contains(x._1)) Some(x) else None).cache()
//
//  }
//
//  object MetaSelectionIndex extends MetaSelection
//  {
//    var username =""
//    var index: Directory = null
//    override def applyContainAttribute(name : String, input : RDD[MetaType]) : RDD[ID] = {
//      val query = name+"_*"
//
//      val searchRes = searchIndex(query, index)
//      if(searchRes.isDefined)
//        sparkContext.parallelize(searchRes.get.split(",").map(x=> Hashing.md5().newHasher().putString(x.replaceAll("/",""),StandardCharsets.UTF_8).hash().asLong()))
//      else
//        sparkContext.emptyRDD[ID]
//    }
//    @throws[SelectFormatException]
//    override def applyMetaPredicateEQ(predicate: Predicate, input: RDD[MetaType]): RDD[ID] = {
//      val query = predicate.attribute_name+"_"+predicate.value
//
//      val searchRes = searchIndex(query, index)
//      if(searchRes.isDefined){
//        sparkContext.parallelize{ searchRes.get.split(",").flatMap{x=> /*println ("eq",x,getURI(x),Hashing.md5().newHasher().putString(getURI(x),StandardCharsets.UTF_8).hash().asLong());*/
//            if(!x.isEmpty)Some(Hashing.md5().newHasher().putString(getURI(x).replaceAll("/",""),StandardCharsets.UTF_8).hash().asLong()) else None}}
//      }else
//        sparkContext.emptyRDD[ID]
//    }
//
//    def getURI (uri:String) ={
//      if(General_Utilities().MODE == General_Utilities().HDFS){
//        val hdfsuri = FSR_Utilities.gethdfsConfiguration().get("fs.defaultFS")+General_Utilities().getHDFSRegionDir(username)+uri
//        hdfsuri.substring(hdfsuri.indexOf(":")+1,hdfsuri.size )
//      } else uri
//    }
//    @throws[SelectFormatException]
//    override def applyMetaPredicateNOTEQ(predicate: Predicate, input: RDD[MetaType]): RDD[ID] = {
//      val query = predicate.attribute_name+"* AND NOT "+predicate.attribute_name+"_"+predicate.value
//
//      val searchRes = searchIndex(query, index)
//      if(searchRes.isDefined)
//        sparkContext.parallelize(searchRes.get.split(",").map(x=>  Hashing.md5().newHasher().putString(getURI(x).replaceAll("/",""),StandardCharsets.UTF_8).hash().asLong()))
//      else
//        sparkContext.emptyRDD[ID]
//    }
//
//    /**
//     * Build Index directory of the input files
//     * First we have to scan all the files and build in memory index (we can have the index on HDD already built)
//     *
//     * @param paths array of URIs to the meta files locations (with the file name)
//     * @return Directory Index of the meta files
//     */
//    def buildIndex(uri: Array[String]): Directory = {
//      var dir = buildInMemIndex(new File(uri(0)), 1, null, false);
//      if (uri.length > 1)
//        for (url <- uri.slice(1, uri.length)) {
//          dir = buildInMemIndex(new File(url), 1, dir, false);
//        }
//      dir
//    }
//
//    def searchIndex(query: String, dir: Directory): Option[String] = {
//      val search = new SearchIndex("/home/abdulrahman/gmql_repository/", "/user/",
//        "abdulrahman", "LOCAL", ""); // when it is local that means do not consider any of the input directories except the file meta dir
//      // LuceneIndex.printIndex(dir)
//      val res =  search.SearchLuceneIndex(query, "abdulrahman", dir)
//      if(res ==null)
//        None;
//      else
//        Some(res);
//    }
//  }
//
//}
//
