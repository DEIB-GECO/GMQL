package it.polimi.genomics.spark.test

/**
  * Created by abdulrahman on 27/04/16.
  */


import it.polimi.genomics.GMQLServer.{DefaultMetaExtensionFactory, DefaultRegionExtensionFactory, GmqlServer}
import it.polimi.genomics.core.DataStructures.MetaAggregate._
import it.polimi.genomics.core.DataStructures.RegionAggregate.{REStringConstant, RegionExtension}
import it.polimi.genomics.core.DataStructures.RegionCondition.{MetaAccessor, Predicate, REG_OP}
import it.polimi.genomics.core.ParsingType.PARSING_TYPE
import it.polimi.genomics.core._
import it.polimi.genomics.spark.implementation.GMQLSparkExecutor
import it.polimi.genomics.spark.implementation.GMQLSparkExecutor.GMQL_DATASET
import it.polimi.genomics.spark.implementation.loaders.BedScoreParser
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}


/**
  * The entry point of the application
  * It initialize the server, call server's methods to build the query and invoke the server's run method to start the execution.
  */
object Project {

  def main(args : Array[String]) {

    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("hello")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");

    val sc = new SparkContext(conf)
    val server = new GmqlServer(new GMQLSparkExecutor(outputFormat = GMQLSchemaFormat.COLLECT,sc =sc))
    val mainPath = "/Users/abdulrahman/Ddownloads/"
    val ex_data_path = List(mainPath + "ann/")
    val ex_data_path_optional = List(mainPath + "exp/")
    val output_path = mainPath + "res/"

    val metaDS: RDD[(Long, (String, String))] = sc.parallelize((1 to 100).map(x=> ((x%2).toLong,("test","Abdo"))) :+ (1l,("score","20")) :+(0l,("score","500")))
    val regionDS = sc.parallelize((1 to 1000).map{x=>(new GRecordKey((x%2).toLong,"Chr"+(x%2),x.toLong,(x+200).toLong,'*'),Array[GValue](GDouble(1)) )})

    val dataAsTheyAre = server READ "" USING(metaDS,regionDS,List[(String, PARSING_TYPE)](("score",ParsingType.DOUBLE)))
    val optionalDS = server READ ex_data_path_optional USING BedScoreParser

//    val what = 4 // project MD (empty)
     val what = 1 // project MD and aggregate something
//     val what = 3 // project MD RD and extends tuple
    // val what = 2 // project MD RD and aggregate something


    val project =
      what match{
        case 0 => {
          //PROJECT MD
          dataAsTheyAre.PROJECT(projected_meta = Some(List("filename")),extended_meta = None,all_but_meta = false, all_but_reg = Some(List("score")), extended_values = None)
        }

        case 1 => {
          //PROJECT MD ATTRIBUTE AND AGGREGATE MD
//          val fun = new MetaExtension {
//            override val newAttributeName: String = "computed_result_C"
//            override val inputAttributeNames: List[String] = List("A","B")
//            override val fun: (Array[Traversable[(String,String)]]) => String =
//            //average of the double
//              (l : Array[Traversable[(String,String)]]) => {
//                val r =
//                  l(0)
//                    .map((a: (String, String)) => (a._2.toDouble * 2, 1))
//                    .reduce((a: (Double, Int), b: (Double, Int)) => (a._1 + b._1, a._2 + b._2))
//
//                (r._1 / r._2).toString
//              }
//          }

          dataAsTheyAre.PROJECT(Some(List("filename")), Some(List(DefaultMetaExtensionFactory.get(MEADD(MEName("ID"),MEFloat(3.0)), "_id1"), DefaultMetaExtensionFactory.get(MESUB(MEFloat(7.0),MEFloat(3.0)), "_id2"))), true, None)
        }

        case 2 => {
          //PROJECT MD/RD ATTRIBUTE AND AGGREGATE RD TUPLE
          val fun = new RegionExtension {
            override val inputIndexes: List[Int] = List(0,1)
            override val fun: (Array[GValue]) => GValue =
            //Concatenation of strand and value
              (l : Array[GValue]) => {
                l.reduce( (a : GValue, b : GValue) => GString ( a.asInstanceOf[GString].v + " " + b.asInstanceOf[GDouble].v.toString ) )
              }
          }

          val projectrd = dataAsTheyAre.PROJECT(Some(List("filename","A", "B", "C")), None, false, None)
          val projectrd2 = projectrd.PROJECT(None, None, false, None)
          projectrd2.SELECT(reg_con = Predicate(0, REG_OP.EQ, "+ 1000.0"))
        }

        case 3 => {
          //PROJECT AGGREGATE RD
          val fun = new RegionExtension {
            override val fun: (Array[GValue]) => GValue = {x=>if( x(1).isInstanceOf[GDouble]) GDouble(x(0).asInstanceOf[GDouble].v + x(1).asInstanceOf[GDouble].v)else GNull()}
            override val inputIndexes: List[Any] = List(0,MetaAccessor("score"))
          }

          dataAsTheyAre.PROJECT(None,extended_values = Some(List(fun)))
        }
        case 4 => {
          dataAsTheyAre.PROJECT(Some(List("a","b")),/*Some(List(DefaultMetaExtensionFactory.get(MEStringConstant("bla"), "test")))*/None,false,/*Some(List(0))*/None,/*Some(List("score"))*/None,Some(List(DefaultRegionExtensionFactory.get(REStringConstant("test"), Left("new")))))
        }

      }

    val projectVal = server.setOutputPath ("").COLLECT (project )

    projectVal.asInstanceOf[GMQL_DATASET]._1.foreach(x=>println(x._1,x._2.mkString("\t")))
    projectVal.asInstanceOf[GMQL_DATASET]._2.foreach(println)
//    server.run()
  }

}
