package it.polimi.genomics.spark.test

import it.polimi.genomics.GMQLServer._
import it.polimi.genomics.core.DataStructures.GroupRDParameters.FIELD
import it.polimi.genomics.core.DataStructures.MetaGroupByCondition.MetaGroupByCondition
import it.polimi.genomics.core.DataStructures.MetaJoinCondition.Default
import it.polimi.genomics.core.GMQLSchemaFormat
import it.polimi.genomics.spark.implementation.GMQLSparkExecutor
import it.polimi.genomics.spark.implementation.loaders.BedScoreParser
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Olga Gorlova on 03.11.2017.
  */
object Group {

  def main(args : Array[String]) {

    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("hello")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");

    val sc = new SparkContext(conf)
    val server = new GmqlServer(new GMQLSparkExecutor(outputFormat = GMQLSchemaFormat.TAB,sc =sc))
    val mainPath = "/home/ogrlova/ds/"
    val ex_data_path = List(mainPath + "joinby_ref/")
    val ex_data_path_optional = List(mainPath + "joinby_exp/")
    val output_path = mainPath + "group_res/"


    val dataAsTheyAre = server READ ex_data_path USING BedScoreParser
    val optionalDS = server READ ex_data_path_optional USING BedScoreParser

    val what = 0 // group MD
    //     val what = 1 // group MD and aggregate something


    val group =
      what match{
        case 0 => {
          //GROUP MD
          optionalDS.GROUP(Some(MetaGroupByCondition(List(Default("bla")))), Some(List(DefaultMetaAggregateFactory.get("COUNTSAMP", Some("Count")),DefaultMetaAggregateFactory.get("BAG", "ID", Some("BAG_ID")))), "_group", None, None)
        }

        case 1 => {
          //GROUP RD
          val fun = DefaultRegionsToRegionFactory.get("Count", Some("count"))
          fun.output_name = Some("count")
          dataAsTheyAre.GROUP(None, None, "_group", Some(List(FIELD(0))),Some(List(fun)))
        }

      }

    server setOutputPath output_path MATERIALIZE group

    //    projectVal.asInstanceOf[GMQL_DATASET]._1.foreach(x=>println(x._1,x._2.mkString("\t")))
    //    projectVal.asInstanceOf[GMQL_DATASET]._2.foreach(println)
    server.run()
  }

}
