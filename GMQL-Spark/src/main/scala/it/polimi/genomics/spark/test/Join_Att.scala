package it.polimi.genomics.spark.test

import it.polimi.genomics.GMQLServer.GmqlServer
import it.polimi.genomics.core.DataStructures.JoinParametersRD._
import it.polimi.genomics.core.{GDouble, GRecordKey, GString, GValue}
import it.polimi.genomics.spark.implementation.GMQLSparkExecutor
import it.polimi.genomics.spark.implementation.loaders.test1Parser
import org.apache.spark.{SparkConf, SparkContext}

/**
  * The entry point of the application
  * It initialize the server, call server's methods to build the query and invoke the server's run method to start the execution.
  */
/**
  *
  */
object Join_Att {

  /**
    *
    * @param args
    */
  def main(args: Array[String]) {

    val timestamp: Long = System.currentTimeMillis / 1000

    val conf = new SparkConf()
      .setAppName("GMQL V2 Spark")
      //    .setSparkHome("/usr/local/Cellar/spark-1.5.2/")
      .setMaster("local[*]")
      //    .setMaster("yarn-client")
      //    .set("spark.executor.memory", "1g")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer").set("spark.kryoserializer.buffer", "64")
      .set("spark.driver.allowMultipleContexts", "true")
      .set("spark.sql.tungsten.enabled", "true")
    val sc: SparkContext = new SparkContext(conf)
    val server = new GmqlServer(new GMQLSparkExecutor(sc = sc))
    val mainPath = "/Users/abka02/Work/Polimi/IDEA/GMQLV2/GMQL-Flink/src/test/datasets/"
    val ex_data_path = List(mainPath + "join_att/ref/")
    val ex_data_path_optional = List(mainPath + "join_att/exp/")
    //    val ex_data_path = List(mainPath + "join/ref/")
    //    val ex_data_path_optional = List(mainPath + "join/exp/")
    val output_path = mainPath + "res2/" + timestamp + "/"


    val dataAsTheyAre = server READ ex_data_path USING test1Parser() // as alternative // new DelimiterSeparatedValuesParser('\t',0,1,2,Some(5),Some(Array(4)),None,Some(Array(3,8)))
    val optionalDS = (server READ ex_data_path_optional USING test1Parser()) //.PROJECT(projected_meta = Some(List("test","antibody")), distinct = Some(List(1,2)))


    println(Array[GValue](GDouble(0)) == Array[GValue](GDouble(5)))
    println(Array[GValue](GDouble(0)) == Array[GValue](GDouble(0)))
    println(Array[GValue](GDouble(0), GString("HH")) == Array[GValue](GDouble(0)))
    println(Array[GValue](GDouble(0), GString("HH")) == Array[GValue](GDouble(0), GString("HH")))
    println(Array[GValue](GDouble(0), GString("HH")) == Array[GValue](GDouble(0), GString("H")))
    println(GDouble(0) == GDouble(0))
    println(GDouble(0).asInstanceOf[GValue] == GDouble(0).asInstanceOf[GValue])
    println(Array(GDouble(0).asInstanceOf[GValue]) == Array(GDouble(0).asInstanceOf[GValue]))
    println(Array(0) == Array(1))
    println(Array(0) == Array(0))
    println("hey")
    implicit val order = Ordering.by { x: (GRecordKey, Array[GValue]) => x._1 == x._2 && x._2.deep == x._2.deep };
    val dd = sc.parallelize(Seq((new GRecordKey(0l, "ch1", 0l, 0l, '+'), Array[GValue](GDouble(0), GString("H"))), (new GRecordKey(0l, "ch1", 0l, 0l, '+'), Array[GValue](GDouble(0), GString("H"))), (new GRecordKey(0l, "ch1", 0l, 0l, '+'), Array[GValue](GDouble(0), GString("A")))))
    //    dd.map(x=>(x,null)).reduceByKey{(l,r)=> println("rr");l}
    //    dd.foreach(println)
    val ll = dd.groupBy(x => (x._1, x._2.deep))
    println(ll.count())
    //    ll.foreach(s=>s._2.foreach(m=>println("one",m._2.mkString("///"))))
    ll.flatMap { s =>
      val set = s._2.toList.sorted;
      var buf = set.head;
      if (set.size > 1) buf :: set.tail.flatMap(record => if (buf._2.deep == record._2.deep) None else {
        buf = record;
        Some(record)
      })
      else set
    }
      .foreach(s => println(s._1, s._2.mkString("\t")))


    val what = 16 // Join distLess


    val difference = what match {
      case 16 =>
        dataAsTheyAre.JOIN(None, List[JoinQuadruple](), RegionBuilder.BOTH, join_on_attributes = Some(List((0, 0))), right_dataset = optionalDS)


    }
    server setOutputPath output_path MATERIALIZE difference

    server.run()
  }

}

