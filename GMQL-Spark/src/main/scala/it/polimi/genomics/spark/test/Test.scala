package it.polimi.genomics.spark.test

import it.polimi.genomics.GMQLServer.GmqlServer
import it.polimi.genomics.core.DataStructures.RegionAggregate.RegionsToRegion
import it.polimi.genomics.core.ParsingType._
import it.polimi.genomics.core.{DataStructures, GDouble, GValue, ParsingType}
import it.polimi.genomics.spark.implementation.GMQLSparkExecutor
import it.polimi.genomics.spark.implementation.loaders.{ANNParser, BedParser}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by Abdulrahman Kaitoua on 24/05/15.
 * Email: abdulrahman.kaitoua@polimi.it
 *
 */
object Test {
  def main(args: Array[String]) {
    import java.text.SimpleDateFormat
    import java.util.Date
    val formatter = new SimpleDateFormat("HH:mm:ss:SSS");
    //    //
//        val conf = new SparkConf().setAppName("GMQL V2 spark").setMaster("local[3]").set("spark.executor.memory", "2g")
//        val sc = new SparkContext(conf)
//        conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer").set("spark.kryoserializer.buffer.mb", "24")

    val conf = new SparkConf()
      .setAppName("GMQL V2 Spark")
      //    .setSparkHome("/usr/local/Cellar/spark-1.5.2/")
      .setMaster("local[*]")
      //    .setMaster("yarn-client")
      //    .set("spark.executor.memory", "1g")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer").set("spark.kryoserializer.buffer", "64")
      .set("spark.driver.allowMultipleContexts","true")
      .set("spark.sql.tungsten.enabled", "true")
    val sc:SparkContext =new SparkContext(conf)
    val runner = new GMQLSparkExecutor(sc=sc)
    val server = new GmqlServer(runner)

    //    val input  = "/home/abdulrahman/V2Spark_TestFiles/Samples/"
//    val input = "/home/abdulrahman/V2Spark_TestFiles/Samples/meta/exp.narrowPeak,/home/abdulrahman/V2Spark_TestFiles/Samples/meta/exp1.narrowPeak"
    val output = "/home/abdulrahman/SparkOutput/V2DAG"+formatter.format(new Date(System.currentTimeMillis()))+"/"

//    val s = server READ input USING BedParser()
    //

    //    server setOutputPath "/home/abdulrahman/SparkOutput/outooo" MATERIALIZE s


    val meta_con =
      DataStructures.MetadataCondition.AND(
        DataStructures.MetadataCondition.Predicate("cell",DataStructures.MetadataCondition.META_OP.GTE, "11"),
        DataStructures.MetadataCondition.NOT(
          DataStructures.MetadataCondition.Predicate("provider", DataStructures.MetadataCondition.META_OP.NOTEQ, "UCSC"
          )
        )
      )
    val   reg_con =
    //        DataStructures.RegionCondition.OR(
    //          DataStructures.RegionCondition.Predicate(3, DataStructures.RegionCondition.REG_OP.GT, 30),
    //DataStructures.RegionCondition.Predicate(0, DataStructures.RegionCondition.REG_OP.EQ, "400")
      DataStructures.RegionCondition.Predicate(2, DataStructures.RegionCondition.REG_OP.GTE, DataStructures.RegionCondition.MetaAccessor("cell"))
    //        )
    val URIs = Array("/home/abdulrahman/V2Spark_TestFiles/Samples/meta/exp.narrowPeak.meta","/home/abdulrahman/V2Spark_TestFiles/Samples/meta/exp1.narrowPeak.meta")

    //        val s = SelectIMD(runner, meta_con, URIs.mkString(","), BedParser(),runner.sc)
    //       val variable =  new IRVariable(IRSelectIMD(meta_con,URIs),null)
    //          s.saveAsTextFile(output+"Meta")
    //        println (s.toDebugString)
    //        val r = SelectIRD(runner,reg_con,Some(variable.metaDag),URIs.mkString(","),runner.sc)
    //        r.saveAsTextFile(output+"exp")
    //    println (r.toDebugString)
    //    val new_meta_dag =  it.polimi.genomics.spark.implementation.MetaOperators.SelectMD(runner,meta_con, s.metaDag,runner.sc)
    //    new_meta_dag.saveAsTextFile("/home/abdulrahman/mapo/")
    //    println (new_meta_dag.toDebugString)


    /*
        //META SELECTION
        val input = "/home/abdulrahman/V2Spark_TestFiles/Samples/meta/exp.narrowPeak,/home/abdulrahman/V2Spark_TestFiles/Samples/meta/exp1.narrowPeak"
        val s = server READ input USING BedParser()
        val metaSelection = s.SELECT(
            meta_con =
              DataStructures.MetadataCondition.AND(
                DataStructures.MetadataCondition.Predicate("cell",DataStructures.MetadataCondition.META_OP.GTE, "11"),
                DataStructures.MetadataCondition.NOT(
                  DataStructures.MetadataCondition.Predicate("provider", DataStructures.MetadataCondition.META_OP.EQ, "UCSC")
                )
              )
          )

          server setOutputPath output MATERIALIZE metaSelection
          */
// Genomitric Map
    /*val dirInput = "/home/abdulrahman/data/narrow/"
    var files =
    if(new java.io.File(dirInput).isDirectory) {
      new java.io.File(dirInput).listFiles.filter(!_.getName.endsWith(".meta")).mkString(",")
    } else ""
    //MAP
    val input = files//"/home/abdulrahman/V2Spark_TestFiles/Samples/meta/exp.narrowPeak,/home/abdulrahman/V2Spark_TestFiles/Samples/meta/exp1.narrowPeak"
    */
    val input = "/home/abdulrahman/V2Spark_TestFiles/Samples/meta/exp.narrowPeak,/home/abdulrahman/V2Spark_TestFiles/Samples/meta/exp1.narrowPeak"
    val s = server READ input.split(",").toList USING BedParser
//    val refin = files//"/home/abdulrahman/V2Spark_TestFiles/Ref/exp/genes.bed,/home/abdulrahman/V2Spark_TestFiles/Ref/exp/genes1.bed"
    val refin = "/home/abdulrahman/V2Spark_TestFiles/Ref/exp/genes.bed,/home/abdulrahman/V2Spark_TestFiles/Ref/exp/genes1.bed"
    val ref = server READ refin.split(",").toList USING ANNParser
    val map = ref.MAP(None,
      List(new RegionsToRegion {
        override val index: Int = 2
        override val associative: Boolean = true
        override val funOut: (GValue,(Int, Int)) => GValue = {(v1,v2)=>v1}
        override val fun: (List[GValue]) => GValue = {
          l => println ("The values are: "+l.mkString("\t"));if(l.size>0)GDouble(l.map(g => g.asInstanceOf[GDouble].v).reduce(_ + _)) else GDouble(0)
        }
        override val resType: PARSING_TYPE = ParsingType.DOUBLE
      }),
      s)

    server setOutputPath output MATERIALIZE map


    /*
    //MAP
    val map = dataAsTheyAre.MAP(
      List(),
      s)

    server setOutputPath output_path MATERIALIZE map
    */






/*
    // group meta
    val atts = List("cell","sex","type")
    val dataset = sc forPath("./GMQL-Spark/src/main/resources/MetaGroupByAtt/exp1.meta,./GMQL-Spark/src/main/resources/MetaGroupByAtt/exp2.meta,./GMQL-Spark/src/main/resources/MetaGroupByAtt/exp3.meta") LoadMetaCombineFiles (BedParser().meta_parser _) cache
    val datasetas = sc forPath("./GMQL-Spark/src/main/resources/MetaGroupByAtt/exp1,./GMQL-Spark/src/main/resources/MetaGroupByAtt/exp2") LoadRegionsCombineFiles (BedParser().region_parser _) cache
    val outer = dataset.groupByKey().flatMap { x =>

        def splatter(grid : List[(String, Iterable[String])]) : String = {
          @tailrec
          def splatterHelper(grid : List[(String, Iterable[String])], acc : List[String]) : String = {
            val sb = new StringBuilder
            grid.size match{
              //returns a list for compatibility with previous splatter (see below) that allowed different grouping methods
              case 0 => acc.tail.mkString("#")
              case _ => splatterHelper(
                grid.tail,
                acc :+ grid.head._2.mkString("§")
              )
            }
          }
          splatterHelper(grid, List[String](""))
        }

        def distributor(l1 : Iterable[String], l2 : Iterable[String]) : Iterable[String] = {
          l1.flatMap(s => l2.map(_ + s))
        }

        val itr = x._2.filter { case (att: String, value: String) => atts.contains(att)};
        if(!itr.iterator.hasNext) None
        else Some((x._1, Hashing.murmur3_128().newHasher().putString(
            splatter(itr.groupBy(r=>r._1).map(d=>(d._1,d._2.map(e=>e._2))).toList.sortBy(_._1))
            ,StandardCharsets.UTF_8).hash().asLong )
        )
      }
    outer.collect.foreach(println _)
*/


    /*
        // join meta with left Join on references
        val atts = List("cell","sex")
        val expMeta = sc forPath("./GMQL-Spark/src/main/resources/MetaGroupByAtt/exp1.meta,./GMQL-Spark/src/main/resources/MetaGroupByAtt/exp2.meta,./GMQL-Spark/src/main/resources/MetaGroupByAtt/exp3.meta") LoadMetaCombineFiles (BedParser().meta_parser _) cache
        val refMeta = sc forPath("./GMQL-Spark/src/main/resources/Ref/ref1.meta,./GMQL-Spark/src/main/resources/Ref/ref2.meta,./GMQL-Spark/src/main/resources/Ref/ref3.meta") LoadMetaCombineFiles (BedParser().meta_parser _) cache
        val exp = sc forPath("./GMQL-Spark/src/main/resources/Ref/exp1,./GMQL-Spark/src/main/resources/MetaGroupByAtt/exp2") LoadRegionsCombineFiles (BedParser().region_parser _) cache
        val rdd: RDD[(Long,( String, String))] =
          refMeta
            .union(
              expMeta
            )
            .filter((v : MetaType) => atts.contains(v._2._1))


        import it.polimi.genomics.spark.implementation.MetaOperators.GroupOperator.MetaGroupMGD._
        val sampleWithGroup: RDD[(Long, Long)] =
          refMeta.MetaWithGroups(atts).flatMap(x=>x._2.map(s=>(s,x._1))) // Key:group, value:sample
        val sampleWithGroup1: RDD[(Long, Long)] =
          expMeta.MetaWithGroups(atts).flatMap(x=>x._2.map(s=>(s,x._1)))// Key:group, value:sample
        //(sampleID, groupID)


        val outer: RDD[(Long, Option[Long])] =
          sampleWithGroup
            .leftOuterJoin(sampleWithGroup1).flatMap(x=>
            x._2._2 match {
              case Some(r) => if(x._2._1 == r) None else Some( (x._2._1,Some(r)));
              case None => Some(x._2._1,None)
            }
            )
            .distinct(1)(new Ordering[(Long,Option[Long])](){ def compare(left:(Long,Option[Long]),right:(Long,Option[Long]))={(left._1+left._2.getOrElse(0l)) compare (right._1+right._2.getOrElse(0l))} })

    //    println (outer.toDebugString)
        outer.collect.foreach(println _)
    */

    //REGION SELECTION
    //    val regionSelection = s.SELECT(

    //    )
    //    server setOutputPath output MATERIALIZE regionSelection

    server.run()
  }
}
