package it.polimi.genomics.spark.test

//import it.polimi.genomics.GMQLServer.GmqlServer

/**
 * The entry point of the application
 * It initialize the server, call server's methods to build the query and invoke the server's run method to start the execution.
 */
object JoinAmazon {

  def main(args : Array[String]) {
    /*
        val env = ExecutionEnvironment.getExecutionEnvironment
        env.readTextFile("hdfs://127.0.0.1:9000/user/bert/exp/").print
        env.execute("prova")
    */

    for (i <- args){
      println(i)
    }

//    val server = new GmqlServer(new GMQLSparkExecutor)

    /*
        val ex_data_path = "s3://genomic.s3-us-west-2.amazonaws.com/flink/ref/"
        val ex_data_path_optional = "s3://genomic.s3-us-west-2.amazonaws.com/flink/exp/"
        val output_path = "s3://genomic.s3-us-west-2.amazonaws.com/flink/res/"
    */
    /*
        val ex_data_path = "hdfs://172.31.32.33:9000/user/ref3/"
        val ex_data_path_optional = "hdfs://172.31.32.33:9000/user/exp3/"
        val output_path = "hdfs://172.31.32.33:9000/user/res/"
    */

    //val mainPath = "/Users/michelebertoni/Workspaces/IdeaProjects/genomic_computing/GMQLv2Scala/scalav2/trunk/GMQL-Flink/src/test/datasets/join2/"

    val mainPath = "hdfs://172.31.10.183:9000/" + args(0) + "/"
    //val ex_data_path = "hdfs://127.0.0.1:9000/user/Inputs/flinkInput/"
    val ex_data_path = List(mainPath + "ref/exp/*")
    val ex_data_path_optional = List(mainPath + "exp/exp/*")
    val output_path = mainPath + "res/"

/*

    val dataAsTheyAre = server READ ex_data_path USING BedScoreParser()
    val optionalDS = server READ ex_data_path_optional USING BedScoreParser()

    val choice = args(1).toInt
    val join = choice match{

      case 0 =>
        dataAsTheyAre.JOIN(None, List(new JoinQuadruple(Some(DistLess(100000)))), RegionBuilder.CONTIG, optionalDS)

      case 1 =>
        dataAsTheyAre.JOIN(None, List(new JoinQuadruple(Some(DistLess(50000)), Some(new MinDistance(2)))), RegionBuilder.CONTIG, optionalDS)

      case 2 =>
        dataAsTheyAre.JOIN(None, List(new JoinQuadruple(Some(DistGreater(50000)), Some(new Upstream()), (Some(DistLess(100000))), Some(new MinDistance(1)))), RegionBuilder.CONTIG, optionalDS)

      case 3 =>
        dataAsTheyAre.JOIN(None, List(new JoinQuadruple(Some(DistGreater(50000)), Some(new Upstream()), (Some(DistLess(100000))))), RegionBuilder.CONTIG, optionalDS)

      case 4 =>
        dataAsTheyAre.JOIN(None, List(new JoinQuadruple(Some(DistLess(200000)), Some(new MinDistance(1)), Some(new Upstream))), RegionBuilder.CONTIG, optionalDS)

      case 5 =>
        dataAsTheyAre.JOIN(None, List(new JoinQuadruple(Some(new MinDistance(1)))), RegionBuilder.CONTIG, optionalDS)

      case 6 =>
        dataAsTheyAre.JOIN(None, List(new JoinQuadruple(Some(new MinDistance(1)), Some(new DownStream), Some(new DistGreater(100)))), RegionBuilder.CONTIG, optionalDS)

      case 7 =>
        dataAsTheyAre.JOIN(None, List(new JoinQuadruple(Some(DistLess(200000)), Some(new MinDistance(2)), Some(DistGreater(50000)))), RegionBuilder.CONTIG, optionalDS)

    }
    server setOutputPath output_path MATERIALIZE join

*/





    //READ
    //server setOutputPath output_path MATERIALIZE dataAsTheyAre




    //META SELECTION
    /*
    val metaSelection = dataAsTheyAre.SELECT(
        meta_con =
            Predicate("cell", META_OP.EQ, "1")
      )
    server setOutputPath output_path MATERIALIZE metaSelection
    */


/*
    //REGION SELECTION
    val regionSelection =
      dataAsTheyAre.SELECT(
        reg_con =
            Predicate(0, REG_OP.GTE, 90)
      )
    server setOutputPath output_path MATERIALIZE regionSelection
*/

    /*
    //REGION SELECTION
    val regionSelection = dataAsTheyAre.SELECT(
      reg_con =
        DataStructures.RegionCondition.AND(
          DataStructures.RegionCondition.Predicate(1, DataStructures.RegionCondition.REG_OP.EQ, "179,45,0"),
          //DataStructures.RegionCondition.Predicate(0, DataStructures.RegionCondition.REG_OP.EQ, "400")
          DataStructures.RegionCondition.Predicate(0, DataStructures.RegionCondition.REG_OP.EQ, DataStructures.RegionCondition.MetaAccessor("bert_value_meta"))
        )
      )
    server setOutputPath output_path MATERIALIZE regionSelection
    */

    /*
    //REGION META SELECTION
    val selection = dataAsTheyAre.SELECT(
      meta_con =
        DataStructures.MetadataCondition.AND(
          DataStructures.MetadataCondition.Predicate("bert_value1",DataStructures.MetadataCondition.META_OP.GTE, "11"),
          DataStructures.MetadataCondition.NOT(
            DataStructures.MetadataCondition.Predicate("bert_value2", DataStructures.MetadataCondition.META_OP.EQ, "11")
          )
        ),
      reg_con =
        DataStructures.RegionCondition.AND(
          DataStructures.RegionCondition.Predicate(1, DataStructures.RegionCondition.REG_OP.GTE, 10),
          DataStructures.RegionCondition.Predicate(1, DataStructures.RegionCondition.REG_OP.EQ, DataStructures.RegionCondition.MetaAccessor("bert_value1"))
        )
      )
    server setOutputPath output_path MATERIALIZE selection
    */


    /*
    //SEMI JOIN ON META
    val semi_join = dataAsTheyAre.SELECT(
      MetaJoinCondition(List("bert_value1", "bert_value2")),
      optionalDS
    )
    server setOutputPath output_path MATERIALIZE semi_join
    */



    /*
    //PROJECT MD
    val projectmd = dataAsTheyAre.PROJECT(Some(List("tableName","bert_value1", "bert_value2", "bert_value3")), None, None, None, None, None)
    server setOutputPath output_path MATERIALIZE projectmd
    */

    /*
    //PROJECT MD ATTRIBUTE AND AGGREGATE MD
    val fun = new MetaAggregateStruct {override val newAttributeName: String = "computed_bert_value1"
      override val inputAttributeName: String = "bert_value1"
      override val fun: (Traversable[String]) => String =
      //average of the double
        (l : Traversable[String]) => {
          val r =
            l
              .map((a: String) => (a.toDouble * 2, 1))
              .reduce((a: (Double, Int), b: (Double, Int)) => (a._1 + b._1, a._2 + b._2))

          (r._1 / r._2).toString
        }

    }

    val projectmd = dataAsTheyAre.PROJECT(Some(List("tableName","bert_value1", "bert_value2", "bert_value3")),
      Some(fun), None, None, None, None)
    server setOutputPath output_path MATERIALIZE projectmd
    */


    /*
    //PROJECT MD/RD ATTRIBUTE AND AGGREGATE RD TUPLE
    val fun = new RegionTupleAggregateStruct {
      override val inputIndexes: List[Int] = List(0,1)
      override val fun: (Array[GValue]) => GValue =
      //Concatenation of strand and value
        (l : Array[GValue]) => {
          l.reduce( (a : GValue, b : GValue) => GString ( a.asInstanceOf[GString].v + " " + b.asInstanceOf[GDouble].v.toString ) )
        }
    }

    val projectrd = dataAsTheyAre.PROJECT(Some(List("tableName","bert_value1", "bert_value2", "bert_value3")), None, None, Some(List(0,1)), Some(fun), None)
    val projectrd2 = projectrd.PROJECT(None, None, None, Some(List(2)), None, None)
    val selectrd = projectrd2.SELECT(reg_con = DataStructures.RegionCondition.Predicate(0, DataStructures.RegionCondition.REG_OP.EQ, "+ 1000.0"))
    server setOutputPath output_path MATERIALIZE selectrd
    */




    /*
    //PROJECT AGGREGATE RD
    val fun = new RegionAggregateStruct {
      override val newAttributeName: String = "computed_bert_value1_region_aggregate"
      override val inputIndex: Int = 1
      override val fun: (GValue, GValue) => GValue =
      //sum of values
        (in1 : GValue, in2 : GValue) => {
          GDouble(in1.asInstanceOf[GDouble].v + in2.asInstanceOf[GDouble].v)
        }
    }

    val projectrd = dataAsTheyAre.PROJECT(Some(List("tableName","bert_value1", "bert_value2", "bert_value3")), None, None, None, None, Some(fun))
    server setOutputPath output_path MATERIALIZE projectrd
    */

    //GROUP


    //GROUPMD

    /*
    val sum =
      (l : List[GValue]) => {
        l.reduce((a,b) => GDouble(a.asInstanceOf[GDouble].v + b.asInstanceOf[GDouble].v))
      }
    val regionsToMeta = new RegionsToMeta{
      override val newAttributeName = "Psum"
      override val inputIndex = 2
      override val fun = sum
    }
    val groupMD = dataAsTheyAre.GROUP(Some(MetaGroupByCondition(List("bert_value1"))), Some(List(regionsToMeta)), "bert_value_1_group", None, None)

    server setOutputPath output_path MATERIALIZE groupMD
    */



    //GROUPRD
    /*
    val sum =
      (l : List[GValue]) => {
        l.reduce((a,b) => GDouble(a.asInstanceOf[GDouble].v + b.asInstanceOf[GDouble].v))
      }
    val regionsToRegion = new RegionsToRegion{
      override val index = 1
      override val fun = sum
    }
    val groupRD = dataAsTheyAre.GROUP(None, None, "pvalue", None, None)
    //val groupRD = dataAsTheyAre.GROUP(None, None, "pvalue", None, Some(List(regionsToRegion)))
    //val groupRD = dataAsTheyAre.GROUP(None, None, "pvalue", Some(List(new FIELD(1))), Some(List(regionsToRegion)))

    server setOutputPath output_path MATERIALIZE groupRD
    */



    //UNION
    /*
    //val union = dataAsTheyAre.UNION(None)
    val union = dataAsTheyAre.UNION(Some(List("bert_value2")))
    server setOutputPath output_path MATERIALIZE union
    */


    //MERGE
    /*
    val merge = dataAsTheyAre.MERGE(List(-1,1,2), optionalDS)
    server setOutputPath output_path MATERIALIZE merge
    */


    //ORDER

    //ORDERMD
    /*
    //val orderMD = dataAsTheyAre.ORDER(Some(List(("bert_value2", Direction.DESC), ("bert_value1", Direction.ASC))), "group_ordering", NoTop(), None, NoTop())
    //val orderMD = dataAsTheyAre.ORDER(Some(List(("bert_value2", Direction.DESC), ("bert_value1", Direction.ASC))), "group_ordering", Top(2), None, NoTop())
    val orderMD = dataAsTheyAre.ORDER(Some(List(("bert_value2", Direction.DESC), ("bert_value1", Direction.ASC))), "group_ordering", TopG(1), None, NoTop())
    server setOutputPath output_path MATERIALIZE orderMD
    */

    //ORDERRD
    /*
    //val orderRD = dataAsTheyAre.ORDER(None, "groupName", NoTop(), Some(List((1, Direction.DESC))), NoTop())
    //val orderRD = dataAsTheyAre.ORDER(None, "groupName", NoTop(), Some(List((1, Direction.DESC))), Top(3))
    val orderRD = dataAsTheyAre.ORDER(None, "groupName", NoTop(), Some(List((0, Direction.ASC), (1, Direction.DESC))), TopG(3))
    server setOutputPath output_path MATERIALIZE orderRD
    */

    /*
    //MAP
    val map = dataAsTheyAre.MAP(
      List(new RegionAggregateMap {
        override val index: Int = 1
        override val fun: (List[GValue]) => GValue = {
          (l) => GDouble(l.map((g) => g.asInstanceOf[GDouble].v).reduce(_ + _))
        }
      }),
      optionalDS)

    server setOutputPath output_path MATERIALIZE map
    */

    /*
    //COVER
    val cover = dataAsTheyAre.COVER(CoverFlag.COVER, N(1), N(5), List(), None )
    server setOutputPath output_path MATERIALIZE cover
    */



    //MAP
    /*
    val map = dataAsTheyAre.MAP(
      List(),
      optionalDS)

    server setOutputPath output_path MATERIALIZE map
    */

    /*
        //MAP with group
        val map = dataAsTheyAre.MAP(
          new MetaJoinCondition(List("bert_value1")),
          List(
            new RegionsToRegion {
              override val index: Int = 1
              override val fun: (List[GValue]) => GValue = {
                (line) => GDouble(line.map((gvalue) => gvalue.asInstanceOf[GDouble].v).reduce(_ + _))
              }
            },
            new RegionsToRegion {
              override val index: Int = 2
              override val fun: (List[GValue]) => GValue = {
                (list) => GString(list.map((gvalue) => gvalue.asInstanceOf[GString].v).reduce((word1 : String, word2 : String) => word1 + " " + word2))
              }
            }),
          optionalDS)

        server setOutputPath output_path MATERIALIZE map


    */
//    server.run()


    //-----------------------------

    //val mapped = dataAsTheyAre MAP something
    // println(dataAsTheyAre)

    //val env = ExecutionEnvironment.getExecutionEnvironment

    /*
        val text = env.fromElements("To be, or not to be,--that is the question:--",
          "Whether 'tis nobler in the mind to suffer", "The slings and arrows of outrageous fortune",
          "Or to take arms against a sea of troubles,")

        val one : List[Any] = List(1)
        val counts = text.flatMap { _.toLowerCase.split("\\W+") }
          .map { (_,1) }
          .groupBy(0)
          .sum(1)


        counts.writeAsCsv("/home/pietro/Desktop/result.csv")

        env.execute("WordCount Example")*/
  }

}
