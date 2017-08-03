package it.polimi.genomics.flink.examples

import it.polimi.genomics.GMQLServer.GmqlServer
import it.polimi.genomics.core.DataStructures.MetaAggregate.MetaExtension
import it.polimi.genomics.core.DataStructures.RegionAggregate.{RegionExtension, RegionsToMeta}
import it.polimi.genomics.core.DataStructures.RegionCondition.{Predicate, REG_OP}
import it.polimi.genomics.core.{GDouble, GString, GValue}
import it.polimi.genomics.flink.FlinkImplementation.FlinkImplementation
import it.polimi.genomics.flink.FlinkImplementation.reader.parser.BedScoreParser

/**
  * The entry point of the application
  * It initialize the server, call server's methods to build the query and invoke the server's run method to start the execution.
  */
object Project {

  def main(args : Array[String]) {

    val server = new GmqlServer(new FlinkImplementation)
    val mainPath = "/Users/michelebertoni/Workspaces/IdeaProjects/genomic_computing/GMQLv2Scala/scalav2/trunk/GMQL-Flink/src/test/datasets/"
    val ex_data_path = List(mainPath + "Inputs/flinkInput/")
    val ex_data_path_optional = List(mainPath + "map/exp/")
    val output_path = mainPath + "res/"

    val dataAsTheyAre = server READ ex_data_path USING BedScoreParser
    val optionalDS = server READ ex_data_path_optional USING BedScoreParser

    val what = 0 // project MD
    // val what = 1 // project MD and aggregate something
    // val what = 2 // project MD RD and extends tuple
    // val what = 2 // project MD RD and aggregate something


    val project =
      what match{
        case 0 => {
          //PROJECT MD
          dataAsTheyAre.PROJECT(Some(List("tableName","bert_value1", "bert_value2", "bert_value3")), None, false, None, None)
        }

        case 1 => {
          //PROJECT MD ATTRIBUTE AND AGGREGATE MD
          val fun = new MetaExtension {
            override val newAttributeName: String = "computed_bert_value1"
            override val inputAttributeNames: List[String] = List("bert_value1")
            override val fun: (Array[Traversable[(String,String)]]) => String =
            //average of the double
              (l : Array[Traversable[(String,String)]]) => {
                val r =
                  l(0)
                    .map((a: (String,String)) => (a._2.toDouble * 2, 1))
                    .reduce((a: (Double, Int), b: (Double, Int)) => (a._1 + b._1, a._2 + b._2))

                (r._1 / r._2).toString
              }

          }

          dataAsTheyAre.PROJECT(Some(List("tableName","bert_value1", "bert_value2", "bert_value3")), Some(fun), false, None, None)
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

          val projectrd = dataAsTheyAre.PROJECT(Some(List("tableName","bert_value1", "bert_value2", "bert_value3")), None, false, None)
          val projectrd2 = projectrd.PROJECT(None, None, false, None)
          projectrd2.SELECT(reg_con = Predicate(0, REG_OP.EQ, "+ 1000.0"))
        }

        case 3 => {
          //PROJECT AGGREGATE RD
          val fun = new RegionsToMeta {
            override val newAttributeName: String = "computed_bert_value1_region_aggregate"
            override val inputIndex: Int = 1
            override val associative : Boolean = true
            override val funOut: (GValue,(Int, Int)) => GValue = {(v1,v2)=>v1}
            override val fun: List[GValue] => GValue =
            //sum of values
              (l : List[GValue]) => {
                l.reduce((in1, in2) => {
                  GDouble(in1.asInstanceOf[GDouble].v + in2.asInstanceOf[GDouble].v)
                })
              }
          }

          dataAsTheyAre.PROJECT(Some(List("tableName","bert_value1", "bert_value2", "bert_value3")))
        }

      }

     server setOutputPath output_path MATERIALIZE project

     server.run()
   }

 }
