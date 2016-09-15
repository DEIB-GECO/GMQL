package it.polimi.genomics.flink.examples

import it.polimi.genomics.core.DataStructures.MetaJoinCondition.MetaJoinCondition
import it.polimi.genomics.core.DataStructures.MetadataCondition.ContainAttribute
import it.polimi.genomics.core.DataStructures.RegionCondition.REG_OP
import it.polimi.genomics.core.DataStructures._
import it.polimi.genomics.GMQLServer.GmqlServer
import it.polimi.genomics.flink.FlinkImplementation.FlinkImplementation
import it.polimi.genomics.flink.FlinkImplementation.reader.parser.BedScoreParser2

/**
 * The entry point of the application
 * It initialize the server, call server's methods to build the query and invoke the server's run method to start the execution.
 */
object Select {

  def main(args : Array[String]) {

    val server = new GmqlServer(new FlinkImplementation)
    val inPath1 = "/Users/michelebertoni/Workspaces/IdeaProjects/genomic_computing/GMQLv2Scala/scalav2/trunk/GMQL-Flink/src/test/datasets/"
    val ex_data_path = List(inPath1 + "select/1/")
    val opt_data_path = List(inPath1 + "select/2/")
    val output_path = inPath1 + "res/"


    val DS1 = server READ ex_data_path USING BedScoreParser2
    val REFDS = server READ opt_data_path USING BedScoreParser2


    val what = 7


    val select =
      what match{
        case 0 => {
          DS1.SELECT(
            meta_con =
              MetadataCondition.Predicate("cell", MetadataCondition.META_OP.EQ, "1")
          )
        }

        case 1 => {
          //REGION AND META SELECTION
          DS1.SELECT(
            meta_con =
              MetadataCondition.AND(
              MetadataCondition.AND(
                MetadataCondition.Predicate("bert_value_meta", MetadataCondition.META_OP.GTE, 50),
                MetadataCondition.NOT(
                  MetadataCondition.Predicate("cell", MetadataCondition.META_OP.GT, 10)
                )
              )
                ,
                MetadataCondition.Predicate("provider", MetadataCondition.META_OP.EQ, "UCSC")
              )
            ,
            reg_con =
              RegionCondition.AND(
                RegionCondition.Predicate(0, RegionCondition.REG_OP.GTE, 0),
                RegionCondition.Predicate(0, RegionCondition.REG_OP.LTE, RegionCondition.MetaAccessor("bert_value_meta"))
              )
          )

        }

        case 2 => {
          //REGION SELECTION
          DS1.SELECT(
            reg_con =
              RegionCondition.Predicate(0, RegionCondition.REG_OP.GTE, 90)
          )
        }

        case 3 => {
          //REGION SELECTION
          DS1.SELECT(
            reg_con =
              RegionCondition.NOT(
                RegionCondition.AND(
                  RegionCondition.StartCondition(REG_OP.GTE, 250L),
                  //RegionCondition.Predicate(1, RegionCondition.REG_OP.EQ, 0),
                  //RegionCondition.Predicate(0, RegionCondition.REG_OP.EQ, "400")
                  RegionCondition.Predicate(0, RegionCondition.REG_OP.LTE, RegionCondition.MetaAccessor("bert_value_meta"))
                )
              )
          )
        }

        case 4 => {
          //SEMI JOIN ON META
          DS1.SELECT(
            MetaJoinCondition(List("bert_value1", "bert_value2")),
            REFDS
          )
        }

        case 5 => {
          //MISSING
          DS1.SELECT(
            meta_con = ContainAttribute("bert_value_meta")
          )
        }

        case 6 => {
          //REGION AND META SELECTION
          DS1.SELECT(
            meta_con =
              MetadataCondition.AND(
                MetadataCondition.AND(
                  MetadataCondition.Predicate("bert_value_meta", MetadataCondition.META_OP.GTE, 50),
                  MetadataCondition.NOT(
                    MetadataCondition.Predicate("cell", MetadataCondition.META_OP.NOTEQ, 2.0)
                  )
                )
                ,
                MetadataCondition.Predicate("provider", MetadataCondition.META_OP.EQ, "UCSC")
              )
            ,
            reg_con =
              RegionCondition.AND(
                RegionCondition.Predicate(0, RegionCondition.REG_OP.GTE, 0),
                RegionCondition.Predicate(0, RegionCondition.REG_OP.LTE, RegionCondition.MetaAccessor("bert_value_meta"))
              )
          )
        }


          case 7 => {
            //REGION AND META SELECTION - OR
            DS1.SELECT(
              meta_con =
                MetadataCondition.OR(
                  MetadataCondition.AND(
                    MetadataCondition.Predicate("bert_value_meta", MetadataCondition.META_OP.GTE, 50),
                    MetadataCondition.NOT(
                      MetadataCondition.Predicate("cell", MetadataCondition.META_OP.NOTEQ, 2.0)
                    )
                  )
                  ,
                  MetadataCondition.Predicate("provider", MetadataCondition.META_OP.EQ, "UCSC")
                )
              ,
              reg_con =
                RegionCondition.AND(
                  RegionCondition.Predicate(0, RegionCondition.REG_OP.GTE, 0),
                  RegionCondition.Predicate(0, RegionCondition.REG_OP.LTE, RegionCondition.MetaAccessor("bert_value_meta"))
                )
            )

        }


        case 8 => {
          //DataAsTheyAre
          DS1
        }

      }

    server setOutputPath output_path MATERIALIZE select

    server.run()
  }

}
