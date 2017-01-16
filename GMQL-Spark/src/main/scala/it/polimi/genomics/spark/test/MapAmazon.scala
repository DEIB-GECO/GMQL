package it.polimi.genomics.spark.test

/**
  * The entry point of the application
  * It initialize the server, call server's methods to build the query and invoke the server's run method to start the execution.
  */
object MapAmazon {
//
//   def main(args : Array[String]) {
//
//     val server = new GmqlServer(new GMQLSparkExecutor)
//
//     val mainPath = "hdfs://172.31.18.123:9000/" + args(0) + "/"
//     //val ex_data_path = "hdfs://127.0.0.1:9000/user/Inputs/flinkInput/"
//     val ex_data_path = List(mainPath + "ref/*")
//     val ex_data_path_optional = List(mainPath + "exp/*")
//     val output_path = mainPath + "res/"
//
//
//     val dataAsTheyAre = server READ ex_data_path_optional USING test2Parser()
//     val optionalDS = server READ ex_data_path USING test2Parser()
//
//     val what = 0 // simple map
//     //val what = 1 // map with aggregation
//     //val what = 2 // map with grouping and aggregation
//
//
//     val map = args(1).toInt match{
//       case 0 =>
//         // MAP
//         optionalDS.MAP(
//           List(),
//           dataAsTheyAre)
//
//       case 1 =>
//         // MAP with aggregation
//         dataAsTheyAre.MAP(
//           List(new RegionsToRegion {
//             override val index: Int = 1
//             override val fun: (List[GValue]) => GValue = {
//               (l) => GDouble(l.map((g) => g.asInstanceOf[GDouble].v).reduce(_ + _))
//             }
//             override val resType: PARSING_TYPE = ParsingType.DOUBLE
//           }),
//           optionalDS
//         )
//
//       case 2 =>
//         //MAP with grouping and aggregation
//         dataAsTheyAre.MAP(
//           new MetaJoinCondition(List("bert_value1")),
//           List(
//             new RegionsToRegion {
//               override val index: Int = 1
//               override val fun: (List[GValue]) => GValue = {
//                 (line) => GDouble(line.map((gvalue) => gvalue.asInstanceOf[GDouble].v).reduce(_ + _))
//               }
//               override val resType: PARSING_TYPE = ParsingType.DOUBLE
//             },
//             new RegionsToRegion {
//               override val index: Int = 2
//               override val fun: (List[GValue]) => GValue = {
//                 (list) => GString(list.map((gvalue) => gvalue.asInstanceOf[GString].v).reduce((word1 : String, word2 : String) => word1 + " " + word2))
//               }
//               override val resType: PARSING_TYPE = ParsingType.STRING
//             }
//           ),
//           optionalDS
//         )
//     }
//     server setOutputPath output_path MATERIALIZE map
//
//     server.run()
//   }

 }
