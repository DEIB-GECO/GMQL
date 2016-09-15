//package it.polimi.genomics.scidb.test.operator
//
//import it.polimi.genomics.core.DataStructures.ExecutionParameters.BinningParameter
//import it.polimi.genomics.core.DataStructures.GroupMDParameters.{TopG, Top, NoTop}
//import it.polimi.genomics.core.DataStructures.MetaJoinCondition.MetaJoinCondition
//import it.polimi.genomics.core.DataStructures._
//import it.polimi.genomics.core.ParsingType
//import it.polimi.genomics.scidb.repository.GmqlSciExporter
//import it.polimi.genomics.scidb.{GmqlSciConfig, GmqlSciImplementation}
//import it.polimi.genomics.core.DataStructures.GroupMDParameters.Direction._
//
///**
//  * Created by Cattani Simone on 15/03/16.
//  * Email: simone.cattani@mail.polimi.it
//  *
//  */
//object OrderTest
//{
//  def main(args: Array[String]): Unit =
//  {
//    println("\n-----------------------------------------------")
//    println("-- Order TEST ---------------------------------")
//    println("-----------------------------------------------")
//
//    val S1 = IRDataSet("BERT_ORDER_REF", List(
//      ("val1",ParsingType.INTEGER),
//      ("val2",ParsingType.STRING)
//    ))
//
//    val RES = IRDataSet("BERT_ORDER_REF_STORE", List(
//      ("val1",ParsingType.INTEGER),
//      ("val2",ParsingType.STRING),
//      ("order",ParsingType.INTEGER)
//    ))
//
//    val ReadAncMD = IRReadMD(List(), new FakeParser, S1)
//    val ReadAncRD = IRReadRD(List(), new FakeParser, S1)
//
//    val OrderMD = IROrderMD(List(("bert_value2", ASC), ("bert_value1", DESC)), "order", Top(2), ReadAncMD)
//    val PurgeRD = IRPurgeRD(OrderMD, ReadAncRD)
//
//    val OrderRD = IROrderRD(List((0, DESC), (1, ASC)), Top(2), ReadAncRD)
//
//    val StoreAncMD = IRStoreMD("", ReadAncMD, RES)
//    val StoreAncRD = IRStoreRD("", OrderRD, RES)
//
//    implicit val binS = BinningParameter(None)
//    var variable = IRVariable(StoreAncMD,StoreAncRD)
//
//    // ------------------------------------------------------------
//    // ------------------------------------------------------------
//
//    val implementation = new GmqlSciImplementation
//
//    implementation.addDAG(variable)
//    implementation.go()
//
//    if(GmqlSciConfig.scidb_server_on)
//      GmqlSciExporter.exportDS(
//        "BERT_ORDER_REF_STORE",
//        "/home/scidb/test/real/bertEXP/order/res",
//        List(
//          ("val1",ParsingType.INTEGER),
//          ("val2",ParsingType.STRING),
//          ("order",ParsingType.INTEGER)
//        ), "tsv"
//      )
//
//  }
//}
