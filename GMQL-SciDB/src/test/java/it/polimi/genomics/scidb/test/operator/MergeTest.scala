//package it.polimi.genomics.scidb.test.operator
//
//import it.polimi.genomics.core.DataStructures.ExecutionParameters.BinningParameter
//import it.polimi.genomics.core.DataStructures.GroupMDParameters.Direction._
//import it.polimi.genomics.core.DataStructures.GroupMDParameters.Top
//import it.polimi.genomics.core.DataStructures.MetaGroupByCondition.MetaGroupByCondition
//import it.polimi.genomics.core.DataStructures._
//import it.polimi.genomics.core.ParsingType
//import it.polimi.genomics.scidb.repository.GmqlSciExporter
//import it.polimi.genomics.scidb.{GmqlSciConfig, GmqlSciImplementation}
//
///**
//  * Created by Cattani Simone on 16/03/16.
//  * Email: simone.cattani@mail.polimi.it
//  *
//  */
//object MergeTest
//{
//  def main(args: Array[String]): Unit =
//  {
//    println("\n-----------------------------------------------")
//    println("-- Merge TEST ---------------------------------")
//    println("-----------------------------------------------")
//
//    val S1 = IRDataSet("BERT_ORDER_REF", List(
//      ("val1",ParsingType.INTEGER),
//      ("val2",ParsingType.STRING)
//    ))
//
//    val RES = IRDataSet("BERT_MERGE_REF_STORE", List(
//      ("val1",ParsingType.INTEGER),
//      ("val2",ParsingType.STRING)
//    ))
//
//    val ReadAncMD = IRReadMD(List(), new FakeParser, S1)
//    val ReadAncRD = IRReadRD(List(), new FakeParser, S1)
//
//    val GroupBy = IRGroupBy(MetaGroupByCondition(List("bert_value3","bert_value2")), ReadAncMD)
//    val MergeMD = IRMergeMD(ReadAncMD, Some(GroupBy))
//    val MergeRD = IRMergeRD(ReadAncRD, Some(GroupBy))
//
//    val StoreAncMD = IRStoreMD("", MergeMD, RES)
//    val StoreAncRD = IRStoreRD("", MergeRD, RES)
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
//        "BERT_MERGE_REF_STORE",
//        "/home/scidb/test/real/bertEXP/merge/res",
//        List(
//          ("val1",ParsingType.INTEGER),
//          ("val2",ParsingType.STRING)
//        ), "tsv"
//      )
//
//  }
//}
