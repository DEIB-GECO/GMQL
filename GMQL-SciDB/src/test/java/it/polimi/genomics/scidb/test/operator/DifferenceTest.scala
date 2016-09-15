//package it.polimi.genomics.scidb.test.operator
//
//import it.polimi.genomics.core.DataStructures.ExecutionParameters.BinningParameter
//import it.polimi.genomics.core.DataStructures.JoinParametersRD._
//import it.polimi.genomics.core.DataStructures.MetaJoinCondition.MetaJoinCondition
//import it.polimi.genomics.core.DataStructures._
//import it.polimi.genomics.core.ParsingType
//import it.polimi.genomics.scidb.GmqlSciImplementation
//import it.polimi.genomics.scidb.repository.GmqlSciExporter
//
///**
//  * Created by Cattani Simone on 15/03/16.
//  * Email: simone.cattani@mail.polimi.it
//  *
//  */
//object DifferenceTest
//{
//  def main(args: Array[String]): Unit =
//  {
//    println("\n-----------------------------------------------")
//    println("-- Difference TEST ----------------------------")
//    println("-----------------------------------------------")
//
//    val S1 = IRDataSet("BERT_JOIN_REF", List(
//      ("val1",ParsingType.INTEGER),
//      ("val2",ParsingType.STRING)
//    ))
//
//    val S2 = IRDataSet("BERT_JOIN_EXP", List(
//      ("val1",ParsingType.INTEGER),
//      ("val2",ParsingType.STRING)
//    ))
//
//    val RES = IRDataSet("BERT_DIFFERENCE_REF_STORE", List(
//      ("val1",ParsingType.INTEGER),
//      ("val2",ParsingType.STRING)
//    ))
//
//    val ReadAncMD = IRReadMD(List(), new FakeParser, S1)
//    val ReadAncRD = IRReadRD(List(), new FakeParser, S1)
//    val ReadExpMD = IRReadMD(List(), new FakeParser, S2)
//    val ReadExpRD = IRReadRD(List(), new FakeParser, S2)
//
//    val MetaJoinMJD = IRJoinBy(MetaJoinCondition(List()), ReadAncMD, ReadExpMD)
//    val DifferenceRD = IRDifferenceRD(SomeMetaJoinOperator(MetaJoinMJD), ReadAncRD, ReadExpRD)
//
//    val StoreAncMD = IRStoreMD("", ReadAncMD, RES)
//    val StoreAncRD = IRStoreRD("", DifferenceRD, RES)
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
//    GmqlSciExporter.exportDS(
//      "BERT_DIFFERENCE_REF_STORE",
//      "/home/scidb/test/real/bertEXP/difference/res",
//      List(
//        ("val1",ParsingType.INTEGER),
//        ("val2",ParsingType.STRING)
//      ), "tsv"
//    )
//
//  }
//}
