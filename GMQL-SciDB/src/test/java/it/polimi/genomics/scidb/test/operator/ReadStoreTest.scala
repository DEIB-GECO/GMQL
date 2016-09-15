//package it.polimi.genomics.scidb.test.operator
//
//import it.polimi.genomics.core.DataStructures.ExecutionParameters.BinningParameter
//import it.polimi.genomics.core.DataStructures._
//import it.polimi.genomics.core.ParsingType
//import it.polimi.genomics.scidb.GmqlSciImplementation
//import it.polimi.genomics.scidb.repository.GmqlSciExporter
//
///**
//  * Created by Cattani Simone on 08/03/16.
//  * Email: simone.cattani@mail.polimi.it
//  *
//  */
//object ReadStoreTest
//{
//  def main(args: Array[String]): Unit =
//  {
//    println("\n-----------------------------------------------")
//    println("-- Read/Store TEST ----------------------------")
//    println("-----------------------------------------------")
//
//    val ANC = IRDataSet("BERT_JOIN_REF", List(
//      ("val1",ParsingType.INTEGER),
//      ("val2",ParsingType.STRING)
//    ))
//
//    val EXP = IRDataSet("BERT_JOIN_EXP", List(
//      ("val1",ParsingType.INTEGER),
//      ("val2",ParsingType.STRING)
//    ))
//
//    val RES = IRDataSet("BERT_JOIN_REF_STORE", List(
//      ("val1",ParsingType.INTEGER),
//      ("val2",ParsingType.STRING)
//    ))
//
//    val ReadAncMD = IRReadMD(List(), new FakeParser, ANC)
//    val ReadAncRD = IRReadRD(List(), new FakeParser, ANC)
//    val ReadExpMD = IRReadMD(List(), new FakeParser, EXP)
//    val ReadExpRD = IRReadRD(List(), new FakeParser, EXP)
//
//    val StoreAncMD = IRStoreMD("", ReadAncMD, RES)
//    val StoreAncRD = IRStoreRD("", ReadAncRD, RES)
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
//      "BERT_JOIN_REF_STORE",
//      "/home/scidb/test/real/bertEXP/store/ref",
//      List(
//        ("val1", ParsingType.INTEGER),
//        ("val2", ParsingType.STRING)
//      ), "tsv"
//    )
//
//  }
//}
