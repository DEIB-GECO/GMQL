//package it.polimi.genomics.scidb.test.operator
//
//import it.polimi.genomics.core.DataStructures.ExecutionParameters.BinningParameter
//import it.polimi.genomics.core.DataStructures.JoinParametersRD._
//import it.polimi.genomics.core.DataStructures.MetaJoinCondition.MetaJoinCondition
//import it.polimi.genomics.core.DataStructures._
//import it.polimi.genomics.core.ParsingType
//import it.polimi.genomics.scidb.GmqlSciImplementation
//import it.polimi.genomics.scidb.repository.GmqlSciExporter
//import it.polimi.genomics.scidb.utility.JoinPredUtils
//
///**
//  * Created by Cattani Simone on 08/03/16.
//  * Email: simone.cattani@mail.polimi.it
//  *
//  */
//object JoinTest
//{
//  def main(args: Array[String]): Unit =
//  {
//    println("\n-----------------------------------------------")
//    println("-- Join TEST ----------------------------------")
//    println("-----------------------------------------------")
//
//    val ANC = IRDataSet("DEV_JOIN_REF", List(
//      ("val1",ParsingType.INTEGER),
//      ("val2",ParsingType.STRING)
//    ))
//
//    val EXP = IRDataSet("DEV_JOIN_EXP", List(
//      ("val1",ParsingType.INTEGER),
//      ("val2",ParsingType.STRING)
//    ))
//
//    val RES = IRDataSet("DEV_JOIN_REF_STORE", List(
//      ("val1",ParsingType.INTEGER),
//      ("val2",ParsingType.STRING)
//    ))
//
//    val ReadAncMD = IRReadMD(List(), new FakeParser, ANC)
//    val ReadAncRD = IRReadRD(List(), new FakeParser, ANC)
//    val ReadExpMD = IRReadMD(List(), new FakeParser, EXP)
//    val ReadExpRD = IRReadRD(List(), new FakeParser, EXP)
//
//    val MetaJoinMJD = IRJoinBy(MetaJoinCondition(List("bert_value2")), ReadAncMD, ReadExpMD)
//    val CombineMD = IRCombineMD(SomeMetaJoinOperator(MetaJoinMJD), ReadAncMD, ReadExpMD)
//
//    val predicates = List(
//      new JoinQuadruple(Some(DistGreater(4)), Some(new Upstream()), Some(new DistLess(20)))//,
//      //new JoinQuadruple(Some(DistGreater(-10)), Some(new DownStream()), Some(new DistLess(20))),
//      //new JoinQuadruple(Some(DistLess(50)), Some(new MinDistance(5)), Some(DistGreater(-20)))
//    )
//    val GenometricJoinRD = IRGenometricJoin(SomeMetaJoinOperator(MetaJoinMJD), predicates, RegionBuilder.LEFT, ReadAncRD, ReadExpRD)
//
//    val StoreAncMD = IRStoreMD("", CombineMD, RES)
//    val StoreAncRD = IRStoreRD("", GenometricJoinRD, RES)
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
//  }
//}
