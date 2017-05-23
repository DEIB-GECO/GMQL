package it.polimi.genomics.scidb.test.operator

import it.polimi.genomics.core.DataStructures.ExecutionParameters.BinningParameter
import it.polimi.genomics.core.DataStructures.JoinParametersRD._
import it.polimi.genomics.core.DataStructures.MetaJoinCondition.{Default, MetaJoinCondition}
import it.polimi.genomics.core.DataStructures.MetadataCondition._
import it.polimi.genomics.core.DataStructures.RegionCondition.{ChrCondition, LeftEndCondition, MetaAccessor, StartCondition}
import it.polimi.genomics.core.DataStructures._
import it.polimi.genomics.core.ParsingType
import it.polimi.genomics.scidb.GmqlSciImplementation
import it.polimi.genomics.core.DataStructures.MetadataCondition.META_OP._

/**
  * Created by Cattani Simone on 10/03/16.
  * Email: simone.cattani@mail.polimi.it
  *
  */
object SelectTest
{
  def main(args: Array[String]): Unit =
  {
    println("\n-----------------------------------------------")
    println("-- Join TEST ----------------------------------")
    println("-----------------------------------------------")
    import scala.collection.JavaConverters._

    val SOURCE = IRDataSet("DEV_MAP_REF", List(
      ("val1",ParsingType.INTEGER),
      ("val2",ParsingType.STRING)
    ).asJava)

    val EXTERN = IRDataSet("DEV_MAP_EXP", List(
      ("val1",ParsingType.INTEGER),
      ("val2",ParsingType.STRING)
    ).asJava)

    val RES = IRDataSet("DEV_SELECT_RES", List(
      ("val1",ParsingType.INTEGER),
      ("val2",ParsingType.STRING)
    ).asJava)

    val ReadSourceMD = IRReadMD(List(), new FakeParser, SOURCE)
    val ReadSourceRD = IRReadRD(List(), new FakeParser, SOURCE)
    val ReadExtMD = IRReadMD(List(), new FakeParser, EXTERN)

    val metaCondition = AND( Predicate("bert_value1", EQ, "1"), Predicate("bert_value2", EQ, "2") )
    val metaCondition2 = Predicate("bert_value2", NOTEQ, "5")
    val metaCondition3 = MissingAttribute("bert_value1")
    val SelectSourceMD = IRSelectMD(metaCondition, ReadSourceMD)

    val SemiJoinMD = IRSemiJoin(ReadExtMD, MetaJoinCondition(List(Default("bert_value2"))), ReadSourceMD)  // toran solo il 2

    val regionCondition =
      it.polimi.genomics.core.DataStructures.RegionCondition.AND(
        it.polimi.genomics.core.DataStructures.RegionCondition.Predicate(1,it.polimi.genomics.core.DataStructures.RegionCondition.REG_OP.GT, MetaAccessor("bert_value1")),
        it.polimi.genomics.core.DataStructures.RegionCondition.Predicate(1,it.polimi.genomics.core.DataStructures.RegionCondition.REG_OP.GT, MetaAccessor("bert_value2"))
      )
    val SelectSourceRD = IRSelectRD(Some(regionCondition), Some(SelectSourceMD), ReadSourceRD)

    val StoreAncMD = IRStoreMD("", SelectSourceMD, RES)
    val StoreAncRD = IRStoreRD("", SelectSourceRD,SelectSourceMD,List(), RES)

    implicit val binS = BinningParameter(None)
    var variable = IRVariable(StoreAncMD,StoreAncRD)

    // ------------------------------------------------------------
    // ------------------------------------------------------------

    val implementation = new GmqlSciImplementation

    implementation.addDAG(variable)
    implementation.go()

    /*GmqlSciExporter.exportDS(
      "BERT_JOIN_REF_STORE",
      "/home/scidb/test/real/bertEXP/select/res",
      List(
        ("val1",ParsingType.INTEGER),
        ("val2",ParsingType.STRING)
      ), "tsv"
    )*/

  }
}
