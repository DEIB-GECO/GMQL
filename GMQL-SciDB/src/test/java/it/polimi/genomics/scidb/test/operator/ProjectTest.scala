package it.polimi.genomics.scidb.test.operator

import it.polimi.genomics.core.DataStructures.ExecutionParameters.BinningParameter
import it.polimi.genomics.core.DataStructures.MetaJoinCondition.MetaJoinCondition
import it.polimi.genomics.core.DataStructures.MetadataCondition.META_OP._
import it.polimi.genomics.core.DataStructures.MetadataCondition.{MissingAttribute, Predicate, AND}
import it.polimi.genomics.core.DataStructures.RegionCondition.MetaAccessor
import it.polimi.genomics.core.DataStructures._
import it.polimi.genomics.core.ParsingType
import it.polimi.genomics.scidb.GmqlSciImplementation

/**
  * Created by Cattani Simone on 14/04/16.
  * Email: simone.cattani@mail.polimi.it
  *
  */
object ProjectTest
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

    val RES = IRDataSet("DEV_SELECT_RES", List(
      ("val1",ParsingType.INTEGER),
      ("val2",ParsingType.STRING)
    ).asJava)

    val ReadSourceMD = IRReadMD(List(), new FakeParser, SOURCE)
    val ReadSourceRD = IRReadRD(List(), new FakeParser, SOURCE)

    val ProjectMD = IRProjectMD(Some(List("bert_value2","bert_value1")), None, ReadSourceMD)
    val ProjectRD = IRProjectRD(Some(List(1)), None, ReadSourceRD)

    val StoreAncMD = IRStoreMD("", ProjectMD, RES)
    val StoreAncRD = IRStoreRD("", ProjectRD, RES)

    implicit val binS = BinningParameter(None)
    var variable = IRVariable(StoreAncMD,StoreAncRD)

    // ------------------------------------------------------------
    // ------------------------------------------------------------

    val implementation = new GmqlSciImplementation

    implementation.addDAG(variable)
    implementation.go()
  }
}
