package it.polimi.genomics.scidb.test

import it.polimi.genomics.core.DataStructures.ExecutionParameters.BinningParameter
import it.polimi.genomics.core.DataStructures.MetaJoinCondition.MetaJoinCondition
import it.polimi.genomics.core.DataStructures._
import it.polimi.genomics.core.ParsingType

/**
  * Created by Cattani Simone on 01/03/16.
  * Email: simone.cattani@mail.polimi.it
  *
  */
object DagTest
{
  def main(args: Array[String]): Unit =
  {
    println("\n-----------------------------------------------")
    println("-- DAG TEST -----------------------------------")
    println("-----------------------------------------------")

    println(getDAG)
  }
  import scala.collection.JavaConverters._
  def getDAG : IRVariable =
  {
    val input = new IRDataSet("TEST_NARROW_PEAK", List(
      ("name",ParsingType.STRING),
      ("score",ParsingType.STRING),
      ("signal",ParsingType.DOUBLE),
      ("pValue",ParsingType.DOUBLE),
      ("qValue",ParsingType.DOUBLE),
      ("peak",ParsingType.INTEGER)
    ).asJava)

    val output = new IRDataSet("TEST_NARROW_PEAK_2", List(
      ("name",ParsingType.STRING),
      ("score",ParsingType.STRING),
      ("signal",ParsingType.DOUBLE),
      ("pValue",ParsingType.DOUBLE),
      ("qValue",ParsingType.DOUBLE),
      ("peak",ParsingType.INTEGER)
    ).asJava)

    val MJD = IRJoinBy(MetaJoinCondition(List("size")),
      IRReadMD(List(),new FakeParser, input),
      IRReadMD(List(), new FakeParser, input))

    val MD = IRStoreMD(
      "",
      IRCombineMD(SomeMetaJoinOperator(MJD), IRReadMD(List(), new FakeParser, input), IRReadMD(List(), new FakeParser, input))
      , output
    )

    val RD = IRStoreRD(
      "", IRReadRD(List(), new FakeParser, input),output
    )


    implicit val binS = BinningParameter(None)                                    // TODO: io non dovrei proprio vedere il bin size se non voglio
    var variable = IRVariable(MD,RD)

    variable
  }
}
