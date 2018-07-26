package it.polimi.genomics.core.tests

import it.polimi.genomics.core.DAG.{DAG, DAGFrame}
import it.polimi.genomics.core.DataStructures.CoverParameters.{ALL, CoverFlag}
import it.polimi.genomics.core.DataStructures.ExecutionParameters.BinningParameter
import it.polimi.genomics.core.DataStructures.MetadataCondition.{META_OP, Predicate}
import it.polimi.genomics.core.DataStructures.RegionAggregate.RegionsToRegion
import it.polimi.genomics.core.DataStructures.RegionCondition.{REG_OP, StartCondition}
import it.polimi.genomics.core.DataStructures._

import scala.collection.JavaConverters._
import it.polimi.genomics.core.{GDouble, GMQLLoader, GValue, ParsingType}
import it.polimi.genomics.core.ParsingType.PARSING_TYPE


object TestDAG extends App {

  val emptySchema: java.util.List[(String,PARSING_TYPE)] = List[(String,PARSING_TYPE)]().asJava
  val binning = BinningParameter(None)

  class FakeGMQLLoader[IR,OR,IM,OM] extends GMQLLoader[IR,OR,IM,OM] {
    override def meta_parser(input: IM): OM = ???
    override def region_parser(input: IR): OR = ???
  }

  def getInitialIRVariable(datasetName: String): IRVariable = {
    val readMD = IRReadMD(List(datasetName), new FakeGMQLLoader, IRDataSet(datasetName,emptySchema))
    val readRD = IRReadRD(List(datasetName), new FakeGMQLLoader, IRDataSet(datasetName,emptySchema))
    val v = IRVariable(readMD, readRD, emptySchema.asScala.toList)(binning)
    v
  }

  def materializeIRVariable(v: IRVariable, outputName: String): IRVariable = {
    val dag_md = IRStoreMD(outputName, v.metaDag, IRDataSet(outputName, List[(String,PARSING_TYPE)]().asJava))
    val dag_rd = IRStoreRD(outputName, v.regionDag, v.metaDag, v.schema ,IRDataSet(outputName, List[(String,PARSING_TYPE)]().asJava))
    IRVariable(dag_md, dag_rd, v.schema)(binning)
  }

  def getRegionsToRegion = new RegionsToRegion {
    override val resType: PARSING_TYPE = ParsingType.CHAR
    override val associative: Boolean = false
    override val index: Int = 0
    override val fun: List[GValue] => GValue = (x: List[GValue]) => GDouble(2.0)
    override val funOut: (GValue, (Int, Int)) => GValue = {
      def fake(x: GValue, y: (Int, Int)) = GDouble(2.0)
      fake
    }
  }

  val metadataCondition = Predicate("attribute", META_OP.EQ, "ciao")
  val regionCondition = StartCondition(REG_OP.GTE, 1000)

  val v1 = getInitialIRVariable("dataset1")
    .SELECT(metadataCondition, regionCondition)

  val v2 = getInitialIRVariable("dataset2")
    .COVER(CoverFlag.COVER, new ALL{}, new ALL {}, List(getRegionsToRegion), None)

  val v3 = v2.MAP(None, List(getRegionsToRegion), v1)

  val dag = new DAG(List(
    materializeIRVariable(v1, "v1"),
    materializeIRVariable(v2, "v2"),
    materializeIRVariable(v3, "v3")
  ))

  //dag.plot("Example")
  //dag.subDAG(x => x.isInstanceOf[MetaOperator]).plot("Example")
  val dagFrame = new DAGFrame(dag)
  dagFrame.setSize(400, 320)
  dagFrame.setVisible(true)
  println("v1 sources\t" + v1.regionDag.sources.union(v1.metaDag.sources))
  println("v2 sources\t" + v2.regionDag.sources.union(v2.metaDag.sources))
  println("v3 sources\t" + v3.regionDag.sources.union(v3.metaDag.sources))

  val resIRVariables = new DAG(dag.toVariables(binning)).raw.toSet == dag.raw.toSet
  println(resIRVariables)
}
