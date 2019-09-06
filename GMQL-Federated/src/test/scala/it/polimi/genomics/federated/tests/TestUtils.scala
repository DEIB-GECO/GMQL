package it.polimi.genomics.federated.tests

import it.polimi.genomics.core.DataStructures.ExecutionParameters.BinningParameter
import it.polimi.genomics.core.DataStructures.JoinParametersRD.{JoinQuadruple, RegionBuilder}
import it.polimi.genomics.core.DataStructures.RegionAggregate.RegionsToRegion
import it.polimi.genomics.core.DataStructures._
import it.polimi.genomics.core.ParsingType.PARSING_TYPE
import it.polimi.genomics.core.{GDouble, GMQLLoader, GValue, ParsingType}

import collection.JavaConverters._


object TestUtils {

  val emptySchema: java.util.List[(String,PARSING_TYPE)] = List[(String,PARSING_TYPE)]().asJava
  val binning = BinningParameter(None)
  val instances = List(LOCAL_INSTANCE,
    Instance("GECO"),
    Instance("GENOMICS"),
    Instance("BROAD_INSTITUTE"))

  class FakeGMQLLoader[IR,OR,IM,OM] extends GMQLLoader[IR,OR,IM,OM] {
    override def meta_parser(input: IM): OM = ???
    override def region_parser(input: IR): OR = ???
  }

  def getInitialIRVariable(datasetName: String, instance: GMQLInstance = LOCAL_INSTANCE, protect: Boolean = false): IRVariable = {
    val readMD = IRReadMD(List(datasetName), new FakeGMQLLoader, IRDataSet(datasetName,emptySchema, instance))
    readMD.addAnnotation(EXECUTED_ON(instance))
    if(protect) readMD.addAnnotation(PROTECTED)
    val readRD = IRReadRD(List(datasetName), new FakeGMQLLoader, IRDataSet(datasetName,emptySchema, instance))
    readRD.addAnnotation(EXECUTED_ON(instance))
    if(protect) readRD.addAnnotation(PROTECTED)
    val v = IRVariable(readMD, readRD, emptySchema.asScala.toList)(binning)
    v
  }

  def materializeIRVariable(v: IRVariable, outputName: String, location: Option[GMQLInstance] = None): IRVariable = {
    val dag_md = IRStoreMD(outputName, v.metaDag, IRDataSet(outputName, List[(String,PARSING_TYPE)]().asJava))
    val dag_rd = IRStoreRD(outputName, v.regionDag, v.metaDag, v.schema ,IRDataSet(outputName, List[(String,PARSING_TYPE)]().asJava))
    IRVariable(v.insert_node(dag_md, location), v.insert_node(dag_rd, location), v.schema, dependencies = List(v))(binning)
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

  def getRegionJoinCondition = List(JoinQuadruple(None, None, None, None))

  def doJOIN(v1: IRVariable, v2: IRVariable, location: Option[GMQLInstance] = None): IRVariable = {
    v1.JOIN(None, getRegionJoinCondition, RegionBuilder.RIGHT, v2, None, None, None, location)
  }

  def doMAP(v1: IRVariable, v2:IRVariable, location: Option[GMQLInstance] = None): IRVariable = {
    v1.MAP(None, List(getRegionsToRegion), v2, None, None, None, location)
  }
}
