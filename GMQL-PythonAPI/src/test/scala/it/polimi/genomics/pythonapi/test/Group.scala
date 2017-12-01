package it.polimi.genomics.pythonapi.test

import java.util

import it.polimi.genomics.core.DataStructures.MetaAggregate.MetaAggregateFunction
import it.polimi.genomics.pythonapi.PythonManager
import it.polimi.genomics.spark.implementation.loaders.{CustomParser, NarrowPeakParser}

object Group {

  val pmg = PythonManager
  pmg.startEngine()
  val opmg = pmg.getOperatorManager
  pmg.setHadoopHomeDir("C:\\Users\\Luca\\Desktop\\PyGMQL\\gmql\\resources\\hadoop")



  val datasetPath = "C:\\Users\\Luca\\Desktop\\PyGMQL\\tests\\data\\" +
    "job__lucananni93_20171111_004548_1b9b0d74-e5cc-4053-99ec-52d44e2f4150\\job__lucananni93_20171111_004548_1b9b0d74-e5cc-4053-99ec-52d44e2f4150\\files"
  val outputPath = "C:\\Users\\Luca\\Desktop\\PyGMQL\\tests\\tmp"

  def main(args: Array[String]): Unit = {
    val d = pmg.read_dataset(datasetPath, new CustomParser())
    val metaAggregateFactory = pmg.getServer.implementation.metaAggregateFunctionFactory
    val expBuild = pmg.getNewExpressionBuilder(d)
    val meta = new java.util.ArrayList[String](util.Arrays.asList("cell"))
    val metaAggregate = new util.ArrayList[MetaAggregateFunction](util.Arrays.asList(
      expBuild.createMetaAggregateFunction("BAG", "newMeta", Some("treatment_tag"))
    ))
    val nd  = opmg.group(d, Some(meta), Some(metaAggregate), "_group", None, None)
    pmg.materialize(nd, outputPath)
    pmg.execute()

  }

}
