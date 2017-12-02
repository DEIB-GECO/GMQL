package it.polimi.genomics.pythonapi.test

import java.util

import it.polimi.genomics.pythonapi.PythonManager
import it.polimi.genomics.spark.implementation.loaders.NarrowPeakParser

object Order {

  val pmg = PythonManager
  pmg.startEngine()
  val opmg = pmg.getOperatorManager

  def main(args: Array[String]): Unit = {
    val d = pmg.read_dataset("fake", NarrowPeakParser)
    val metaOrdering = new java.util.ArrayList[String](util.Arrays.asList("a", "b", "c"))
    val metaAscending = new util.ArrayList[Boolean](util.Arrays.asList(false, true, false))
    opmg.order(d, Some(metaOrdering), Some(metaAscending), None, None, None, None, None, None)
  }
}
