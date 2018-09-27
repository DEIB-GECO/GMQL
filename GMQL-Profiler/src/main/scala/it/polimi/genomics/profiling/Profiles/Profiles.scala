package it.polimi.genomics.profiling.Profiles



import it.polimi.genomics.profiling.Profilers.Feature.Feature

import scala.collection.Map


/**
  *
  *  Set of profiling information needed to be collected for every dataset.
  *
  * @param samples
  */
case class GMQLDatasetProfile( samples:List[(GMQLSampleStats)] ) {
  var stats: Map[String, String] = Map[String, String]()

  def get(feature : Feature): String = {
    val v = stats.get(feature.toString)
    if (v.isDefined)  v.get else "undefined"
  }
}


/**
  *  GMQLSample is an abstraction  of the path of the sample and its metadata along with the ID
  * @param ID Integer of the id of the sample
  */
case class GMQLSampleStats(ID:String) {
  var name:String="nothing"
  var stats: Map[String, String]   = Map[String, String]()
  var stats_num: Map[String, Double] = Map[String, Double]()

  def get(feature : Feature): String = {
    val v = stats.get(feature.toString)
    if (v.isDefined)  v.get else "undefined"
  }
}