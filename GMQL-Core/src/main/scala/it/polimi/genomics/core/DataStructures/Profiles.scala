package it.polimi.genomics.core.DataStructures

import it.polimi.genomics.core.DataStructures.Feature.Feature
import scala.collection.Map


class ExecutionProfile( user: String,
                        queryname: String,
                        query: List[IRVariable],
                        inputDatasets: Map[String, GMQLDatasetProfile],
                        rootTimestamp: Long ){

  var profileDAG : List[IntermediateProfile] = _


}


abstract class IntermediateProfile( operator: IROperator,
                                //outputProfile: GMQLDatasetProfile,
                                leftProfile: IntermediateProfile,
                                rightProfile: Option[IntermediateProfile],
                                initialTimestamp: Long,
                                finalTimestamp: Long,
                                rootTimestamp: Long) {

  val executionTime: Long = initialTimestamp - rootTimestamp
  val profilingTime: Long = finalTimestamp - initialTimestamp

  var operatorParams: Map[String, String] = Map()

  if( operator.isInstanceOf[IRSelectRD] ){
    val rtOp = operator.asInstanceOf[IRSelectRD]
    rtOp.reg_cond.getOrElse("NULL")
    operatorParams = operatorParams + ("reg_cond"->rtOp.reg_cond.getOrElse("NULL").toString )
   // operatorParams = operatorParams + ("reg_cond"->rtOp.filtered_meta.get.)

  }

  def getOutputProfile

}




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

object Feature extends Enumeration {
  type Feature = Value
  val NUM_SAMP:    Feature.Value = Value("num_samp")
  val NUM_REG:     Feature.Value = Value("num_reg")
  val AVG_REG_LEN: Feature.Value = Value("avg_reg_length")
  val MIN_COORD:   Feature.Value = Value("min")
  val MAX_COORD:   Feature.Value = Value("max")
}