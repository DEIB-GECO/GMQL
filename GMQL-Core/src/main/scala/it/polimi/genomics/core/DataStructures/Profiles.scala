package it.polimi.genomics.core.DataStructures

import it.polimi.genomics.core.DataStructures.Feature.Feature
import org.slf4j.LoggerFactory

import scala.collection.Map
import scala.xml._

/**
  *
  *  Set of profiling information needed to be collected for every dataset.
  *
  * @param samples
  */
class GMQLDatasetProfile( val samples:List[(GMQLSampleStats)] ) {

  private var _stats: Map[Feature, Double] = Map[Feature, Double]()

  def stats = _stats
  def statsString = _stats.map( x => (x._1, numToString(x._2)))

  def get(feature: Feature): Double = {
    val v = stats.get(feature)
    if (v.isDefined) v.get else 0
  }

  def getString(feature : Feature): String = {
    val v = stats.get(feature)
    if (v.isDefined)  numToString(v.get) else "undefined"
  }

  def set(feature: Feature, value: Double): GMQLDatasetProfile = {
    _stats += feature -> value
    this
  }

  private def numToString(x: Double): String = {
    if (x % 1 == 0) {
      "%.0f".format(x)
    } else {
      "%.2f".format(x)
    }
  }


}

object GMQLDatasetProfile {

  final val logger = LoggerFactory.getLogger(this.getClass)

  def getEmpty = new GMQLDatasetProfile(List())

  /**
    * Parse profile.xml, possibly filtering by sample id
    * @param xml
    * @param samples
    * @return
    */
  def fromXML(xml : Elem, samples: Map[String, Long]): GMQLDatasetProfile = {

    var xmlSamples = (xml \\ "dataset" \\ "samples" \\"sample").filter((x: Node) => {
      samples.keys.toList.contains(x.attribute("name").get.text)
    })

    val sampleProfiles = xmlSamples.map (xmlSample => {

      val name = xmlSample.attribute("name").get.text
      val id = samples(name)

      val sample = new GMQLSampleStats(id)
      sample.setName(name)

      xmlSamples \\ "feature" map( feature => {
        sample.set(Feature.withName( feature.attribute("name").get.text), feature.text.toDouble)
      })

      sample

    }).toList

    new GMQLDatasetProfile(sampleProfiles)
  }
}

/**
  *  GMQLSample is an abstraction  of the path of the sample and its metadata along with the ID
  * @param ID Integer of the id of the sample
  */
class GMQLSampleStats(val ID:Long) {

  private var _name:String="nothing"
  private var _stats: Map[Feature, Double]   = Map[Feature, Double]()

  def name  = _name
  def stats = _stats
  def statsString = _stats.map( x => (x._1, numToString(x._2)))


  def get(feature: Feature): Double = {
    val v = stats.get(feature)
    if (v.isDefined) v.get else 0
  }

  def getString(feature : Feature): String = {
    val v = stats.get(feature)
    if (v.isDefined)  numToString(v.get) else "undefined"
  }

  def setName(name: String) = _name = name

  def set(feature: Feature, value: Double): GMQLSampleStats = {
    _stats += feature -> value
    this
  }

  private def numToString(x: Double): String = {
    if (x % 1 == 0) {
      "%.0f".format(x)
    } else {
      "%.2f".format(x)
    }
  }
}

object Feature extends Enumeration {
  type Feature = Value

  val NUM_SAMP: Feature.Value = Value("num_samp")
  val NUM_REG: Feature.Value = Value("num_reg")
  val AVG_REG_LEN: Feature.Value = Value("avg_reg_length")
  val MIN_COORD: Feature.Value = Value("min")
  val MAX_COORD: Feature.Value = Value("max")

  val MIN_LENGTH: Feature.Value = Value("min_length")
  val MAX_LENGTH: Feature.Value = Value("max_length")
  val VARIANCE_LENGTH: Feature.Value = Value("variance_length")
}