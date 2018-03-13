package it.polimi.genomics.profiling.Profilers

import it.polimi.genomics.core.DataStructures.{Feature, GMQLDatasetProfile, GMQLSampleStats}
import it.polimi.genomics.core.DataTypes._
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.slf4j.LoggerFactory

import scala.collection.Map
import scala.xml.Elem

/**
  * Created by andreagulino on 10/04/17.*
  */

object Profiler extends java.io.Serializable {

  final val logger = LoggerFactory.getLogger(this.getClass)

  /**
    * Get an XML representation of the profile for the web interface (partial features)
    *
    * @param profile
    * @return
    */
  def profileToWebXML(profile: GMQLDatasetProfile): Elem = {

    <dataset>
      <feature name="Number of samples">{profile.get(Feature.NUM_SAMP)}</feature>
      <feature name="Number of regions">{profile.get(Feature.NUM_REG)}</feature>
      <feature name="Average region length">{profile.get(Feature.AVG_REG_LEN)}</feature>
      <samples>
        { profile.samples.sortBy(x => x.name).map(y =>
        <sample name={y.name}>
          <feature name="Number of regions">{y.get(Feature.NUM_REG)}</feature>
          <feature name="Average region length">{y.get(Feature.AVG_REG_LEN)}</feature>
        </sample>
      )
        }
      </samples>
    </dataset>

  }

  /**
    * Get an XML representation of the profile for optimization (full features)
    *
    * @param profile
    * @return
    */
  def profileToOptXML(profile: GMQLDatasetProfile): Elem = {

    <dataset>
      {profile.stats.map(x => <feature name={x._1}>{x._2}</feature>)}
      <samples>
        { profile.samples.sortBy(x => x.name).map(y =>
        <sample id={y.ID} name={y.name}>
          {y.stats.map(z => <feature name={z._1}>{z._2}</feature>)}
        </sample>)
        }
      </samples>
    </dataset>

  }

  case class ProfilerValue(leftMost: Long, rightMost: Long, minLength: Long, maxLength: Long, sumLength: Long, sumLengthOfSquares: Long, count: Long)

  case class ProfilerResult(leftMost: Long, rightMost: Long, minLength: Long, maxLength: Long, count: Long, avgLength: Double, varianceLength: Double)


  val reduceFunc: ((ProfilerValue, ProfilerValue) => ProfilerValue) = {
    case (l: ProfilerValue, r: ProfilerValue) =>
      ProfilerValue(
        math.min(l.leftMost, r.leftMost),
        math.max(l.rightMost, r.rightMost),
        math.min(l.minLength, r.minLength),
        math.max(l.maxLength, r.maxLength),
        l.sumLength + r.sumLength,
        l.sumLengthOfSquares + r.sumLengthOfSquares,
        l.count + r.count
      )
  }

  val calculateResult: (ProfilerValue => ProfilerResult) = { profile =>
    val mean = profile.sumLength.toDouble / profile.count
    val variance = profile.sumLengthOfSquares.toDouble / profile.count - mean * mean
    ProfilerResult(profile.leftMost, profile.rightMost, profile.minLength, profile.maxLength, profile.count, mean, variance)
  }

  /**
    * Profile a dataset providing the RDD representation of
    *
    * @param regions
    * @param meta
    * @param sc Spark Contxt
    * @return the profile object
    */
  //TODO remove meta and make name mandatory
  def profile(regions: RDD[GRECORD], meta: RDD[(Long, (String, String))], sc: SparkContext, namesOpt: Option[Map[Long, String]] = None): GMQLDatasetProfile = {

    //TODO move to profile function
    val names = {
      namesOpt.getOrElse {
        val outSample = s"S_%05d"
        val ids: Array[Long] = meta.keys.distinct().collect().sorted
        val newIDS: Map[Long, String] = ids.zipWithIndex.map(s => (s._1, outSample.format(s._2))).toMap
        val bc = sc.broadcast(newIDS)
        val res = bc.value
        bc.unpersist()
        res
      }
    }

    if (names.isEmpty) {
      logger.warn("Samples set is empty, returning.")
      GMQLDatasetProfile(List())
    } else {
      //remove the one that doesn't have corresponding meta
      val filtered = regions.filter(x => names.contains(x._1.id))

      //if we need chromosome
      val mappedSampleChrom = filtered
        .map { x =>
          val gRecordKey = x._1
          val distance = gRecordKey.stop - gRecordKey.start
          val profiler = ProfilerValue(gRecordKey.start, gRecordKey.stop, distance, distance, distance, distance * distance, 1)
          ((gRecordKey.id, gRecordKey.chrom), profiler)
        }


      val reducedSampleChrom = mappedSampleChrom.reduceByKey(reduceFunc)

      val mappedSample = reducedSampleChrom
        .map { x: ((Long, String), ProfilerValue) =>
          (x._1._1, x._2)
        }

      val reducedSample = mappedSample.reduceByKey(reduceFunc)


      val resultSamples: Map[Long, ProfilerValue] = reducedSample.collectAsMap()

      val resultSamplesToSave = resultSamples.map { inp => (inp._1, calculateResult(inp._2)) }



      val resultDsToSave = calculateResult(resultSamples.values.reduce(reduceFunc))


      def numToString(x: Double): String = {
        if (x % 1 == 0) {
          "%.0f".format(x)
        } else {
          "%.2f".format(x)
        }
      }

      logger.info("Profiling " + names.size + " samples.")

      val sampleProfiles = resultSamplesToSave.map { case (sampleId: Long, profile: ProfilerResult) =>

        val sample = GMQLSampleStats(ID = sampleId.toString)
        sample.name = names(sampleId)

        sample.stats_num += Feature.NUM_SAMP.toString -> 1.0

        sample.stats_num += Feature.NUM_REG.toString -> profile.count
        sample.stats_num += Feature.AVG_REG_LEN.toString -> profile.avgLength
        sample.stats_num += Feature.MIN_COORD.toString -> profile.leftMost
        sample.stats_num += Feature.MAX_COORD.toString -> profile.rightMost

        sample.stats_num += Feature.MIN_LENGTH.toString -> profile.minLength
        sample.stats_num += Feature.MAX_LENGTH.toString -> profile.maxLength
        sample.stats_num += Feature.VARIANCE_LENGTH.toString -> profile.varianceLength

        sample.stats = sample.stats_num.map(x => (x._1, numToString(x._2)))

        sample
      }

      val dsProfile = GMQLDatasetProfile(samples = sampleProfiles.toList)

      dsProfile.stats += Feature.NUM_SAMP.toString -> names.size.toString

      dsProfile.stats += Feature.NUM_REG.toString -> numToString(resultDsToSave.count)
      dsProfile.stats += Feature.AVG_REG_LEN.toString -> numToString(resultDsToSave.avgLength)
      dsProfile.stats += Feature.MIN_COORD.toString -> numToString(resultDsToSave.leftMost)
      dsProfile.stats += Feature.MAX_COORD.toString -> numToString(resultDsToSave.rightMost)

      dsProfile.stats += Feature.MIN_LENGTH.toString -> numToString(resultDsToSave.minLength)
      dsProfile.stats += Feature.MAX_LENGTH.toString -> numToString(resultDsToSave.maxLength)
      dsProfile.stats += Feature.VARIANCE_LENGTH.toString -> numToString(resultDsToSave.varianceLength)

      dsProfile

    }

  }

}

