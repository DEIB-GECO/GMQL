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
      <feature name="Number of samples">{profile.getString(Feature.NUM_SAMP)}</feature>
      <feature name="Number of regions">{profile.getString(Feature.NUM_REG)}</feature>
      <feature name="Average region length">{profile.getString(Feature.AVG_REG_LEN)}</feature>
      <samples>
        { profile.samples.sortBy(x => x.name).map(y =>
        <sample name={y.name}>
          <feature name="Number of regions">{y.getString(Feature.NUM_REG)}</feature>
          <feature name="Average region length">{y.getString(Feature.AVG_REG_LEN)}</feature>
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
      {profile.statsString.map(x => <feature name={x._1.toString}>{x._2}</feature>)}
      <samples>
        { profile.samples.sortBy(x => x.name).map(y =>
        <sample id={y.ID.toString} name={y.name}>
          {y.statsString.map(z => <feature name={z._1.toString}>{z._2}</feature>)}
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
      GMQLDatasetProfile.getEmpty
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

      val resultSampleChrom: Map[(Long, String), ProfilerValue] = reducedSampleChrom.collectAsMap()


      val resultSampleChromToSave: Map[(Long, String), ProfilerResult] = resultSampleChrom.map { inp => (inp._1, calculateResult(inp._2)) }


      val mappedSample = reducedSampleChrom
        .map { x: ((Long, String), ProfilerValue) =>
          (x._1._1, x._2)
        }

      val reducedSample: RDD[(Long, ProfilerValue)] = mappedSample.reduceByKey(reduceFunc)


      val resultSamples: Map[Long, ProfilerValue] = reducedSample.collectAsMap()

      val resultSamplesToSave = resultSamples.map { inp => (inp._1, calculateResult(inp._2)) }



      val resultDsToSave = calculateResult(resultSamples.values.reduce(reduceFunc))


      logger.info("Profiling " + names.size + " samples.")

      val sampleProfiles = resultSamplesToSave.map { case (sampleId: Long, profile: ProfilerResult) =>

        val sample = new GMQLSampleStats(sampleId)
        sample.setName(names(sampleId))

        sample.set( Feature.NUM_SAMP, 1)
              .set( Feature.NUM_REG, profile.count)
              .set( Feature.AVG_REG_LEN, profile.avgLength)
              .set( Feature.MIN_COORD, profile.leftMost)
              .set( Feature.MAX_COORD , profile.rightMost)
              .set( Feature.MIN_LENGTH, profile.minLength)
              .set( Feature.MAX_LENGTH, profile.maxLength)
              .set( Feature.VARIANCE_LENGTH, profile.varianceLength)

      }

      val dsProfile = new GMQLDatasetProfile(sampleProfiles.toList)

      dsProfile.set( Feature.NUM_SAMP, names.size)
               .set( Feature.NUM_REG, resultDsToSave.count)
               .set( Feature.AVG_REG_LEN, resultDsToSave.avgLength)
               .set( Feature.MIN_COORD, resultDsToSave.leftMost)
               .set( Feature.MAX_COORD, resultDsToSave.rightMost)
               .set( Feature.MIN_LENGTH, resultDsToSave.minLength)
               .set( Feature.MAX_LENGTH, resultDsToSave.maxLength)
               .set( Feature.VARIANCE_LENGTH, resultDsToSave.varianceLength)
    }

  }

}

