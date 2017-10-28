package it.polimi.genomics.profiling.Profilers

import it.polimi.genomics.core.DataTypes._
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext}
import it.polimi.genomics.profiling.Profiles.{GMQLDatasetProfile, GMQLSampleStats}

import scala.collection.Map
import scala.collection.mutable.ListBuffer
import scala.xml.Elem

/**
  * Created by andreagulino on 10/04/17.*
  */

object Profiler extends java.io.Serializable {

  /**
    * Get an XML representation of the profile for the web interface (partial features)
    * @param profile
    * @return
    */
  def profileToWebXML(profile:GMQLDatasetProfile) : Elem = {

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
    * @param profile
    * @return
    */
  def profileToOptXML(profile: GMQLDatasetProfile) : Elem = {

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


  /**
    * Profile a dataset providing the RDD representation of
    * @param regions
    * @param meta
    * @param sc Spark Contxt
    * @return the profile object
    */
  def profile(regions:RDD[GRECORD], meta:RDD[(Long, (String, String))], sc: SparkContext, names:Option[Map[Long,String]] = None): GMQLDatasetProfile = {

    // GRECORD    =  (GRecordKey,Array[GValue])
    // GRecordKey =  (id, chrom, start, stop, strand)
    // data       =  (id , (chr, width, start, stop, strand) )

    val data: RDD[ ( Long, (String, Long, Long, Long, Char) ) ] =
      regions.map(
        x => (x._1._1, (x._1._2, x._1._4 - x._1._3, x._1._3, x._1._4, x._1._5))
      )

    val samples:List[Long] = data.keys.distinct().collect().toList

    val Ids = meta.keys.distinct()
    val newIDS: Map[Long, Long] = Ids.zipWithIndex().collectAsMap()
    val newIDSbroad = sc.broadcast(newIDS)

    // Counting regions in each sample
    val counts: Map[Long, Long] = getRegionsCount(data)

    // Averaging region length
    val avg: Map[Long, Double] = getRegionsAvgWidth(data)


    val minmax = getMinMax(data)


    var sampleProfiles: ListBuffer[GMQLSampleStats] = ListBuffer[GMQLSampleStats]()


    def numToString(x:Double): String = {
      if(x%1==0) {
        return "%.0f".format(x)
      } else {
        return "%.2f".format(x)
      }
    }

    samples.foreach( x => {

      val sample = GMQLSampleStats(ID = x.toString)
      if( !names.isDefined ) {
        sample.name = "S" + "%05d".format(newIDSbroad.value.get(x).getOrElse(x))
      } else {
        sample.name = names.get.get(x).get
      }
      sample.stats_num   += Feature.NUM_REG.toString      -> counts(x)
      sample.stats_num   += Feature.AVG_REG_LEN.toString  -> avg(x)
      sample.stats_num   += Feature.MIN_COORD.toString    -> minmax(x)._1
      sample.stats_num   += Feature.MAX_COORD.toString    -> minmax(x)._2

      sample.stats = sample.stats_num.map(x => (x._1, numToString(x._2)))

      sampleProfiles += sample

    })

    val dsprofile    = new GMQLDatasetProfile(samples = sampleProfiles.toList)



    val totReg = sampleProfiles.map(x=>x.stats_num.get(Feature.NUM_REG.toString).get).reduce((x,y)=>x+y)
    val sumAvg = sampleProfiles.map(x=>x.stats_num.get(Feature.AVG_REG_LEN.toString).get).reduce((x,y)=>x+y)
    val totAvg = sumAvg/samples.size


    dsprofile.stats += Feature.NUM_SAMP.toString     -> samples.size.toString
    dsprofile.stats += Feature.NUM_REG.toString      -> numToString(totReg)
    dsprofile.stats += Feature.AVG_REG_LEN.toString  -> numToString(totAvg)

    dsprofile

  }

  /**
    * Returns the number of regions for each sample
    * @param data  (id  , ( chr, width , strand) )
    * @return
    */
  private def getRegionsCount(data:RDD[(Long,(String,Long,Long, Long,Char))])  : Map[Long, Long]  = data.countByKey()

  /**
    * Get, for each sample, the average length of the regions contained in it
    * @param data
    * @return
    */
  private def getRegionsAvgWidth( data: RDD[(Long,(String,Long,Long, Long,Char))]) : Map[Long, Double] =
  {


    // (sample_id, (chr, width, start, stop, str)) = data

    val pair: RDD[(Long, Long)] = data.map(x => (x._1, x._2._2))


    val createSumsCombiner: (Long) => (Double, Long) = (width: Long) => (1, width)

    val sumCombiner = ( collector: (Double, Long),  width: Long) => {
      val (numberElems, totalSum) = collector
      //println("total sum :"+totalSum+" number:"+numberElems);
      (numberElems + 1, totalSum + width)
    }

    val sumMerger   = ( collector1: (Double, Long), collector2: (Double, Long)) => {
      val (numSums1, totalSums1) = collector1
      //print("num1: "+numSums1+" total1: "+totalSums1)

      val (numSums2, totalSums2) = collector2
      //print("num1: "+numSums2+" total1: "+totalSums2)

      (numSums1 + numSums2, totalSums1 + totalSums2)
    }

    val averagingFunction = (regionSum: (Long, (Double, Long)) ) => {
      val (id, (numberScores, totalScore)) = regionSum
      //println("finalnum: "+numberScores+" finaltotal: "+totalScore)
      (id, totalScore / numberScores)
    }

    val scores = pair.combineByKey(createSumsCombiner, sumCombiner, sumMerger)

    val averages: Map[Long, Double] = scores.collectAsMap().map(averagingFunction)

    averages

  }

  /**
    * Get, for each sample, minimum and maximum region coordinates
    * @param data
    * @return
    */
  private def getMinMax( data: RDD[ (Long,(String,Long,Long,Long,Char))] ) : Map[Long, (Long, Long)] =
  {

    val pair: RDD[(Long, (Long, Long))] = data.map(x => (x._1, (x._2._3, x._2._4)))

    val createSumsCombiner: ((Long, Long)) => (Long, Long) =
      x => (x._1, x._2)

    val sumCombiner = (collector: (Long, Long), coords: (Long, Long)) => {
      val (min, max) = collector
      //println("min :"+min+" max:"+max);
      (math.min(min, coords._1), math.max(max, coords._2))
    }

    val sumMerger = (collector1: (Long, Long), collector2: (Long, Long)) => {
      val (min1, max1) = collector1
      val (min2, max2) = collector2

      (math.min(min1, min2), math.max(max1, max2))
    }


    val scores = pair.combineByKey(createSumsCombiner, sumCombiner, sumMerger)

    val averages: Map[Long, (Long, Long)] = scores.collectAsMap()

    averages
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