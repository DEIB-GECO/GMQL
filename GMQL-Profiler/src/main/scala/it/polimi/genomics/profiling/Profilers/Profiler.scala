package it.polimi.genomics.profiling.Profilers

import it.polimi.genomics.core.DataTypes._
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import com.google.common.hash._
import it.polimi.genomics.profiling.Profiles.{GMQLDatasetProfile, GMQLSampleStats}

import scala.collection.Map
import scala.collection.mutable.ListBuffer
import scala.xml.Elem

/**
  * Created by andreagulino on 10/04/17.*
  */

object Profiler extends java.io.Serializable {

//  def profile(datasetpath: String): Status.Value = {
//
//    val fs: FileSystem = FileSystem.get(FS_Utilities.gethdfsConfiguration())
//
//    val sc = Spark.sc
//    val path   = new Path(datasetpath)
//
//    // regions files are those files with filename s.t. exists a filename.meta
//    val selectedURIs: List[String] =  fs.listStatus(path,
//      new PathFilter {
//        override def accept(path: Path): Boolean = {
//          fs.exists(new Path(path.toString+".meta"))
//        }
//    }).map(x=>x.getPath.toString).toList
//
//
//    // [sample_id : sample_name]
//    val samples : Map[String, String] =
//      selectedURIs.map(
//        x => { val name  = new Path(x) getName
//               val next = name.substring(0, name.lastIndexOf("."))
//               (getSampleID(name).toString, name) }
//      ).toMap[String, String]
//
//
//    def parser(x: (Long, String)) = {
//      BasicParser.asInstanceOf[GMQLLoader[(Long, String), Option[GRECORD], (Long, String), Option[MetaType]]].region_parser(x)
//    }
//
//    // Get the dataset as an RDD
//    val dataset = sc forPath selectedURIs.mkString(",") LoadRegionsCombineFiles(parser)
//
//
//    val startTime = System.currentTimeMillis()
//    println("# Started timer")
//
//    // Profile the dataset
//    val dsprofile: GMQLDatasetProfile = profile(dataset, sc)
//
//    // Get ds size from fs and add it to the profile
//    val size: Long = fs.getContentSummary(path).getSpaceConsumed()
//    dsprofile.stats  += "size" -> size.toString
//
//    // Bind name to sample id
//    dsprofile.samples.foreach( x => x.name = samples(x.ID))
//
//    val elapsedTime = (System.currentTimeMillis() - startTime)/1000
//
//    // Add profiling time
//    dsprofile.stats  += "profiling_time" -> elapsedTime.toString
//
//    println("\n\n\n### Profiling completed in "+elapsedTime+" seconds.")
//
//    // Store the profile
//    store(dsprofile, datasetpath)
//
//    Status.PENDING
//  }


  def profileToWebXML(profile:GMQLDatasetProfile) : Elem = {

    <dataset>
      <feature name="Number of samples">{profile.get(Feature.NUM_SAMP)}</feature>
      <feature name="Number of regions">{profile.get(Feature.NUM_REG)}</feature>
      <feature name="Average region length">{profile.get(Feature.AVG_REG_LEN)}</feature>
      <samples>
        { profile.samples.map(y =>
        <sample name={y.name}>
          <feature name="Number of regions">{y.get(Feature.NUM_REG)}</feature>
          <feature name="Average region length">{y.get(Feature.AVG_REG_LEN)}</feature>
        </sample>
      )
        }
      </samples>
    </dataset>

  }


  def profileToOptXML(profile: GMQLDatasetProfile) : Elem = {

    <dataset>
      {profile.stats.map(x => <feature name={x._1}>{x._2}</feature>)}
              <samples>
                { profile.samples.map(y =>
                  <sample id={y.ID} name={y.name}>
                    {y.stats.map(z => <feature name={z._1}>{z._2}</feature>)}
                  </sample>)
                }
              </samples>
    </dataset>

  }


  /**
    * Profile a dataset providing the RDD representation of
    * @param dataset
    * @param sc Spark Contxt
    * @return the profile object
    */
  def profile(dataset:RDD[GRECORD], sc: SparkContext): GMQLDatasetProfile = {

    // GRECORD    =  (GRecordKey,Array[GValue])
    // GRecordKey =  (id, chrom, start, stop, strand)
    // data       =  (id , (chr, width, start, stop, strand) )

    val data: RDD[ ( Long, (String, Long, Long, Long, Char) ) ] =
    dataset.map(
      x => (x._1._1, (x._1._2, x._1._4 - x._1._3, x._1._3, x._1._4, x._1._5))
    )

    val samples:List[Long] = data.keys.distinct().collect().toList

    // Counting regions in each sample
    val counts: Map[Long, Long] = getRegionsCount(data)

    // Averaging region length
    val avg: Map[Long, Double] = getRegionsAvgWidth(data)


    val minmax = getMinMax(data)


    var sampleProfiles: ListBuffer[GMQLSampleStats] = ListBuffer[GMQLSampleStats]()


    def longToString(x:Double): String = {
      return "%.2f".format(x)
    }

    samples.foreach( x => {

      val sample = GMQLSampleStats(ID = x.toString)
      sample.stats_num   += Feature.NUM_REG.toString      -> counts(x)
      sample.stats_num   += Feature.AVG_REG_LEN.toString  -> avg(x)
      sample.stats_num   += Feature.MIN_COORD.toString    -> minmax(x)._1
      sample.stats_num   += Feature.MAX_COORD.toString    -> minmax(x)._2

      sample.stats = sample.stats_num.map(x => (x._1, longToString(x._2)))

      sampleProfiles += sample

    })

    val dsprofile    = new GMQLDatasetProfile(samples = sampleProfiles.toList)



    val totReg = sampleProfiles.map(x=>x.stats_num.get(Feature.NUM_REG.toString).get).reduce((x,y)=>x+y)
    val sumAvg = sampleProfiles.map(x=>x.stats_num.get(Feature.AVG_REG_LEN.toString).get).reduce((x,y)=>x+y)
    val totAvg = sumAvg/samples.size


    dsprofile.stats += Feature.NUM_SAMP.toString         -> samples.size.toString
    dsprofile.stats += Feature.NUM_REG.toString      -> longToString(totReg)
    dsprofile.stats += Feature.AVG_REG_LEN.toString  -> longToString(totAvg)

    dsprofile

  }


//  def store(profile: GMQLDatasetProfile, outputPath: String): Unit = {
//
//    val fs: FileSystem = FileSystem.get(FS_Utilities.gethdfsConfiguration())
//
//    val xml =
//      <GMQLDatasetProfile>
//        {profile.stats.map(x => <property name={x._1}>{x._2}</property>)}
//        <samples>
//          { profile.samples.map(y =>
//            <sample id={y.ID} name={y.name}>
//              {y.stats.map(z => <property name={z._1}>{z._2}</property>)}
//            </sample>)
//          }
//        </samples>
//      </GMQLDatasetProfile>
//
//    // Store the xml to hdfs in the dataset folder
//    val xmlfile = fs.create(new Path(outputPath+"/profile.xml"))
//    xmlfile.writeChars(xml.toString().trim)
//    xmlfile.close()
//
//    println("\n\n\n ### Profile stored in : "+outputPath)
//
//  }

  /**
    * Get the sample ID provided its file name
    * @param fileName
    * @return
    */
  private def getSampleID(fileName: String) : Long = {
    Hashing.md5().newHasher().putString(fileName.replaceAll("/",""),java.nio.charset.StandardCharsets.UTF_8).hash().asLong()
  }


  /**
    * Returns the number of regions for each sample
    * @param data  (id  , ( chr, width , strand) )
    * @return
    */
  private def getRegionsCount(data:RDD[(Long,(String,Long,Long, Long,Char))])  : Map[Long, Long] = {
    val ret: Map[Long, Long] = data.countByKey()
    ret
  }

  /**
    * Returns the the average region length for each sample
    * @param data
    * @return
    */
//  private def getRegionsAvgWidth(data:RDD[(Long,(String ,Long,Char))]) : Map[Long, Double] = {
//
//    val drdd = new DoubleRDDFunctions( data.map(x => x._2._2) )
//
//    val mean: Map[Long, Double] =  Map[Long,Double]((0.toLong, drdd.mean()))
//
//    mean
//  }

  private def getRegionsAvgWidth( data: RDD[(Long,(String,Long,Long, Long,Char))]) : Map[Long, Double] = {


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

object Spark {
  val conf = new SparkConf()
    .setAppName("GMQL V2 Spark")
    .setMaster("local[*]")
    .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .set("spark.kryoserializer.buffer", "64")
    .set("spark.driver.allowMultipleContexts","true")
    .set("spark.sql.tungsten.enabled", "true")

  var sc: SparkContext =  new SparkContext(conf)
}