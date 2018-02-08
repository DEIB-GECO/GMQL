package it.polimi.genomics.compiler


import it.polimi.genomics.GMQLServer.GmqlServer
import it.polimi.genomics.core.GMQLSchemaFormat
import it.polimi.genomics.spark.implementation.GMQLSparkExecutor
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.LoggerFactory

import scala.util.Random

/**
 * Created by pietro on 17/06/15.
 */
sealed trait Execution
case class FlinkExectution() extends Execution
case class SparkExecution() extends Execution
case class GMQLConfig(exec : Either[String, Execution] = Left(""),
                      query_path: Option[String] = None,
                      bin_size : Option[Long] = Some(5000))

object gmqlc {


  private final val logger = LoggerFactory.getLogger(gmqlc.getClass)

  def main (args: Array[String]) {


    /*val parser = new scopt.OptionParser[GMQLConfig]("gmqlc") {
      head("GMQLC", "2.x")

      opt[String]('x', "exec") required() valueName "<FLINK|SPARK>" action { (x, c) =>
        x match {
          case "FLINK" => c.copy (exec = Right(FlinkExectution()))
          case "SPARK" => c.copy (exec = Right(SparkExecution()))
          case _ => c.copy(exec = Left(x))
        }
      } text("Execution platform on which run the query")

      opt[String]('p',"path") required() valueName "<file>" action { (x,c) =>
        c.copy(query_path = Some(x))
      } text ("path to the query file")

      opt[Long]('b',"bin_size") valueName "<N>" action { (x,c) =>
        c.copy(bin_size = Some(x))
      } text ("lenght of the bins")


      help("help") text("prints this usage text")

      checkConfig {c=>
        c match {
          case GMQLConfig(Left(s),_,_) => failure("Invalid exec option: \"" + s + "\"")
          case _ => success
        }
      }
    }

    parser.parse(args, GMQLConfig()) match {
      case Some(config) => {

        val query = try {
          scala.io.Source.fromFile(config.query_path.get).mkString.replaceAll("\\s+$", "")
        } catch {
          case e:Exception => {
            println("Couldn't open " + config.query_path.get)
            System.exit(-1)
          }
        }


        val server = config.exec match {
          case Right(SparkExecution()) => new GmqlServer(new GMQLSparkExecutor(testingIOFormats = true), config.bin_size)
          case Right(FlinkExectution()) => new GmqlServer(new FlinkImplementation(testingIOFormats = true), config.bin_size)
        }

        val translator = new Translator(server, "/some/path/")
        try {
          if (translator.phase2(translator.phase1(query.asInstanceOf[String]))) {
            server.run()
          }
        } catch {
          case e: CompilerException => {
            println(e.getMessage)
            System.exit(-1)
          }
        }
      }

      case None => System.exit(-1)
    }*/

    val conf = new SparkConf()
      .setAppName("GMQL V2 Spark")
      //    .setSparkHome("/usr/local/Cellar/spark-1.5.2/")
//      .setMaster("local[*]")
      //    .setMaster("yarn-client")
      //    .set("spark.executor.memory", "1g")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryoserializer.buffer", "256")
      .set("spark.driver.allowMultipleContexts","true")
//      .set("spark.sql.tungsten.enabled", "true")
    val sc:SparkContext =new SparkContext(conf)
    val server = new GmqlServer(new GMQLSparkExecutor(testingIOFormats = false,sc=sc,outputFormat = GMQLSchemaFormat.TAB), Some(1000)/*Some(args(3).toInt)*/)
//    val server = new GmqlServer(new FlinkImplementation(), Some(10000))
    val translator = new Translator(server, "/Users/pietro/Desktop/test_gmql/testout/")

      val path = "/Users/pietro/Desktop/test_gmql/queries/query"+".gmql"
      val query = "R = SELECT(NOT(leaveout==\"something\")) [RnaSeqParser] /Users/abdulrahman/Downloads/gmql_testing/annotations/; " +
        "E = SELECT(NOT(leaveout==\"something\")) [BedScoreParser] /Users/abdulrahman/Downloads/gmql_testing/beds/; " +
        "M = MAP(antibody,cell;) R E;" +
        "MATERIALIZE M into  /Users/abdulrahman/Polimi/trash/t1/;"//scala.io.Source.fromFile(path).mkString.replaceAll("\\s+$", "")

      val joinQuery="S = SELECT(NOT(leaveout==\"something\")) [BedScoreParser] /Users/abdulrahman/Desktop/join/beds/;" +
        "          A = SELECT(NOT(leaveout==\"something\")) [RnaSeqParser] /Users/abdulrahman/Desktop/join/annotations/;" +
        "          J = JOIN(antibody;distance < 1000; left) S A; " +
        "MATERIALIZE J into /Users/abdulrahman/Desktop/join/out;"

      val summit = " E = SELECT(NOT(leaveout==\"something\")) [BedScoreParser]/Users/abdulrahman/Desktop/summit/beds/;" +
        "C = SUMMIT(1,2) E;" +
        "MATERIALIZE C into /Users/abdulrahman/Desktop/summit/outS/;"

      val flat = "E = SELECT(NOT(leaveout==\"something\"))  [BedScoreParser]/Users/abdulrahman/Desktop/summit/beds/;" +
        "C = FLAT(1,2) E;" +
        "MATERIALIZE C into /Users/abdulrahman/Desktop/summit/flat/;"

      val testSpark = "S = SELECT(NOT(leaveout==\"something\")) [BedScoreParser] hdfs://localhost:9000/user/abdulrahman/regions/Users/abdulrahman/Desktop/join/beds/;\n" +
        "A = SELECT(NOT(leaveout==\"something\")) [RnaSeqParser] hdfs://localhost:9000/user/abdulrahman/regions/Users/abdulrahman/Desktop/join/annotations/;\n" +
        "J = MAP(antibody;) A S;\n" +
        "MATERIALIZE J into hdfs://localhost:9000/user/abdulrahman/regions/Users/abdulrahman/Desktop/out/;\n" +
        "MATERIALIZE A into hdfs://localhost:9000/user/abdulrahman/regions/Users/abdulrahman/Desktop/out1/;"

    val testMapSpark = "S = SELECT(NOT(leaveout==\"something\")) [narrowpeakparser] hdfs://genomic.elet.polimi.it:9000//user/abdulrahman/regions/home/abdulrahman/bedss//;\n" +
      "A = SELECT(NOT(leaveout==\"something\")) [annparser] hdfs://genomic.elet.polimi.it:9000/user/abdulrahman/regions/home/abdulrahman/annotationss/;\n" +
      "J = Cover(1,2) S;\n" +
      "MATERIALIZE J into hdfs://genomic.elet.polimi.it:9000/user/abdulrahman/regions/Users/abdulrahman/Desktop/out1111/;"

    val nature ="G1 = SELECT(tipo==\"geni\") [BroadProjParser] /Users/abdulrahman/Downloads/geni/;\n" +
      "G2 = SELECT(tipo==\"geni\") [BroadProjParser] /Users/abdulrahman/Downloads/geni/;\n" +
      "L = SELECT(tipo == \"junction\") [BasicParser] /Users/abdulrahman/Downloads/domini/;\n" +
      "COUPLES = JOIN(distance < 500000; CONTIG) G1 G2;\nMAPPING = MAP() COUPLES L;\n" +
      "MATERIALIZE MAPPING into /Users/abdulrahman/Desktop/g1/;"

    val order = " E = SELECT(NOT(leaveout==\"something\"))  /Users/abdulrahman/Downloads/ann/;\n" +
      "          S = EXTEND (MedianScore AS MEDIAN(score), MinScore as MIN(score)) E; \n" +
      "            MATERIALIZE S into outDSDesc;"
    val group = "DATA = SELECT(NOT(leaveout==\"something\"))[testorderparser] /Users/abdulrahman/Downloads/order/;\n" +
      "THEPOT = GROUP(antibody_target) DATA;\n" +
      "MATERIALIZE THEPOT into /Users/abdulrahman/Desktop/out2/;"

    val difference = "DATA = SELECT(cell == \"Urothelia\")[testorderparser] /Users/abdulrahman/Downloads/order/;\n" +
      "NEG = SELECT(lab == \"DUKE\") DATA;\n" +
      "REST = DIFFERENCE(lab) DATA NEG;\n" +
      "MATERIALIZE REST into /Users/abdulrahman/Downloads/res/;"

    val cover = "DATA = SELECT()  ann;\n" +
      "THECOVER = COVER(ALL,ANY;aggregate: AVG($1),SUM($1)) DATA;\n" +
      "MATERIALIZE DATA into annData/;\n" +
      "MATERIALIZE THECOVER into coverRes1/;"

    val merge = "DATA = SELECT()  ann;\n" +
      "THEmerge = MERGE() DATA;\n" +
      "MATERIALIZE THEmerge into mergeRes1/;"

    val histogram  = "DATA = SELECT(*)[customParser]  histo;\n" +
      "THECOVER = HISTOGRAM(1,ANY) DATA;\n" +
      "MATERIALIZE THECOVER into res111/;"

    val group1 = "DATA = SELECT(*)[CustomParser] ann;\n" +
      "MATERIALIZE DATA into res1111;"

    val Simone = "R = SELECT(*) liubaIn1;\n" +
      "M = COVER(GROUPBY cell, antibody_target; 1, ANY; AVG(signal)) R;\n" +
      "MATERIALIZE M into res;"

    val liuba = "HM_TF_PROMnot= SELECT() ann;\n" +
      "HM_TF_PROMnot_GENEonly = SELECT() beds;\n" +
      "HM_TF_PROMnot_GENEnot_0 = DIFFERENCE(JOINBY: antibody_target|ddd) HM_TF_PROMnot HM_TF_PROMnot_GENEonly;\n" +
      "MATERIALIZE HM_TF_PROMnot_GENEnot_0 into HM_TF_PROM;"

    val map =  "S = SELECT() [BedScoreParser] hdfs://ip-172-31-3-242.us-west-2.compute.internal:8020/user/hadoop/;\n" +
      "A = SELECT() [RnaSeqParser] hdfs://ip-172-31-3-242.us-west-2.compute.internal:8020/user/hadoop/DS1/annotations/;\n" +
      "J = MAP() A S;\n" +
      "MATERIALIZE J into hdfs://ip-172-31-3-242.us-west-2.compute.internal:8020/user/hadoop/newOut/;"

    val projectss = "D = SELECT() /Users/abka02/Downloads/job_meta_agg_abdo_20170619_134513_data/files/;\n" +
      "D1 = PROJECT(metadata_update: age AS 100) D;" +
      "\nMATERIALIZE D1 INTO D1;\n" +
      "D2 = PROJECT(metadata_update: age AS age + 100) D1;\n" +
      "MATERIALIZE D2 INTO D2;\n" +
      "D3 = PROJECT(metadata_update: age_plus AS age + 100) D;\n" +
      "MATERIALIZE D3 INTO D3;"

    val ran = Random.nextInt()
    val Histogram =  "S = SELECT(NOT(leaveout==\"something\");parser: BedScoreParser) hdfs://ip-172-31-12-101.us-west-2.compute.internal:8020/user/hadoop/"/*+args(0)*/+"/;\n" +
      "J = Histogram() S;\n" +
      "MATERIALIZE J into hdfs://ip-172-31-12-101.us-west-2.compute.internal:8020/user/hadoop/newOut"+ran+";"

//    val Map_server =  "S = SELECT(NOT(leaveout==\"something\");parser: BedScoreParser) hdfs://ip-172-31-5-18.us-west-2.compute.internal:8020/user/hadoop/"+args(0)+"/;\n" +
//      "A = SELECT(NOT(leaveout==\"something\");parser:RnaSeqParser) hdfs://ip-172-31-5-18.us-west-2.compute.internal:8020/user/hadoop/"+args(1)+"/;\n" +
//      "J = MAP() A S;\n" +
//      "MATERIALIZE J into hdfs://ip-172-31-5-18.us-west-2.compute.internal:8020/user/hadoop/newOut"+ran+";"

    val Map_server =  "S = SELECT(NOT(leaveout==\"something\");parser: NarrowPeakParser) hdfs://ip-172-31-5-18.us-west-2.compute.internal:8020/user/hadoop/"/*+args(0)*/+"/;\n" +
      "A = SELECT(NOT(leaveout==\"something\");parser:ANNParser) hdfs://ip-172-31-5-18.us-west-2.compute.internal:8020/user/hadoop/"/*+args(1)*/+"/;\n" +
      "J = MAP() A S;\n" +
      "MATERIALIZE J into hdfs://ip-172-31-5-18.us-west-2.compute.internal:8020/user/hadoop/newOut"+ran+";"

    val orders = "EXP = SELECT() data;\n" +
      "OUT = ORDER(region_order: signal, pvalue; region_topg: 7) EXP;\n " +
      "MATERIALIZE OUT into GH;"

    val select = "EXP = SELECT() project1;\n" +
      "MATERIALIZE EXP into outoo;"

    val cover_ALL = "RAW = SELECT(ibody == 'ATF3') cover_in;\n" +
//      "HISTO_TEST = COVER_HISTOGRAM(ALL,ALL;GROUPBY: cell) RAW;\n" +
      "MATERIALIZE RAW into histo_res;\n"

    val union = " TEAD4_rep_narrow = SELECT() /Users/abdulrahman/Desktop/ann/;\n " +
      "TEAD4_rep_broad = SELECT() /Users/abdulrahman/Desktop/exp/;\n  " +
      "TEAD4_rep = UNION()  TEAD4_rep_narrow  TEAD4_rep_broad;\n  " +
      "MATERIALIZE TEAD4_rep into TEAD4_rep;"

    val selectEmpty = "A = SELECT(dd == 'hello') ann; \n " +
      "Materialize A into emptyset;"

    val project = "A = SELECT( cell == 'K562' AND antibody == 'c-Jun' ) project;\n" +
      "B = PROJECT( region_update: score AS score * 2)A;\n" +
      "MATERIALIZE B INTO AddedDatasetPlusMinus2;"

    val covertToGTF = "S = SELECT() /Users/abdulrahman/Polimi/IDEA/GMQLV2/GMQL-Flink/src/test/datasets/cover2/;\n" +
      "C = COVER(2,ANY) S;" +
      "MATERIALIZE C INTO GTF_out2;"

    val projectEirini = "DATA_SET_VAR = SELECT(assay == 'iCLIP') /Users/abdulrahman/Downloads/job_eirini_abdo_20170228_112927_data/;\n" +
      "RES = PROJECT(ALLBUT score; region_update: new_score AS score/100.0 )DATA_SET_VAR;\n" +
      "MATERIALIZE RES INTO res_gtf;"

    val gtfScource = "DATA_SET_VAR = SELECT() /Users/abdulrahman/Polimi/IDEA/GMQLV2/res_gtf/exp/;\n" +
    "MATERIALIZE DATA_SET_VAR INTO res_gtf1;"

    val  summitBin = "RAW = SELECT( antibody_target == 'ATF3') /Users/abdulrahman/Downloads/job_summit_abdulrahman_20170302_091615_raw/;\n" +
      "SUMMIT_TEST = HISTOGRAM(1,ALL) RAW;\n" +
      "MATERIALIZE SUMMIT_TEST into histogram;"

    val coverEirini = "DATA_SET_VAR = SELECT()/Users/abdulrahman/Downloads/bug/real_example/dataset/;\n" +
      "RES1 = COVER(2,ANY) DATA_SET_VAR;\n" +
      "MATERIALIZE RES1 INTO EX1;"

    val dif =  "DATA_SET_VAR = SELECT()/Users/abdulrahman/Polimi/IDEA/GMQLV2/res_gtf/exp/;\n" +
      "DATA_SET_VAR1 = SELECT()/Users/abdulrahman/Polimi/IDEA/GMQLV2/res_gtf1/exp/;\n" +
      "RES1 = difference() DATA_SET_VAR DATA_SET_VAR1;\n" +
      "MATERIALIZE RES1 INTO EX1;"


    val semijoin = "S0 = SELECT() /Users/abka02/Downloads/job_meta_agg_abdo_20170619_134513_data/files/;\n" +
      "S2 = SELECT( semijoin: provider IN S0) /Users/abka02/Downloads/job_meta_agg_abdo_20170619_134513_data/files1/;\n" +
      "MATERIALIZE S2 INTO DD;\n"

    val orderTop = "DATA = SELECT(cell == \"Urothelia\") /Users/abka02/Downloads/job_order_abdo_20170619_110728_SS/files/;\n" +
      "THETOP = ORDER(composite, treatment_type; meta_topg: 1) DATA;\n" +
      "MATERIALIZE THETOP into res;"

    val testOrderMD = "DATASET = SELECT() /Users/abka02/Downloads/group_test;\n" +
      "RESULT = ORDER(biosample_term_name, region_count DESC; meta_topg: 1) DATASET;\n" +
      "MATERIALIZE RESULT INTO RESULT;"

    val testPerformance = "HM_TF = SELECT(dataType == 'ChipSeq' AND view == 'Peaks') hdfs://genomic.elet.polimi.it:9000/user/token/regions/queryname_20180204_191742_HG_PEAKS/exp/; \n" +
      "TSS = SELECT(annotation_type == 'TSS' AND provider == 'UCSC') hdfs://genomic.elet.polimi.it:9000/user/public/regions/home/canakoglu/hadoop_backup/user/public/regions/home/venco/hg19/;\n" +
      "PROM = PROJECT(region_update: start AS start - 2000, stop AS stop + 1000) TSS;\n" +
      "PROM_HM_TF = MAP() PROM HM_TF;\n" +
      "MATERIALIZE PROM_HM_TF INTO hdfs://genomic.elet.polimi.it:9000/user/token/regions/PROM_HM_TF;"


    val mappingNew =  "HM_TF = SELECT() /Users/abka02/Downloads/filename_20180205_001228_DATA_SET_VAR/files/; \n" +
      "TSS = SELECT() /Users/abka02/Downloads/filename_20180205_001228_TSS/files/;\n" +
      "PROM_HM_TF = MAP() TSS HM_TF;\n" +
      "MATERIALIZE PROM_HM_TF INTO /Users/abka02/Desktop/output5/;"
//    val execQuery = args(2) match {
//      case "histo" => Histogram
//      case "map" => Map_server
//    }
    val test_double_select = ""
      try {
        if (translator.phase2(translator.phase1(testPerformance))) {
          server.run()
          //server.getDotGraph()
        }
      } catch {
        case e: CompilerException => println(e.getMessage)
      }


      println("\n\nQuery" +"\n" + testPerformance + "\n\n")
      // "open /Users/pietro/Desktop/test_gmql/output/".!
      //  Console.readLine()
  }

}
