package it.polimi.genomics.comparison

import java.io.File

import it.polimi.genomics.DotImplementation.DotImplementation
import it.polimi.genomics.GMQLServer.GmqlServer
import it.polimi.genomics.compiler.{MaterializeOperator, Translator}
import it.polimi.genomics.flink.FlinkImplementation.FlinkImplementation
import it.polimi.genomics.repository.FSRepository.{FS_Utilities => FSR_Utilities}
import it.polimi.genomics.spark.implementation.GMQLSparkExecutor
import it.polimi.genomics.tooling.{Comparators, DataGenerator, DocumentManager, FS_tools}
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.LoggerFactory

import scala.xml.XML

/**
  * Created by pietro on 27/07/15.
  */
object Compare {

  final val logger = LoggerFactory.getLogger(this.getClass)
  private final val usage = "gmql " + " [-exec FLINK|SPARK] [-generate TRUE|FALSE] [-input XMLFILE] [-output /where/gmql/script/is] "
  private final val FLINK = "FLINK"
  private final val SPARK = "SPARK"
  private final val ALL = "ALL"
//  val root:ch.qos.logback.classic.Logger = org.slf4j.LoggerFactory.getLogger("org").asInstanceOf[ch.qos.logback.classic.Logger];
//  root.setLevel(ch.qos.logback.classic.Level.WARN);


  def main(args: Array[String]) {

    var executionType = ALL
    var output = ""
    var inputFile = ""
    var generate = true
    var maxdis = 5000

    for (i <- 0 until args.length if (i % 2 == 0)) {
      if ("-h".equals(args(i)) || "-help".equals(args(i))) {
        println(usage)
      } else if ("-exec".equals(args(i))) {
        executionType = args(i + 1).toUpperCase()
        logger.info("Execution Type is set to: ", executionType)
      }else if ("-maxdis".equals(args(i))) {
        maxdis =args(i + 1).toInt
        logger.info("Max distance for MinDistance and Greater than is set to: ", executionType)
      } else if ("-output".equals(args(i))) {
        output = args(i + 1).toLowerCase()
        logger.info("Output path: ", output)
      } else if ("-generate".equals(args(i))) {
        generate = (args(i + 1).toLowerCase()).toBoolean
        logger.info("Output path: ", generate)
      } else if ("-input".equals(args(i))) {
        if (new java.io.File(args(i + 1)).exists()) {
          inputFile = args(i + 1)
          logger.info("Input File set to: ", inputFile)
        } else {
          logger.error("GMQL input script path is not valid")
          sys.exit()
        }
      }
    }

    FS_tools.init_file_system()
    if (output != "") FS_tools.gmql_test_folder_path = output + File.separator + FS_tools.gmql_test_folder_name

    logger.warn("using directory " + FS_tools.gmql_test_folder_path)
    var test_log: List[String] = List.empty

    val docManager = new DocumentManager()

    val test_conf = XML.loadFile(if (inputFile != "") inputFile else "conf/test_join.xml")
    val binSizes = (test_conf \\ "config" \\ "binsizes" \\ "binsize").map(_.text.toInt)

    val datasets: Seq[(String, String)] = for (x <- (test_conf \\ "datasets" \\ "dataset")) yield {

      val placeholder = (x \\ "placeholder").text
      val dstype = (x \\ "type").text
      val name = (x \\ "name").text
      val numfiles = (x \\ "numfiles").text.toInt
      val numchrom = (x \\ "numchrom").text.toInt
      val numregchrom = (x \\ "numregchrom").text.toInt
      val chrlen = (x \\ "chromlen").text.toInt
      val minreglen = (x \\ "minreglen").text.toInt
      val maxreglen = (x \\ "maxreglen").text.toInt
      val nummeta = (x \\ "nummeta").text.toInt
      val minval = (x \\ "minval").text.toDouble
      val maxval = (x \\ "maxval").text.toDouble
      val names = dstype match {
        case "RNASEQ" => (x \\ "namevalues" \\ "value").map(n => n.text)
        case "BEDSCORE" => List.empty
      }

      val path = FS_tools.add_dataset(name)

      if (generate) {
        println("generating data")
        for (f <- 1 to numfiles) {
          val strands = dstype match {
            case "RNASEQ" => List("+", "-", "*")
            case "BEDSCORE" => List.empty
          }
          val coords = DataGenerator.generateRegionData(numchrom, numregchrom, chrlen, minreglen, maxreglen, strands)
          val reg_list = dstype match {
            case "RNASEQ" => DataGenerator.add_score_value(DataGenerator.add_name_value(coords, names.toList), minval, maxval)
            case "BEDSCORE" => DataGenerator.add_score_value(coords, minval, maxval)
          }
          val meta_list = DataGenerator.generateMetaData(nummeta)
          FS_tools.new_sample_in_dataset(name, "file" + f + ".bed", meta_list, reg_list)
        }
        println("done generating data")
      }
      (placeholder, path)
    }

    val queries = (test_conf \\ "queries" \\ "query")
    for (bs <- 0 to binSizes.size - 1; qn <- 0 to queries.size - 1) {
      println("\n\n\n----------------------" + binSizes(bs) + "----------------------\n\n\n")
      val index = qn
      val q = queries(qn)
      val bin_size = binSizes(bs)
      val query_name = "query-" + index
      var query = q.text.replaceAll("\\s+$", "")
        .replaceAll("&lt;", "<")
        .replaceAll("&gt;", ">")
      for (d <- datasets) {
        query = query.replaceAll(d._1, d._2)
      }
      val query_dot = query.replace("#OUTPUT#", "out/path")
      val query_flink = query.replace("#OUTPUT#", FS_tools.flink_ouput_path)
      val query_spark = query.replace("#OUTPUT#", FS_tools.spark_ouput_path)

      val dot_graph_path = FS_tools.response_path_source + File.separator + query_name + ".dot"
      val png_graph_path = FS_tools.response_path_source + File.separator + query_name + ".png"

      println("dot_graph_path", dot_graph_path)
      val dot_out = true
      try {
        val dot_server = new GmqlServer(new DotImplementation(dot_graph_path, png_graph_path, "png", shorten_paths = true))
        val dot_translator = new Translator(dot_server, FS_tools.flink_ouput_path)
        dot_translator.phase2(dot_translator.phase1(query_flink))
        dot_server.run()
        FS_tools rmFile dot_graph_path
        true
      } catch {
        case e: Exception => logger.error(e.getMessage)
          false
      }


      val flink_output_couple = if (executionType.equals(FLINK) || executionType.equals(ALL)) {
        try {
          val flink_server = new GmqlServer(new FlinkImplementation(testingIOFormats = true,maxBinDistance = maxdis),
            binning_size = Some(bin_size))
          val flink_translator = new Translator(flink_server, FS_tools.flink_ouput_path)
          flink_translator.phase2(flink_translator.phase1(query_flink))
          val flink_start: Long = System.currentTimeMillis
          flink_server.run()
          val flink_stop: Long = System.currentTimeMillis
          (true, Some(flink_stop - flink_start))
        }
        catch {
          case e: Exception => logger.error(e.getMessage)
            (false, None)
        }
      } else {
        (false, None)
      }

      val conf = new SparkConf()
        .setAppName("GMQL V2 Spark")
        //    .setSparkHome("/usr/local/Cellar/spark-1.5.2/")
        .setMaster("local[*]")
        //    .setMaster("yarn-client")
        //    .set("spark.executor.memory", "1g")
        .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer").set("spark.kryoserializer.buffer", "64")
        .set("spark.driver.allowMultipleContexts","true")
        .set("spark.sql.tungsten.enabled", "true")
      var sc:SparkContext = null

      val spark_output_couple = if (executionType.equals(SPARK) || executionType.equals(ALL)) {
        try {
          sc = new SparkContext(conf)
          val spark_server = new GmqlServer(new GMQLSparkExecutor(testingIOFormats = true,maxBinDistance = maxdis,sc=sc),
            binning_size = Some(bin_size))
          val spark_translator = new Translator(spark_server, FS_tools.spark_ouput_path)
          val dd = spark_translator.phase1(query_spark)

          //extract the outpath
          val outputs = dd.flatMap(x => x match {
            case d: MaterializeOperator =>
              Some(d.store_path)
            case _ => None
          })

          spark_translator.phase2(dd)
          val spark_start: Long = System.currentTimeMillis
          spark_server.run()
          val spark_stop: Long = System.currentTimeMillis

          //delete the output path if it is on hdfs
          if(outputs.head.startsWith("hdfs")&& binSizes.size>1){
           outputs.map(x=> println("deleting Spark out( "+x+" ) "+FSR_Utilities.deleteDFSDir(x)))
          }

          (true, Some(spark_stop - spark_start))
        } catch {
          case e: Exception => e.printStackTrace();//logger.error(e.getMessage)
            (false, None)
        }finally {sc.stop()}

      } else {
        (false, None)
      }

      val flink_output = flink_output_couple._1
      val spark_output = spark_output_couple._1
      val flink_time = flink_output_couple._2
      val spark_time = spark_output_couple._2
      val meta_comparison = if (!(flink_output && spark_output)) {
        false
      } else {
        Comparators.compareMeta(FS_tools.flink_ouput_path, FS_tools.spark_ouput_path)
      }

      val reg_comparison = if (!(flink_output && spark_output)) {
        false
      } else {
        Comparators.compareExp(FS_tools.flink_ouput_path, FS_tools.spark_ouput_path)
      }

      if (!(meta_comparison && reg_comparison)) {
        FS_tools.moveToErrorFolder(FS_tools.spark_ouput_path, query_name)
        FS_tools.moveToErrorFolder(FS_tools.flink_ouput_path, query_name)
      }

      docManager.table_add_line(query_name + "-" + bin_size, dot_out, flink_output, spark_output, meta_comparison, reg_comparison)
      docManager.time_table_add_line(query_name + "-" + bin_size, flink_time, spark_time)
      if (bs == 0) docManager.add_query(query_name, q.text, dot_out)
      FS_tools.clean_outputs()
    }
    docManager.generate()
    logger.warn("Find results at: " + FS_tools.gmql_test_folder_path)
  }

}
