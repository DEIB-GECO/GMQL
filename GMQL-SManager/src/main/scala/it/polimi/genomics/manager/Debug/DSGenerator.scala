package it.polimi.genomics.manager.Debug

import it.polimi.genomics.manager.Debug.QueryCollector.{logger}
import java.io._

object DSGenerator {

  private final val usage: String = "MakeDebug Options: \n" +
  "\t"+List("conf", "num_samples", "num_regions", "len_avg", "len_var", "chrom_max", "out_folder", "out_name")

  def main(args: Array[String]): Unit = {

    val r = scala.util.Random

    var conf =  "/Users/andreagulino/Projects/GMQL-WEB/conf/gmql_conf/"

    var num_samples = 5
    var num_regions = 100
    var len_avg = 500
    var len_var = 1
    var chrom_max = 30000000

    var temp_folder = "/Users/andreagulino/tmp/"
    var out_folder = "/Users/andreagulino/tmp/"
    var out_name   = "prova"


    // Read Options
    for (i <- args.indices if i % 2 == 0) {
      if ("-h".equals(args(i)) || "-help".equals(args(i))) {
        println(usage)
        System.exit(0)
      } else if ("-conf".equals(args(i))) {
        conf = args(i + 1)
        logger.info("-conf: " + conf)
      } else if ("-num_samples".equals(args(i))) {
        num_samples = args(i + 1).toInt
        logger.info("-num_samples: " + num_samples)
      } else if ("-num_regions".equals(args(i))) {
        num_regions = args(i + 1).toInt
        logger.info("-num_regions: " + num_regions)
      } else if ("-len_avg".equals(args(i))) {
        len_avg = args(i + 1).toInt
        logger.info("-len_avg: " + len_avg)
      } else if ("-len_var".equals(args(i))) {
        len_var = args(i + 1).toInt
        logger.info("-len_var: " + len_var)
      } else if ("-chrom_max".equals(args(i))) {
        chrom_max = args(i + 1).toInt
        logger.info("-chrom_max: " + chrom_max)
      } else if ("-temp_folder".equals(args(i))) {
        temp_folder = args(i + 1)
        logger.info("-temp_folder: " + temp_folder)
      } else if ("-out_folder".equals(args(i))) {
        out_folder = args(i + 1)
        logger.info("-out_folder: " + out_folder)
      } else if ("-out_name".equals(args(i))) {
        out_name = args(i + 1)
        logger.info("-out_name: " + out_name)
      } else {
        logger.warn(s"Command option is not found ${args(i)}")
        System.exit(0)
      }
    }



    // Remove and create the output folder

    println("removing/creating dir "+out_folder+out_name)
    val file = new File(out_folder+out_name)
    file.delete()
    file.mkdirs()



    for (sample_id <- 0 to num_samples) {
      val sample_file_name = out_folder+"/"+out_name+"/"+sample_id+".gdm"
      val pw = new PrintWriter(new File(sample_file_name))
      for (i <- 1 to num_regions) {
        val start = r.nextInt(chrom_max)
        val region = (start, start+len_avg)
        pw.write("chr1"+"\t"+region._1+"\t"+"*"+"\n")
      }
      pw.close
    }








  }
}
