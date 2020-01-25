package it.polimi.genomics.manager.Debug

import java.io.File

import scala.reflect.io.Directory
import scala.xml.{Elem, XML}

object AutomatedGenerator {

  private def getTriplet(xml: Elem, property: String): Array[Int] = {
    Array.range( (xml \\ "conf" \\ "datasets" \\ property \@ "from").toInt, (xml \\ property \@ "to").toInt, (xml \\ property \@ "step").toInt)
  }


  def getNumDatasets(confFile: String): Int = {

    val xmlFile = XML.load(confFile)

    val chromosome_max = (xmlFile  \\ "conf" \\ "datasets" \\ "chromosome" \@ "max").toLong

    val num_samples = getTriplet(xmlFile, "num_samples")
    val num_regions = getTriplet(xmlFile, "num_regions")
    val avg_length = getTriplet(xmlFile, "avg_length")
    val num_columns = getTriplet(xmlFile, "num_columns")

    val total_num = num_samples.length * num_regions.length * avg_length.length * num_columns.length

    total_num
  }


  def go(confFile: String, tempDir: String, outDir: String): Unit = {


    val xmlFile = XML.load(confFile)

    val chromosome_max = (xmlFile  \\ "conf" \\ "datasets" \\ "chromosome" \@ "max").toLong

    val clean_out = (xmlFile  \\ "conf" \\ "execution" \\ "clean_out").text == "true"

    if(clean_out) {
      new Directory(new File(outDir)).deleteRecursively()
      new File(outDir).mkdirs()
    }


    val num_samples = getTriplet(xmlFile, "num_samples")
    val num_regions = getTriplet(xmlFile, "num_regions")
    val avg_length = getTriplet(xmlFile, "avg_length")
    val num_columns = getTriplet(xmlFile, "num_columns")

    val total_num = num_samples.length * num_regions.length * avg_length.length * num_columns.length

    println("GENERATING "+total_num+" DATASETS")

    for (samp_num <- num_samples) {
      for (reg_num <- num_regions) {
        for (cols_num <- num_columns) {
          for (len_avg <- avg_length) {

            val out_name = "ds_" + samp_num + "_" + reg_num + "_" + len_avg + "_" + cols_num

            val options: Array[String] = Array("-num_samples", samp_num.toString,
              "-num_regions", reg_num.toString,
              "-num_columns", cols_num.toString,
              "-len_avg", len_avg.toString,
              "-chrom_max", chromosome_max.toString,
              "-temp_folder", tempDir,
              "-out_folder", outDir,
              "-out_name", out_name)

            DSGenerator.main(options)

          }
        }
      }
    }
  }

  def main(args: Array[String]): Unit = {

    var tempDir= "/Users/andreagulino/tmp/"
    var outDir = "/Users/andreagulino/tmp/"

    var confFile = "/Users/andreagulino/tmp/generator/conf.xml"

    if(args.length>0)
      confFile = args(0)

    go(confFile, tempDir, outDir)

  }
}
