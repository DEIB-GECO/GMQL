package it.polimi.genomics.manager.Debug

import java.io.File

import scala.reflect.io.Directory
import scala.xml.{NodeSeq, XML}

object AutomatedGenerator {


  def generate(confFile: String, tempDir: String, outDir: String, binary: Boolean): Unit = {

    val xmlFile = XML.load(confFile)

    /*
      - unary
      - binary
      -- reference
      -- experiment
     */

    val destinationFolder = if(binary) outDir+"/binary" else outDir+"/unary"
    val clean_out = (xmlFile  \\ "conf" \\ "execution" \\ "clean_out").text == "true"

    if(binary) {
      println("GENERATING DATASETS FOR BINARY OPERATOR")
      val ref_conf =  xmlFile \\ "conf" \\ "datasets" \\ "binary" \\ "reference"
      val exp_conf =  xmlFile \\ "conf" \\ "datasets" \\ "binary" \\ "experiment"

      _generate(ref_conf, tempDir, destinationFolder+"/reference/",clean_out)
      _generate(exp_conf, tempDir, destinationFolder+"/experiment/",clean_out)
    } else {
      println("GENERATING DATASETS FOR UNARY OPERATOR")
      val conf =  xmlFile \\ "conf" \\ "datasets" \\ "unary"
      _generate(conf, tempDir, destinationFolder, clean_out)
    }


  }


  def getNumDatasets(confFile: String, binary:Boolean): Int = {

    val xmlFile = XML.load(confFile)
    if(binary) {
      val ref = _getNumDatasets(xmlFile \\ "conf" \\ "datasets" \\ "binary" \\ "reference")
      val exp = _getNumDatasets(xmlFile \\ "conf" \\ "datasets" \\ "binary" \\ "experiment")
      ref*exp
    } else {
      _getNumDatasets(xmlFile \\ "conf" \\ "datasets" \\ "unary")
    }


  }


  private def _getNumDatasets(conf: NodeSeq): Int = {
    val num_samples = Main.getTriplet(conf, "num_samples")
    val num_regions = Main.getTriplet(conf, "num_regions")
    val avg_length = Main.getTriplet(conf, "avg_length")
    val num_columns = Main.getTriplet(conf,"num_columns")

    val total_num = num_samples.length * num_regions.length * avg_length.length * num_columns.length

    total_num
  }

  private def _generate(conf: NodeSeq, tempDir: String, outDir: String, clean_out: Boolean): Unit = {


    val chromosome_max = (conf \\ "chromosome" \@ "max").toLong



    if(clean_out) {
      new Directory(new File(outDir)).deleteRecursively()
      new File(outDir).mkdirs()
    }


    val num_samples = Main.getTriplet(conf, "num_samples")
    val num_regions = Main.getTriplet(conf, "num_regions")
    val avg_length = Main.getTriplet(conf, "avg_length")
    val num_columns = Main.getTriplet(conf, "num_columns")

    val total_num = num_samples.length * num_regions.length * avg_length.length * num_columns.length

    println("GENERATING DS")

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

}
