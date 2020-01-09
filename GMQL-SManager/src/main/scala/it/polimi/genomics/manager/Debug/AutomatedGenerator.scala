package it.polimi.genomics.manager.Debug

import scala.xml.{Elem, XML}

object AutomatedGenerator {

  def go(confFile: String, tempDir: String, outDir: String): Unit = {

    def getTriplet(xml: Elem, property: String): Array[Int] = {
      Array.range( (xml \\ property \@ "from").toInt, (xml \\ property \@ "to").toInt, (xml \\ property \@ "step").toInt)
    }


    val xmlFile = XML.load(confFile)

    val chromosome_max = (xmlFile \\ "chromosome" \@ "max").toLong
    println(chromosome_max)

    val num_samples = getTriplet(xmlFile, "num_samples")
    val num_regions = getTriplet(xmlFile, "num_regions")
    val avg_length = getTriplet(xmlFile, "avg_length")


    for (samp_num <- num_samples) {
      for (reg_num <- num_regions) {
        for(len_avg <- avg_length) {

          val out_name = "ds_"+samp_num+"_"+reg_num+"_"+len_avg

          val options: Array[String] = Array("-num_samples" , samp_num.toString,
            "-num_regions", reg_num.toString,
            "-len_avg", len_avg.toString,
            "-chrom_max", chromosome_max.toString,
            "-temp_folder",tempDir,
            "-out_folder", outDir,
            "-out_name",out_name)

          DSGenerator.main(options)

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
