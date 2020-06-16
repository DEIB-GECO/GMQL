package it.polimi.genomics.manager.Debug

import java.io.File

import scala.reflect.io.Directory
import scala.util.Random
import scala.xml.{NodeSeq, XML}

object AutomatedGenerator {

  case class DatasetConfig(samp_num:Int, reg_num: Int,  len_avg: Int, cols_num:Int, chrom_len: Long, name: String, folder_path: String)


  def cleanGenFolder(dir: String) = {
    new Directory(new File(dir)).deleteRecursively()
    new File(dir).mkdirs()
  }


  def getConfigurations(confFile: String, binary: Boolean, destFolder: String, complex:Boolean = false, numDatasets:Array[Int] = Array(), maxDatasetsPerNum:Int = -1) : List[List[DatasetConfig]] = {



    val xmlFile = XML.load(confFile)


    if(complex) {

      val ref = _getConfigurations(xmlFile \\ "conf" \\ "datasets" \\ "binary" \\ "reference", destFolder)
      val exp = _getConfigurations(xmlFile \\ "conf" \\ "datasets" \\ "binary" \\ "experiment", destFolder)


      var finalSet: List[List[DatasetConfig]] = List[List[DatasetConfig]]()

      numDatasets.foreach(N=>{

        var NSet: List[List[DatasetConfig]] = ref.map(r=>List(r,Random.shuffle(exp).take(1).head))

        //finalSet = finalSet.map(r => List(r))

        2 until N foreach { _ => {

          NSet = NSet.map(current_tuple =>  Random.shuffle(ref).take(1).head :: current_tuple)

        }

        }

        finalSet = finalSet.union(Random.shuffle(NSet).take(maxDatasetsPerNum))


      })


      println("Generated ==>",finalSet.length+" datasets")
      println("Each conf num ==>", finalSet.head.length)
      Random.shuffle(finalSet)


    } else {

      if (!binary) {
        List(_getConfigurations(xmlFile \\ "conf" \\ "datasets" \\ "unary", destFolder + "/"))
      } else {

        val ref = _getConfigurations(xmlFile \\ "conf" \\ "datasets" \\ "binary" \\ "reference", destFolder)
        val exp = _getConfigurations(xmlFile \\ "conf" \\ "datasets" \\ "binary" \\ "experiment", destFolder)

        ref.flatMap(r => exp.map(e => List(r, e)))
      }
    }


  }

  private def _getConfigurations(conf:NodeSeq, folderPath: String): List[DatasetConfig] = {

    val chromosome_max = (conf \\ "chromosome" \@ "max").toLong

    val num_samples = Main.getTriplet(conf, "num_samples")
    val num_regions = Main.getTriplet(conf, "num_regions")
    val avg_length  = Main.getTriplet(conf, "avg_length")
    val num_columns = Main.getTriplet(conf, "num_columns")

    val max_density = (conf \\ "max_density").text.toFloat

    println("MAX DENSITY: "+max_density)

    val configurations = num_samples.flatMap(
      ns =>
        num_regions.flatMap(
          nr => avg_length.flatMap(
            al => num_columns.flatMap(
              nc => {
                if(nr*al/chromosome_max > max_density) List()
                else {
                  val out_name = "ds_" + ns + "_" + nr + "_" + al + "_" + nc
                  val path = folderPath
                  List(DatasetConfig(ns, nr, al, nc, chromosome_max,  out_name, path ))
                }
              } )))).toList

    configurations

  }

  def generate(conf: DatasetConfig, tempDir: String): Unit = {


    val options: Array[String] = Array("-num_samples", conf.samp_num.toString,
      "-num_regions", conf.reg_num.toString,
      "-num_columns", conf.cols_num.toString,
      "-len_avg", conf.len_avg.toString,
      "-chrom_max", conf.chrom_len.toString,
      "-temp_folder", tempDir,
      "-out_folder", conf.folder_path,
      "-out_name", conf.name)

    DSGenerator.main(options)

  }

}
