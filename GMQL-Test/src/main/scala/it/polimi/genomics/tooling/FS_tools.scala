package it.polimi.genomics.tooling

import java.io.{PrintWriter, File}
import org.apache.commons.io.FileUtils


/**
 * Created by pietro on 05/08/15.
 */
object FS_tools {

  def root_dir:String = System.getProperty("java.io.tmpdir")
  val gmql_test_folder_name = "gmql_testing"
  var gmql_test_folder_path = root_dir + File.separator + gmql_test_folder_name
  val error_folder_name = "error_dir"
  def error_folder_path = gmql_test_folder_path + File.separator + error_folder_name
  def flink_ouput_path = gmql_test_folder_path + File.separator + "OUTF" + File.separator
  def spark_ouput_path = gmql_test_folder_path + File.separator + "OUTS" + File.separator
  def response_path =  gmql_test_folder_path + File.separator + "response_doc"
  def response_path_source = response_path + File.separator + "source"
  def response_doc_path = response_path + File.separator + "response.tex"

  def cleanErrorFolder() = {
    val dir : File = new File(error_folder_path)
    if(dir.exists()){
      rmDirectory(dir.getAbsolutePath)
    }
    dir.mkdirs()
  }

  def moveToErrorFolder(folder : String, queryName : String) = {
    if(new File(folder).exists()){
      FileUtils.moveDirectory(new File(folder), new File(error_folder_path + "/" + queryName + "/" + folder.split("/").last))
    }
  }

  def rmDirectory(folder : String) = {
    FileUtils.deleteDirectory(new File(folder))
  }

  def rmFile(file : String) = {
    val f : File = new File(file)
    f.delete()
  }

  def init_file_system() = {
    val dir : File = new File(gmql_test_folder_path)
    if (dir.exists() && !dir.isDirectory) {
      dir.delete()
    }
    else if (dir.exists()) {
      FileUtils.deleteDirectory(dir)
    }
    dir.mkdir()
    val response_source_dir : File = new File(response_path_source)
    response_source_dir.mkdirs()
    cleanErrorFolder()
  }

  def clean_outputs() = {
    val f_dir : File = new File(flink_ouput_path)
    if (f_dir.exists()) rmDirectory(flink_ouput_path)
    val s_dir : File = new File(spark_ouput_path)
    if (s_dir.exists()) rmDirectory(spark_ouput_path)
  }

  def add_dataset(name : String) : String = {
    val dir : File = new File(gmql_test_folder_path + File.separator + name)
    if (dir.exists()) {
      println("Dataset " + name + " already exists. Aborting test.\n")
//      System.exit(-1)
    } else {
      dir.mkdir()
    }
    gmql_test_folder_path + File.separator + name
  }

  def new_sample_in_dataset(ds_name:String,sample_name:String,
                            meta_tuples:List[String],region_tuples:List[String]) = {
    val file_path = gmql_test_folder_path + File.separator + ds_name + File.separator + sample_name

    val file_region : File = new File(file_path)
    val file_meta : File = new File(file_path + ".meta")
    if (file_meta.exists() || file_region.exists()) {
      println("Sample " + sample_name + " within dataset " + ds_name + " already exists. Aborting test.\n")
      System.exit(-1)
    } else {
      val region_writer = new PrintWriter(file_region)
      val meta_writer = new PrintWriter(file_meta)
      region_tuples.foreach(x=>region_writer.write(x+"\n"))
      meta_tuples.foreach(x=>meta_writer.write(x+"\n"))
      region_writer.close()
      meta_writer.close()
    }
  }

  def write_report_file(text : String) = {
    val w = new PrintWriter(new File(response_doc_path))
    w.write(text)
    w.close()
  }

}
