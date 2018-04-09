package it.polimi.genomics.spark.implementation.loaders

/**
 * Created by Abdulrahman Kaitoua on 30/07/15.
 * Email: abdulrahman.kaitoua@polimi.it
 *
 */

import java.io._

import org.apache.hadoop.io.NullWritable

//import it.polimi.genomics.repository.{Utilities => General_Utilities}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.Text
import org.apache.hadoop.io.compress.CompressionCodec
import org.apache.hadoop.mapred.lib.MultipleTextOutputFormat
import org.apache.hadoop.mapred.{FileOutputFormat, JobConf}
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

/**
  *  Write the output files in separated files based on the sample id.
  */
object writeMultiOutputFiles{

  /**
    * Generate file for each key ( sample ID)
    */
  class RDDMultipleTextOutputFormat extends MultipleTextOutputFormat[Any, Any] {
    override def generateFileNameForKeyValue(key: Any, value: Any, name: String): String = {
      key.asInstanceOf[String]
    }

    override def generateActualKey(key: Any, value: Any):Any = NullWritable.get()
  }

  /**
    * Save the RDD rows based on the sample id in separated files.
    *
    * @param src [[RDD]] of the key value pair. The key is the sample id and the value is the row as string
    * @param root [[String]] as the path to the folder location
    * @tparam T The type of the rows
    */
  def saveAsMultipleTextFiles[T:ClassTag](src: RDD[(String,String)],  root: String) {
    saveAsMultipleTextFiles(src, root, None)
  }

  /**
    *
    * Save the RDD rows based on the sample id in separated files.
    *
    * @deprecated
    * @param src [[RDD]] of the key value pair. The key is the sample id and the value is the row as string
    * @param root [[String]] as the path to the folder location
    * @param codec The default codec to be used to compress the data
    * @tparam T The type of the rows
    */
  def saveAsMultipleTextFiles[T:ClassTag](src: RDD[(String,String)], root: String, codec: Class[_ <: CompressionCodec]) {
    saveAsMultipleTextFiles(src, root, Option(codec))
  }

  /**
    * Save the RDD rows based on the sample id in separated files.
    *
    * @param src [[RDD]] of the key value pair. The key is the sample id and the value is the row as string
    * @param root [[String]] as the path to the folder location
    * @param codec [[Option]]  as the default codec to be used to compress the data
    * @tparam T The type of the rows
    */
  def saveAsMultipleTextFiles[T:ClassTag](src: RDD[(String,String)], root: String, codec: Option[Class[_ <: CompressionCodec]]) {
    val hadoopConf = new Configuration()
    val jobConf = new JobConf(hadoopConf)

    jobConf.setOutputFormat(classOf[RDDMultipleTextOutputFormat])

    if(codec.isDefined) {
      jobConf.setBoolean("mapred.output.compress", true)
      jobConf.setClass("mapred.output.compression.codec", codec.get, classOf[CompressionCodec])
    }

    FileOutputFormat.setOutputPath(jobConf, new Path(root))

    src
      .map{v => (new Text(v._1), new Text(v._2))}
      .saveAsHadoopDataset(jobConf)
  }

  /**
    * Move the meta data from the /meta folder to /files file to be in the same folder with the samples files.
    *
    * @param originalPath [[String]] of the meta directory
    */
  def fixOutputMetaLocation(originalPath:String): Unit = {
    println(originalPath)
    val conf = new Configuration();
    val path = new org.apache.hadoop.fs.Path(originalPath);
    val fs = FileSystem.get(path.toUri(), conf);
      try {
        import org.apache.hadoop.fs._
        val files = fs.listStatus(new Path(originalPath));
        files.foreach { sampleFile =>
          val pt = new Path(sampleFile.getPath.toString)
          val dist = new Path(new Path(originalPath).getParent.toString+"/files/"+sampleFile.getPath.getName)
          if(pt.getName !="_SUCCESS")
            fs.rename(pt,dist)
        }
        fs.delete(new Path(originalPath),true)

      } catch {
        case ex1:IOException => println(ex1.getMessage)
      }
  }

  @deprecated
  def fixOutputNamingLocation(originalPath:String,outName:String): Unit = {
    val conf = new Configuration();
    val path = new org.apache.hadoop.fs.Path(originalPath);
    val fs = FileSystem.get(path.toUri(), conf);
      try {
        import org.apache.hadoop.fs._
        val files = fs.listStatus(new Path(originalPath));

        files.foreach { sampleFolder =>
          if (sampleFolder.isDirectory) {
            val pt = new Path(sampleFolder.getPath.getParent.getParent.toString + "/" + sampleFolder.getPath.getName + outName)
            val fw:BufferedWriter =new BufferedWriter(new OutputStreamWriter(fs.create(pt,true)));
            var br:BufferedReader =null;
            val listOfFiles = fs.listStatus(sampleFolder.getPath).filter(p=> !p.getPath.getName.startsWith("."))
            if (listOfFiles.size > 0) {
              for (f <- listOfFiles) {
                br=new BufferedReader(new InputStreamReader(fs.open(new Path(f.getPath.toString)))); var line:String = br.readLine();
                while ( line  != null)
                {fw.append(line+"\n"); line= br.readLine() ;}
                br.close
                fs.delete(f.getPath,true)
              }
              fw.close()
            }
            println(sampleFolder.getPath.getName, fs.delete(sampleFolder.getPath,true))
          }
          else fs.delete(sampleFolder.getPath,true)
        }
        fs.delete(new Path(originalPath),true)

      } catch {
        case ex1:IOException => println(ex1.getMessage)
      }
  }
}
