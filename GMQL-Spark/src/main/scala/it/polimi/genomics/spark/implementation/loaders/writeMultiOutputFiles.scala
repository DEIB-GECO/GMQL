package it.polimi.genomics.spark.implementation.loaders

/**
 * Created by Abdulrahman Kaitoua on 30/07/15.
 * Email: abdulrahman.kaitoua@polimi.it
 *
 */

import java.io._
import java.net.URI

import it.polimi.genomics.repository.util.Utilities
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.Text
import org.apache.hadoop.io.compress.CompressionCodec
import org.apache.hadoop.mapred.lib.MultipleTextOutputFormat
import org.apache.hadoop.mapred.{FileOutputFormat, JobConf}
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

object writeMultiOutputFiles{
  class RDDMultipleTextOutputFormat extends MultipleTextOutputFormat[Text, Text] {
    override def generateFileNameForKeyValue(key: Text, value: Text, name: String): String = {
      key.toString// + "/" + name
    }

    override def generateActualKey(key: Text, value: Text) = null
  }

  def saveAsMultipleTextFiles[T:ClassTag](src: RDD[(String,String)],  root: String) {
    saveAsMultipleTextFiles(src, root, None)
  }

  def saveAsMultipleTextFiles[T:ClassTag](src: RDD[(String,String)], root: String, codec: Class[_ <: CompressionCodec]) {
    saveAsMultipleTextFiles(src, root, Option(codec))
  }

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

  def fixOutputMetaLocation(originalPath:String)= {
    println(originalPath)
    if(Utilities.getInstance.MODE== Utilities.HDFS)
      try {
        import org.apache.hadoop.conf._
        import org.apache.hadoop.fs._
        val fs = FileSystem.get(URI.create(originalPath),new Configuration());
        val files = fs.listStatus(new Path(originalPath));
        files.foreach { sampleFile =>
          val pt = new Path(sampleFile.getPath.toString)
          val dist = new Path(new Path(originalPath).getParent.toString+"/exp/"+sampleFile.getPath.getName)
//          println (pt.toString,dist.toString,this.getClass)
          if(pt.getName !="_SUCCESS")
            fs.rename(pt,dist)
        }
        fs.delete(new Path(originalPath),true)

      } catch {
        case ex1:IOException => println(ex1.getMessage)
      }
    else
      try {
        println ("LOCAL")
        val files = new java.io.File(originalPath).listFiles(
          new FileFilter() {
            @Override def accept(pathname: java.io.File) = !pathname.getName.startsWith("_") && !pathname.getName.startsWith(".");
          }
        )
        files.foreach { sampleFile =>
          val source = new File(sampleFile.getPath)
          val dist = new File(new File(originalPath).getParent+"/exp/"+sampleFile.getName)
          //          println(sampleFile.getPath,new File(originalPath).getParent,sampleFile.getName)
          org.apache.commons.io.FileUtils.moveFile(source,dist)
        }
        org.apache.commons.io.FileUtils.deleteDirectory(new java.io.File(originalPath))
      }catch{
        case ex:FileNotFoundException => println(ex.getMessage)
      }
  }

  def fixOutputNamingLocation(originalPath:String,outName:String)= {
    import scala.io.Source
    if(Utilities.getInstance.MODE== "MAPREDUCE")
      try {
        import org.apache.hadoop.conf._
        import org.apache.hadoop.fs._
        val fs = FileSystem.get(URI.create(originalPath),new Configuration());
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
                while ( line  != null/*sc.textFile(f.getPath.toUri.toString)*/)
                {fw.append(line+"\n"); line= br.readLine() ;}
                br.close
                fs.delete(f.getPath,true)
              }
              fw.close()
            }
            println(sampleFolder.getPath.getName, fs.delete(sampleFolder.getPath,true))
          }
          else fs.delete(sampleFolder.getPath,true)
          //            println(sampleFolder.getPath.getParent.getName)
        }
        fs.delete(new Path(originalPath),true)

      } catch {
        case ex1:IOException => println(ex1.getMessage)
      }
    else
      try {
        val files = new java.io.File(originalPath).listFiles()
        files.foreach { sampleFolder =>
          if (sampleFolder.isDirectory) {
            val fw = new FileWriter(sampleFolder.getParentFile.getParent + "/" + sampleFolder.getName + outName, true)
            val listOfFiles = sampleFolder.listFiles(
              new FileFilter() {
                @Override def accept(pathname: java.io.File) = !pathname.getName.startsWith(".");
              }
            )
            if (listOfFiles.size > 0) {
              for (f <- listOfFiles) {
                for (line <- Source.fromFile(f).getLines())
                  fw.append(line+"\n")
                f.delete()
              }
              fw.close()
            }
            org.apache.commons.io.FileUtils.deleteDirectory(sampleFolder)
          }
          else sampleFolder.delete()
          //        println(sampleFolder.getParent)
        }
        org.apache.commons.io.FileUtils.deleteDirectory(new java.io.File(originalPath))
      }catch{
        case ex:FileNotFoundException => println(ex.getMessage)
      }
  }
}
