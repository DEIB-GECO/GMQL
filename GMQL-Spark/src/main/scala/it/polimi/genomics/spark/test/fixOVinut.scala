package it.polimi.genomics.spark.test

/**
 * Created by abdulrahman on 06/10/15.
 */

import java.io.FileWriter
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}

object fixOVinut {
  def main(args: Array[String]): Unit = {
    for( i <- 1 to 29){

      println ("/Users/abdulrahman/Downloads/data set of ov/sample"+i+".txt")
      val fw = new java.io.BufferedWriter(new FileWriter(new java.io.File("/Users/abdulrahman/Downloads/data set of ov/sample"+i+".gtf")))
      import scala.collection.JavaConverters._
      val lines = Files.readAllLines(
        Paths.get("/Users/abdulrahman/Downloads/data set of ov/sample"+i+".txt"),StandardCharsets.UTF_8).asScala
      for(line:String <-lines){
        val constracted_line = new StringBuilder
        try{
          val splittedLine = line.split("\t")
          constracted_line.append(splittedLine(0))
          constracted_line.append("\t")
          constracted_line.append(splittedLine(1))
          constracted_line.append("\t")
          constracted_line.append(splittedLine(2))
          constracted_line.append("\t")
          constracted_line.append(splittedLine(3))
          constracted_line.append("\t")
          constracted_line.append(splittedLine(4))
          constracted_line.append("\t")
          constracted_line.append(splittedLine(5))
          constracted_line.append("\t")
          constracted_line.append(splittedLine(6))
          constracted_line.append("\t")
          constracted_line.append(splittedLine(7))
          constracted_line.append("\t")
          constracted_line.append(splittedLine(8).substring(0,splittedLine(8).size-1))
          constracted_line.append(" \"")
          constracted_line.append(splittedLine(9))
          constracted_line.append("\";\n")
          fw.write(constracted_line.toString());
        }
        catch{
          case ex:Exception => println("not available")
        }

        println(constracted_line)
      }

      fw.close()

    }



  }

}
