package it.polimi.genomics.spark.implementation.RegionsOperators

import it.polimi.genomics.core.DataStructures.RegionOperator
import it.polimi.genomics.core.DataTypes.{ GRECORD}
import it.polimi.genomics.core.{GValue, GDouble}
import it.polimi.genomics.core.exception.SelectFormatException
//import it.polimi.genomics.repository.{Utilities => General_Utilities}
//import it.polimi.genomics.repository.FSRepository.{LFSRepository, Utilities => FSR_Utilities}
import it.polimi.genomics.spark.implementation.GMQLSparkExecutor
import org.apache.hadoop.fs.Path
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.slf4j.LoggerFactory

import scala.xml.Elem

/**
 * Created by abdulrahman kaitoua on 25/05/15.
 */
object StoreRD {
  private final val logger = LoggerFactory.getLogger(StoreRD.getClass);
  private final val ENCODING = "UTF-8"

  @throws[SelectFormatException]
  def apply(executor : GMQLSparkExecutor, path : String, value: RegionOperator, sc : SparkContext) : RDD[GRECORD] = {
    val input = executor.implement_rd(value, sc)
    input
  }

//  def storeSchema(schema: Elem, path : String)= {
////    if (schema.isDefined) {
//      logger.info(path)
//      val schemaHeader = schema//.get
//      val dir = new java.io.File(path);
//      val parentDir =
//        if(dir.getPath.startsWith("hdfs")) {
//          new java.io.File((new Path(dir.getPath).getParent.getName));
//        } else new java.io.File(dir.getParent)
//      if (!parentDir.exists) parentDir.mkdirs()
//      //TODO when taking off GMQL V1 then refine these conditions
//      //Where to store the schema file. In Case GMQL V2 only, we need to store it only on local
//      if ((new java.io.File("/" + parentDir.getPath)).exists())
//        scala.xml.XML.save(parentDir.getPath + "/test.schema", schemaHeader, ENCODING, true, null)
//      else if (General_Utilities().MODE == "LOCAL")
//        scala.xml.XML.save(General_Utilities().RepoDir + General_Utilities().USERNAME + "/schema/" + parentDir.getPath + ".schema", schemaHeader, ENCODING, true, null)
//      else {
//        // HDFS verison is needed only for GMQL V1
//        val localPath = General_Utilities().RepoDir + General_Utilities().USERNAME + "/schema/" + parentDir.getPath + ".schema"
//        val hdfsPath = General_Utilities().HDFSRepoDir + General_Utilities().USERNAME + "/schema/" + parentDir.getPath + ".schema"
//        scala.xml.XML.save(localPath, schemaHeader, ENCODING, true, null)
//        logger.info("\n\n"+localPath+"\n\n")
//        FSR_Utilities.copyfiletoHDFS(localPath, hdfsPath)
//      }
////    } else logger.warn("The result DataSet is empty and no Schema file is generated \n "+path)
//  }
//
//  def generateSchema(input: RDD[GRECORD]): Elem ={
//        val values = input.first()._2
//        val schemaHeader =
//        //http://www.bioinformatics.deib.polimi.it/GMQL/
//          <gmqlSchemaCollection name="DatasetName_SCHEMAS" xmlns="http://genomic.elet.polimi.it/entities">
//            <gmqlSchema type="narrowPeak">
//              <field type="STRING">CHROM</field>
//              <field type="LONG">START</field>
//              <field type="LONG">STOP</field>
//              <field type="STRING">STR</field>{for (i <- 0 to values.size - 1) yield <field type={values(i) match {
//              case GDouble(x) => "DOUBLE";
//              case _ => "STRING";
//            }}>
//              {i}
//            </field>}
//            </gmqlSchema>
//          </gmqlSchemaCollection>
//        schemaHeader
//  }
}
