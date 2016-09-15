package it.polimi.genomics.repository.FSRepository

import java.io.{File, FileInputStream, FileOutputStream, FilenameFilter}

import com.google.common.io.Files
import it.polimi.genomics.core.DataStructures.IRDataSet
import it.polimi.genomics.core.ParsingType
import it.polimi.genomics.core.ParsingType._
import it.polimi.genomics.repository.FSRepository.datasets.GMQLDataSetXML
import it.polimi.genomics.repository.GMQLRepository
import it.polimi.genomics.repository.GMQLRepository.Utilities._
import it.polimi.genomics.repository.GMQLRepository._
import org.apache.hadoop.fs.{FileSystem, Path}
import org.slf4j.LoggerFactory

import scala.xml.XML
import scala.collection.JavaConverters._
/**
  * Created by abdulrahman on 12/04/16.
  */
class DFSRepository extends GMQLRepository{
  private final val logger = LoggerFactory.getLogger(this.getClass)
    GMQLRepository.Utilities()
  /**
    *
    * DO NOT Forget to check the existance ot the datasetname before creating the dataset
    *
    * @param dataSet
    * @param Schema
    * @param Samples
    * @param GMQLScriptPaht
    * @throws GMQLDSNotFound
    * @throws it.polimi.genomics.repository.GMQLRepository.GMQLUserNotFound
    * @throws it.polimi.genomics.repository.GMQLRepository.GMQLSampleNotFound
    */
  override def createDs(dataSet:IRDataSet, userName: String, Samples: java.util.List[GMQLSample], GMQLScriptPaht: String,schemaType:GMQLSchemaTypes.Value): Unit = {
    import it.polimi.genomics.repository.GMQLRepository.Utilities._
    new File(GMQLRepository.Utilities.GMQLHOME+"/tmp/"+userName+"/"+dataSet.position+"_/").mkdirs()
    val samples = Samples.asScala.map{x=>
      val metaFile= GMQLRepository.Utilities.GMQLHOME+"/tmp/"+userName+"/"+dataSet.position+"_/"+new File(x.name+".meta").getName
      Utilities.copyfiletoLocal(HDFSRepoDir + userName + "/regions/" + x.name+".meta", metaFile)
      new GMQLSample(x.name, metaFile,x.ID)
    }.toList
    val gMQLDataSetXML = new GMQLDataSetXML(dataSet,userName,samples,GMQLScriptPaht, schemaType,"GENERATED_HDFS")
    gMQLDataSetXML.Create()
  }

  /**
    *
    * DO NOT Forget to check the existance ot the datasetname before creating the dataset
    *
    * @param dataSet
    * @param Schema
    * @param Samples
    * @param GMQLScriptPaht
    * @throws GMQLDSNotFound
    * @throws it.polimi.genomics.repository.GMQLRepository.GMQLUserNotFound
    * @throws it.polimi.genomics.repository.GMQLRepository.GMQLSampleNotFound
    */
  override def importDs(dataSetName:String, userName: String, Samples: java.util.List[GMQLSample], schemaPath:String): Unit = {
//    Files.copy((new File(schemaPath)),(new File(GMQLRepository.Utilities.RepoDir + userName + "/schema/" + dataSetName + ".schema")))
   if(Utilities.validate(schemaPath)) {
     val xmlFile = XML.load(schemaPath)
     val cc = (xmlFile \\ "field")
     val schemaType = (xmlFile \\ "gmqlSchema").head.attribute("type").get.head.text
     val schema = cc.map { x => (x.text.trim, attType(x.attribute("type").get.head.text)) }.toList.asJava
     val dataSet = new IRDataSet(dataSetName, schema)
     val gMQLDataSetXML = new GMQLDataSetXML(dataSet, userName, Samples.asScala.toList, Utilities.getType(schemaType), "IMPORTED_HDFS")
     gMQLDataSetXML.Create()

     // Move data into HDFS
     Samples.asScala.map(x => Utilities.copyfiletoHDFS(x.name, GMQLRepository.Utilities.HDFSRepoDir + userName + "/regions/" + x.name))
   }else {
      logger.info("The dataset schema does not confirm the schema style (XSD)")
   }
  }

  /**
    *
    * @param dataSet
    * @param Sample
    * @throws GMQLDSNotFound
    * @throws it.polimi.genomics.repository.GMQLRepository.GMQLDSException
    * @throws it.polimi.genomics.repository.GMQLRepository.GMQLUserNotFound
    * @throws it.polimi.genomics.repository.GMQLRepository.GMQLSampleNotFound
    */
  override def AddSampleToDS(dataSet: IRDataSet, userName: String, Sample: GMQLSample): Unit = ???

  /**
    *
    * @param dataSet
    * @param sample
    * @return
    */
  override def getSampleMeta(dataSet: IRDataSet, userName: String, sample: GMQLSample): String = ???

  /**
    *
    * @param dataSet
    * @throws GMQLDSNotFound
    * @throws it.polimi.genomics.repository.GMQLRepository.GMQLDSException
    * @throws it.polimi.genomics.repository.GMQLRepository.GMQLUserNotFound
    */
  override def DeleteDS(dataSetName:String, userName:String): Unit = {
    val dataset = new GMQLDataSetXML(dataSetName,userName).loadDS()
    val conf = Utilities.gethdfsConfiguration()
    val fs = FileSystem.get(conf)
    val hdfspath = conf.get("fs.defaultFS") +GMQLRepository.Utilities.HDFSRepoDir+ userName + "/regions/"
    dataset.samples.map{x=> fs.delete(new Path(hdfspath+x.name),true);fs.delete(new Path(hdfspath+x.meta),true)}
    dataset.Delete()
  }

  /**
    *
    * @param dataSet
    * @param Sample
    * @throws GMQLDSNotFound
    * @throws it.polimi.genomics.repository.GMQLRepository.GMQLDSException
    * @throws it.polimi.genomics.repository.GMQLRepository.GMQLUserNotFound
    * @throws it.polimi.genomics.repository.GMQLRepository.GMQLSampleNotFound
    */
  override def DeleteSampleFromDS(dataSet:IRDataSet, userName: String, Sample:GMQLSample): Unit = ???

  /**
    *
    * @param userName
    * @throws GMQLDSException
    * @throws it.polimi.genomics.repository.GMQLRepository.GMQLUserNotFound
    */
  override def ListAllDSs(userName: String): java.util.List[IRDataSet] = {
    val dSs = new File(it.polimi.genomics.repository.GMQLRepository.Utilities.RepoDir + userName+"/datasets/").listFiles(new FilenameFilter() {
      def accept(dir: File, name: String): Boolean = {
        return name.endsWith(".xml")
      }
    })
    import scala.collection.JavaConverters._
    dSs.map(x=>new GMQLDataSetXML(new IRDataSet(x.getName().subSequence(0, x.getName().length() - 4).toString(),List[(String,PARSING_TYPE)]().asJava),userName).loadDS().dataSet).toList.asJava
  }

  /**
    *
    * @param dataSet
    * @throws GMQLDSException
    * @return
    */
  override def DSExists(dataSet: IRDataSet, userName: String): Boolean = ???

  /**
    *
    * @param dataSet
    * @throws GMQLDSException
    * @return
    */
  override def DSExistsInPublic(dataSet: IRDataSet, userName: String): Boolean = ???

  /**
    *
    * @param dataSet
    * @throws GMQLDSException
    * @return
    */
  override def ListDSSamples(dataSetName:String, userName: String): java.util.List[GMQLSample] ={
    new GMQLDataSetXML(dataSetName,userName).loadDS().samples.asJava
  }

  /**
    *
    * @param dataSet
    * @throws GMQLDSException
    * @return
    */
  override def ListResultDSSamples(dataSetName:String , userName: String): java.util.List[GMQLSample] = {
    val conf = Utilities.gethdfsConfiguration()
    val fs = FileSystem.get(conf);
    fs.listStatus(new Path(conf.get("fs.defaultFS") +GMQLRepository.Utilities.HDFSRepoDir+ userName + "/regions/" + dataSetName))
        .flatMap(x => {
          if (fs.exists(new Path(x.getPath.toString+".meta")) ) {
            Some(new GMQLSample(dataSetName+x.getPath.getName))
          } else
            None
        }).toList.asJava;
  }
  /**
    *
    * @param dataSet
    * @return
    */
  override def getDSStatistics(dataSet: IRDataSet, userName: String): GMQLStatistics = ???



  /**
    * DO NOT Forget to check the existance ot the dataset name before copying the dataset
    *
    * @param dataSet
    * @throws GMQLDSException
    */
  override def exportDsToLocal(dataSetName: String, userName: String, localDir:String): Unit = {

    import it.polimi.genomics.repository.GMQLRepository.Utilities._
    val gMQLDataSetXML = new GMQLDataSetXML(dataSetName, userName).loadDS()

    val dest = new File(localDir)
    dest.mkdir()

    gMQLDataSetXML.samples.map { x =>
      Utilities.copyfiletoLocal(HDFSRepoDir + userName + "/regions/" + x.name, localDir + "/" + new File(x.name).getName)
    }

    val srcSchema = new File(gMQLDataSetXML.schemaDir)
    if (srcSchema.exists()) {
    val schemaOS = new FileOutputStream(dest + "/" + srcSchema.getName)
    schemaOS getChannel() transferFrom(
      new FileInputStream(srcSchema) getChannel, 0, Long.MaxValue)
    }

    val srcScript = new File (gMQLDataSetXML.GMQLScriptUrl)
    if(srcScript.exists()) {
      val scriptOS = new FileOutputStream(dest + "/" + srcScript.getName)
      scriptOS getChannel() transferFrom(
        new FileInputStream(srcScript) getChannel, 0, Long.MaxValue)
      scriptOS.close()
    }
  }


  def getSchema(dataSetName: String, userName:String): java.util.List[(String,PARSING_TYPE)] = {
    val gtfFields = List("seqname","source","feature","start","end","score","strand","frame")
    val tabFields = List("chr","left","right","strand")
    val xmlFile = XML.load(GMQLRepository.Utilities.RepoDir + userName + "/schema/" + (new File(dataSetName)).getParent + ".schema")
    val cc = (xmlFile \\ "field")
    cc.flatMap{x => if(gtfFields.contains(x.text.trim)||tabFields.contains(x.text.trim)) None else Some(x.text.trim, attType(x.attribute("type").get.head.text))}.toList.asJava
  }

  /**
    *
    * @param dataSet
    * @param query
    * @return
    */
  override def searchMeta(dataSet: IRDataSet, userName: String, query: String): java.util.List[GMQLSample] = ???

  /**
    *
    * @param dataSet
    * @return
    */
  override def getMeta(dataSet: IRDataSet, userName: String): String = ???

  def attType(x: String) = x.toUpperCase match {
    case "STRING" => ParsingType.STRING
    case "CHAR" => ParsingType.STRING
    case "CHARACTAR" => ParsingType.STRING
    case _ => ParsingType.DOUBLE
  }
}
