package it.polimi.genomics.repository.FSRepository

import java.io.{File, FileFilter, FilenameFilter}

import it.polimi.genomics.core.DataStructures.IRDataSet
import it.polimi.genomics.core.ParsingType
import it.polimi.genomics.core.ParsingType.PARSING_TYPE
import it.polimi.genomics.repository.GMQLRepository
import it.polimi.genomics.repository.GMQLRepository._
import it.polimi.genomics.repository.FSRepository.datasets.GMQLDataSetXML
import org.slf4j.LoggerFactory

import scala.xml.XML
import scala.collection.JavaConverters._
/**
  * Created by abdulrahman on 12/04/16.
  */
class LFSRepository extends GMQLRepository{
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
    @throws(classOf[GMQLNotValidDatasetNameException])
    @throws(classOf[GMQLUserNotFound])
  override def createDs(dataSet:IRDataSet, userName: String, Samples: java.util.List[GMQLSample], GMQLScriptPaht: String,schemaType:GMQLSchemaTypes.Value): Unit = {
      val samples = Samples.asScala.map{x=>
        new GMQLSample(x.name, x.name+".meta",x.ID)
      }.toList
      val gMQLDataSetXML = new GMQLDataSetXML(dataSet,userName,Samples.asScala.toList,GMQLScriptPaht,schemaType, "GENERATED_LOCAL")
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
  @throws(classOf[GMQLNotValidDatasetNameException])
  @throws(classOf[GMQLUserNotFound])
  override def importDs(dataSetName: String, userName: String, Samples: java.util.List[GMQLSample], schemaPath: String): Unit = {
    if (Utilities.validate(schemaPath)) {
      val xmlFile = XML.load(schemaPath)
      val cc = (xmlFile \\ "field")
      val schemaType = (xmlFile \\ "gmqlSchema").head.attribute("type").get.head.text
      val schema = cc.map { x => (x.text.trim, attType(x.attribute("type").get.head.text)) }.toList.asJava
      val dataSet = new IRDataSet(dataSetName, schema)
      val gMQLDataSetXML = new GMQLDataSetXML(dataSet, userName, Samples.asScala.toList, Utilities.getType(schemaType), "IMPORTED_LOCAL")
      gMQLDataSetXML.Create()
      // Move files to REGIONS FOLDER

    } else {
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
  @throws(classOf[GMQLDSException])
  override def AddSampleToDS(dataSet: IRDataSet, userName: String, Sample: GMQLSample) ={
    val ds = new GMQLDataSetXML(dataSet,userName).loadDS()
    ds.addSample(Sample)
  }

  /**
    *
    * @param dataSet
    * @param sample
    * @return
    */
  @throws(classOf[GMQLSampleNotFound])
  override def getSampleMeta(dataSet: IRDataSet, userName: String, sample: GMQLSample): String = {
    val ds = new GMQLDataSetXML(dataSet,userName).loadDS()
    ds.getMeta(sample)
  }

  /**
    *
    * @param dataSet
    * @throws GMQLDSNotFound
    * @throws it.polimi.genomics.repository.GMQLRepository.GMQLDSException
    * @throws it.polimi.genomics.repository.GMQLRepository.GMQLUserNotFound
    */
  @throws(classOf[GMQLDSException])
  override def DeleteDS(dataSetName:String, userName:String): Unit = {
    new GMQLDataSetXML(dataSetName,userName).loadDS().Delete()
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
  override def DeleteSampleFromDS(dataSet:IRDataSet, userName: String, sample:GMQLSample): Unit = {
    new GMQLDataSetXML(dataSet,userName).loadDS().delSample(sample)
  }

  /**
    *
    * @param userName
    * @throws GMQLDSException
    * @throws it.polimi.genomics.repository.GMQLRepository.GMQLUserNotFound
    */
  override def ListAllDSs(userName: String): java.util.List[IRDataSet] = {
    val dSs = new File(it.polimi.genomics.repository.GMQLRepository.Utilities().RepoDir + userName+"/datasets/").listFiles(new FilenameFilter() {
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
  override def DSExists(dataSet: IRDataSet, userName: String): Boolean = {
    new GMQLDataSetXML(dataSet,userName ).exists()
  }

  /**
    *
    * @param dataSet
    * @throws GMQLDSException
    * @return
    */
  override def DSExistsInPublic(dataSet: IRDataSet, userName: String): Boolean = {
    new GMQLDataSetXML(dataSet,"public").exists()
  }

  /**
    *
    * @param dataSet
    * @throws GMQLDSException
    * @return
    */
  override def ListDSSamples(dataSetName:String, userName: String): java.util.List[GMQLSample] = {
    new GMQLDataSetXML(dataSetName,userName).loadDS().samples.asJava
  }


  /**
    *
    * @param dataSet
    * @throws GMQLDSException
    * @return
    */
  override def ListResultDSSamples(dataSetName:String , userName: String): java.util.List[GMQLSample] = {
    new java.io.File(GMQLRepository.Utilities().RepoDir + dataSetName).listFiles(
      new FileFilter() {
        @Override def accept(pathname: java.io.File) = !pathname.getName.startsWith("_") && !pathname.getName.startsWith(".") && !pathname.getName.endsWith(".meta");
      }
    ).map(x=> new GMQLSample(x.getPath)).toList.asJava
  }
  /**
    *
    * @param dataSet
    * @return
    */
  override def getDSStatistics(dataSet: IRDataSet, userName: String): GMQLStatistics = ???

  /**
    *
    * @param dataSet
    * @return
    */
  override def getMeta(dataSet: IRDataSet,userName:String): String = {
    new GMQLDataSetXML(dataSet,userName).getMeta()
  }

  /**
    * DO NOT Forget to check the existance ot the dataset name before copying the dataset
    *
    * @param dataSet
    * @throws GMQLDSException
    */
  override def exportDsToLocal(dataSetName: String, userName: String, localDir:String): Unit = {
    val gMQLDataSetXML = new GMQLDataSetXML(dataSetName, userName).loadDS()

    import java.io.{File, FileInputStream, FileOutputStream}

    val dest = new File(localDir)
    val srcSchema = new File(gMQLDataSetXML.schemaDir)
    dest.mkdir()

    gMQLDataSetXML.samples.map { x =>
      val src = new File(x.name)
      val srcMeta = new File(x.meta)

      new FileOutputStream(dest + "/" + src.getName) getChannel() transferFrom(
        new FileInputStream(src) getChannel, 0, Long.MaxValue)

      new FileOutputStream(dest + "/" + srcMeta.getName) getChannel() transferFrom(
        new FileInputStream(srcMeta) getChannel, 0, Long.MaxValue)
    }

    if (srcSchema.exists()){
    val schemaOS = new FileOutputStream(dest + "/" + srcSchema.getName)
      schemaOS getChannel() transferFrom(
      new FileInputStream(srcSchema) getChannel, 0, Long.MaxValue)
    schemaOS.close()
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
    val gtfFields = List("seqname","source","feature","start","end","strand","frame")
    val tabFields = List("chr","left","right","strand")
    val xmlFile = XML.load(GMQLRepository.Utilities().RepoDir + userName + "/schema/" + new File(dataSetName).getParent + ".schema")
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

  def attType(x: String) = x.toUpperCase match {
    case "STRING" => ParsingType.STRING
    case "CHAR" => ParsingType.STRING
    case "CHARACTAR" => ParsingType.STRING
    case _ => ParsingType.DOUBLE
  }
  }
