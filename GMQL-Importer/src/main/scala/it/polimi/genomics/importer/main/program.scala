package it.polimi.genomics.importer.main

import java.io.File
import it.polimi.genomics.importer.GMQLImporter.utils.SCHEMA_LOCATION
import it.polimi.genomics.importer.GMQLImporter._
import it.polimi.genomics.importer.FileDatabase.FileDatabase
import org.slf4j._
import scala.xml.{Elem, XML}

object program {
  val logger: Logger = LoggerFactory.getLogger(this.getClass)

  /**
    * depending on the arguments, can run download/transform/load procedure or
    * delete transformed folder.
    * @param args arguments for GMQLImporter (help for more information)
    */
  def main(args: Array[String]): Unit = {
    if(args.length == 0){
      logger.info("arguments specified, run with help for more information")
    }
    else{
      if(args.contains("help")){
        logger.info("GMQLImporter help:\n"
          +"\t Run with configuration_xml_path as argument\n"
          +"\t\t -Will run whole process defined in xml file\n"
          +"\t Run with configuration_xml_path dt\n"
          +"\t\t-Will delete Transformations folder for datasets\n"
          +"\t\t defined to transform in configuration xml\n"
        )
      }
      else if(args.contains("dt")){
        if(args.head.endsWith(".xml"))
          deleteTransformations(args.head)
        else
          logger.warn("No configuration file specified")

      }
      else if(args.head.endsWith(".xml")){
        run(args.head)
      }
    }
  }

  /**
    * by having a configuration xml file runs downloaders/transformers/loader for the sources and their
    * datasets if defined to.
    * @param xmlConfigPath xml configuration file location
    */
  def run(xmlConfigPath: String): Unit = {
    //general settings
    if (new File(xmlConfigPath).exists()) {
      val file: Elem = XML.loadFile(xmlConfigPath)
      val outputFolder = (file \\ "settings" \ "output_folder").text
      val downloadEnabled = if ("true".equalsIgnoreCase((file \\ "settings" \ "download_enabled").text)) true else false
      val transformEnabled = if ("true".equalsIgnoreCase((file \\ "settings" \ "transform_enabled").text)) true else false
      val loadEnabled = if ("true".equalsIgnoreCase((file \\ "settings" \ "load_enabled").text)) true else false
      //load sources
      val sources = loadSources(xmlConfigPath)
      //start database
      FileDatabase.setDatabase(outputFolder)
      //start run
      val runId = FileDatabase.runId(
        downloadEnabled.toString,transformEnabled.toString,loadEnabled.toString,outputFolder)

      //start DTL
      sources.foreach(source => {
        val sourceId=FileDatabase.sourceId(source.name)
        val runSourceId = FileDatabase.runSourceId(runId,sourceId,source.url,source.outputFolder,source.downloadEnabled.toString,source.downloader,source.transformEnabled.toString,source.transformer,source.loadEnabled.toString,source.loader)
        source.parameters.foreach(parameter =>{
          FileDatabase.runSourceParameterId(runSourceId,parameter._3,parameter._1,parameter._2)
        })
        source.datasets.foreach(dataset =>{
          val datasetId = FileDatabase.datasetId(sourceId,dataset.name)
          val runDatasetId = FileDatabase.runDatasetId(runId,datasetId,dataset.outputFolder,dataset.downloadEnabled.toString,dataset.transformEnabled.toString,dataset.loadEnabled.toString,dataset.schemaUrl,dataset.schemaLocation.toString)
          dataset.parameters.foreach(parameter =>{
            FileDatabase.runDatasetParameterId(runDatasetId,parameter._3,parameter._1,parameter._2)
          })
        })
        if (downloadEnabled && source.downloadEnabled) {
          Class.forName(source.downloader).newInstance.asInstanceOf[GMQLDownloader].download(source)
        }
        if (transformEnabled && source.transformEnabled) {
          Class.forName(source.transformer).newInstance.asInstanceOf[GMQLTransformer].transform(source)
        }
        if (loadEnabled && source.loadEnabled) {
          GMQLLoader.loadIntoGMQL(source)
        }
      })
      //end DTL

      //end database run
      FileDatabase.endRun(runId)
      //close database

      FileDatabase.printDatabase()

      FileDatabase.closeDatabase()
    }
    else
      logger.warn(xmlConfigPath+" does not exist")
  }

  /**
    * from xmlConfigPath loads the sources there defined. All folder paths defined inside sources and
    * datasets are referred from the base root output folder (working directory).
    * @param xmlConfigPath xml configuration file location
    * @return Seq of sources with their respective datasets and settings.
    */
  def loadSources(xmlConfigPath: String): Seq[GMQLSource]={
    val file: Elem = XML.loadFile(xmlConfigPath)
    val outputFolder = (file \\ "settings" \ "output_folder").text
    //load sources
    val sources = (file \\ "source_list" \ "source").map(source => {
      GMQLSource(
        (source \ "@name").text,
        (source \ "url").text,
        outputFolder + File.separator + (source \ "output_folder").text,
        outputFolder,
        if ((source \ "download_enabled").text.toLowerCase == "true") true else false,
        (source \ "downloader").text,
        if ((source \ "transform_enabled").text.toLowerCase == "true") true else false,
        (source \ "transformer").text,
        if ((source \ "load_enabled").text.toLowerCase == "true") true else false,
        (source \ "loader").text,
        (source \ "parameter_list" \ "parameter").map(parameter => {
          ((parameter \ "key").text, (parameter \ "value").text,(parameter \ "description").text)
        }),
        (source \ "dataset_list" \ "dataset").map(dataset => {
          GMQLDataset(
            (dataset \ "@name").text,
            (dataset \ "output_folder").text,
            outputFolder + File.separator +(dataset \ "schema").text,
            SCHEMA_LOCATION.withName((dataset \ "schema" \ "@location").text),
            if ((dataset \ "download_enabled").text.toLowerCase == "true") true else false,
            if ((dataset \ "transform_enabled").text.toLowerCase == "true") true else false,
            if ((dataset \ "load_enabled").text.toLowerCase == "true") true else false,
            (dataset \ "parameter_list" \ "parameter").map(parameter => {
              ((parameter \ "key").text, (parameter \ "value").text,(parameter \ "description").text )
            })
          )
        }),
        (source \ "dataset_list" \ "merged_dataset").map(dataset => {
          GMQLDataset(
            (dataset \ "@name").text,
            (dataset \ "output_folder").text,
            "",//because merged schema has to be created.
            SCHEMA_LOCATION.LOCAL,
            downloadEnabled = false,//I dont use download. and I use transform to put the merge.
            transformEnabled = if ((dataset \ "merge_enabled").text.toLowerCase == "true") true else false,
            loadEnabled = if ((dataset \ "load_enabled").text.toLowerCase == "true") true else false,
            (dataset \ "origin_dataset_list" \ "origin_dataset").map(parameter => {
              ("origin_dataset", parameter.text,"asdas")
            })
          )
        })
      )
    })
    sources
  }

  /**
    * deletes folders of transformations on the transformation_enabled datasets.
    * @param xmlConfigPath xml configuration file location
    */
  def deleteTransformations(xmlConfigPath: String): Unit = {
    if (new File(xmlConfigPath).exists()) {
      val file: Elem = XML.loadFile(xmlConfigPath)
      val transformEnabled = if ("true".equalsIgnoreCase((file \\ "settings" \ "transform_enabled").text)) true else false
      if(transformEnabled) {
        val sources = loadSources(xmlConfigPath)
        sources.foreach(source => {
          if (source.transformEnabled) source.datasets.foreach(dataset => {
            if (dataset.transformEnabled) {
              val path = source.outputFolder + File.separator + dataset.outputFolder + File.separator + "Transformations"
              val folder = new File(path)
              if (folder.exists()) {
                deleteFolder(folder)
                logger.info("Folder: " + path + " DELETED")
              }
            }
          })
        })
      }
    }
    else
      logger.warn(xmlConfigPath+" does not exist")
  }

  /**
    * deletes folder recursively
    * @param path base folder path
    */
  def deleteFolder(path: File): Unit ={
    if( path.exists() ) {
      val files = path.listFiles()
      files.foreach(file=>{
        if(file.isDirectory)
          deleteFolder(file)
        else
          file.delete()
      })
    }
  }
}

