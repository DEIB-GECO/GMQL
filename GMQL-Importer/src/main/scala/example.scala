import java.io.File

import it.polimi.genomics.importer.FileDatabase.FileDatabase
import it.polimi.genomics.importer.GMQLImporter._
import it.polimi.genomics.importer.GMQLImporter.utils.SCHEMA_LOCATION
import org.slf4j._

import scala.xml.{Elem, XML}

object example {
  val logger: Logger = LoggerFactory.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {
//    runTest("GMQL-Importer/Example/xml/ExampleConfiguration.xml")
    run("GMQL-Importer/Example/xml/ExampleConfiguration.xml")
  }
  /*
  def demoReplaceMeta(): Unit ={
    val file: Elem = XML.loadFile("GMQL-Importer/Example/xml/metadataReplacement.xml")
    val metadataChanges: Seq[(String, String)] = (file\\"metadata_replace_list"\"metadata_replace").map(replacement => ((replacement\"regex").text,(replacement\"replace").text))
    new it.polimi.genomics.importer.DefaultImporter.NULLTransformer().changeMetadataKeys(metadataChanges,"GMQL-Importer/Example/change.meta")
  }*/
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
}
