import it.polimi.genomics.importer.GMQLImporter._
import it.polimi.genomics.importer.GMQLImporter.utils.SCHEMA_LOCATION

import scala.xml.{Elem, XML}
import java.io.File

import org.slf4j._
import slick.driver.H2Driver.api._
import slick.jdbc.meta.MTable

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.DurationInt
import scala.concurrent.Await
import it.polimi.genomics.importer.FileLogger.FileDatabase

object example {
  val logger: Logger = LoggerFactory.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {
    runTest("GMQL-Importer/Example/xml/ExampleConfiguration.xml")
//    FileDatabase.setDatabase("/home/nachon/git-repos/GMQL/GMQL-Importer/Example")
    //FileDatabase.dummies()
//    FileDatabase.test()
  }
  /*
  def demoReplaceMeta(): Unit ={
    val file: Elem = XML.loadFile("GMQL-Importer/Example/xml/metadataReplacement.xml")
    val metadataChanges: Seq[(String, String)] = (file\\"metadata_replace_list"\"metadata_replace").map(replacement => ((replacement\"regex").text,(replacement\"replace").text))
    new it.polimi.genomics.importer.DefaultImporter.NULLTransformer().changeMetadataKeys(metadataChanges,"GMQL-Importer/Example/change.meta")
  }*/
  def runTest(xmlConfigPath: String): Unit = {
    //general settings
    val file: Elem = XML.loadFile(xmlConfigPath)
    val outputFolder = (file \\ "settings" \ "output_folder").text
    val downloadEnabled = if ("true".equalsIgnoreCase((file \\ "settings" \ "download_enabled").text)) true else false
    val transformEnabled = if ("true".equalsIgnoreCase((file \\ "settings" \ "transform_enabled").text)) true else false
    val loadEnabled = if ("true".equalsIgnoreCase((file \\ "settings" \ "load_enabled").text)) true else false
    //load sources
    val sources = (file \\ "source_list" \ "source").map(source => {
      GMQLSource(
        (source \ "@name").text,
        (source \ "url").text,
        outputFolder+File.separator+(source \ "output_folder").text,
        outputFolder,
        (source \ "gmql_user").text,
        (source \ "downloader").text,
        (source \ "transformer").text,
        if ((source \ "download_enabled").text.toLowerCase == "true") true else false,
        if ((source \ "transform_enabled").text.toLowerCase == "true") true else false,
        if ((source \ "load_enabled").text.toLowerCase == "true") true else false,
        (source \ "parameter_list" \ "parameter").map(parameter => {
          ((parameter \ "key").text, (parameter \ "value").text)
        }),
        (source \ "dataset_list" \ "dataset").map(dataset => {
          GMQLDataset(
            (dataset \ "@name").text,
            (dataset \ "output_folder").text,
            outputFolder + File.separator+ (dataset \ "schema").text,
            SCHEMA_LOCATION.withName((dataset \ "schema" \ "@location").text),
            if ((dataset \ "download_enabled").text.toLowerCase == "true") true else false,
            if ((dataset \ "transform_enabled").text.toLowerCase == "true") true else false,
            if ((dataset \ "load_enabled").text.toLowerCase == "true") true else false,
            (dataset \ "parameter_list" \ "parameter").map(parameter => {
              ((parameter \ "key").text, (parameter \ "value").text)
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
              ("origin_dataset", parameter.text)
            })
          )
        })

      )
    })
    //start DTL
    sources.foreach(source => {
      if (downloadEnabled && source.downloadEnabled) {
        Class.forName(source.downloader).newInstance.asInstanceOf[GMQLDownloader].download(source)
      }
      if (transformEnabled && source.transformEnabled) {
        Class.forName(source.transformer).newInstance.asInstanceOf[GMQLTransformer].transform(source)
        GDMDatasetMerger.mergeDatasets(source)
      }
      if (loadEnabled && source.loadEnabled) {
        //GMQLLoader.loadIntoGMQL(source)
      }
    })
  }
}
