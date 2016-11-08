package it.polimi.genomics.importer.main

import java.io.File

import it.polimi.genomics.importer.GMQLImporter.utils.SCHEMA_LOCATION
import it.polimi.genomics.importer.GMQLImporter._
import org.slf4j._

import scala.xml.{Elem, XML}

object program {
  val logger: Logger = LoggerFactory.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {
    run(args.head)
  }

  def run(xmlConfigPath: String): Unit = {
    //general settings
    if (new File(xmlConfigPath).exists()) {
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
          outputFolder + File.separator + (source \ "output_folder").text,
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
              (dataset \ "schema").text,
              SCHEMA_LOCATION.withName((dataset \ "schema" \ "@location").text),
              if ((dataset \ "download_enabled").text.toLowerCase == "true") true else false,
              if ((dataset \ "transform_enabled").text.toLowerCase == "true") true else false,
              if ((dataset \ "load_enabled").text.toLowerCase == "true") true else false,
              (dataset \ "parameter_list" \ "parameter").map(parameter => {
                ((parameter \ "key").text, (parameter \ "value").text)
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
        }
        if (loadEnabled && source.loadEnabled) {
          GMQLLoader.loadIntoGMQL(source)
        }
      })
    }
    else
      logger.warn(xmlConfigPath+" does not exist")
  }
}

