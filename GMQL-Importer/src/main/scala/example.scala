import it.polimi.genomics.importer.GMQLImporter.{GMQLSource,GMQLDataset,GMQLLoader}
import it.polimi.genomics.importer.GMQLImporter.utils.SCHEMA_LOCATION
import it.polimi.genomics.importer.GMQLImporter.Defaults.{FTPDownloader, NULLTransformer}
import it.polimi.genomics.importer.ENCODEImporter.{ENCODEDownloader, ENCODETransformer}
import scala.xml.XML
import org.slf4j._
object example {
  val logger = LoggerFactory.getLogger(example.getClass)

  def main(args: Array[String]): Unit = {
    org.slf4j.LoggerFactory.getLogger(example.getClass).debug("hello")
    try{
      throw new Exception("asd")
    }
    catch {
      case e:Exception => logger.warn("ARIF!",e)
    }
    runDemo()

  }
  /**
    * Starts a demonstration of the functionality of GMQL-Importer,
    * xml configuration file is defined inside the code.
    * meant to show the functionality and usage of the project classes.
    */
  def runDemo(): Unit = {
    val sources = (XML.loadFile("GMQL-Importer/Example/xml/ExampleConfiguration.xml")\\"source_list"\"source").map(source=> {
      GMQLSource(
        (source \ "@name").text,
        (source \ "url").text,
        (source \ "output_folder").text,
        (source \ "gmql_user").text,
        (source \ "downloader").text,
        if ((source \ "download_enabled").text.toLowerCase == "true") true else false,
        (source \ "transformer").text,
        if ((source \ "transform_enabled").text.toLowerCase == "true") true else false,
        if ((source \ "load_enabled").text.toLowerCase == "true") true else false,
        (source \ "parameter_list" \ "parameter").map(parameter => {
          ((parameter \ "key").text, (parameter \ "value").text)
        }),
        (source \ "dataset_list" \ "dataset").map(dataset => {
          GMQLDataset(
            (dataset \ "output_folder").text,
            (dataset \ "schema").text,
            SCHEMA_LOCATION.withName((dataset \ "schema" \ "@location").text),
            (dataset \ "parameter_list" \ "parameter").map(parameter => {
              ((parameter \ "key").text, (parameter \ "value").text)
            })
          )
        })
      )
    })
    sources.foreach(source =>{
      if(source.download){
        source.downloader match{
          case "FTPDownloader" => FTPDownloader.download(source)
          case "ENCODEDownloader" => ENCODEDownloader.download(source)
          case _ => println("no downlaoder")
        }
      }
      if(source.transform){
        source.transformer match {
          case "NULLTransformer" => NULLTransformer.transform(source)
          case "ENCODETransformer" => ENCODETransformer.transform(source)
          case _ => println("no transformer")
        }
      }
      if(source.load){
        GMQLLoader.loadIntoGMQL(source)
      }
    })
  }
}
