import it.polimi.genomics.importer.GMQLImporter.{GMQLDataset, GMQLLoader, GMQLSource, GMQLDownloader, GMQLTransformer}
import it.polimi.genomics.importer.GMQLImporter.utils.SCHEMA_LOCATION
import scala.xml.XML
import org.slf4j._
object example {
  val logger: Logger = LoggerFactory.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {
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
      if(source.download_enabled){
        Class.forName(source.downloader).newInstance.asInstanceOf[GMQLDownloader].download(source)
      }
      if(source.transform_enabled){
        Class.forName(source.transformer).newInstance.asInstanceOf[GMQLTransformer].transform(source)
      }
      if(source.load_enabled){
        GMQLLoader.loadIntoGMQL(source)
      }
    })
  }
}
