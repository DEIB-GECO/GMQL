import it.polimi.genomics.importer.GMQLImporter._
import it.polimi.genomics.importer.GMQLImporter.utils.SCHEMA_LOCATION

import scala.xml.{Elem, XML}
import java.io.File

import org.slf4j._

import slick.driver.H2Driver.api._

import scala.concurrent.ExecutionContext.Implicits.global

import scala.collection.immutable.Seq

object example {
  val logger: Logger = LoggerFactory.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {
//    runTest("GMQL-Importer/Example/xml/ExampleConfiguration.xml")
    testh2()
  }
  // Definition of the SUPPLIERS table
  class Suppliers(tag: Tag) extends Table[(Int, String, String, String, String, String)](tag, "SUPPLIERS") {
    def id = column[Int]("SUP_ID", O.PrimaryKey) // This is the primary key column
    def name = column[String]("SUP_NAME")
    def street = column[String]("STREET")
    def city = column[String]("CITY")
    def state = column[String]("STATE")
    def zip = column[String]("ZIP")
    // Every table needs a * projection with the same type as the table's type parameter
    def * = (id, name, street, city, state, zip)
  }
  val suppliers = TableQuery[Suppliers]

  // Definition of the COFFEES table
  class Coffees(tag: Tag) extends Table[(String, Int, Double, Int, Int)](tag, "COFFEES") {
    def name = column[String]("COF_NAME", O.PrimaryKey)
    def supID = column[Int]("SUP_ID")
    def price = column[Double]("PRICE")
    def sales = column[Int]("SALES")
    def total = column[Int]("TOTAL")
    def * = (name, supID, price, sales, total)
    // A reified foreign key relation that can be navigated to create a join
    def supplier = foreignKey("SUP_FK", supID, suppliers)(_.id)
  }
  val coffees = TableQuery[Coffees]
  def testh2(): Unit ={
    val db = Database.forConfig("h2mem1")
    val setup = DBIO.seq(
      // Create the tables, including primary and foreign keys
      (suppliers.schema ++ coffees.schema).create,

      // Insert some suppliers
      suppliers += (101, "Acme, Inc.",      "99 Market Street", "Groundsville", "CA", "95199"),
      suppliers += ( 49, "Superior Coffee", "1 Party Place",    "Mendocino",    "CA", "95460"),
      suppliers += (150, "The High Ground", "100 Coffee Lane",  "Meadows",      "CA", "93966"),
      // Equivalent SQL code:
      // insert into SUPPLIERS(SUP_ID, SUP_NAME, STREET, CITY, STATE, ZIP) values (?,?,?,?,?,?)

      // Insert some coffees (using JDBC's batch insert feature, if supported by the DB)
      coffees ++= Seq(
        ("Colombian",         101, 7.99, 0, 0),
        ("French_Roast",       49, 8.99, 0, 0),
        ("Espresso",          150, 9.99, 0, 0),
        ("Colombian_Decaf",   101, 8.99, 0, 0),
        ("French_Roast_Decaf", 49, 9.99, 0, 0)
      )
      // Equivalent SQL code:
      // insert into COFFEES(COF_NAME, SUP_ID, PRICE, SALES, TOTAL) values (?,?,?,?,?)
    )

    val setupFuture = db.run(setup)
    println("Coffees:")
    db.run(coffees.result).map(_.foreach {
      case (name, supID, price, sales, total) =>
        println("  " + name + "\t" + supID + "\t" + price + "\t" + sales + "\t" + total)
    })
    try {
      // ...
    } finally db.close
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
