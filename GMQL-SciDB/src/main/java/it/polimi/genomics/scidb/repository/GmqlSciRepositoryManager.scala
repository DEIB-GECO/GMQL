package it.polimi.genomics.scidb.repository

import java.io.File

import it.polimi.genomics.core.DataStructures.IRDataSet
import it.polimi.genomics.core.ParsingType._
import it.polimi.genomics.scidb.GmqlSciConfig
import it.polimi.genomics.scidb.utility.SchemaUtils
import it.polimi.genomics.scidbapi.SciArray
import it.polimi.genomics.scidbapi.schema.DataType
import it.polimi.genomics.scidbapi.script.{SciOperation, SciScript}
import org.slf4j.LoggerFactory

import scala.io.Source
import scala.sys.process._

class GmqlSciRepositoryManager
{
  final val log = LoggerFactory.getLogger(this.getClass)

  // ------------------------------------------------------------
  // -- IMPORT --------------------------------------------------

  /**
    * This method load the dataset inside the remote server
    * and import it into SciDB
    *
    * @param name
    * @param path
    * @param columns
    * @param format
    * @param complement
    */
  def importation(name:String,
                  path:String,
                  columns:List[(String, PARSING_TYPE, Int)],
                  format:String = "tsv",
                  complement:Option[String] = None) : Unit =
  {
    var loading_dir = path

    // ------------------------------------------------

    if( GmqlSciConfig.scidb_server_remote ) {
      loading_dir = GmqlSciConfig.scidb_server_import_dir +"/"+ name

      // remove previous importation directory
      try{ GmqlSciFileSystem.rmR(loading_dir) }
      catch{ case _:Throwable => }

      // creates the target directory
      GmqlSciFileSystem.mkdir(loading_dir)

      // loads the dataset
      log.info("Copy input files into local directory")
      var cmd = "sshpass -p "+ GmqlSciConfig.scidb_server_password +
        " scp -r "+ path +" "+
        GmqlSciConfig.scidb_server_username +"@"+ GmqlSciConfig.scidb_server_ip +":"+ loading_dir
      cmd.!!

      // finalize
      loading_dir = loading_dir + "/" + (new File(path)).getName
    }

    // ------------------------------------------------

    fetch(name) match
    {
      case None =>
      case Some(dataset) => {
        log.info("Remove previous version of dataset")
        delete(dataset)
      }
    }

    // ------------------------------------------------

    log.info("Import dataset")
    GmqlSciImporter.importDS(
      name,
      loading_dir,
      columns,
      format,
      complement
    )

  }

  // ------------------------------------------------------------
  // -- EXPORT --------------------------------------------------

  /**
    * This method exports a required dataset to files and
    * transfer them to local directory if required
    *
    * @param dataset
    * @param path
    * @param format
    */
  def exportation(dataset:IRDataSet,
                  path:String,
                  format:String = "tsv") : Unit =
  {
    var downloading_dir = path

    // ------------------------------------------------

    if( GmqlSciConfig.scidb_server_remote ) {
      downloading_dir = GmqlSciConfig.scidb_server_export_dir + "/" + dataset.position

      // remove previous importation directory
      try{ GmqlSciFileSystem.rmR(downloading_dir) }
      catch{ case _:Throwable=> }

      // creates the target directory
      GmqlSciFileSystem.mkdir(downloading_dir)
    }

    // ------------------------------------------------

    import scala.collection.JavaConverters._
    GmqlSciExporter.exportDS(
      dataset.position,
      downloading_dir,
      dataset.schema,
      format
    )

    // ------------------------------------------------

    if( GmqlSciConfig.scidb_server_remote ) {
      var cmd = "sshpass -p "+ GmqlSciConfig.scidb_server_password +
        " scp -r "+
        GmqlSciConfig.scidb_server_username +"@"+ GmqlSciConfig.scidb_server_ip +":"+ downloading_dir +
        " "+ path
      cmd.!!
    }

  }

  // ------------------------------------------------------------
  // -- FETCH ---------------------------------------------------

  /**
    * The method returns a dataset if it exists
    * at the required location
    *
    * @param location dataset location inside technology
    * @return fetched dataset
    */
  def fetch(location:String) : Option[IRDataSet] =
  {
    val ATTRIBUTES = SciArray.attributes(location + "_RD")

    // ----------------------------------------------
    // Fetch schema ---------------------------------

    val script = new SciScript
    script.addStatement(ATTRIBUTES)

    val result = (GmqlSciConfig.scidb_server_on || true) match
    {
      case false => None
      case true =>
      {
        try{
          Some(script.ocsv(
            GmqlSciConfig.scidb_server_ip, GmqlSciConfig.scidb_server_username,
            GmqlSciConfig.scidb_server_password, GmqlSciConfig.scidb_server_runtime_dir
          ))
        } catch {
          case ex:Throwable => None
        }
      }
    }

    // ----------------------------------------------
    // Dataset fetch --------------------------------

    val dataset = if(result.isEmpty) None else
    {import scala.collection.JavaConverters._
      val csv = Source.fromString(result.get)
      val schema = csv.getLines()
        .toList//.tail
        .map(_.replace("'",""))
        .map(_.split(",").map(_.trim))
        .map(attr => (attr(0), SchemaUtils.toGmqlType( DataType.fromValue(attr(1)) ))).asJava

      Some(IRDataSet(location, schema))
    }

    // ----------------------------------------------
println(dataset)
    return dataset
  }

  // ------------------------------------------------------------
  // ------------------------------------------------------------

  def delete(dataset:IRDataSet) : Unit =
  {
    val script = new SciScript
    script.addStatement(new SciOperation("remove", dataset.position+"_MD"))
    script.addStatement(new SciOperation("remove", dataset.position+"_RD"))

    val result = GmqlSciConfig.scidb_server_on match
    {
      case false =>
      case true => {
        try{
          script.run(
            GmqlSciConfig.scidb_server_ip, GmqlSciConfig.scidb_server_username,
            GmqlSciConfig.scidb_server_password, GmqlSciConfig.scidb_server_runtime_dir
          )
        } catch { case ex:Throwable => }
      }
    }
  }

}
