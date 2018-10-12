package it.polimi.genomics.repository.federated

import java.util

import it.polimi.genomics.core.DataStructures.IRDataSet
import it.polimi.genomics.core._
import it.polimi.genomics.core.ParsingType.PARSING_TYPE
import it.polimi.genomics.repository.FSRepository.FS_Utilities
import it.polimi.genomics.repository.federated.communication._
import it.polimi.genomics.repository.{FSRepository, GMQLSample, Utilities}
import org.slf4j.{Logger, LoggerFactory}
import java.io.File

import javax.ws.rs.Path

import scala.xml.NodeSeq
import scala.collection.JavaConverters._


class GF_Communication private {

  val logger: Logger = LoggerFactory.getLogger(Utilities.getClass)

  val ns = new NameServer()
  val federation = new GMQLInstances(ns)

  private var downloadStatus :  Map[String, DownloadStatus] =  Map()


  def listDatasets() : util.List[IRDataSet] = {

    val datasets  = ns.get("/api/dataset")

    val names: NodeSeq = datasets \ "list-item" \ "identifier"

    val result = for( name <- names ) yield new IRDataSet(name.text, List[(String,PARSING_TYPE)]().asJava)

    return result.asJava

  }

  def getDataset(identifier : String) : FederatedDataset = {

    val dataset_xml = ns.get("/api/dataset/"+identifier)
    new FederatedDataset(dataset_xml)

  }

  def getLocation(identifier: String) : Location = {

    val location_xml = ns.get("/api/location/"+identifier)
    new Location(location_xml)

  }


  def getSamples(dataset_identifier:String) : util.List[GMQLSample]  = {

    val dataset = getDataset(dataset_identifier)
    val location = dataset.locations.head

    val uri = "/datasets/public."+dataset.name

    val samples_xml = federation.get(uri, location.id)

    val samples_set_xml = samples_xml \\ "sample"

    val samples =
      for(i <- 0 to samples_set_xml.length-1)
        yield GMQLSample(ID=i.toString, name = samples_set_xml(i) \ "name" text)

    samples.asJava

  }

  def getMeta(dataset_identifier: String) : String = {

    val samples = getSamples(dataset_identifier).asScala

    val result = for (sample : GMQLSample <- samples )
      yield getSampleMeta(dataset_identifier, sample.name, true, sample.ID.toString)

    result.reduce((x,y)=>x+"\n"+y)

  }

  def getSampleMeta(dataset_identifier: String, sample: String, withID: Boolean = false, ID: String = "0") : String = {

    val dataset = getDataset(dataset_identifier)
    val location = dataset.locations.head

    val uri = "/metadata/public."+dataset.name+"/sample/"+sample

    val metadata_xml = federation.get(uri, location.id)

    val metadata =
      for (meta <- metadata_xml \\ "attribute" )
        yield {
          ( (meta \ "key" text) -> (meta \ "value" text) )
        }

    if (withID)
      metadata.map(x=>ID+"\t"+x._1+"\t"+x._2).reduce((x,y)=>x+"\n"+y)
    else
      metadata.map(x=>x._1+"\t"+x._2).reduce((x,y)=>x+"\n"+y)

  }


  /**
    * Retrieve the schema of a dataset from one of its locations
    * @param dataset_identifier
    * @return
    */
  def getSchema (dataset_identifier:String) : GMQLSchema = {

    val dataset = getDataset(dataset_identifier)
    val location = dataset.locations.head

    val uri = "/datasets/public."+dataset.name+"/schema"

    val schema_xml = federation.get(uri, location.id )

    var coordinate_system = GMQLSchemaCoordinateSystem.Default

    coordinate_system = GMQLSchemaCoordinateSystem.getType(schema_xml \ "coordinate_system"  text)

    val schema = GMQLSchema ( schema_xml \ "name" text,
      GMQLSchemaFormat.withName(schema_xml \ "type" text),
      coordinate_system,
      (for(item <- schema_xml \ "field") yield new GMQLSchemaField( item \ "name" text, ParsingType.attType(item \ "type" text) ) ) toList
    )

    schema
  }

  def getDasetProfile(dataset_identifier: String) : Map[String, String] = {

    val dataset = getDataset(dataset_identifier)
    val location = dataset.locations.head

    val uri = "/datasets/public."+dataset.name+"/info"

    val info_xml = federation.get(uri, location.id)

    var info  =
      for ( item <- info_xml \ "info")
        yield  (item \ "key" text , item \ "value" text)

    info.toMap[String, String]

  }

  def getSampleProfile(dataset_identifier: String, sample_name: String) : Map[String, String] = {

    val dataset = getDataset(dataset_identifier)
    val location = dataset.locations.head

    val uri = "/datasets/public."+dataset.name+"/"+sample_name+"/info"

    val info_xml = federation.get(uri, location.id)

    var info  =
      for ( item <- info_xml \ "info")
        yield  (item \ "key" text , item \ "value" text)

    info.toMap[String, String]
  }

  def getDatasetMeta(dataset_identifier: String) : Map[String, String] = {

    val dataset = getDataset(dataset_identifier)
    Map( "Owner" -> dataset.namespace,
      "Author" -> dataset.author,
      "Description" -> dataset.description,
      "Locations" -> dataset.locations.map(_.id).reduce((x,y)=>x+", "+y) )
  }


  def getDownloadStatus(job_id: String, dataset_id: String) =  {

    print(downloadStatus.map(_._2.getClass.getName).reduce((x,y)=>x+" "+y))

    val entity_id = job_id+"."+dataset_id

    if ( !downloadStatus.contains(entity_id) )
      NotFound()
    else
      downloadStatus(job_id+"."+dataset_id)
  }


  def importDataset(job_id: String, ds_name: String, location_id: String, destination_path: String) = {

    // IP Resolution
    val location = getLocation(location_id)

    val entity_id = job_id+"."+ds_name
    val folder_name = destination_path+"/"+job_id+"."+ds_name
    val dest_zip = folder_name+".zip"
    val final_dest_parent  = Utilities().getResultDir("federated")+"/"+job_id
    val final_dest  = final_dest_parent+"/"+ds_name+"/"


    downloadStatus += (entity_id -> Pending())

    // Start the download in a separated thread
    val thread = new Thread {
      override def run {

        val DEBUG_MODE = true

        // If is not already in final dest
        if( !FS_Utilities.checkExists(final_dest) || DEBUG_MODE) {


          // If the folder is not aready in temp
          if (!(new File(folder_name) exists) ) {

            // If the zip is not already in temp
            if (!(new File(dest_zip) exists) ) {
              logger.debug("Downloading the zip: " + dest_zip)

              try {
                downloadStatus += (entity_id -> Downloading())
                federation.download(location_id, job_id, ds_name, dest_zip)
              } catch {
                case e: NotFoundException => {
                  logger.error("Requested data not found at target location")
                  downloadStatus += (entity_id -> NotFound())
                  return

                }
                case e: Throwable => {
                  logger.error (e.getMessage)
                  downloadStatus += (entity_id -> Failed ("Download failed."))
                  return
                }
              }
            }

            // Unzip
            try {
              logger.debug("Unzipping to: " + folder_name)
              Unzip.unZipIt(dest_zip, destination_path)
            } catch {
              case e: Throwable => {
                logger.error (e.getMessage)
                downloadStatus += (entity_id -> Failed ("Unzipping failed."))
                return
              }
            }

          }


          // todo: delete these lines
          if( DEBUG_MODE && FS_Utilities.checkExists(final_dest) ) {
            FS_Utilities.deleteDFSDir(final_dest)
            logger.debug("Deleting "+final_dest)
          } else {
            logger.debug("Not found: "+final_dest)
          }


          // Move the folder from temp to final destination
          try {


            FS_Utilities.createDFSDir(final_dest_parent)
            FS_Utilities.createDFSDir(final_dest)

            val files = (new File(folder_name) listFiles).toList.map(_.getName)
            files.foreach(file => {
              logger.debug("Moving "+file+ " to " + final_dest)
              FS_Utilities.copyfiletoHDFS(folder_name+"/"+file, final_dest)
            })
          } catch {
            case e: Throwable => {
              logger.error (e.getMessage)
              downloadStatus += (entity_id -> Failed ("Moving to final location failed."))
              return
            }
          }

          // If the zip exists delete it
          if ( DEBUG_MODE && (new File(dest_zip) exists) ) {
            logger.debug("Deleting the zip file.")
            (new File(dest_zip)).delete()
          }

          // If the folder exists delete it
          if ( DEBUG_MODE && (new File(folder_name) exists) ) {
            logger.debug("Deleting the uncompressed folder "+folder_name)
            val f =  new File(folder_name)
            FS_Utilities.deleterecursive(f)
            f.delete()
          }


        }

        downloadStatus+= (entity_id -> Success())

      }
    }

    thread.start

  }

}

object GF_Communication {

  private var _instance : GF_Communication = null
  def instance() = {
    if (_instance == null)
      _instance = new GF_Communication()
    _instance
  }
}