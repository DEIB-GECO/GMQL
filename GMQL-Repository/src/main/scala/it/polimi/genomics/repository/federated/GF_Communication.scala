package it.polimi.genomics.repository.federated

import java.util
import java.util.Calendar
import java.time.ZonedDateTime

import com.softwaremill.sttp._
import it.polimi.genomics.core.DataStructures.IRDataSet
import it.polimi.genomics.core._
import it.polimi.genomics.core.ParsingType.PARSING_TYPE
import it.polimi.genomics.repository.{GMQLSample, Utilities}
import org.slf4j.{Logger, LoggerFactory}

import scala.xml.{Elem, NodeSeq, XML}
import scala.collection.JavaConverters._

class GF_Communication {

  val logger: Logger = LoggerFactory.getLogger(Utilities.getClass)

  var ns_address = ""
  var ns_namespace  = ""
  var ns_token      = ""

  // Map [ namespace, token ]
  private var authentication = Map[String, Token]()


  if (Utilities().GF_NAMESERVER_ADDRESS.isEmpty )
    logger.error("GF_NAMESERVER_ADDRESS is not set in repository.xml. Please provide the NameServer address.")
  else
    ns_address = Utilities().GF_NAMESERVER_ADDRESS.get

  if (Utilities().GF_NAMESPACE.isEmpty)
    logger.error("GF_NAMESPACE is not set in repository.xml. Please provide your institutional namespace, e.g. it.polimi.")
  else
    ns_namespace = Utilities().GF_NAMESPACE.get

  if (Utilities().GF_TOKEN.isEmpty)
    logger.error("GF_TOKEN is not set in repository.xml. Please provide the token associated to your namespace")
  else
    ns_token =  Utilities().GF_TOKEN.get

  // Token Class
  class Token (value: String, expiration_string: String) {

    val expiration = ZonedDateTime.parse(expiration_string).toInstant.toEpochMilli

    def isExpired (): Boolean = {
      val now = Calendar.getInstance().getTime().getTime
      now > expiration
    }

  }

  // Helpers

  private def rest_get(uri: Uri, authorization: (String, String)) : Elem = {

    val request = sttp.get(uri)
      .header("Accept","application/xml")
      .header(authorization._1, authorization._2)

    implicit val backend = HttpURLConnectionBackend()
    val response = request.send()

    XML.loadString(response.unsafeBody)

  }

  // NAMESERVER COMMUNICATION

  /**
    * List the datasets provided by the federation
    * @return
    */
  def listDatasets () : util.List[IRDataSet] = {

    //getToken("it.polimi")
    //getToken("it.polimi")

    //getSchema("it.polimi.HG19_BED_ANNOTATIONS")
    //getDasetProfile("it.polimi.HG19_BED_ANNOTATIONS")
    //print(getSampleProfile("it.polimi.HG19_BED_ANNOTATIONS", "S_00000"))
    //getSamples("it.polimi.HG19_BED_ANNOTATIONS")
    //println(getSampleMeta("it.polimi.HG19_BED_ANNOTATIONS", "S_00000"))
    //println(getMeta("it.polimi.HG19_BED_ANNOTATIONS"))


    val datasets  = rest_get(uri"$ns_address/api/dataset", ("Authorization", s"Token $ns_token") )

    val names: NodeSeq = datasets \ "list-item" \ "identifier"

    val result = for( name <- names ) yield new IRDataSet(name.text, List[(String,PARSING_TYPE)]().asJava)

    return result.asJava

  }

  def getDataset(identifier : String) : FederatedDataset = {

    val dataset_xml = rest_get(uri"$ns_address/api/dataset/$identifier", ("Authorization", s"Token $ns_token") )
    new FederatedDataset(dataset_xml)

  }

  private def getToken (namespace:String) =  {

    if ( authentication.contains(namespace) && !authentication.get(namespace).get.isExpired() ) {
      authentication(namespace)
    } else {
      resetToken(namespace)
    }

    "63c4e817-c107-4a46-a0e4-556eccfeeb5f"
  }

  private def resetToken (target_namespace:String) = {

    val request = sttp
          .body(Map("target"->target_namespace))
          .post(uri"$ns_address/api/authentication/")
          .header("Accept","application/xml")
          .header("Authorization",s"Token $ns_token")

    implicit val backend = HttpURLConnectionBackend()
    val response = request.send()


    val token_xml     = XML.loadString(response.body.right.get)
    val token_string  = token_xml \ "token" text
    val token_expdate = token_xml \ "expiration" text

    authentication += (target_namespace -> new Token(token_string, token_expdate))

  }


  // GMQL INSTANCES COMMUNICATION

  /**
    * Get dataset samples
    * @param dataset_identifier
    * @return
    */
  def getSamples  (dataset_identifier:String) : util.List[GMQLSample]  = {

    val dataset = getDataset(dataset_identifier)
    val location = dataset.locations.head

    val uri = uri"${location.URI}/gmql-rest/datasets/public.${dataset.name}"

    val samples_xml = rest_get(uri,("X-AUTH-TOKEN", getToken(dataset.namespace)) )

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

    val uri = uri"${location.URI}/gmql-rest/metadata/public.${dataset.name}/sample/$sample"

    val metadata_xml = rest_get(uri, ("X-AUTH-TOKEN", getToken(dataset.namespace)) )

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

    val uri = uri"${location.URI}/gmql-rest/datasets/public.${dataset.name}/schema"

    val schema_xml = rest_get(uri, ("X-AUTH-TOKEN", getToken(dataset.namespace)) )

    var coordinate_system = GMQLSchemaCoordinateSystem.Default
    try {
      coordinate_system = GMQLSchemaCoordinateSystem.getType(schema_xml \ "coordinate_system"  text)
    }


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

    val uri = uri"${location.URI}/gmql-rest/datasets/public.${dataset.name}/info"

    val info_xml = rest_get(uri, ("X-AUTH-TOKEN", getToken(dataset.namespace)) )

    var info  =
    for ( item <- info_xml \ "info")
      yield  (item \ "key" text , item \ "value" text)

    info.toMap[String, String]

  }

  def getSampleProfile(dataset_identifier: String, sample_name: String) : Map[String, String] = {

    val dataset = getDataset(dataset_identifier)
    val location = dataset.locations.head

    val uri = uri"${location.URI}/gmql-rest/datasets/public.${dataset.name}/$sample_name/info"

    val info_xml = rest_get(uri, ("X-AUTH-TOKEN", getToken(dataset.namespace)) )

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
         "Locations" -> dataset.locations.map(_.name).reduce((x,y)=>x+", "+y) )
  }

}