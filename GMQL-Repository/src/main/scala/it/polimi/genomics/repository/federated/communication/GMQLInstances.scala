package it.polimi.genomics.repository.federated.communication

import java.io.{FileOutputStream, OutputStreamWriter}
import java.net.HttpURLConnection

import com.softwaremill.sttp._
import it.polimi.genomics.repository.Utilities
import org.slf4j.{Logger, LoggerFactory}

import scala.concurrent.duration.Duration
import scala.xml.{Elem, XML}

class GMQLInstances(ns: NameServer) {

  val logger: Logger = LoggerFactory.getLogger(Utilities.getClass)

  // Tokens: Map [ target_namespace, token ]
  private var authentication = Map[String, Token]()


  val AUTH_HEADER_NAME_F = "X-AUTH-FED-TOKEN"
  val AUTH_HEADER_NAME_G = "X-AUTH-TOKEN"

  val AUTH_HEADER_VALUE_G = "FEDERATED-TOKEN"


  private def getToken (namespace:String) : String =  {

    logger.info("Getting the token for "+namespace)

    if ( !authentication.contains(namespace) || authentication.get(namespace).get.isExpired() ) {
      val token = ns.resetToken(namespace)
      authentication += (namespace -> token)
    }

    authentication(namespace).get

  }

  // perform a get request
  def get(URI: String, target_location_id: String) : Elem = {

    val location = ns.resolveLocation(target_location_id)
    val address = location.URI+URI

    logger.info("rest_get->uri " + address)
    logger.info("rest_get->authorization " + getToken(location.namespace))

    val request = sttp.get(uri"$address").readTimeout(Duration.Inf)
      .header("Accept","application/xml")
      .header(AUTH_HEADER_NAME_G, AUTH_HEADER_VALUE_G)
      .header(AUTH_HEADER_NAME_F, getToken(location.namespace))

    implicit val backend = HttpURLConnectionBackend()
    val response = request.send()

    val responseUnsafeBody = response.unsafeBody
    logger.info("rest_get->response.unsafeBody " + responseUnsafeBody)

    XML.loadString(response.unsafeBody)

  }


  private def inputToFile(is: java.io.InputStream, f: java.io.File) {
    val in = scala.io.Source.fromInputStream(is)
    val out = new java.io.PrintWriter(f)
    try { in.getLines().foreach(out.println(_)) }
    finally { out.close }
  }

  // download zip
  // @throws NotFound
  def download(location_id: String, job_id: String, ds_name: String, dest: String) = {

    val location = ns.resolveLocation(location_id)

    import java.net.URL
    import java.io.File

    val uri = s"${location.URI}/federated/download/${job_id}/${ds_name}?authToken="+AUTH_HEADER_VALUE_G
    val url = new URL(uri)

    val connection: HttpURLConnection = url.openConnection.asInstanceOf[HttpURLConnection]
    connection.setRequestMethod("GET")
    connection.connect()

    val code =  connection.getResponseCode

    logger.debug("Received code: "+code)

    code match {
      case 200 => {
        logger.debug("Input stream to file")
        val outputStream: FileOutputStream = new FileOutputStream(dest)
        val inputStream = connection.getInputStream

        Iterator
          .continually(inputStream.read)
          .takeWhile(-1 !=)
          .foreach(outputStream.write)


        outputStream.close()
        inputStream.close()

      }
      case 404 => {
        logger.debug("Received not found from target location")
        throw new NotFoundException
      }
      case _ => {
        logger.error("Error downloading ...")
        throw new Exception
      }

    }


  }


}
