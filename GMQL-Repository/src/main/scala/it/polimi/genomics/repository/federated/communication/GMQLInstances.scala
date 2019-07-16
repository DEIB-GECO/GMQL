package it.polimi.genomics.repository.federated.communication

import java.io.FileOutputStream
import java.net.HttpURLConnection

import com.softwaremill.sttp._
import it.polimi.genomics.repository.Utilities
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable
import scala.concurrent.duration.Duration
import scala.xml.{Elem, XML}

object GMQLInstances {
  val authentication = mutable.Map[String, Token]()
}

class GMQLInstances(ns: NameServer) {

  val logger: Logger = LoggerFactory.getLogger(Utilities.getClass)

  private val authentication = GMQLInstances.authentication

  val AUTH_HEADER_NAME_FN = "X-AUTH-FED-INSTANCE"
  val AUTH_HEADER_NAME_FT = "X-AUTH-FED-TOKEN"
  val AUTH_HEADER_NAME_G = "X-AUTH-TOKEN"


  val AUTH_HEADER_VALUE_G = "FEDERATED-TOKEN"


  def getToken(target: String): String = {

    logger.info("Getting the token for communication with " + target)

    if (!authentication.contains(target) ) {
      val token = ns.resetToken(target)
      authentication += (target -> token)
    }

    authentication(target).get

  }

  // perform a get request
  def get(URI: String, target_location_id: String): Elem = {

    val location = ns.resolveLocation(target_location_id)
    val address = location.URI + URI

    logger.info("rest_get->uri " + address)
    logger.info("rest_get->authorization " + getToken(location.instance))

    val request = sttp.get(uri"$address").readTimeout(Duration.Inf)
      .header("Accept", "application/xml")
      .header(AUTH_HEADER_NAME_G, AUTH_HEADER_VALUE_G)
      .header(AUTH_HEADER_NAME_FN, ns.NS_INSTANCENAME)
      .header(AUTH_HEADER_NAME_FT, getToken(location.instance))

    implicit val backend = HttpURLConnectionBackend()
    val response = request.send()

    val responseUnsafeBody = response.unsafeBody
    logger.info("rest_get->response.unsafeBody " + responseUnsafeBody)

    XML.loadString(response.unsafeBody)

  }

  // perform a post request returning the response string
  def post(URI: String, body:String, target_location_id: String): String = {

    val location = ns.resolveLocation(target_location_id)
    val address = location.URI + URI

    logger.info("rest_post->uri " + address)
    logger.info("rest_post->authorization " + getToken(location.instance))
    logger.info("rest_post->body " + body)

    val request = sttp.body(body).post(uri"$address").readTimeout(Duration.Inf)
      .header("Content-Type", "application/json")
      .header("Accept", "application/json")
      .header(AUTH_HEADER_NAME_G, AUTH_HEADER_VALUE_G)
      .header(AUTH_HEADER_NAME_FN, ns.NS_INSTANCENAME)
      .header(AUTH_HEADER_NAME_FT, getToken(location.instance))

    implicit val backend = HttpURLConnectionBackend()
    val response = request.send()

    val responseUnsafeBody = response.unsafeBody
    logger.info("rest_get->response.unsafeBody " + responseUnsafeBody)

    responseUnsafeBody
  }


  private def inputToFile(is: java.io.InputStream, f: java.io.File) {
    val in = scala.io.Source.fromInputStream(is)
    val out = new java.io.PrintWriter(f)
    try {
      in.getLines().foreach(out.println(_))
    }
    finally {
      out.close
    }
  }

  // download zip
  // @throws NotFound
  def download(location_id: String, job_id: String, ds_name: String, dest: String) = {

    val location = ns.resolveLocation(location_id)

    import java.net.URL

    val uri = s"${location.URI}federated/download/${job_id}/${ds_name}?authToken=$AUTH_HEADER_VALUE_G&$AUTH_HEADER_NAME_FN=${ns.NS_INSTANCENAME}&$AUTH_HEADER_NAME_FT=${getToken(location.instance)}"
    println("uri: " + uri)
    val url = new URL(uri)

    val connection: HttpURLConnection = url.openConnection.asInstanceOf[HttpURLConnection]
    connection.setRequestMethod("GET")
    connection.connect()

    val code = connection.getResponseCode

    logger.debug("Received code: " + code)

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
