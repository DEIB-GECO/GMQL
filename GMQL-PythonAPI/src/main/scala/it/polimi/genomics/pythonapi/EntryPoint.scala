package it.polimi.genomics.pythonapi

import java.io.{BufferedReader, IOException, InputStreamReader}
import java.nio.charset.Charset

import org.slf4j.{Logger, LoggerFactory}
import py4j.{GatewayServer, Py4JNetworkException}

/**
  * Created by Luca Nanni on 08/04/17.
  * Email: luca.nanni@mail.polimi.it
  */

/**
  * Main of the application. It instantiates a gateway server for Python
  * to access to the JVM (through py4j)
  **/

class EntryPoint {

  def getPythonManager = PythonManager
}

object EntryPoint {

  val logger: Logger = LoggerFactory.getLogger(this.getClass)
  val properties: AppProperties.type = AppProperties
  val usage = "Usage: GMQL-PythonAPI [port]"

  def main(args: Array[String]): Unit = {

    var port = 25333
    var foundPort = false
    var gatewayServer: Option[GatewayServer] = None

    if(args.length == 0) {
      // We have to find the port ourselves
      while(!foundPort){
        try {
          gatewayServer = Some(new GatewayServer(new EntryPoint, port))
          gatewayServer.get.start()
          foundPort = true
        }
        catch {
          case x:Py4JNetworkException => {
            this.logger.error(s"Failed connection to port $port")
            port = port + 1
          }
          case _ => {
            this.logger.error("Network error")
            System.exit(-1)
          }
        }
        this.logger.info(s"Found free port at $port")
      }
    } else if(args.length == 1) {
        if(args(0) == "--help")
          println(usage)
        else {
          try {
            port = args(0).toInt
            gatewayServer = Some(new GatewayServer(new EntryPoint, port))
            gatewayServer.get.start()
          }
          catch {
            case x: Py4JNetworkException => {
              this.logger.error(s"Failed connection", x)
              System.exit(-1)
            }
          }
        }
    } else {
      println("Wrong command line options\n"+usage)
    }
    this.logger.info(s"GatewayServer started at port $port")
    println(port)

    // Die when the connection with Python is ended
    try {
      val stdin = new BufferedReader(new InputStreamReader(System.in, Charset.forName("UTF-8")))
      stdin.readLine
      System.exit(0)
    } catch {
      case e: IOException =>
        System.exit(1)
    }
  }
}
