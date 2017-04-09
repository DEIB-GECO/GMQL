package it.polimi.genomics.pythonapi
import py4j.GatewayServer
/**
  * Created by Luca Nanni on 08/04/17.
  * Email: luca.nanni@mail.polimi.it
  */

/**
  * Main of the application. It instantiates a gateway server for Python
  * to access to the JVM (through py4j)
  * */
object EntryPoint {
  def main(args: Array[String]): Unit = {
    val gatewayServer : GatewayServer = new GatewayServer()
    gatewayServer.start()
  }
}
