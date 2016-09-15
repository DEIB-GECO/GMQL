package it.polimi.genomics.wsc.Livy

import play.api.libs.ws._
import play.api.libs.ws.ning._
/**
  * Created by abdulrahman on 25/05/16.
  */


class StandAloneWSAPI() extends WSAPI with java.io.Closeable  {
  import play.api.libs.ws.ning.{NingWSClient, NingAsyncHttpClientConfigBuilder}

  lazy val configuration = new NingAsyncHttpClientConfigBuilder().build()
  lazy val ningWsClient = new NingWSClient(configuration)
  override val client: WSClient = ningWsClient

  override def url(url: String): WSRequest = client.url(url)

  def close(): Unit = {
    ningWsClient.close()
  }
}
