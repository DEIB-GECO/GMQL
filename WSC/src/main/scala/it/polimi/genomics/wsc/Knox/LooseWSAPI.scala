package it.polimi.genomics.wsc.Knox

import com.ning.http.client.AsyncHttpClientConfig
import play.api.libs.ws.ning.{NingAsyncHttpClientConfigBuilder, NingWSClient, NingWSClientConfig}
import play.api.libs.ws.ssl.{SSLConfig, SSLLooseConfig}
import play.api.libs.ws.{WSAPI, WSClient, WSRequest}

import scala.concurrent.duration.Duration

/**
  * Created by abdulrahman Kaitoua on 25/05/16.
  */


class LooseWSAPI() extends WSAPI with java.io.Closeable {

  val builder: AsyncHttpClientConfig.Builder = new AsyncHttpClientConfig.Builder()
  val nn = NingWSClientConfig()
  configureWS(nn)
  lazy val config = builder.setAcceptAnyCertificate(true).build()

  //accept any certificate (this is needed for Knox to work since it does not provide a registered certificate).
  (new NingAsyncHttpClientConfigBuilder()).configureSSL(SSLConfig(loose = new SSLLooseConfig(acceptAnyCertificate = true)))

  lazy val ningWsClient = new NingWSClient(config)
  override val client: WSClient = ningWsClient


  override def url(url: String): WSRequest = client.url(url)

  def close(): Unit = {
    ningWsClient.close()
  }

  def configureWS(ningConfig: NingWSClientConfig): Unit = {
    val config = ningConfig.wsClientConfig

    def toMillis(duration: Duration): Int = {
      if (duration.isFinite()) duration.toMillis.toInt
      else -1
    }

    builder.setConnectTimeout(toMillis(config.connectionTimeout))
      .setReadTimeout(toMillis(config.idleTimeout))
      .setRequestTimeout(toMillis(config.requestTimeout))
      .setFollowRedirect(config.followRedirects)
      .setUseProxyProperties(config.useProxyProperties)
      .setCompressionEnforced(config.compressionEnabled)

    config.userAgent foreach builder.setUserAgent

    builder.setAllowPoolingConnections(ningConfig.allowPoolingConnection)
    builder.setAllowPoolingSslConnections(ningConfig.allowSslConnectionPool)
    builder.setIOThreadMultiplier(ningConfig.ioThreadMultiplier)
    builder.setMaxConnectionsPerHost(ningConfig.maxConnectionsPerHost)
    builder.setMaxConnections(ningConfig.maxConnectionsTotal)
    builder.setConnectionTTL(toMillis(ningConfig.maxConnectionLifetime))
    builder.setPooledConnectionIdleTimeout(toMillis(ningConfig.idleConnectionInPoolTimeout))
    builder.setWebSocketTimeout(toMillis(ningConfig.webSocketIdleTimeout))
    builder.setMaxRedirects(ningConfig.maxNumberOfRedirects)
    builder.setMaxRequestRetry(ningConfig.maxRequestRetry)
    builder.setDisableUrlEncodingForBoundedRequests(ningConfig.disableUrlEncoding)
  }

}



