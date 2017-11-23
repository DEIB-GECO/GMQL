package it.polimi.genomics.r

object RESTManager {

  var service_url = ""
  var service_token = ""


  def save_tokenAndUrl(token: String, url: String): Unit = {
    service_url = url
    service_token = token
  }

  def delete_token(): Unit = {
    service_token = ""
  }

}