package it.polimi.genomics.repository.federated.communication

import java.time.ZonedDateTime
import java.util.Calendar

class Token (value: String, expiration_string: String) {

  val expiration = ZonedDateTime.parse(expiration_string).toInstant.toEpochMilli

  def get = value

  def isExpired (): Boolean = {
    val now = Calendar.getInstance().getTime().getTime
    now > expiration
  }

}