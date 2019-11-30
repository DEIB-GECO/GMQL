package it.polimi.genomics.repository.federated

import scala.collection.immutable
import scala.xml.{Elem, NodeSeq}

class Location (location:NodeSeq) {
  val instance: String = (location \ "instance").text
  val name: String = (location \ "name").text
  val details: String =  (location \ "details").text
  val URI: String = if( (location \ "URI").text.endsWith("/") )
              (location \ "URI").text
            else
              (location \ "URI").text + "/"

  val alive: Boolean = (location \ "alive").text == "True"

  override def toString: String = s"$name ($URI)"

}
class FederatedDataset (dataset:Elem)  {

  val identifier: String = (dataset \ "identifier").text
  val name: String = (dataset \ "name").text
  val owner: String = (dataset \ "owner").text
  val author: String = (dataset \ "author").text
  val description: String = (dataset \  "description").text
  val pub_date: String = (dataset \ "pub_date").text

  val locations: List[String] =  (dataset \ "copies" \ "list-item" map(_.text)).toList

  override def toString: String = s"$identifier (found in ${locations.size} locations.)"

}