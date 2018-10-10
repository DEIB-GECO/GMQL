package it.polimi.genomics.repository.federated

import scala.xml.{Elem, NodeSeq}

class Location (location:NodeSeq) {
  val id = location \ "identifier" text
  val name = location \ "name" text
  val namespace = location \ "namespace" text
  val details = location \ "details" text
  val URI = location \ "URI" text

  override def toString: String = s"$name ($URI)"

}
class FederatedDataset (dataset:Elem)  {

  val identifier = dataset \ "identifier" text
  val name = dataset \ "name" text
  val namespace = dataset \ "namespace" text
  val author = dataset \ "author" text
  val description = dataset \  "description" text
  val pub_date = dataset \ "pub_date" text

  val locations = for ( location <- dataset \ "locations" \ "list-item" )
    yield new Location(location )

  override def toString: String = s"$identifier (found in ${locations.size} locations.)"

}