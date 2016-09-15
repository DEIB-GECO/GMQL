package it.polimi.genomics.scidb.test

import it.polimi.genomics.scidb.GmqlSciImplementation

/**
  * Created by Cattani Simone on 26/02/16.
  * Email: simone.cattani@mail.polimi.it
  *
  */
object ImplementationTest
{
  def main(args: Array[String]): Unit =
  {
    val implementation = new GmqlSciImplementation

    implementation.addDAG(DagTest.getDAG)
    implementation.go()
  }
}
