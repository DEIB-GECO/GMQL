package it.polimi.genomics.scidb.test

import it.polimi.genomics.scidb.GmqlSciConfig
import it.polimi.genomics.scidb.operators.GDS
import it.polimi.genomics.scidbapi.script.SciScript

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.{Failure, Success}
import scala.util.Random


/**
  * Created by Cattani Simone on 17/05/16.
  * Email: simone.cattani@mail.polimi.it
  *
  */
object Garden
{
  def main(args: Array[String]): Unit =
  {
    val script = new SciScript;



  }

  def sleep(duration: Long) { Thread.sleep(duration) }

}
