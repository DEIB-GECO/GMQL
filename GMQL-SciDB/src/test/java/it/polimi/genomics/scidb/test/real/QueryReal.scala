package it.polimi.genomics.scidb.test.real

import it.polimi.genomics.GMQLServer.GmqlServer
import it.polimi.genomics.compiler.Translator
import it.polimi.genomics.scidb.GmqlSciAdapter

/**
  * Created by Cattani Simone on 26/05/16.
  * Email: simone.cattani@mail.polimi.it
  *
  */
object QueryReal
{

  def main(args: Array[String]): Unit =
  {
    /*val query_scidb = "" +
      "R = SELECT(NOT(leaveout==\"something\")) [RnaSeqParser]PROMOTERS;\n"+
      "E = SELECT(NOT(leaveout==\"something\"))  [BedScoreParser]HG_NARROWPEAKS_2;\n" +
      "M = MAP(BAG(score1)) R E;\n" +
      "MATERIALIZE M into TEST_PN_2;"

    val scidb_server = new GmqlServer(new GmqlSciAdapter())
    val scidb_translator = new Translator(scidb_server, "")

    scidb_translator.phase2(scidb_translator.phase1(query_scidb))

    val scidb_start: Long = System.currentTimeMillis
    scidb_server.run()
    val scidb_stop: Long = System.currentTimeMillis

    println("Time: "+(scidb_stop - scidb_start))*/

    test()

  }

  def test() : Unit =
  {
    var promoters : List[Long] = List()
    var genes : List[Long] = List()

    for(t <- List("GENES"))
      for(s <- List(2,4,6,8))
        {
          val query_scidb = "" +
            "R = SELECT(NOT(leaveout==\"something\")) [RnaSeqParser]"+t+";\n"+
            "E = SELECT(NOT(leaveout==\"something\"))  [BedScoreParser]HG_NARROWPEAKS_"+s+";\n" +
            "M = MAP(BAG(score1)) R E;\n" +
            "MATERIALIZE M into TEST_"+t+"_"+s+";"

          val scidb_server = new GmqlServer(new GmqlSciAdapter())
          val scidb_translator = new Translator(scidb_server, "")

          scidb_translator.phase2(scidb_translator.phase1(query_scidb))

          val scidb_start: Long = System.currentTimeMillis
          scidb_server.run()
          val scidb_stop: Long = System.currentTimeMillis

          println("\t\t\t"+s+": "+(scidb_stop - scidb_start))

          if(t == "PROMOTERS"){
            promoters = promoters ::: List((scidb_stop - scidb_start))
          }else{
            genes = genes ::: List((scidb_stop - scidb_start))
          }
        }

    println("PROMOTERS ["+promoters.mkString(",")+"]")
    println("GENES ["+genes.mkString(",")+"]")
  }

}
