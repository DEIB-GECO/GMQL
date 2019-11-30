package it.polimi.genomics.repository

import java.io.File

import it.polimi.genomics.core.GDMSUserClass
import it.polimi.genomics.repository.FSRepository.LFSRepository
import it.polimi.genomics.repository.FSRepository.DFSRepository
import it.polimi.genomics.repository.federated.GF_Decorator

/**
  * Created by abdulrahman on 15/02/2017.
  */
object test {
  def main(args: Array[String]): Unit = {

//    println(new File(".").getAbsoluteFile.toString)
//    val rep = new LFSRepository();
//    println(rep.readSchemaFile("/Users/abdulrahman/Downloads/chr1_only/test.schema"))

//    val username = if (args.length >= 1) args(0) else "public"
    //    val dsname   = if (args.length >= 2) args(1) else "GRCh38_ENCODE_BROAD_AUG_2017"
    //    val samplename  = if (args.length >= 3) args(2) else "ENCFF986QSO.bed"
    //
    //    Utilities.confFolder = "./"
    //    val repo: GMQLRepository = new DFSRepository()
    //
    //    println("Showing dataset profile for dataset "+dsname+" :")
    //    repo.getDatasetProfile(dsname, username).foreach(x=>println(x._1+"\t"+x._2))
    //    println()
    //    println("Showing profile for sample "+samplename+" :")
    //    repo.getSampleProfile(dsname,samplename,username).foreach(x=>println(x._1+"\t"+x._2))

        Utilities.confFolder = "/Users/andreagulino/Projects/GMQL-WEB/conf/gmql_conf"
        val repo: GMQLRepository = Utilities().getRepository()


    val ans  = repo.listAllDSs("federated")
    println(ans)


  }
}
