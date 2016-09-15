package it.polimi.genomics.manager.Launchers

import it.polimi.genomics.manager.{GMQLJob, Status}

  abstract class GMQLLauncher(val job:GMQLJob) {

    def run(): GMQLLauncher

    def getStatus(): Status.Value

    def getAppName (): String

  }

//object GMQLLauncher {

  /**
    * For testing the launcher from Commmand line.
    *
    * @param args
    */
//  def main(args: Array[String]) {
//    println(args.mkString("\t"))
//
////        val sc:SparkContext = null
//
//    val user = args(1)
//    val script =args(0)
//
//    val server = GMQLExecute();
//    val job = server.registerJob(script, scala.io.Source.fromFile(script).mkString,"spark", 5000, user, "",GTFoutput = false);
//    val launcher = new GMQLLauncherSpark(job).run
//
//    print("RUNNGING")
//    while(launcher.getStatus().equals(Status.PENDING) || launcher.getStatus().equals(Status.RUNNING)){Thread.sleep(100); print(".")}
//    println("\nDONE")
//
//  }
//}

