package it.polimi.genomics.cli
import org.apache.spark.{SparkConf, SparkContext}


object GMQLExecuteGoogle {

  def main(args: Array[String]): Unit = {

    val projId      = "gmql-188714"

    val credentials = "/usr/src/myapp/credentials.json"
    //val gcjar       = "/usr/local/Cellar/hadoop/2.8.2/libexec/share/hadoop/common/gcs-connector-latest-hadoop2.jar"
    // anzichè il jar + classpath (che non andava) ho aggiunto la dipendenza maven

    val conf = new SparkConf().setAppName("Prova").setMaster("local[*]")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer").set("spark.kryoserializer.buffer", "128")
      .set("spark.driver.allowMultipleContexts","true")
      .set("spark.sql.tungsten.enabled", "true")//.setMaster("local[*]")
      .set("spark.hadoop.google.cloud.auth.service.account.enable","true")
      .set("spark.hadoop.google.cloud.auth.service.account.json.keyfile", credentials)
      .set("spark.hadoop.fs.gs.impl","com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
      .set("spark.hadoop.fs.AbstractFileSystem.gs.impl","com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
      .set("spark.hadoop.fs.gs.project.id",projId)
      //.set("spark.executor.extraClassPath",gcjar)
      //.set("spark.driver.extraClassPath",gcjar)

    val sc: SparkContext = new SparkContext(conf)

    val file = sc.textFile("gs://gmql_encode/narrow/file1.bed")
    file.collect().foreach(println)

  }
}

