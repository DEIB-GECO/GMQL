package it.polimi.genomics.manager.Debug

import java.io._

import scala.xml.{Node, NodeSeq, XML}

object MatrixConverter {

  val basic_features = Array("date",
    "jobId",
    "irOperator",
    "gmqlOperator",
    "cores",
    "cpu_freq",
    "memory"
  )

  val bin_size = Array("bin_size")

  val input_left = Array(
    "in_num_samples",
    "in_avg_num_reg",
    "in_avg_reg_len",
    "in_avg_min_coord",
    "in_avg_max_coord",
    "in_num_cols",
    "in_tuple_size")

  val input_right = Array(
    "in_R_num_samples",
    "in_R_avg_num_reg",
    "in_R_avg_reg_len",
    "in_R_avg_min_coord",
    "in_R_avg_max_coord",
    "in_R_num_cols",
    "in_R_tuple_size")

  val outcome = Array("execution_time")

  val output = Array(
    "out_num_samples",
    "out_avg_num_reg",
    "out_avg_reg_len",
    "out_avg_max_coord",
    "out_avg_min_coord",
    "out_num_cols",
    "out_tuple_size")


  def getExtraFeatures(node: Node): Array[String] = {

    val irOperatorName = (node \\ "operatorName").text

    irOperatorName match {
      case "IRRegionCover" => Array("minAcc", "maxAcc", "coverType")
      case "IRGenometricJoin" => Array("output_type", "DL", "DG", "stream", "MD")
      case _ => Array()
    }
  }

  def getSchema(node: Node): Array[String] = {

    val irOperatorName = (node \\ "operatorName").text

    irOperatorName match {
      case "IRSelectRD" => basic_features++input_left++outcome++output
      case "IRRegionCover" => basic_features++bin_size++input_left++getExtraFeatures(node)++outcome++output
      case "IRGenometricJoin" => basic_features++bin_size++input_left++input_right++getExtraFeatures(node)++outcome++output
      case "IRReadRD" => basic_features++output++outcome
      case "IRStoreRD" => basic_features++output++outcome
      case _ => basic_features++outcome
    }
  }


  def addNode(node: Node, dirPath: String, date: String,  add: Map[String, String] , binSize: Int) = {

    val irOperatorName = (node \\ "operatorName").text

    // Check if dirs exist and create / append to file
    val dir = new File(dirPath+"/"+irOperatorName)

    if(!dir.exists()) dir.mkdirs()

    val file = new File(dir.getAbsolutePath+"/"+irOperatorName+".csv")

    var pw: PrintWriter = null

    var createSchema = false

    if(!file.exists()) {file.createNewFile(); createSchema = true}

    val bw = new BufferedWriter(new FileWriter(file, true))

    var schema = getSchema(node)

    if(createSchema) {
      pw = new PrintWriter(file)
      pw.println(schema.mkString(",") + "\n")
      pw.close()
    }


    val values = schema.map(x=>"NULL")

    // Add basic features
    values(schema.indexOf("date")) = date.toString
    values(schema.indexOf("jobId")) = add("job_id")
    values(schema.indexOf("irOperator")) = (node \\ "operatorName").text
    values(schema.indexOf("gmqlOperator")) = (node \\ "GMQLoperator" \\ "name").text
    values(schema.indexOf("cores")) = add("cores")
    values(schema.indexOf("memory")) = add("memory")
    values(schema.indexOf("cpu_freq")) = add("cpu_freq")


    if(schema contains bin_size.head)
      values(schema.indexOf("bin_size")) = binSize.toString


    // Add input features
    if(schema contains input_left.head) {
      val left_input = (node \\ "inputs" \\ "input").filter(_ \@ "isRegion" == "true").head
      val left_properties: Map[String, String] = (left_input \\ "property").map(n => (n \@ "name") -> n.text).toMap[String, String]


      values(schema.indexOf("in_num_samples")) = left_properties("num_samp")
      values(schema.indexOf("in_avg_num_reg")) = left_properties("num_reg")
      values(schema.indexOf("in_avg_reg_len")) = left_properties("avg_reg_length")
      values(schema.indexOf("in_avg_max_coord")) = left_properties("max")
      values(schema.indexOf("in_avg_min_coord")) = left_properties("min")
      values(schema.indexOf("in_num_cols")) = left_properties("num_cols")
      values(schema.indexOf("in_tuple_size")) = left_properties("tuple_size")
    }

    if(schema contains input_right.head ) {
      val right_input = (node \\ "inputs" \\ "input").filter(_ \@ "isRegion" == "true").tail
      val right_properties = (right_input \\ "property").map(n => (n \@ "name") -> n.text).toMap[String, String]
      values(schema.indexOf("in_R_num_samples")) = right_properties("num_samp")
      values(schema.indexOf("in_R_avg_num_reg")) = right_properties("num_reg")
      values(schema.indexOf("in_R_avg_reg_len")) = right_properties("avg_reg_length")
      values(schema.indexOf("in_R_avg_max_coord")) = right_properties("num_cols")
      values(schema.indexOf("in_R_avg_min_coord")) = right_properties("tuple_size")
      values(schema.indexOf("in_R_num_cols")) = right_properties("num_cols")
      values(schema.indexOf("in_R_tuple_size")) = right_properties("tuple_size")
    }


    val xtra_schema = getExtraFeatures(node)

    if(xtra_schema.nonEmpty) {
      val xtra = (node \\ "params" \\ "param").map(p => p \@ "name" -> p.text).toMap[String, String]
      xtra_schema.foreach(x=>values(schema.indexOf(x)) = xtra(x))
    }

    if(schema contains output.head) {
      val out = node \\ "output"
      val out_properties = (out \\ "property").map(n=> (n \@ "name") -> n.text).toMap[String, String]

      values(schema.indexOf("out_num_samples")) = out_properties("num_samp")
      values(schema.indexOf("out_avg_num_reg")) = out_properties("num_reg")
      values(schema.indexOf("out_avg_reg_len")) = out_properties("avg_reg_length")
      values(schema.indexOf("out_avg_max_coord")) = out_properties("max")
      values(schema.indexOf("out_avg_min_coord")) = out_properties("min")
      values(schema.indexOf("out_num_cols")) = out_properties("num_cols")
      values(schema.indexOf("out_tuple_size")) = out_properties("tuple_size")

    }

    values(schema.indexOf("execution_time")) = (node \\ "executionTime").text

    bw.write(values.mkString(",")+"\n")

    bw.close()

  }



  def convert(xmlPath: String, cores: Long, memory: Float, outDir: String,  add: Map[String, String]): Unit = {

    val xml = XML.load(xmlPath)
    val date = System.nanoTime()

    val outFolder = outDir+"/table/"
    new File(outFolder) mkdirs()

    val binSize = (xml \\ "binSize" \\ "cover" text).toInt
    val nodes = xml \\ "node"



    for(node <- nodes) addNode(node, outFolder, date.toString, add, binSize)



  }

}
