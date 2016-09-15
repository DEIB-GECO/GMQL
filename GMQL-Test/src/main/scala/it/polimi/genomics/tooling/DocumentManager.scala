package it.polimi.genomics.tooling

/**
 * Created by pietro on 05/10/15.
 */
class DocumentManager {

  val res_table : StringBuilder = new StringBuilder
  val time_table : StringBuilder = new StringBuilder

  res_table.append("\\begin{table}[h!]\n" +
    "\\centering\n" +
    "\\caption{Comparison results}\n" +
    "\\label{my-label}\n" +
    "\\begin{tabular}{cccccc}\n" +
    "query & DOT & FLINK & SPARK & META & EXP \\\\\\hline\n")

  time_table.append("\\begin{table}[h!]\n" +
    "\\centering\n" +
    "\\caption{Execution times (milliseconds)}\n" +
    "\\label{my-label}\n" +
    "\\begin{tabular}{ccc}\n" +
    "query & FLINK & SPARK \\\\\\hline\n")

  val queries : StringBuilder = new StringBuilder
  queries.append("\\section{queries}")

  def table_add_line(name : String,dot : Boolean, flink : Boolean, spark : Boolean, meta : Boolean, exp : Boolean) = {
    res_table.append(name +" & "+
      {if(dot) {"$\\checkmark$"} else {"$\\blacksquare$"}} +" & "+
      {if(flink) {"$\\checkmark$"} else {"$\\blacksquare$"}} +" & " +
      {if(spark) {"$\\checkmark$"} else {"$\\blacksquare$"}} +" & " +
      {if(meta) {"$\\checkmark$"} else {"$\\blacksquare$"}} +" & " +
      {if(exp) {"$\\checkmark$"} else {"$\\blacksquare$"}}  + "\\\\\n")

  }

  def time_table_add_line(name : String, flink : Option[Long], spark : Option[Long]) = {
    time_table.append(name + " & " + flink.getOrElse("FAILED") + " & " + spark.getOrElse("FAILED") + " \\\\ \n")
  }

  def generate() : Unit = {

    val document = new StringBuilder
    document
      .append(header)
      .append(open_doc)
      .append(table_section)
      .append(queries)
      .append(close_doc)
    FS_tools.write_report_file(document.toString())
    println(document.toString())
  }

  def open_doc : StringBuilder = {
    val start_doc = new StringBuilder
    start_doc.append("\\begin{document}\n")
    start_doc.append("\\title{Serial Tester Output}\n")
    start_doc.append("\\date{\\today} \n")
    start_doc.append("\\maketitle\n")
    start_doc.append("\\tableofcontents\n")
    start_doc.append("\\newpage\n")
    start_doc
  }

  def table_section : StringBuilder = {
    this.close_table
    this.close_time_table
    val s = new StringBuilder
    s.append("\\section{Comparison table}\n")
      .append(res_table)
      .append("\\newpage\n")
      .append("\\section{Time table}\n")
      .append(time_table)
      .append("\\newpage\n")
    s
  }

  def close_table : Unit = res_table.append("\\end{tabular}\n" + "\\end{table}\n")
  def close_time_table : Unit = time_table.append("\\end{tabular}\n" + "\\end{table}\n")

  def header : StringBuilder = {
    val h = new StringBuilder
    h.append("\\documentclass{article}\n")
      .append("\\usepackage{times}\n")
      .append("\\usepackage{amssymb}\n")
      .append("\\usepackage{graphicx}\n\\graphicspath{ {source/} }")
    h
  }

  def add_query(query_name : String, query_code : String, add_img : Boolean = true ) = {
    val query_code_tex = query_code
      .trim
      .replaceAll("\n", "\\\\\\\\\n")
      .replaceAll("#", "")
      .replace("$", "\\$")
      .replaceAll(">", "\\$>\\$")
      .replaceAll("<", "\\$<\\$")
    queries.append("\\subsection{"+query_name+"}\n")
      .append(query_code_tex)
    if(add_img) queries.append("\n\n\\includegraphics[width=\\textwidth]{"+query_name+"}\n")
    queries.append("\\newpage")
  }

  def close_doc : StringBuilder = {
    val c = new StringBuilder
    c.append("\\end{document} \n")
  }

}
