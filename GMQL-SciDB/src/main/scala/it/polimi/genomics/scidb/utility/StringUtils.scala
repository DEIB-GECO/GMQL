package it.polimi.genomics.scidb.utility

/**
  * define utilities on strings
  *
  * @param string self object
  */
object StringUtils
{
  def serialize(string:String) : String = string.replace("\t","").replace("\n","\t")

  def tab(string:String) : String = string.split("\n").map((s:String) => "\t" + s).mkString("\n")

  def tabf(string:String) : String = tabf_recursive(tab(string))
  def tabf_recursive(string:String) : String = if( string.head == '\t' ) tabf_recursive(string.tail) else string

}
