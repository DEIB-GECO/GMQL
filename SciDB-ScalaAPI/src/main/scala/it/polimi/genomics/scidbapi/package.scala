package it.polimi.genomics

import it.polimi.genomics.scidbapi.exception.{PropertyAmbiguitySciException, SyntaxErrorSciException}

/**
  * Provides some utility used inside the package
  */
package object scidbapi
{

  /**
    * Provides the keywords for the sorting operations
    */
  object SortingKeyword extends Enumeration
  {
    type SortingKeyword = Value

    val asc = Value("asc")
    val desc = Value("desc")
  }

  /**
    * define operations on strings
    *
    * @param string self object
    */
  implicit class StringManipulation(string:String)
  {
    /**
      * Inserts a more tab for each line in the string
      * */
    def tab() : String = string.split("\n").map((s:String) => "\t" + s).mkString("\n")

    /**
      * Removes the alias prefix specified with dot notation
      *
      * @param alias searched alias
      * @return clean string
      */
    def unalias(alias:String) : String =
    {
      if( alias == "" ) return string

      string.indexOf(alias) match {
        case 0 => string.replace(alias+".", "")
        case i if(i<0) => throw new PropertyAmbiguitySciException("Property '"+ string +"' is ambiguos, please specify the correct alias")
        case i if(i>0) => throw new SyntaxErrorSciException("Property '"+ string +"' can't be interpretated, please specify only the correct alias")
      }
    }
  }
}
