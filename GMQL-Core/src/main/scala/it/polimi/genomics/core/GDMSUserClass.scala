package it.polimi.genomics.core

/**
  * Created by andreagulino on 28/09/17.
  */
object GDMSUserClass extends Enumeration {

    type GDMSUserClass = Value

    val DISABLED: GDMSUserClass.Value     = Value("DISABLED")
  
    val GUEST:  GDMSUserClass.Value     = Value("GUEST")
    val BASIC:  GDMSUserClass.Value     = Value("BASIC")
    val PRO:    GDMSUserClass.Value     = Value("PRO")
    val ADMIN:  GDMSUserClass.Value     = Value("ADMIN")
    val PUBLIC: GDMSUserClass.Value     = Value("PUBLIC")

    val ALL:    GDMSUserClass.Value     = Value("ALL")

    def withNameOpt(s: String): Value = values.find(_.toString == s).getOrElse(BASIC)
}
