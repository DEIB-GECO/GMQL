package it.polimi.genomics.core

/**
  * Created by andreagulino on 28/09/17.
  */
object GDMSUserClass extends Enumeration {



    type GDMSUserClass = Value
    val GUEST: GDMSUserClass.Value      = Value("GUEST")
    val BASIC: GDMSUserClass.Value      = Value("BASIC")
    val PRO:   GDMSUserClass.Value      = Value("PRO")
    val PUBLIC: GDMSUserClass.Value     = Value("PUBLIC")
    val ADMIN: GDMSUserClass.Value      = Value("ADMIN")

    def withNameOpt(s: String): Value = values.find(_.toString == s).getOrElse(BASIC)
}
