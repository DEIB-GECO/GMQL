package it.polimi.genomics.core

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, ObjectInputStream, ObjectOutputStream}
import java.util.Base64

import it.polimi.genomics.core.DataStructures.IRVariable

/**
  * Created by Luca Nanni on 06/06/17.
  * Email: luca.nanni@mail.polimi.it
  */
object Utilities {

  def serializeToBase64(variableList : List[IRVariable]): String = {
    val baos = new ByteArrayOutputStream()
    val oos = new ObjectOutputStream(baos)
    oos.writeObject(variableList)
    oos.close()
    Base64.getEncoder.encodeToString(baos.toByteArray)
  }

  def deserializeDAG(serialized: String): List[IRVariable] = {
    val data = Base64.getDecoder.decode(serialized)
    val ois = new ObjectInputStream(new ByteArrayInputStream(data))
    val deserialized = ois.readObject().asInstanceOf[List[IRVariable]]
    ois.close()
    deserialized
  }

}
