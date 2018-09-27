package it.polimi.genomics.core.DAG

//import scala.pickling.Defaults._
//import scala.pickling.json._
import java.io._
import java.util.Base64

/**
  * Created by Luca Nanni on 04/11/17.
  * Email: luca.nanni@mail.polimi.it
  *
  * This object holds all the routines used for the serialization and deserialization of the GMQL DAG
  */
object DAGSerializer {

  /**
    * Serialization of the DAG
    * @param dag: DAG Wrapper
    * @return dag in the form of serialized String
    */
  def serializeDAG(dag: DAGWrapper): String = {
    javaSerialize(dag)
  }

  /**
    * Deserialization of the DAG
    * @param serialized: dag in the form of serialized String
    * @return: DAG Wrapper
    */
  def deserializeDAG(serialized: String): DAGWrapper = {
    javaDeserialize(serialized)
  }

  /* Java Serialization of the DAG */
  private def javaSerialize(dag: DAGWrapper): String = {
    val baos = new ByteArrayOutputStream()
    val oos = new ObjectOutputStream(baos)
    oos.writeObject(dag)
    oos.close()
    Base64.getEncoder.encodeToString(baos.toByteArray)
  }

  /* Java Deserialization of the DAG */
  private def javaDeserialize(serialized: String): DAGWrapper = {
    val bais = new ByteArrayInputStream(Base64.getDecoder.decode(serialized))
    val ois = nonStupidObjectInputStream(bais)
    val dag = ois.readObject().asInstanceOf[DAGWrapper]
    ois.close()
    dag
  }

  /*This is a very long story. We cannot use the classical ObjectInputStream of java, since
  * the DAG is formed using also pure Scala constructs and Collections. In particular, the classical
  * ObjectInputStream yielded an exception of the following type
  *
  * java.lang.classcastexception: cannot assign instance of scala.collection.immutable.list$serializationproxy
  *
  * In order to avoid this problem we need to create our own ObjectInputStream which, differently from the
  * original one, uses as ClassLoader the one of the current thread and not the one used by the serialization
  * side. This enable the GMQL Cli to deserialize the DAG without errors. */
  private def nonStupidObjectInputStream(stream: InputStream): ObjectInputStream = {
    new ObjectInputStream(stream) {
      override def resolveClass(desc: ObjectStreamClass): Class[_] = {
        try {
          val currentTccl: ClassLoader = Thread.currentThread.getContextClassLoader
          currentTccl.loadClass(desc.getName)
        } catch {
          case e: Exception => super.resolveClass(desc)
        }
      }
    }
  }

}
