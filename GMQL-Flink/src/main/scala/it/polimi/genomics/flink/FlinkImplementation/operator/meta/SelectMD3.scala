package it.polimi.genomics.flink.FlinkImplementation.operator.meta

import java.io.{File, FileNotFoundException}
import java.nio.file.Paths
import java.util.Locale
import javax.xml.bind.JAXBException

import it.polimi.genomics.core.DataStructures.MetadataCondition._
import it.polimi.genomics.core.DataStructures.{IRReadMD, MetaOperator}
import it.polimi.genomics.core.DataTypes.FlinkMetaType
import it.polimi.genomics.core.GMQLLoader
import it.polimi.genomics.core.exception.SelectFormatException
import it.polimi.genomics.flink.FlinkImplementation.FlinkImplementation
import it.polimi.genomics.flink.FlinkImplementation.reader.DataSetFilter
import it.polimi.genomics.repository.FSRepository.Indexing.LuceneIndex._
import it.polimi.genomics.repository.FSRepository.Indexing.SearchIndex
import it.polimi.genomics.repository.datasets.GMQLDataSetCollection
import it.polimi.genomics.repository.util.Utilities
import org.apache.flink.api.scala._
import org.apache.flink.hadoop.shaded.com.google.common.base.Charsets
import org.apache.flink.hadoop.shaded.com.google.common.hash.Hashing
import org.apache.flink.util.Collector
import org.apache.hadoop.fs.PathFilter
import org.apache.lucene.store.{Directory, FSDirectory}
import org.slf4j.LoggerFactory

/**
 * Created by michelebertoni on 05/05/15.
 */
object SelectMD3 {

  final val logger = LoggerFactory.getLogger(this.getClass)

  val locale = Locale.getDefault()

  @throws[SelectFormatException]
  def apply(executor : FlinkImplementation, metaCondition: MetadataCondition, inputDataset: MetaOperator, lucene : Boolean, env : ExecutionEnvironment) : DataSet[FlinkMetaType] = {

    //logger.warn("Executing SelectMD3")

    // Indexed search
    // creating index
    val index =
      if(lucene) {
        inputDataset match {
          // Selection from HD
          // use an index
          case IRReadMD(paths: List[String], loader: GMQLLoader[Any, Any, Any, Any],_) => {
            println ("reading")
            if (paths.size == 1 && Utilities.getInstance().checkDSNameinRepo(Utilities.USERNAME, paths.head)) {
              val username = if(Utilities.getInstance().checkDSNameinPublic(paths.head)) "public" else Utilities.USERNAME
              // load index from repository
              logger.info("GMQL loading index from Repository ...")
              var GMQLDSCol = new GMQLDataSetCollection();
              try {
                GMQLDSCol = GMQLDSCol.parseGMQLDataSetCollection(Paths.get(Utilities.getInstance().RepoDir + username + "/datasets/" + paths.head +".xml"));
                val dataset = GMQLDSCol.getDataSetList.get(0)
                Some(FSDirectory.open(new File(dataset.getIndexURI)))
              } catch {
                case ex: JAXBException => {
                  logger.error("The xml file of the dataset is not parsable...\n" + ex.getMessage)
                  None
                }
                case ex: FileNotFoundException => {
                  logger.error("XML file of the dataset is not found. recheck the xml path...\n " + ex.getMessage)
                  None
                }
              }

            } else {
              // create index
              logger.info("GMQL creating index from files ...")
              // New file -> create index
              val newPaths: List[String] = paths.flatMap((p) => {
                val fs = Utilities.getInstance().getFileSystem
                if (new File(p).isDirectory) {
                  new File(p).listFiles(DataSetFilter).map((subFile) => {
                    (subFile.toString)
                  })
                } else if (fs.exists(new org.apache.hadoop.fs.Path(p))) {
                  fs.listStatus(new org.apache.hadoop.fs.Path(p), new PathFilter {
                    override def accept(path: org.apache.hadoop.fs.Path): Boolean = !path.toString.endsWith(".meta")
                  }).map(x => x.getPath.toString).toList
                } else {
                  List(p)
                }
              })
//              newPaths.foreach(println _)
              Some(buildIndex(newPaths.map(x => x + ".meta").toArray))
            }
          }

          //Selection from memory
          //No index
          case _ => {
            logger.debug("GMQL Select from memory: no index available")
            None
          }
        }
      } else {
        // No lucene -> no index
        None
      }

    val input = executor.implement_md(inputDataset, env)

    /*
    input.join(applyMetaSelect(metaCondition, input, index, env)).where(0).equalTo(0){
      (a,b) => (a)
    }
    */

    val ddd = input.coGroup(applyMetaSelect(metaCondition, input, index, env)).where(0).equalTo(0){
      (left : Iterator[FlinkMetaType], right : Iterator[Tuple1[Long]], out : Collector[FlinkMetaType]) => {
        if(!right.size.equals(0)){
          for(metaTuple <- left){
            out.collect(metaTuple)
          }
        }
      }
    }
//    ddd.collect().foreach(println _)
    ddd

  }

  @throws[SelectFormatException]
  def applyMetaSelect(metaCondition: MetadataCondition, input: DataSet[FlinkMetaType], index : Option[Directory], env : ExecutionEnvironment): DataSet[Tuple1[Long]] = {
    metaCondition match {
      case metaCondition: Predicate => {
        val predicate = metaCondition.asInstanceOf[Predicate]
        predicate.operator match {
          case META_OP.EQ => {
            applyMetaPredicateEQ(predicate, input, index, env)
          }
          case META_OP.NOTEQ => {
            applyMetaPredicateNOTEQ(predicate, input, index, env)
          }
          case META_OP.GT => {
            applyMetaPredicateGT(predicate, input)
          }
          case META_OP.GTE => {
            applyMetaPredicateGTE(predicate, input)
          }
          case META_OP.LT => {
            applyMetaPredicateLT(predicate, input)
          }
          case META_OP.LTE => {
            applyMetaPredicateLTE(predicate, input)
          }
        }
      }

      case missing : MissingAttribute => {
        applyMetaSelect(NOT(ContainAttribute(missing.attribute)), input, index, env)
      }

      case contain : ContainAttribute => {
        applyContainAttribute(contain.attribute, input, index, env)
      }

      case metaCondition : NOT => {
        val subRes = applyMetaSelect(metaCondition.predicate, input, index, env)
        // take the complete list of ids
        // take the ids that satisfies son condition
        // take the difference of the two set
        input
          .coGroup(subRes).where(0).equalTo(0){
          (left, right, out : Collector[Tuple1[Long]]) => {
            if(right.size.equals(0)){
              out.collect(Tuple1(left.next()._1))
            }
          }
        }
      }

      case metaCondition : OR => {
        applyMetaConditionOR(metaCondition.first_predicate, metaCondition.second_predicate, input, index, env)
      }

      case metaCondition : AND => {
        applyMetaConditionAND(metaCondition.first_predicate, metaCondition.second_predicate, input, index, env)
      }
    }
  }





  //Predicate evaluation methods


  @throws[SelectFormatException]
  def applyContainAttribute(name : String, input : DataSet[FlinkMetaType], index : Option[Directory], env : ExecutionEnvironment) : DataSet[Tuple1[Long]] = {
    //TODO
    if(index.isDefined){
      val query = name + "_*"
      env.fromCollection(searchIndex(query, index.get).split(",").map(x =>Tuple1(idHasher(x))))
    } else {
      input.filter(_._2.equals(name)).distinct().map(v => Tuple1(v._1))
    }
  }

  @throws[SelectFormatException]
  def applyMetaPredicateEQ(predicate: Predicate, input: DataSet[FlinkMetaType], index : Option[Directory], env : ExecutionEnvironment): DataSet[Tuple1[Long]] = {
    //TODO
    if(index.isDefined){
      val query = predicate.attribute_name + "_" + predicate.value
      env.fromCollection(searchIndex(query, index.get).split(",").map(x => Tuple1(idHasher(x))))
    } else {
      castDoubleOrString(predicate.value) match {
        case value: Double => {
          input.filter(_._2.toLowerCase(locale).equals(predicate.attribute_name.toLowerCase(locale))).filter((a: FlinkMetaType) => {
            try {
              a._3.toDouble.equals(value)
            } catch {
              case _: Throwable =>logger.error("could not convert to double"); false
            }
          })
            .map(v => Tuple1(v._1))
        }
        case value: String => {
          input
            .filter((v) => v._2.toString.toLowerCase(locale).equals(predicate.attribute_name.toLowerCase(locale)) && v._3.toString.toLowerCase(locale).equals(value.toLowerCase(locale)))
            .map(v => Tuple1(v._1))
        }
      }
    }
  }

  @throws[SelectFormatException]
  def applyMetaPredicateNOTEQ(predicate: Predicate, input: DataSet[FlinkMetaType], index : Option[Directory], env : ExecutionEnvironment): DataSet[Tuple1[Long]] = {
    //TODO
    if(index.isDefined){
      val query = predicate.attribute_name + "* AND NOT " + predicate.attribute_name + "_" + predicate.value
      env.fromCollection(searchIndex(query, index.get).split(",").map(x => Tuple1(idHasher(x))))
    } else {
      castDoubleOrString(predicate.value) match {
        case value: Double => {
          input.filter(_._2.toLowerCase(locale).equals(predicate.attribute_name.toLowerCase(locale))).filter((a: FlinkMetaType) => {
            try {
              !a._3.toDouble.equals(value)
            } catch {
              case _: Throwable => logger.error("could not convert to double");false
            }
          })
            .map(v => Tuple1(v._1))
        }
        case value: String => {
          input
            .filter((v) => v._2.toLowerCase(locale).equals(predicate.attribute_name.toLowerCase(locale)) && !v._3.toString.toLowerCase(locale).equals(value.toLowerCase(locale)))
            .map(v => Tuple1(v._1))
        }
      }
    }
  }

  @throws[SelectFormatException]
  def applyMetaPredicateLT(predicate: Predicate, input: DataSet[FlinkMetaType]): DataSet[Tuple1[Long]] = {
    castDoubleOrString(predicate.value) match {
      case value: Double => {
        input.filter(_._2.toLowerCase(locale).equals(predicate.attribute_name.toLowerCase(locale))).filter((a : FlinkMetaType) => {
          try{
            a._3.toDouble < value
          } catch {
            case _ : Throwable => logger.error("could not convert to double");false
          }
        })
        .map(v => Tuple1(v._1))
      }
      case value: String => {
        throw SelectFormatException.create("Your SELECT statement cannot be executed: you are doing a < comparison between string. Attribute = " + predicate.value)
      }
    }
  }

  @throws[SelectFormatException]
  def applyMetaPredicateLTE(predicate: Predicate, input: DataSet[FlinkMetaType]): DataSet[Tuple1[Long]] = {
    castDoubleOrString(predicate.value) match {
      case value: Double => {
        input.filter(_._2.toLowerCase(locale).equals(predicate.attribute_name.toLowerCase(locale))).filter((a : FlinkMetaType) => {
          try{
            a._3.toDouble <= value
          } catch {
            case _ : Throwable => logger.error("could not convert to double");false
          }
        })
        .map(v => Tuple1(v._1))
      }
      case value: String => {
        throw SelectFormatException.create("Your SELECT statement cannot be executed: you are doing a <= comparison between string. Attribute = " + predicate.value)
      }
    }
  }

  @throws[SelectFormatException]
  def applyMetaPredicateGT(predicate: Predicate, input: DataSet[FlinkMetaType]): DataSet[Tuple1[Long]] = {
    castDoubleOrString(predicate.value) match {
      case value : Double => {
        input.filter(_._2.toLowerCase(locale).equals(predicate.attribute_name.toLowerCase(locale))).filter((a : FlinkMetaType) => {
          try{
            a._3.toDouble > value
          } catch {
            case _ : Throwable => logger.error("could not convert to double");false
          }
        })
        .map(v => Tuple1(v._1))
      }
      case value : String => {
        throw SelectFormatException.create("Your SELECT statement cannot be executed: you are doing a > comparison between string. Attribute = " + predicate.value)
      }
    }
  }

  @throws[SelectFormatException]
  def applyMetaPredicateGTE(predicate: Predicate, input: DataSet[FlinkMetaType]): DataSet[Tuple1[Long]] = {
    castDoubleOrString(predicate.value) match {
      case value : Double => {
        input.filter(_._2.toLowerCase(locale).equals(predicate.attribute_name.toLowerCase(locale))).filter((a : FlinkMetaType) => {
          try{
            a._3.toDouble >= value
          } catch {
            case _ : Throwable => logger.error("could not convert to double");false
          }
        })
        .map(v => Tuple1(v._1))
      }

      case value : String => {
        throw SelectFormatException.create("Your SELECT statement cannot be executed: you are doing a >= comparison between string. Attribute = " + predicate.value)
      }
    }
  }






  //Composed metacondition evaluation methods

  @throws[SelectFormatException]
  def applyMetaConditionOR(meta1: MetadataCondition, meta2: MetadataCondition, input: DataSet[FlinkMetaType], index : Option[Directory], env : ExecutionEnvironment) : DataSet[Tuple1[Long]] = {
    applyMetaSelect(meta1, input, index, env).union(applyMetaSelect(meta2, input, index, env))
  }

  @throws[SelectFormatException]
  def applyMetaConditionAND(meta1: MetadataCondition, meta2: MetadataCondition, input: DataSet[FlinkMetaType], index : Option[Directory], env : ExecutionEnvironment) : DataSet[Tuple1[Long]] = {
    applyMetaSelect(meta1, input, index, env).join(applyMetaSelect(meta2, input, index, env)).where(0).equalTo(0) {
      (a, b) => (a)
    }
  }



  //Other usefull methods

  def castDoubleOrString(value : Any) : Any = {
    try{
      value.toString.toDouble
    } catch {
      case e : Throwable => logger.error("could not convert to double");value.toString.toLowerCase(locale)
    }
  }

  /**
   * Build Index directory of the input files
   * First we have to scan all the files and build in memory index (we can have the index on HDD already built)
   *
   * @return Directory Index of the meta files
   */
  def buildIndex(uri: Array[String]): Directory = {
    var dir = buildInMemIndex(new File(uri(0)), 1, null, false);
    if (uri.length > 1)
      for (url <- uri.slice(1, uri.length)) {
        dir = buildInMemIndex(new File(url), 1, dir, false);
      }
    dir
  }

  def searchIndex(query: String, dir: Directory): String = {
    val search = new SearchIndex("/user/", "abdulrahman", "LOCAL"); // when it is local that means do not consider any of the input directories except the file meta dir
    // LuceneIndex.printIndex(dir)
    search.SearchLuceneIndex(query, dir)
  }

  def idHasher(path : String) : Long = {
    val name : String = new File(path).toString
//    logger.info(path+"\t"+name.replaceAll("/",""))
    Hashing.md5().hashString(name.replaceAll("/",""), Charsets.UTF_8).asLong()
  }

}
