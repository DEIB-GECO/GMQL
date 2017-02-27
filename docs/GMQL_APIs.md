[TOC]
GMQL APIs
=========

This section does not use GMQL compiler.  GMQL Directed Acyclic Graph (DAG) APIs are called directly. We constructed an API  for each command of GMQL (11 commands), which allow the use of GMQL in programming languages. GMQL Engines was developed using [SCALA language](https://www.scala-lang.org/), the following examples are created in SCALA.

Maven imports, needed to run GMQL in your application. You can choose between Spar, Flink, or SciDB Implementations. In case of Flink implementation you should include Flink GMQL implementation. It is better not to include both spark and Flink in the same execution.

```  
<dependency>
    <groupId>it.polimi.genomics</groupId>
    <artifactId>GMQL-Core</artifactId>
    <version>2.0</version>
</dependency>

<dependency>
    <groupId>it.polimi.genomics</groupId>
    <artifactId>GMQL-Spark</artifactId>
    <version>4.0</version>
</dependency>

<dependency>
    <groupId>it.polimi.genomics</groupId>
    <artifactId>GMQL-Server</artifactId>
    <version>2.0</version>
</dependency>
```  
All the imports bellow are used in all the documentation in GMQL APIs section, in addition to the call for GMQL server and the choose of the executor. To start writing GMQL application, start by defining the server implementation that you will use by setting the executor type. 

```  
import it.polimi.genomics.GMQLServer.GmqlServer
import it.polimi.genomics.core.DataStructures
import it.polimi.genomics.spark.implementation._
import it.polimi.genomics.spark.implementation.loaders.BedParser._
import org.apache.spark.{SparkConf, SparkContext}

val conf = new SparkConf()
      .setAppName("GMQL V2.1 Spark")
      .setMaster("local[*]")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
val sc:SparkContext =new SparkContext(conf)

val executor = new GMQLSparkExecutor(sc=sc)
val server = new GmqlServer(executor)

val expInput = Array("/home/V2Spark_TestFiles/Samples/exp/")
val refInput = Array("/home/V2Spark_TestFiles/Samples/ref/")
val output = "/home/V2Spark_TestFiles/Samples/out/"
```  

#### Full example

All GMQL applications should contain the data parsing and reading operation and the materialising operation, as shown bellow, all the rest of the operations can fit in between the read and the store operations. The type of the parser to use is dependent on the dataset regions schema. There are set of builtin parsers , such as Bed parser, Narrow peaks parser, and broad peaks parser. In addition to the CustomParser, which takes as an argument the path to the schema XML file. The first command should be reading the input datasets, remember that this is lazy processing, nothing will run until the server command *server.run()* is executed. 

Each GMQL operation returns [IRVariable](../GMQL-Core/src/main/scala/it/polimi/genomics/core/DataStructures/IRVariable.scala), each IRVariable can be materialised or used in as input for other IR variables
```  
//Set data parsers
val DS1: IRVariable = server READ ex_data_path USING BedParser
val DS2 = server READ REF_DS_Path USING BedScoreParser2

//Selection
val ds1S: IRVariable = DS1.SELECT(Predicate("antibody", META_OP.EQ, "CTCF"))
val DSREF = DS2.SELECT(Predicate("cell", META_OP.EQ, "genes"))

//Cover Operation
val cover: IRVariable = ds1S.COVER(CoverFlag.COVER, N(1), N(2), List(), None)

//Store Data
server setOutputPath output_path MATERIALIZE cover

server.run()
```  


####Select operation APIs

```  
import DataStructures.MetadataCondition._
val meta_condition =
      DataStructures.MetadataCondition.AND(
	      Predicate("cell",META_OP.GTE, "11"),
		  Predicate("provider",META_OP.NOTEQ, "UCSC")
		)
		
import DataStructures.RegionCondition._
val   reg_condition =
      DataStructures.RegionCondition.Predicate(2, REG_OP.GT, MetaAccessor("cell"))

val dag = s.SELECT(meta_condition, reg_condition)
``` 

#### Genometric JOIN, set of examples


``` 
import it.polimi.genomics.core.DataStructures.JoinParametersRD._

DS1.JOIN(None, List(new JoinQuadruple(Some(DistLess(200000)), Some(new MinDistance(1)), Some(new Upstream))), RegionBuilder.CONTIG, REFDS)

``` 
``` 
  DS1.JOIN(None, List(new JoinQuadruple(Some(new MinDistance(1)))), RegionBuilder.CONTIG, REFDS)
``` 
``` 

  DS1.JOIN(None, List(new JoinQuadruple(Some(DistLess(100)))), RegionBuilder.CONTIG, REFDS)

``` 
``` 
  DS1.JOIN(None, List(new JoinQuadruple(Some(DistLess(20)), Some(new MinDistance(1)))), RegionBuilder.CONTIG, REFDS)

``` 
``` 

  DS1.JOIN(None, List(new JoinQuadruple(Some(DistLess(20)), Some(new Upstream()))), RegionBuilder.CONTIG, REFDS)
``` 
``` 

  DS1.JOIN(None, List(new JoinQuadruple(Some(DistGreater(4)), Some(new Upstream()), Some(new DistLess(20)))), RegionBuilder.CONTIG, REFDS)

``` 
``` 

  DS1.JOIN(None, List(new JoinQuadruple(Some(DistLess(20)), Some(new MinDistance(1)), Some(DistGreater(3)))), RegionBuilder.CONTIG, REFDS)
``` 
``` 

  DS1.JOIN(None, List(new JoinQuadruple(Some(new DistLess(0)))), RegionBuilder.CONTIG, REFDS)
``` 
``` 

  DS1.JOIN(None, List(new JoinQuadruple(Some(new DistGreater(0)))), RegionBuilder.CONTIG, REFDS)
``` 
``` 

  DS1.JOIN(None, List(new JoinQuadruple(Some(new Upstream()))), RegionBuilder.CONTIG, REFDS)
``` 
``` 

  DS1.JOIN(None, List(new JoinQuadruple(Some(new DownStream()))), RegionBuilder.CONTIG, REFDS)

``` 
``` 
  DS1.JOIN(None, List(new JoinQuadruple(Some(new MinDistance(1)))), RegionBuilder.CONTIG, REFDS)
``` 
``` 

  DS1.JOIN(None, List(new JoinQuadruple(Some(new MinDistance(2)))), RegionBuilder.CONTIG, REFDS)

``` 
``` 
  DS1.JOIN(None, List(new JoinQuadruple(Some(new DistGreater(0)), Some(new MinDistance(1)))), RegionBuilder.CONTIG, REFDS)

``` 
``` 
  DS1.JOIN(None, List(new JoinQuadruple(Some(new DistLess(0)), Some(new MinDistance(1)))), RegionBuilder.CONTIG, REFDS)

``` 
``` 
  DS1.JOIN(None, List(new JoinQuadruple(Some(new DistGreater(0)), Some(new Upstream()), Some(new MinDistance(1)))), RegionBuilder.CONTIG, REFDS)

``` 
``` 
  DS1.JOIN(None, List(new JoinQuadruple(Some(new MinDistance(10)))), RegionBuilder.CONTIG, REFDS)

``` 
``` 
  DS1.JOIN(None, List(new JoinQuadruple(Some(new MinDistance(10)), Some(DistLess(-1)))), RegionBuilder.CONTIG, REFDS)
``` 

#### Cover Operation, set of examples

``` 
import it.polimi.genomics.core.DataStructures.CoverParameters.{ANY, CoverFlag, N}

  DS1.COVER(CoverFlag.COVER, N(2), N(3), List(), None )
``` 
``` 
  DS1.COVER(CoverFlag.FLAT, N(2), N(3), List(), None )
``` 
``` 
  DS1.COVER(CoverFlag.SUMMIT, N(2), N(5), List(), None )
``` 
``` 
  DS1.COVER(CoverFlag.HISTOGRAM, ANY(), ANY(), List(), None )
``` 

#### Genometric Map, set of examples


``` 
optionalDS.MAP(None, List(), dataAsTheyAre)
``` 

``` 
dataAsTheyAre.MAP(None, 
	List(DefaultRegionsToRegionFactory.get("MAX",0,Some("newScore")) ),
		 optionalDS)
``` 

``` 
dataAsTheyAre.MAP(
           Some(new MetaJoinCondition(List(Default("Cell_Type")))),
           List(
             new RegionsToRegion {
              override val resType = ParsingType.DOUBLE
              override val index: Int = 0
              override val associative: Boolean = true
              override val funOut: (GValue,Int) => GValue = {(v1,v2)=>v1}
              override val fun: (List[GValue]) => GValue = {
               (line) =>{val ss = line.map(_.asInstanceOf[GDouble].v)
                if(!ss.isEmpty) GDouble(ss.reduce(_ + _))else GDouble (0)
               }
              }}
           ),
           optionalDS
         )
``` 