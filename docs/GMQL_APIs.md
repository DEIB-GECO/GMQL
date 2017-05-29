[TOC]
GMQL APIs
=========

This section does not use GMQL compiler.  GMQL Directed Acyclic Graph (DAG) APIs are called directly. We constructed an API  for each command of GMQL (11 commands); they allow the use of GMQL in programming languages. GMQL Engine was developed using [SCALA language](https://www.scala-lang.org/), the following examples are created in SCALA.

Maven imports, needed to run GMQL in your application. You can choose between Spark, Flink, or SciDB Implementations. In case of Flink implementation, you should include Flink GMQL implementation. It is better not to include both Spark and Flink in the same execution.

  

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


All the imports below are used in all the documentation in GMQL APIs section, in addition to the call for GMQL server and the choice of the executor. To start writing a GMQL application, start by defining the server implementation that you will use by setting the executor type. 


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

#### Full example

All GMQL applications should contain the data parsing and reading operation and the materialising operation, as shown below, all the rest of the operations can fit in between the read and the store operations. The type of the parser to use is dependent on the dataset regions schema. There are set of built-in parsers, such as Bed parser, Narrow peaks parser, and Broad peaks parser, in addition to the CustomParser, which takes as an argument the path to the schema XML file. The first command should be reading the input datasets; remember that this is lazy processing, nothing will run until the server command *server.run()* is executed. 

Each GMQL operation returns [IRVariable](../GMQL-Core/src/main/scala/it/polimi/genomics/core/DataStructures/IRVariable.scala), each IRVariable can be materialised or used as input for other IR variables
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


----------


#### Genometric Map, set of examples


``` 
DS1.MAP(None, List(), REFDS)
``` 

``` 
DS1.MAP(None, 
	List(DefaultRegionsToRegionFactory.get("MAX",0,Some("newScore")) ),
		 REFDS)
``` 

``` 
DS1.MAP(
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
           REFDS
         )
``` 


----------

#### Genometric Difference, set of examples

```
	DS1.DIFFERENCE(None, List(), REFDS)
```

``` 
	DS1.DIFFERENCE(None, 
		List(DefaultRegionsToRegionFactory.get("MAX",0,Some("newScore")) ),
		REFDS)
```


---------

#### Union, set of examples

```
	val resDS = DS1.UNION(DS2)
```

----------

#### Merge, set of examples

```
	val resDS = DS1.MERGE(groupBy = Some(List("Cell")))
```

----------

#### Order, set of examples

```
	DS1.ORDER(
		meta_ordering = Some(List(("bert_value2", Direction.DESC), 
					("bert_value1", Direction.ASC))),
		meta_new_attribute = "group_ordering", 
		meta_top_par =  TopG(1), 
		region_top_par = NoTop())
```

```
	DS1.ORDER(
		meta_ordering = None, 
		meta_top_par =  NoTop(), 
		region_ordering =  Some(List((0, Direction.ASC), (1, Direction.DESC))), 
		region_top_par = TopG(3))
```

----------

#### Project, set of examples

```
	DS1.PROJECT(
		projected_meta = Some(List("antibody","cell")), 
		projected_values = None, 
		extended_values = None)
```

```
	val fun = new MetaAggregateStruct {
            override val newAttributeName: String = "computed_value1"
            override val inputAttributeNames: List[String] = List("score")
            override val fun: (Array[Traversable[String]]) => String =
            //average of the double
              (l : Array[Traversable[String]]) => {
                val r =
                  l(0)
                    .map((a: String) => (a.toDouble * 2, 1))
                    .reduce((a: (Double, Int), b: (Double, Int)) => 
	                    (a._1 + b._1, a._2 + b._2))
                (r._1 / r._2).toString
              }
          }

          DS1.PROJECT(
	          projected_meta = Some(List("antibody","cell")), 
	          extended_meta = Some(fun),
			  extended_values = None, 
			  projected_values = None)
```

```
          //PROJECT AGGREGATE RD
          val fun = new RegionExtension {
            override val fun: (Array[GValue]) => GValue = 
	         {x=>
				if( x(1).isInstanceOf[GDouble]) 
					GDouble(x(0).asInstanceOf[GDouble].v + 
					x(1).asInstanceOf[GDouble].v)
				else GNull()
			 }
            override val inputIndexes: List[Any] =
	            List(0,MetaAccessor("score"))
          }

          dataAsTheyAre.PROJECT(None,extended_values = Some(List(fun)))
```

#### Extend, set of examples

```
DS1.EXTEND(
  region_aggregates = List(
	DefaultRegionsToMetaFactory.get("MAX",0,Some("maxScore")),
	DefaultRegionsToMetaFactory.get("MIN",0,Some("miniScore"))
  )
)

```

```
dataAsTheyAre.EXTEND(
      region_aggregates = List(
        new RegionsToMeta {
          override val newAttributeName = "MAXscore"
          override val inputIndex: Int = 0
          override val associative: Boolean = true
          override val fun: (List[GValue]) => 
	          GValue = {
	            (line) => GDouble(
		            line.map((gvalue) =>
			            gvalue.asInstanceOf[GDouble].v
			            )
				        .reduce((x, y) => Math.max(x, y)))
          }
          override val funOut: (GValue, Int) => GValue = 
	          { (v1, v2) => v1 }
        }
      )
    )
```

#### Full Example of reading from memory and Collecting data to memory after execution.
Data can be passed to GMQL as an RDD of meta and an RDD of regions. The following example shows how to pass the RDD as input for GMQL operation.

You can collect the data from GMQL dataset in memory architecture (Arrays). This will trigger the lazy execution of GMQL. In case the data does not fit in memory, the system will give exception which is not handles (the user should handle this exception or make sure not to exceed the memory size). The output format set in GMQLSpzek Executor should be ***GMQLSchemaFormat.COLLECT***

```
    val conf = new SparkConf()
	    .setAppName("test GMQL")
	    .setMaster("local[4]")
	    .set("spark.serializer",
	"org.apache.spark.serializer.KryoSerializer")
		.set("spark.kryoserializer.buffer", "64")

    val sc:SparkContext =new SparkContext(conf)

    val server = new GmqlServer(new GMQLSparkExecutor(sc=sc,outputFormat = GMQLSchemaFormat.COLLECT))

// Generate meta data of type (ID:Long,(Att:String, Value:String))
    val metaDS = sc.parallelize((1 to 10).map(x=> (1,("test","Abdo"))))

//Generate in memory regions as (key: GrecordKey,values: Array[GValue]) 
    val regionDS = sc.parallelize((1 to 1000).map{x=>(new GRecordKey(1,"Chr"+(x%2),x,x+200,'*'),Array[GValue](GDouble(1)) )})


    val DS1 = server.READ("")
	    .USING(metaDS,
		    regionDS,
			List[(String, PARSING_TYPE)](("score",ParsingType.DOUBLE)))

import it.polimi.genomics.core.DataStructures.CoverParameters._
    val cover = DS1.COVER(
	    CoverFlag.HISTOGRAM, 
	    N(1),
	    ANY(), 
		List(), 
		None )

    val output = server.setOutputPath("").COLLECT(cover)

    output.asInstanceOf[GMQL_DATASET]._1.foreach(println _)
```

#### TAKE operation
Just like Collect opertion, take executes the DAG of the operations and can be mixed with collect and Materialize operations in the same code. Take takes as an argument the number of regions to extract from the result dataset. This allow the programmer to control the memory overflow, in case he is not interested in loading the full dataset in memory.

Note: as for the collect operation, the output format should be selected as   ***GMQLSchemaFormat.COLLECT***
```
val output = server.setOutputPath("").TAKE(ds1,100)
```



