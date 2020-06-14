package it.polimi.genomics.core.Debug

import it.polimi.genomics.core.BinSize
import it.polimi.genomics.core.DataStructures._
import org.slf4j.LoggerFactory

import scala.xml.NodeSeq

object EPDAG {

  def getCurrentTime:Float = {
    val t = System.nanoTime() / 1000000000F
    t
  }

  // Builds the upper EPDag given a root node
  // alreadyAdded: list of EPNodes already generated starting from a different root
  private def getUpperStructure(root: IROperator, alreadyAdded: scala.collection.mutable.MutableList[EPNode], startupNode: EPNode) : List[EPNode] = {

    def getRes: List[EPNode] = {
      var ePNode : EPNode = new EPNode(root)
      if(alreadyAdded.exists(_.iRDebugOperator==root)) {
        ePNode = alreadyAdded.filter(_.iRDebugOperator == root).head
      } else {
        alreadyAdded += ePNode

        var upperStructure = root.getDependencies.flatMap(d => getUpperStructure(d, alreadyAdded, startupNode))

        // Check if IRCombineMD or IRDifferenceRD have an actual dependency on the metajoin
        if (root.getDependencies.head.isInstanceOf[IRCombineMD] &&
          root.getDependencies.head.asInstanceOf[IRCombineMD].grouping.isInstanceOf[NoMetaJoinOperator] ||
          root.getDependencies.head.isInstanceOf[IRDifferenceRD] &&
            root.getDependencies.head.asInstanceOf[IRDifferenceRD].meta_join.isInstanceOf[NoMetaJoinOperator])
          upperStructure = upperStructure.filter(!_.getiROperator.isInstanceOf[IRJoinBy])

        upperStructure.foreach(ePNode.setParent)

        if(upperStructure.isEmpty)
        // If there is no dependency add as depenednecy the startup node
          ePNode.setParent(startupNode)


      }

      List(ePNode)
    }

    root match {
      case IRDebugRD(p) => getRes
      case IRDebugMD(p) => getRes
      case IRDebugMJ(p) => getRes
      case IRDebugMG(p) => getRes
      case _ => root.getDependencies.flatMap(n=>getUpperStructure(n,alreadyAdded, startupNode))
    }
  }

  private def getStartupNode : EPNode = {
    val startupOp =  IRStartup()
    val debugStartpOp =  IRDebugRD(startupOp)
    startupOp.addAnnotation(OPERATOR(OperatorDescr(GMQLOperator.Startup)))

    new EPNode(debugStartpOp)

  }

  private def getShutDownNode : EPNode = {
    val shutdownOp = IRShutdown()
    val debugShutdOp =  IRDebugRD(shutdownOp)
    shutdownOp.addAnnotation(OPERATOR(OperatorDescr(GMQLOperator.Shutdown)))

    new EPNode(debugShutdOp)
  }

  def build( dag: List[IRVariable]): EPDAG  = {

    val startupNode = getStartupNode
    val shutdownNode = getShutDownNode


    val originalExitNodes = dag.flatMap(v=>List(v.regionDag, v.metaDag))
    val exitNodes = scala.collection.mutable.MutableList[EPNode]()
    val allNodes = scala.collection.mutable.MutableList[EPNode](startupNode, shutdownNode)

    for (exitNode <- originalExitNodes) {
      exitNodes ++=  getUpperStructure(exitNode, allNodes, startupNode)
    }

    exitNodes.foreach(shutdownNode.setParent)

    new EPDAG(List(shutdownNode), allNodes.toList, startupNode, shutdownNode)

  }

}


class EPDAG(val exitNodes: List[EPNode], allNodes: List[EPNode], val startupNode: EPNode, val shutdownNode: EPNode) {



  var executionStartTime: Option[Float] = None
  var executionEndTime: Option[Float] = None

  var binSize: BinSize = BinSize()

  def setBinSize(b: BinSize) = {binSize = b}

  def startupEnded():Unit = {
    startupNode.trackOutputReady()
  }

  def executionStarted() : Unit = {
    val t = EPDAG.getCurrentTime

    executionStartTime = Some(t)
    allNodes.foreach(_.setGlobalStartTime(t))
  }


  def profilerStarted(): Unit = {
    shutdownNode.trackProfilingStarted()
  }


  def profilerEnded(): Unit = {
    shutdownNode.trackProfilingEnded()
  }

  def executionEnded() : Unit = {
    executionEndTime = Some(EPDAG.getCurrentTime)
    shutdownNode.trackOutputReady()
  }

  def executionTime: Float = {
    if(executionEndTime.isDefined && executionEndTime.isDefined)
      executionEndTime.get - executionStartTime.get
    else throw new Exception("Start and end of the execution must be both set before calling this method.")
  }


  def getNodeByDebugOperator(debugOperator: IROperator): EPNode = {
    if(!allNodes.exists(_.iRDebugOperator==debugOperator))
      throw new Exception("Not found DebugOperator: "+debugOperator.getOperator.name+" in a list of "+allNodes.length+" operators.")
    else {
      allNodes.filter(_.iRDebugOperator == debugOperator).head
    }
  }

  def getExpectedTimeSum : Float = {
    return allNodes.map(n=>n.getOperatorExecutionTime+n.getProfilingTime).sum
  }

  def getExpectedTimeCritical : Float = {
    return shutdownNode.getExpectedFinishedAfter.getOrElse(-1)
  }

  def save( name:String, path: String ): Unit = {

    val fullPath = path+name+".xml"

    println("Saving EPDAG as "+fullPath)

    val xml = <dag>
      <binSize>
        <cover>{binSize.Cover}</cover>
        <map>{binSize.Map}</map>
        <join>{binSize.Join}</join>
      </binSize>
      <responseTime>{this.executionTime}</responseTime>
      <expectedTimeSum>{this.getExpectedTimeSum}</expectedTimeSum>
      <expectedTimeCritical>{this.getExpectedTimeCritical}</expectedTimeCritical>
      {allNodes.map(_.toXml())}
    </dag>


    scala.xml.XML.save(fullPath, xml)
  }

  def toXML(): String = {

    val xml = <dag>
      <binSize>
        <cover>{binSize.Cover}</cover>
        <map>{binSize.Map}</map>
        <join>{binSize.Join}</join>
      </binSize>
      <responseTime>{this.executionTime}</responseTime>
      <expectedTimeSum>{this.getExpectedTimeSum}</expectedTimeSum>
      <expectedTimeCritical>{this.getExpectedTimeCritical}</expectedTimeCritical>
      {allNodes.map(_.toXml())}
    </dag>


    xml.toString()

  }


  private def criticalRecursion(n:EPNode, cost: Float): Unit = {

    if(n.getChildren.isEmpty) {
      //println("CHILD")
      // Leaf node
      val costUpToHere = cost+n.getOperatorExecutionTime
      if( n.getExpectedFinishedAfter.isEmpty || n.getExpectedFinishedAfter.isDefined &&  n.getExpectedFinishedAfter.get <= costUpToHere ) {
        n.setExpectedFinishedAfter(costUpToHere)
        //println("mod")
      }
    } else {
      //println("NOCHILD")
      n.getChildren.foreach(c=>criticalRecursion(c,cost+n.getOperatorExecutionTime+n.getProfilingTime))
    }

  }

  def computeCriticalPath() : Unit = {

    val root = startupNode
    criticalRecursion(root,0)


  }

}


class EPNode(val iRDebugOperator: IROperator) {

  final val uid = java.util.UUID.randomUUID.toString

  final private val logger =  LoggerFactory.getLogger(this.getClass)

  private val parents  =  scala.collection.mutable.MutableList[EPNode]()
  private val children =  scala.collection.mutable.MutableList[EPNode]()
  //private var outputProfile: DataProfile = _
  private var GMQLoperator: OperatorDescr = iRDebugOperator.getDependencies.head.getOperator
  private var iROperator: IROperator = iRDebugOperator.getDependencies.head


  var globalStartTime: Option[Float] = None

  private var outputReadyTime: Option[Float] = None
  private var expectedOutputReadyTime: Option[Float] = None
  private var outputProfileStartTime: Option[Float] = None
  private var outputProfileEndTime: Option[Float] = None

  private var outputProfile: Map[String, String] = Map()


  def isEntryNode: Boolean = {
    iROperator.isInstanceOf[IRReadRD[_, _, _, _]]
  }


  def setParent(node: EPNode) : Unit = {
    parents += node
    node.setChild(this)
  }
  def getParents:List[EPNode] = parents.toList

  def setChild(node:EPNode) : Unit = children += node
  def getChildren:List[EPNode] = children.toList

  //def setGMQLOperator(operator: OperatorDescr): Unit =  {GMQLoperator = operator}
  def getGMQLOperator: OperatorDescr = GMQLoperator

  //def setiROperator(operator: IROperator): IROperator = iROperator
  def getiROperator: IROperator = iROperator

  def trackOutputReady(): Unit = {
    if(outputReadyTime.isEmpty)
      outputReadyTime = Some(EPDAG.getCurrentTime)
    //else logger.warn("The output ready time was already set.")
  }
  def trackProfilingStarted(): Unit =  {
    if(outputProfileStartTime.isEmpty)
      outputProfileStartTime = Some(EPDAG.getCurrentTime)
    //else logger.warn("The profile start time was already set.")
  }

  def trackProfilingEnded(): Unit = {
    if(outputProfileEndTime.isEmpty)
      outputProfileEndTime = Some(EPDAG.getCurrentTime)
    // else logger.warn("The profile end time was already set.")
  }

  def getProfilingTime: Float = {
    if(outputProfileStartTime.isDefined && outputProfileEndTime.isDefined)
      outputProfileEndTime.get-outputProfileStartTime.get
    else {
      logger.info("Either outputProfileStartTime or outputProfileEndTime was note set for " + this.GMQLoperator.name)
      0
    }
  }

  def getStartedAfter: Float = {
    if(globalStartTime.isDefined)
      getStartedAt - globalStartTime.get
    else
      throw  new Exception("globalStartTime was not set." )
  }

  def getFinishedAfter: Float = {
    if(outputReadyTime.isDefined && globalStartTime.isDefined)
      outputReadyTime.get - globalStartTime.get
    else
      throw  new Exception("Either outputReadyTime or globalStartTime was not set." )
  }

  def getExpectedFinishedAfter: Option[Float] = expectedOutputReadyTime
  def setExpectedFinishedAfter(time: Float): Unit = {
    expectedOutputReadyTime = Some(time)
  }


  def getOperatorExecutionTime: Float = {
    if(outputReadyTime.isEmpty)
      throw new Exception("outputReadyTime is not set")
    else
      outputReadyTime.get - getStartedAt

  }

  def getStartedAt: Float = {
    if(parents.nonEmpty &&  !parents.forall(_.outputReadyTime.isDefined))
      throw new Exception("All parents must have finished executing before calling getStartedAt")
    else if(parents.isEmpty) {
      if(globalStartTime.isDefined)
        globalStartTime.get
      else
        throw new Exception("Global start time is not set.")
    } else {
      parents.map(_.outputReadyTime.get).max
    }
  }

  def setGlobalStartTime(time: Float):Unit = {
    globalStartTime = Some(time)
  }

  def setOutputProfile(profile: Map[String, String]): Unit = {
    outputProfile = profile
  }


  override def toString: String = {

    var str = "IROperator: "+getiROperator.getClass.getSimpleName+
      "\n\t"+"GMQLOperator: "+getGMQLOperator.name+" id:"+getGMQLOperator.id

    try {
      str += "\n\t"+"outputReadyTime: "+outputReadyTime.get
    } catch {
      case e: Exception =>  str += "\n\t"+"outputReadyTime Time: N/A"
    }


    try {
      str += "\n\t"+"Execution Time: "+getOperatorExecutionTime
    } catch {
      case e: Exception =>  str += "\n\t"+"Execution Time: N/A"
    }


    try {
      str += "\n\t"+"Started After: "+getStartedAt
    } catch {
      case e: Exception =>  str += "\n\t"+"Started After: N/A"
    }

    try {
      str += "\n\t"+"Finished After: "+getFinishedAfter
    } catch {
      case e: Exception =>  str += "\n\t"+"Finished Time: N/A"
    }

    try {
      str += "\n\t"+"Profiling Time: "+getProfilingTime
    } catch {
      case e: Exception =>  str += "\n\t"+"Profiling Time: N/A"
    }

    if(expectedOutputReadyTime.isDefined)
      str += "\n\t"+"Expected Finish Time: "+getExpectedFinishedAfter.get


    str += "\n\t"+"Profile: "
    outputProfile.foreach((k)=>str += "\n\t\t"+k._1+":"+k._2)




    str

  }

  def toXml() = {
    <node>
      <operatorName>{iROperator.getClass.getSimpleName}</operatorName>
      <GMQLoperator>
        <name>{GMQLoperator.name}</name>
        <id>{uid}</id>
        <params>
          {
          if(iROperator.getOperator.params.isDefined)
            iROperator.getOperator.params.get.map(p => <param name={p._1}>{p._2}</param>)
          }
        </params>
      </GMQLoperator>
      <executionTime>{try{getOperatorExecutionTime} catch{ case e: Exception => "n/a"}}</executionTime>
      <profilingTime>{try{getProfilingTime}catch{ case e: Exception => "n/a"}}</profilingTime>
      <inputs>
        {
        parents.map( p=> {
        <input operatorName={p.iROperator.getClass.getSimpleName} id={p.uid}
               isRegion={p.iROperator.isRegionOperator.toString}>
          {p.outputProfile.map(v=> <property name={v._1}>{v._2}</property>)}
        </input>})
        }
      </inputs>
      <output>
        {outputProfile.map(v=> <property name={v._1}>{v._2}</property>)}
      </output>
    </node>
  }

}