import org.apache.spark.SparkConf
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.collection.mutable.ListBuffer

/*
  Trait to extends in order to use the approximated personalized pagerank
 */
trait Node extends Serializable {
  var residual: Double = 0.0
  var pagerank: Double = 0.0
  var degree: Int = 0
  def newNode(res: Double = 0.0, pr: Double = 0.0, deg: Int = 0) : Node
}
/*
  This class defines the object associated with vertices in a graphX instance for the example run
 */
class NodeExample(res: Double = 0.0, pr: Double = 0.0, deg: Int = 0, val name: String = "") extends Node {
  residual = res
  pagerank = pr
  degree = deg

  override def newNode(res: Double, pr: Double, deg: Int): NodeExample = {
    new NodeExample(res,pr,deg,name)
  }

}

object PregelPagerank {

  // If this object is run as a program, it will test the class with a simple example

  def main(args: Array[String]): Unit = {

    val appName = "PregelPagerankExample"
    val checkPointDir = "/tmp"

    val conf = new SparkConf()
      .setAppName(appName)
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .setMaster("local[*]")

    val spark = SparkSession
      .builder()
      .config(conf)
      .getOrCreate()

    spark
      .sparkContext.setCheckpointDir(checkPointDir)

    val nodes: RDD[(VertexId, Node)] = spark.sparkContext.parallelize(Seq((1.toLong, new NodeExample(1.0 / 2, name = "a")), (2.toLong, new NodeExample(0, name = "b")), (3.toLong, new NodeExample(name = "c")), (4.toLong, new NodeExample(name = "d")), (5.toLong, new NodeExample(1.0 / 2, name = "e"))))
    val edges: RDD[Edge[Nothing]] = spark.sparkContext.parallelize(Seq(new Edge(1, 2), new Edge(2, 3), new Edge(2, 4), new Edge(4, 5), new Edge(3, 5)).flatMap(edge => Seq(edge, new Edge(edge.dstId, edge.srcId))))

    val graph = Graph[Node, Nothing](nodes, edges)

    val tmpDegree = graph.ops.outDegrees.join(graph.vertices)
    val vertices = tmpDegree.map {
      case (id, (degree, node)) =>
        node.degree = degree
        (id, node)
    }

    val graphWithDegree = Graph.apply[Node, Nothing](vertices, edges)

    run(graphWithDegree,0.01).vertices.collect().foreach { case (id, vertex) => println("Node no : " + id +
      " PR : " + vertex.pagerank  +
      " Residu : " + vertex.residual +
      " Name : "+vertex.asInstanceOf[NodeExample].name)}
  }

  /*
    This function run the approximated personalized pagerank
    Arguments :
      graph : a graph with the vertices conforming to the class declared in this object
      epsi : the parameter concerning the stopping criteria and also the precision of the pagerank
      alpha : a parameter representing the quantity to save as pagerank compared to the one to send to neighbors when
      diffusing
      maxIter : the maximum iterations the algorithm will run for if it does not converge before
    Return :
      The graph with the attributes pagerank and residual updated
 */
  def run(graph: Graph[Node, Nothing], epsi: Double = 0.01, alpha: Double = 0.15, maxIter: Int = Int.MaxValue): Graph[Node, Nothing] = {

    // The first message is just to start the Pregel algorithm, so in our case it needs to be "zero"
    graph.ops.pregel[(Boolean, Double)]((false, 0),maxIter)(vertexProgram(_, _, _, alpha, epsi), sendMessage(_, epsi), mergeMessage)
  }

  /*
    This function defines the program that is executed by every vertex that received a message in order to update its state
    Arguments :
      id : the id of the current node
      current : the object associated with the current node
      message : a unique message or an aggregation of different messages destined to the current node
      epsi : the parameter concerning the stopping criteria and also the precision of the pagerank
      alpha : a parameter representing the quantity to save as pagerank compared to the one to send to neighbors when
    Return :
      The new state associated with the current node
   */
  private def vertexProgram(id: Long, current: Node, message: (Boolean, Double), alpha: Double, epsi: Double): Node = {
    val (update, m) = message

      var pagerank = current.pagerank
      var residual = current.residual
      val degree = current.degree

      if (update) {
        pagerank += alpha * residual
        residual = (1 - alpha) * residual / 2
      }
      residual += (1 - alpha) * m / 2
      current.newNode(residual,pagerank,degree)

  }

  /*
   This function iterate over all out edges of vertices that received messages on the previous iterations and decide
   which messages should be sent
   Arguments :
     triplet : the representation of the current edge with information on the source and destination
     epsi : the parameter concerning the stopping criteria and also the precision of the pagerank
    Return :
     An iterator of all the messages to send with the ids of the concerned vertex
  */
  private def sendMessage(triplet: EdgeTriplet[Node, Nothing], epsi: Double): Iterator[(VertexId, (Boolean, Double))] = {
    val ret = ListBuffer[(VertexId, (Boolean, Double))]()


    if (triplet.srcAttr.residual / triplet.srcAttr.degree >= epsi) {

      ret.append((triplet.dstId, (false, triplet.srcAttr.residual / triplet.srcAttr.degree)))

      // This is the message to make the sending vertex updates itself
      ret.append((triplet.srcId, (true, 0)))
    }

    ret.toIterator
  }

  /*
    This function defines how the messages should be merged
    Arguments :
      messA : the first message
      messB : the second message
    Return :
      The merged message
   */
  private def mergeMessage(messA: (Boolean, Double), messB: (Boolean, Double)): (Boolean, Double) = {
    (messA._1 || messB._1, messA._2 + messB._2)
  }
}
