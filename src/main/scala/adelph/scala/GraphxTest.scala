package adelph.scala

// $example on$
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.spark.graphx.GraphLoader
// $example off$
import org.apache.spark.sql.SparkSession

/**
 * Suppose I want to build a graph from some text files, restrict the graph
 * to important relationships and users, run page-rank on the sub-graph, and
 * then finally return attributes associated with the top users.
 * This example do all of this in just a few lines with GraphX.
 *
 * Run with
 * {{{
 * bin/run-example graphx.ComprehensiveExample
 * }}}
 */
object GraphxTest {

  def main(args: Array[String]): Unit = {


    // Creates a SparkSession.
    val spark = SparkSession
      .builder()
      .appName(s"${this.getClass.getSimpleName}").master("local")
      .getOrCreate()
    val sc = spark.sparkContext

    // $example on$
    // Load my user data and parse into tuples of user id and attribute list
    val users = (sc.textFile("data/graphx/users.txt")
      .map(line => line.split(",")).map( parts => (parts.head.toLong, parts.tail) ))

    // Parse the edge data which is already in userId -> userId format
    val followerGraph = GraphLoader.edgeListFile(sc, "data/graphx/followers.txt")

    // Attach the user attributes
    val graph = followerGraph.outerJoinVertices(users) {
      case (uid, deg, Some(attrList)) => attrList
      // Some users may not have attributes so we set them as empty
      case (uid, deg, None) => Array.empty[String]
    }

    // Restrict the graph to users with usernames and names
    val subgraph = graph.subgraph(vpred = (vid, attr) => attr.size == 2)

    // Compute the PageRank
    val pagerankGraph = subgraph.pageRank(0.001)

    // Get the attributes of the top pagerank users
    val userInfoWithPageRank = subgraph.outerJoinVertices(pagerankGraph.vertices) {
      case (uid, attrList, Some(pr)) => (pr, attrList.toList)
      case (uid, attrList, None) => (0.0, attrList.toList)
    }

    println(userInfoWithPageRank.vertices.top(5)(Ordering.by(_._2._1)).mkString("\n"))
    // $example off$

    spark.stop()
  }
}
// scalastyle:on println