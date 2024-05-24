import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

object GraphAnalysis {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Graph Analysis").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val spark = SparkSession.builder.config(conf).getOrCreate()

    // Lire les données des sommets et des arêtes à partir des fichiers CSV
    val vertices = spark.read.option("header", "true").csv("files/vertices.csv")
    val edges = spark.read.option("header", "true").csv("files/edges.csv")

    val verticesRDD: RDD[(VertexId, Int)] = vertices.rdd.map(row => (row.getAs[String]("id").toLong, row.getAs[String]("attr").toInt))
    val edgesRDD: RDD[Edge[Int]] = edges.rdd.map(row => Edge(row.getAs[String]("src").toLong, row.getAs[String]("dst").toLong, row.getAs[String]("attr").toInt))

    // Create the graph
    val graph: Graph[Int, Int] = Graph(verticesRDD, edgesRDD)

    // Compute shortest paths
    val sourceId: VertexId = 1
    val shortestPaths = graph.shortestPaths.landmarks(Seq(sourceId)).run()
    println("Shortest Paths:")
    shortestPaths.vertices.collect.foreach(println)

    // Compute PageRank
    val ranks = graph.pageRank(0.0001).vertices
    println("PageRank:")
    ranks.collect.foreach(println)

    // Compute clustering coefficient
    val clusteringCoefficients = graph.triangleCount().vertices
    println("Clustering Coefficients:")
    clusteringCoefficients.collect.foreach(println)

    sc.stop()
    spark.stop()
  }
}

