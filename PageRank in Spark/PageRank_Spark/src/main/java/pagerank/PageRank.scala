package pagerank

import org.apache.spark.{SparkConf, SparkContext}
import scala.collection.JavaConversions._

object PageRank {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("PageRank")//.setMaster("local[*]")
      .setMaster("yarn")
    val sc = new SparkContext(conf)

    val ALPHA = 0.15
    val ITERATIONS = 10
    val TOP_K = 100

    /*    PRE-PROCESSING      */

    /* PairRDD: (PageName: String, LinkedPageNames: List[String])
       RDD represents pages and their linked page names
       Sequence of operations:
          - reads input
          - map & filter: for each line in the input removes null objects (pages having invalid urls)
          - map: converts each row to tuple of form (String, List[String])
          - persist: stores the RDD in memory
    */
    val pageWithLinks = sc.textFile(args(0))
      .map(line => Bz2WikiParser.ParseLine(line))
      .filter(row => row != null)
      .map(row => (row.PageName, row.LinkedPageNames.toList))
      .persist()

    /* PairRDD: (PageName: String, AdjList: List[String])
       RDD represents pages and their adjacency list. Nodes without any linked pages
       will have empty adjacency list
       Sequence of operations:
          - flatMap: for each node in adjacency list, emits an empty list
          - reduceByKey: reduces the adjacency lists for each page
          - union: unions the reduced data set with pageWithLinks RDD
          - reduceByKey: reduces the adjacency lists for each page
     */
    val nodesAndAdjList = pageWithLinks.flatMap
      {case(pageName, links) => links.map(node => (node, List[String]()))}
      .reduceByKey((adjList1, adjList2) => adjList1 ::: adjList2)
      .union(pageWithLinks)
      .reduceByKey((adjList1, adjList2) => adjList1 ::: adjList2)


    val TOTAL_NODES = nodesAndAdjList.count()


    /* PairRDD: (PageName: String, PageRank: Double)
       RDD represents pages and their initial page ranks
    */
    var nodesAndPageRanks = nodesAndAdjList.map(node => (node._1, 1.0/TOTAL_NODES))


    /*    PAGE RANK CALCULATIONS     */
    for(i <- 1 to ITERATIONS){
      var DELTA = 0.0

      /* Delta (sum of dangling nodes contributions)
         Sequence of operations:
            - filter: retrieves dangling nodes
            - join: joins filtered data set with nodesAndPageRanks RDD to obtain page ranks
                    of dangling nodes
            - map: converts to RDD of type (Double) (only page ranks)
            - reduce: sums up the dangling nodes contributions
       */
      DELTA = nodesAndAdjList.filter(node => node._2.isEmpty)
        .join(nodesAndPageRanks)
        .map(node => node._2._2)
        .reduce((acc, n) => acc + n)

      /* PairRDD: (PageName: String, PageRank: Double)
       RDD represents pages and their page ranks after iteration i
       Sequence of operations:
          - join: joins page ranks and adjacency list
          - flatMap: for each node in adjacency list of a particular page, it emits
                     it's outlinks contribution (it also emits (page, 0.0) to handle
                     pages having no inlinks)
          - reduceByKey: for each page, it sums it's inlinks contribution
          - map: calculates the page rank of each node
      */
      nodesAndPageRanks = nodesAndAdjList.join(nodesAndPageRanks)
        .flatMap{
          case (pageName, (adjList, pageRank)) => {
            List(List((pageName, 0.0)), adjList.map(node => (node, pageRank / adjList.size))).flatten
          }
        }
        .reduceByKey((acc, n) => acc + n)
        .map(node => (node._1, ALPHA/TOTAL_NODES + (1 - ALPHA) * ((DELTA/TOTAL_NODES) + node._2)))
    }


    /*    TOP-K JOB     */

    /* Array[(String, Double)]
       Represnts the top-100 pages along with their ranks
       Sequence of operations:
          - map: swaps position of page ranks and pages => Array[(Double, String)]
          - top: takes top 100 elements from array
          - map: swaps position of page and page ranks => Array[(String, Double)]
     */
    val top100PageRanks = nodesAndPageRanks.map(node => node.swap).top(TOP_K).map(node => node.swap)

    /* Converts top100PageRanks to RDD and writes to file */
    sc.parallelize(top100PageRanks, 1).saveAsTextFile(args(1))
  }
}
