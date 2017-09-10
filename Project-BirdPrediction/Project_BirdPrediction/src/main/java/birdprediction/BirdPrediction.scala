package birdprediction

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.linalg.SparseVector
import org.apache.spark.mllib.tree.model.RandomForestModel

import scala.collection.mutable.ArrayBuffer

/*
Class BirdPrediction
It is used to Predict the unlabeled data. Random Forest Model generated from
RandomForestTrainer is used for the same.
(Used Spark's MLlib library)
(Reference: https://spark.apache.org/docs/1.2.0/mllib-ensembles.html#Random-Forests)
 */
object BirdPrediction {

  def main(args: Array[String]): Unit = {

     /*Spark Configuration and Context */
    val conf = new SparkConf().setAppName("BirdPrediction")
      .setMaster("local[*]")
//      .setMaster("yarn")
    val sc = new SparkContext(conf)

    /* Loads the Model */
    val randomForestModel = RandomForestModel.load(sc, args(0))


    /* RDD: [String]
       RDD represents a string which contains the sampling event id and it's prediction
       for bird 'AGELAIUS_PHOENICEUS'
       Sequence of operations:
          - reads the unlabeled input
          - mapPartitionsWithIndex : drops the header row
          - map : extracts the required variables and forms a list
          - map : creates a Sparse Vector of the features list and uses the loaded model for prediction
          - map : concatenates the Sampling Event Id with the prediction by comma to write as CSV
    */
    val unlabeledPredictedData = sc.textFile(args(1))
      .mapPartitionsWithIndex { (idx, iter) => if (idx == 0) iter.drop(1) else iter }
      .map(line => {
        val lines = line.split(',')
        List(lines(0),
          lines(2), lines(3), lines(4), lines(5), lines(6), lines(7),
          lines(12), lines(13), lines(14), lines(16),
          lines(955), lines(956), lines(957), lines(958),
          lines(960), lines(962), lines(963), lines(966), lines(967),
          lines(1090), lines(1091), lines(1092), lines(1093), lines(1094), lines(1095), lines(1096),
          lines(1097), lines(1098), lines(1099), lines(1100), lines(1101))
      })
      .map(row =>
        (row(0).toString, randomForestModel.predict(createVector(row.slice(1, row.size-1)))))
      .map(line => line._1 + "," + line._2.toString)

    /* Creates the header row for output file */
    val headerInOutput = sc.parallelize(Array("SAMPLING_EVENT_ID,SAW_AGELAIUS_PHOENICEUS"))

    /* Combines the header row with predicted data and saves the output */
    sc.parallelize(headerInOutput.union(unlabeledPredictedData).collect(), 1)
      .saveAsTextFile(args(2))
  }



  /*
    Method creates a Sparse Vector of the feature list provided.
    Here, if any feature does not have a value, i.e. if it has value '?',
    it will not be useful to include it in the features vector for training,
    and hence we don't include in the Sparse vector
   */
  def createVector(features: List[String]): SparseVector = {
    var indices = ArrayBuffer[Int]()
    var values = ArrayBuffer[Double]()

    for(i <- 0 to features.size-1){
      if(!features(i).equals("?") && !features(i).equals("X")){
        indices += i
        values += features(i).toDouble
      }
    }

    return new SparseVector(features.size, indices.toArray, values.toArray)
  }
}