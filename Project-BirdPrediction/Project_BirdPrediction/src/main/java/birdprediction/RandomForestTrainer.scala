package birdprediction

import org.apache.spark.mllib.linalg.SparseVector
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.RandomForest

import scala.collection.mutable.ArrayBuffer

/*
Class RandomForestTrainer
It is used to Train the labeled data. Random Forest Model is used for the same.
(Used Spark's MLlib library)
(Reference: https://spark.apache.org/docs/1.2.0/mllib-ensembles.html#Random-Forests)
 */
object RandomForestTrainer {

  def main(args: Array[String]): Unit = {

    /* Spark Configuration and Context */
    val conf = new SparkConf().setAppName("RandomForestModel")
      .setMaster("local[*]")
//          .setMaster("yarn")
    val sc = new SparkContext(conf)


    /* RDD: List[String]
       RDD represents the variables/columns extracted from labeled input
       Sequence of operations:
          - reads the labeled input
          - mapPartitionsWithIndex : drops the header row
          - map : extracts the required variables and forms a list
    */
    val inputData = sc.textFile(args(0))
      .mapPartitionsWithIndex { (idx, iter) => if (idx == 0) iter.drop(1) else iter }
      .map(line => {
      val lines = line.split(',')
      List(lines(26),
        lines(2), lines(3), lines(4), lines(5), lines(6), lines(7),
        lines(12), lines(13), lines(14), lines(16),
        lines(955), lines(956), lines(957), lines(958),
        lines(960), lines(962), lines(963), lines(966), lines(967),
        lines(1090), lines(1091), lines(1092), lines(1093), lines(1094), lines(1095), lines(1096),
        lines(1097), lines(1098), lines(1099), lines(1100), lines(1101))
    })


    /* RDD: List[LabeledPoint]
       RDD represents the Labeled Point input to be passed to the model. Label will
       be the column/variable that has to be used for prediction and features will
       Sparse vector whose values will be those features whose values are not '?'
       Sequence of operations:
          - filter: removes all the records whose value is '?' for field 'AGELAIUS_PHOENICEUS'
                    (since there is no point in predicting/training on those records whose
                    prediction we don't know)
          - map : creates LabeledPoint input to pass it model
    */
    val transformedData = inputData.filter(record => !(record(0).equals("?")))
      .map(row =>
        (LabeledPoint(transformPredictionColumn(row(0).toString), createVector(row.slice(1, row.size-1)))))


    /* Array[RDD[LabeledPoint]]
       It randomly splits the data.
       70% data is used for training and 30% is used for test prediction
    */
    val splits = transformedData.randomSplit(Array(0.7, 0.3))
    val (trainingData, testData) = (splits(0), splits(1))


    /* Trains a RandomForest model */
    val numClasses = 2    /* number of classes for classification */
    val categoricalFeaturesInfo = Map[Int, Int]()     /* all features are continuous */
    val numTrees = 10
    val featureSubsetStrategy = "auto"
    val impurity ="gini"
    val maxDepth = 10
    val maxBins = 32


    /* RandomForestModel
       Creates a Random Forest model after training on the labeled data
    */
    val model = RandomForest.trainClassifier(trainingData, numClasses, categoricalFeaturesInfo,
      numTrees, featureSubsetStrategy, impurity, maxDepth, maxBins)


    /* RDD: [(Double, Double)]
       RDD represents the prediction provided and prediction done by using RandomForestModel
       Sequence of operations:
          - map : evaluates model on test labeled data and computes prediction
    */
    val labelAndPreds = testData.map { point =>
      val prediction = model.predict(point.features)
      (point.label, prediction)
    }

    /* Calculates the test error rate */
    val testErr = labelAndPreds.filter(r => r._1 != r._2).count.toDouble / testData.count()
    System.out.println("Accuracy Rate = " + (1 - testErr))

    /* Saves the model */
    model.save(sc, args(1))
  }



  /*
    Method sets value of field (here 'AGELAIUS_PHOENICEUS') to 1.0
    if value of that field is either 'X' or greater than 0. This is because
    since we are making a prediction on this column, we don't need the count of the
    species (Also, as documentation states that 'X' as value indicates that species
    is present without count, we can assume that the species is present and hence
    we can assign it 1.0 prediction)
   */
  def transformPredictionColumn(value: String): Double = {
    if(value.equalsIgnoreCase("X") || value.toDouble > 0.0) return 1.0
    return 0.0
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
      if(!features(i).equals("?") && !features(i).equalsIgnoreCase("X")){
        indices += i
        values += features(i).toDouble
      }
    }

    return new SparseVector(features.size, indices.toArray, values.toArray)
  }
}
