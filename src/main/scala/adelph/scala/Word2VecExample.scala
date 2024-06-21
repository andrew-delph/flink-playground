package adelph.scala


import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
// $example on$
import org.apache.spark.mllib.feature.{Word2Vec, Word2VecModel}
// $example off$

object Word2VecExample {

  def main(args: Array[String]): Unit = {


    val spark = SparkSession
      .builder()
      .appName(s"${this.getClass.getSimpleName}").master("local")
      .getOrCreate()
    val sc = spark.sparkContext

    // $example on$
    val input = sc.textFile("data/mllib/sample_lda_data.txt").map(line => line.split(" ").toSeq)

    val word2vec = new Word2Vec()

    val model = word2vec.fit(input)

    val synonyms = model.findSynonyms("1", 5)

    for((synonym, cosineSimilarity) <- synonyms) {
      println(s"$synonym $cosineSimilarity")
    }

    // Save and load model

    // TODO fix save model
//    model.save(sc, "data/mllib/myModelPath")
//    val sameModel = Word2VecModel.load(sc, "data/mllib/myModelPath")
    // $example off$

    sc.stop()
  }
}