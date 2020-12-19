import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, Encoders}
import org.apache.spark.sql.{Dataset, Row}
import org.apache.spark.sql.types.{StructType}
import java.sql.Date

object Movies extends App {
  // "hdfs://localhost:9000/user/shreevari_sp/imdb-movies.csv"
  val source = args(0)

  implicit val assignmentParams = (args(2), args(3).toInt, args(4).toInt)
  val outputFormat = args(1)
  dataframes(source, outputFormat)
  datasets(source, outputFormat)
  rdds(source, outputFormat)

  case class MovieType(
      imdb_title_id: String,
      title: Option[String],
      original_title: Option[String],
      year: Option[Int],
      date_published: Option[Date],
      genre: Option[String],
      duration: Option[Int],
      country: Option[String],
      language: Option[String],
      director: Option[String],
      writer: Option[String],
      production_company: Option[String],
      actors: Option[String],
      description: Option[String],
      avg_vote: Option[Double],
      votes: Option[Int],
      budget: Option[String],
      usa_gross_income: Option[Long],
      worlwide_gross_income: Option[Long],
      metascore: Option[Int],
      reviews_from_users: Option[Int],
      reviews_from_critics: Option[Int]
  )

  def sparkSession = (sessionName: String) => {
    SparkSession
      .builder()
      .appName(sessionName)
      .getOrCreate()
  }

  def readDF(
      spark: SparkSession,
      format: String,
      options: Map[String, String],
      schema: Option[StructType] = None
  ): Dataset[Row] = {
    val readerWithOpts = spark.read
      .options(options)
      .format(format)
    schema match {
      case Some(v) => readerWithOpts.schema(v).load()
      case None    => readerWithOpts.option("inferSchema", "true").load()
    }
  }

  def readMoviesDF(spark: SparkSession, source: String) = {
    val movieSchema = Encoders.product[MovieType].schema

    val csvOpts = Map(("header", "true"), ("sep", "\t"), ("nullValue", ""))
    val jsonOpts = Map[String, String]()

    val fileExt = source.split('.').last

    val formatOpts = fileExt match {
      case "csv"  => csvOpts
      case "json" => jsonOpts
    }
    val opts = formatOpts + ("path" -> source)
    readDF(spark, fileExt, opts, Some(movieSchema))
  }

  def writeOutput[T](dataset: Dataset[T], location: String) =
    dataset.write
      .format(outputFormat)
      .mode(SaveMode.Overwrite)
      .save(location)

  def dataframes(
      source: String, outputFormat: String
  )(implicit assignmentParams: Tuple3[String, Int, Int]) {
    val assignmentPostfix = "dataframes"
    val spark = sparkSession("assignment-3 dataframes")

    val moviesDF = readMoviesDF(spark, source)

    val parsedMoviesDF = moviesDF
      .withColumn(
        "cleaned_budget",
        regexp_extract(col("budget"), "([0-9]+)", 1)
      )
      .drop("budget")
      .withColumnRenamed("cleaned_budget", "budget")

    val (lang, minYear, maxYear) = assignmentParams
    val first = parsedMoviesDF
      .select("title", "year")
      .where(col("language") === lang)
      .groupBy("year")
      .count()

    val second = parsedMoviesDF
      .select("title", "votes", "year")
      .groupBy("year")
      .max("votes")

    val third = parsedMoviesDF
      .filter(col("year") > minYear && col("year") < maxYear)
      .groupBy("director")
      .sum("votes")

    first.show()
    writeOutput(first, s"$source.$assignmentPostfix.first")
    second.show()
    writeOutput(second, s"$source.$assignmentPostfix.second")
    third.show()
    writeOutput(third, s"$source.$assignmentPostfix.third")
    spark.close()
  }

  def cleanBudget(budgetString: String) =
    "[0-9]+".r.findFirstIn(budgetString) match {
      case Some(value) => value.toLong
      case None        => 0
    }

  def datasets(
      source: String, outputFormat: String
  )(implicit assignmentParams: Tuple3[String, Int, Int]) {
    val assignmentPostfix = "datasets"
    val spark = sparkSession("assignment-3 datasets")
    val moviesDF = readMoviesDF(spark, source)

    import spark.implicits._
    val moviesDS = moviesDF.as[MovieType]

    // moviesDS.map(movie => cleanBudget(movie.budget.getOrElse("0"))).show()

    val (lang, minYear, maxYear) = assignmentParams
    val first = moviesDS
      .filter(movie => movie.language.getOrElse("") == lang)
      .groupByKey(movie => movie.year.getOrElse(0))
      .count()

    val second = moviesDS
      .groupByKey(movie => movie.year.getOrElse(0))
      .reduceGroups((movie1, movie2) =>
        if (movie1.votes.getOrElse(0) > movie2.votes.getOrElse(0)) movie1
        else movie2
      )
      .map((movie) =>
        (movie._1, movie._2.title.getOrElse(""), movie._2.votes.getOrElse(0))
      )

    val third = moviesDS
      .filter(movie =>
        movie.year.getOrElse(0) > minYear && movie.year.getOrElse(0) < maxYear
      )
      .groupByKey(movie => movie.director.getOrElse(""))
      .mapGroups((director, movies) =>
        (
          director,
          movies
            .map(movie => movie.votes.getOrElse(0))
            .fold(0)((v1, v2) => v1 + v2)
        )
      )

    first.show()
    writeOutput(first, s"$source.$assignmentPostfix.first")
    second.show()
    writeOutput(second, s"$source.$assignmentPostfix.second")
    third.show()
    writeOutput(third, s"$source.$assignmentPostfix.third")
 
    spark.close()
  }
  def rdds(
      source: String, outputFormat: String
  )(implicit assignmentParams: Tuple3[String, Int, Int]) {
    val assignmentPostfix = "rdds"
    val spark = sparkSession("assignment-3 rdds")
    val moviesDF = readMoviesDF(spark, source)

    import spark.implicits._
    val moviesDS = moviesDF.as[MovieType]

    // moviesDS.map(movie => cleanBudget(movie.budget.getOrElse("0"))).show()
    val moviesRDD = moviesDS.rdd

    val (lang, minYear, maxYear) = assignmentParams
    val first = moviesRDD
      .filter(movie => movie.language.getOrElse("") == lang)
      .groupBy(movie => movie.year.getOrElse(0))
      .map(moviesByYear => (moviesByYear._1, moviesByYear._2.count(_ => true)))

    val second = moviesRDD
      .groupBy(movie => movie.year.getOrElse(0))
      .map(moviesByYear =>
        (moviesByYear._1, moviesByYear._2.maxBy(movie => movie.votes))
      )

    val third = moviesRDD
      .filter(movie =>
        movie.year.getOrElse(0) > minYear && movie.year.getOrElse(0) < maxYear
      )
      .groupBy(movie => movie.director.getOrElse(""))
      .map(moviesByDirector =>
        (
          moviesByDirector._1,
          moviesByDirector._2
            .map(movie => movie.votes.getOrElse(0))
            .fold(0)((v1, v2) => v1 + v2)
        )
      )

    first.toDS.show()
    writeOutput(first.toDS, s"$source.$assignmentPostfix.first")
    second.toDS.show()
    writeOutput(second.toDS, s"$source.$assignmentPostfix.second")
    third.toDS.show()
    writeOutput(third.toDS, s"$source.$assignmentPostfix.third")

    spark.close()
  }
}
