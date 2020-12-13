import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, Encoders}
import java.sql.Date

object Movies {
  def main(args: Array[String]) {
    dataframes()
    /datasets()
    rdds()
  }

  def dataframes() {
    val spark = SparkSession
      .builder()
      .appName("assignment-3 dataframes")
      .getOrCreate()

    val source = "hdfs://localhost:9000/user/shreevari_sp/imdb-movies.csv"
    val moviesDF = spark.read
      .option("header", "true")
      .option("sep", "\t")
      .option("nullValue", "")
      .option("inferSchema", "true")
      .csv(source)

    val parsedMoviesDF = moviesDF
      .withColumn(
        "cleaned_budget",
        regexp_extract(col("budget"), "([0-9]+)", 1)
      )
      .drop("budget")
      .withColumnRenamed("cleaned_budget", "budget")
      .withColumn("date", to_date(col("date_published"), "yyyy-MM-dd"))
      .drop("date_published")
      .withColumnRenamed("date", "date_published")

    val lang = "English"
    val first = parsedMoviesDF
      .select("title", "year")
      .where(col("language") === lang)
      .groupBy("year")
      .count()

    val second = parsedMoviesDF
      .select("title", "votes", "year")
      .groupBy("year")
      .max("votes")

    val minYear = 2000
    val maxYear = 2010
    val third = parsedMoviesDF
      .filter(col("year") > minYear && col("year") < maxYear)
      .groupBy("director")
      .sum("votes")

    first.show()
    second.show()
    third.show()

    spark.close()
  }

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

  def datasets() {
    val spark = SparkSession
      .builder()
      .appName("assignment-3 datasets")
      .getOrCreate()

    val source = "hdfs://localhost:9000/user/shreevari_sp/imdb-movies.csv"

    val movieSchema = Encoders.product[MovieType].schema

    val moviesDF = spark.read
      .option("header", "true")
      .option("sep", "\t")
      .option("nullValue", "")
      .schema(movieSchema)
      .csv(source)

    def cleanBudget(budgetString: String) =
      "[0-9]+".r.findFirstIn(budgetString) match {
        case Some(value) => value.toLong
        case None        => 0
      }
    import spark.implicits._
    val moviesDS = moviesDF.as[MovieType]

    moviesDS.map(movie => cleanBudget(movie.budget.getOrElse("0"))).show()

    val lang = "English"
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

    val minYear = 2000
    val maxYear = 2010
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
    second.show()
    third.show()

    spark.close()
  }
  def rdds() {
    val spark = SparkSession
      .builder()
      .appName("assignment-3 rdds")
      .getOrCreate()

    val source = "hdfs://localhost:9000/user/shreevari_sp/imdb-movies.csv"

    val movieSchema = Encoders.product[MovieType].schema

    val moviesDF = spark.read
      .option("header", "true")
      .option("sep", "\t")
      .option("nullValue", "")
      .schema(movieSchema)
      .csv(source)

    def cleanBudget(budgetString: String) =
      "[0-9]+".r.findFirstIn(budgetString) match {
        case Some(value) => value.toLong
        case None        => 0
      }
    import spark.implicits._
    val moviesDS = moviesDF.as[MovieType]

    // moviesDS.map(movie => cleanBudget(movie.budget.getOrElse("0"))).show()
    val moviesRDD = moviesDS.rdd

    val lang = "English"
    val first = moviesRDD
      .filter(movie => movie.language.getOrElse("") == lang)
      .groupBy(movie => movie.year.getOrElse(0))
      .map(moviesByYear => (moviesByYear._1, moviesByYear._2.count(_ => true)))

    val second = moviesRDD
      .groupBy(movie => movie.year.getOrElse(0))
      .map(moviesByYear =>
        (moviesByYear._1, moviesByYear._2.maxBy(movie => movie.votes))
      )

    val minYear = 2000
    val maxYear = 2010
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
    second.toDS.show()
    third.toDS.show()

    spark.close()
  }
}
