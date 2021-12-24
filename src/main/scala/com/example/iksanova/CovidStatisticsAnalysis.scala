package com.example.iksanova

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object CovidStatisticsAnalysisApp
  extends SparkSessionProviderComponent
  with CovidStatisticsAnalysis {
  override def sparkSessionProvider: SparkSessionProvider = new DefaultSparkSessionProvider("CovidAnalysis")

  def main(args: Array[String]): Unit = {
    val path = args.head

    compute(path)
  }
}

trait CovidStatisticsAnalysis {
  this: SparkSessionProviderComponent =>

  private val sparkSession = sparkSessionProvider.sparkSession

  def compute(path: String): Unit = {

    val df: DataFrame =
      sparkSession.read
      .option("header", "true")
      .csv(path)

    val cast_df = df.select(df.columns.map {
      case column@"new_cases" =>
        col(column).cast("double").as(column)
      case column@"people_fully_vaccinated_per_hundred" =>
        col(column).cast("double").as(column)
      case column =>
        col(column)
    }: _*)
      .cache()

    val windowSpec  = Window.partitionBy("location")

    val dfMaxCases = cast_df
      .withColumn("max_cases", max(col("new_cases")).over(windowSpec))
      .select("location", "max_cases", "date")
      .filter("new_cases = max_cases")

    dfMaxCases.show(100)

    val dfVaccinated = cast_df
      .groupBy("location")
      .agg(max(col("people_fully_vaccinated_per_hundred")).as("fully_vaccinated_per_hundred"))
//      .withColumnRenamed("max(people_fully_vaccinated_per_hundred)", "fully_vaccinated_per_hundred")
      .sort(desc("fully_vaccinated_per_hundred"))

    val windowSpecTop  = Window
      .orderBy(desc("fully_vaccinated_per_hundred"))

    val dfTopVaccinated =  dfVaccinated
      .withColumn("position", rank().over(windowSpecTop))
      .select("position", "location", "fully_vaccinated_per_hundred")

    dfTopVaccinated.show(100)
  }
}

