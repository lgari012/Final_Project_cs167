package edu.ucr.cs.cs167.kdoan024

import org.apache.spark.SparkConf
import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.hadoop.fs.{FileSystem, Path}

object A3_TemporalAnalysis {
  def main(args: Array[String]): Unit = {
    // Validate input arguments (expecting startDate and endDate)
    if (args.length < 3) {
      println("Usage: A3_TemporalAnalysis <inputFile> <startDate> <endDate>")
      println("Example: A3_TemporalAnalysis Chicago_Crimes_10k.csv.bz2 01/01/2018 12/31/2018")
      System.exit(1)
    }

    // Extract input parameters
    val inputFile = args(0)
    val startDate = args(1)
    val endDate = args(2)

    // Initialize Spark session
    val conf = new SparkConf().setAppName("Chicago Crime Temporal Analysis")
    if (!conf.contains("spark.master")) conf.setMaster("local[*]")

    val spark = SparkSession.builder().config(conf).getOrCreate()

    try {
      println(s"Processing crimes from $startDate to $endDate using dataset: $inputFile")

      // Read crime data (CSV or Parquet)
      val crimesDF = readCrimeData(spark, inputFile)
      crimesDF.createOrReplaceTempView("crimes")

      // SQL query to filter crimes by date range and count occurrences by type
      val query =
        s"""
           |SELECT PrimaryType, COUNT(*) as crime_count
           |FROM crimes
           |WHERE to_timestamp(Date, 'MM/dd/yyyy hh:mm:ss a')
           |      BETWEEN to_date('$startDate', 'MM/dd/yyyy')
           |      AND to_date('$endDate', 'MM/dd/yyyy')
           |GROUP BY PrimaryType
           |ORDER BY crime_count DESC
        """.stripMargin

      val resultDF = spark.sql(query)

      // Show results in terminal
      resultDF.show(20, false)

      // Save the output as a CSV file
      val outputPath = "CrimeTypeCount.csv"
      resultDF.coalesce(1).write.option("header", "true").mode("overwrite").csv(outputPath)

      println(s"Results saved to $outputPath")

    } finally {
      // Stop Spark session
      spark.stop()
    }
  }


  // Reads the crime dataset, supporting both CSV and Parquet formats.
  def readCrimeData(spark: SparkSession, filePath: String): DataFrame = {
    if (filePath.endsWith(".parquet")) {
      println("Reading Parquet dataset...")
      spark.read.parquet(filePath)
    } else {
      println("Reading CSV dataset...")
      spark.read.option("header", "true").option("inferSchema", "true").csv(filePath)
    }
  }
}
