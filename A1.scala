package edu.ucr.cs.cs167.yli971

import edu.ucr.cs.bdlab.beast.geolite.{Feature, IFeature}
import org.apache.spark.SparkConf
import org.apache.spark.beast.SparkSQLRegistration
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import scala.collection.Map

object A1 {
  def main(args: Array[String]): Unit = {
    // Initialize Spark context
    val conf = new SparkConf().setAppName("Beast Example")
    // Set Spark master to local if not already set
    if (!conf.contains("spark.master"))
      conf.setMaster("local[*]")

    val spark: SparkSession.Builder = SparkSession.builder().config(conf)
    val sparkSession: SparkSession = spark.getOrCreate()
    val sparkContext = sparkSession.sparkContext
    SparkSQLRegistration.registerUDT
    SparkSQLRegistration.registerUDF(sparkSession)

    // The first argument chooses which branch to run
    val operation: String = args(0)
    // The second argument is often the input CSV or input data
    val inputFile: String = args(1)

    try {
      import edu.ucr.cs.bdlab.beast._
      val t1 = System.nanoTime()
      var validOperation = true

      operation match {
        //-------------------------------------------------------------------
        // Existing operations in your code:
        //   "count-by-county", "convert", "count-by-keyword", "choropleth-map"
        //-------------------------------------------------------------------

        /**
         * New case: Prepare Chicago Crimes data with ZIP codes
         *   Usage Example:
         *     prepare-crimes-zip Crimes_-_One_Year_Prior_to_Present.csv Chicago_ZIPCodes.zip output.parquet
         */
        case "prepare-crimes-zip" =>
          // Read the third argument as the ZIP shapefile
          val zipShapefile: String = args(2)
          // Read the fourth argument as the output path
          val outputFile: String = args(3)

          // 1) Read the CSV into a DataFrame
          val crimesDF: DataFrame = sparkSession.read
            .option("header", "true")
            .option("inferSchema", "true")
            .csv(inputFile)

          // 2) Rename columns that have spaces to be Parquet-friendly
          val renamedDF: DataFrame = crimesDF
            .withColumnRenamed("Case Number", "Case_Number")
            .withColumnRenamed("Primary Type", "Primary_Type")
            .withColumnRenamed("Location Description", "Location_Description")
            .withColumnRenamed("Community Area", "Community_Area")
            .withColumnRenamed("X Coordinate", "X_Coordinate")
            .withColumnRenamed("Y Coordinate", "Y_Coordinate")
            .withColumnRenamed("Updated On", "Updated_On")
          // The rest (x, y, ID, Date, etc.) don't have spaces

          // 3) Create a geometry column using (x, y)
          //    If "x, y" are indeed the coordinates. Adjust if you prefer "X_Coordinate, Y_Coordinate".
          val crimesRDD: SpatialRDD = renamedDF
            .selectExpr("*", "ST_CreatePoint(x, y) AS geometry")
            .toSpatialRDD

          // 4) Load the ZIP code polygons from a zipped shapefile
          //    This is the same pattern used for counties in your example.
          val zipRDD: SpatialRDD = sparkContext.shapefile(zipShapefile)
          // e.g., "Chicago_ZIPCodes.zip" which internally has .shp, .dbf, .shx, etc.

          // 5) Perform a spatial join: Each crime point is matched to the ZIP polygon that contains it
          val joinedRDD: RDD[(IFeature, IFeature)] = crimesRDD.spatialJoin(zipRDD)

          // 6) Add a new ZIPCode column to each crime (the first element in the pair is the crime, the second is the ZIP polygon).
          //    Suppose the shapefile has an attribute "ZCTA5CE10" or "ZIP" that contains the ZIP code.
          //    If your shapefile uses a different attribute name, change accordingly.
          val appendedRDD: RDD[IFeature] = joinedRDD.map { case (crimeFeature, zipFeature) =>
            // The ZIP code attribute from the polygon
            val zipValue = zipFeature.getAs[String]("ZCTA5CE10")  // Adjust if your shapefile uses a different attribute
            // Append a new attribute "ZIPCode" to the crime
            Feature.append(crimeFeature, zipValue, "ZIPCode")
          }

          // 7) Convert back to DataFrame
          val joinedDF: DataFrame = appendedRDD.toDataFrame(sparkSession)

          // 8) Drop the geometry column or any others you do not need
          val finalDF = joinedDF.drop("geometry")

          // 9) Write the result in Parquet format
          finalDF.write.mode(SaveMode.Overwrite).parquet(outputFile)

          println(s"Saved crimes with ZIPCode to $outputFile")

        //-------------------------------------------------------------------
        // Keep or extend your other cases as needed
        //-------------------------------------------------------------------
        case _ => validOperation = false
      }

      val t2 = System.nanoTime()
      if (validOperation)
        println(s"Operation '$operation' on file '$inputFile' took ${(t2 - t1) * 1E-9} seconds")
      else
        Console.err.println(s"Invalid operation '$operation'")

    } finally {
      sparkSession.stop()
    }
  }
}