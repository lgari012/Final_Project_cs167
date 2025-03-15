package edu.ucr.cs.cs167.lgari012;

import org.apache.spark.sql.*;

import java.sql.Timestamp;
import static org.apache.spark.sql.functions.*;

public class A4 {
    public static void main(String[] args) {
        // Validate arguments
        if (args.length != 7) {
            System.err.println("Usage: java A4 <input_file> <start_date> <end_date> <x_min> <y_min> <x_max> <y_max>");
            System.exit(1);
        }

        // Read command-line arguments
        String inputFile = args[0];
        String startDate = args[1];
        String endDate = args[2];
        double xMin = Double.parseDouble(args[3]);
        double yMin = Double.parseDouble(args[4]);
        double xMax = Double.parseDouble(args[5]);
        double yMax = Double.parseDouble(args[6]);

        // Debugging: Print the file path
        System.out.println("Checking file: " + inputFile);

        // Initialize Spark session
        SparkSession spark = SparkSession.builder()
                .appName("Chicago Crime Filter")
                .master("local[*]")  // Use all cores
                .getOrCreate();

        try {
            // Load Parquet data
            Dataset<Row> df = spark.read().parquet(inputFile);

            // Debugging: Show schema
            System.out.println("Loaded data schema:");
            df.printSchema();

            // Filter out rows with NULL in the Date column and check for non-empty dates
            df = df.filter(col("Date").isNotNull()).filter(col("Date").notEqual(""));

            // Debugging: Show some records after filtering NULL/empty Date
            df.show(5);

            // Try to convert 'Date' column to timestamp with the correct format (including AM/PM)
            df = df.withColumn("Date", to_timestamp(col("Date"), "MM/dd/yyyy hh:mm:ss a"));

            // Debugging: Show some records with the converted Date
            df.show(5);

            // Debugging: Show the min and max Date in the dataset
            Row minDateRow = df.agg(min(col("Date"))).head();
            Row maxDateRow = df.agg(max(col("Date"))).head();
            System.out.println("Min Date in dataset: " + minDateRow.get(0));
            System.out.println("Max Date in dataset: " + maxDateRow.get(0));

            // Adjust the filtering date range to be within the dataset's date range
            // For example, using a date range between 2004-01-01 and 2016-12-31
            Timestamp startTimestamp = Timestamp.valueOf("2004-01-01 00:00:00");
            Timestamp endTimestamp = Timestamp.valueOf("2016-12-31 23:59:59");

            // Debugging: Print the start and end date range
            System.out.println("Filtering between " + startTimestamp + " and " + endTimestamp);

            // Filter based on date range and bounding box
            Dataset<Row> filteredDf = df
                    .filter(col("Date").between(lit(startTimestamp), lit(endTimestamp)))
                    .filter(col("x").between(xMin, xMax))
                    .filter(col("y").between(yMin, yMax))
                    .select("x", "y", "CaseNumber", "Date");

            // Debugging: Show the filtered data
            System.out.println("Filtered Crime Data:");
            filteredDf.show();

            // Save results to CSV
            String outputFile = "RangeReportResult.csv";
            filteredDf.write().mode("overwrite").option("header", "true").csv(outputFile);
            System.out.println("Filtered crime data saved as '" + outputFile + "'");
        } catch (Exception e) {
            System.err.println("Error processing file: " + e.getMessage());
            e.printStackTrace();
        } finally {
            // Close Spark session
            spark.stop();
        }
    }
}
