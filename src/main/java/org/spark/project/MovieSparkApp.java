package org.spark.project;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;
import org.spark.project.config.SchemaLoader;
import org.spark.project.jobs.MostOftenCreditedJob;
import org.spark.project.jobs.Top10RatedMoviesJob;

import java.util.HashMap;
import java.util.Map;

public class MovieSparkApp {

    private static final String LOG_LEVEL = "logLevel";
    private static final String RATINGS_CSV_FILE = "/dataset/title.ratings.tsv";
    private static final String MOVIES_CSV_FILE = "/dataset/title.basics.tsv";
    private static final String CREDITS_CSV_FILE = "/dataset/name.basics.tsv";
    private static final String SPARK_APP_NAME = "MoviesSparkApp";

    public static void main(String... args)  {
        Map<String, Object> parameters = parseArguments(args);
        String sLogLevel = (String) parameters.get(LOG_LEVEL);
        new MovieSparkApp().run(sLogLevel);
    }

    private static Map<String, Object> parseArguments(String... parameters) {
        Map<String, Object> argsMap = new HashMap<>();
        // If no log level parameter is received, take WARN as default.
        argsMap.put(LOG_LEVEL, "WARN");

        // This iteration allows us to make parameters optional and position-agnostic.
        for (String sParameter : parameters) {
            if (sParameter.toUpperCase().contains("LOGLEVEL=")) {
                argsMap.put(LOG_LEVEL, sParameter.split("=")[1].toUpperCase());
            }
        }

        return argsMap;
    }

    /**
     * Run and display the specified Jobs.
     */
    public void run(String sLogLevel) {
        // Build spark session
        SparkSession spark = SparkSession
                .builder()
                .config("spark.master", "local")
                .appName(SPARK_APP_NAME)
                .getOrCreate();

        spark.sparkContext().setLogLevel(sLogLevel);

        Dataset<Row> ratingsDataset = readCsvIntoDataframe(
                spark, RATINGS_CSV_FILE, SchemaLoader.getRatingSchema()
        );

        Dataset<Row> moviesDataset = readCsvIntoDataframe(
                spark, MOVIES_CSV_FILE, SchemaLoader.getMovieSchema()
        );

        Dataset<Row> creditsDataset = readCsvIntoDataframe(
                spark, CREDITS_CSV_FILE, SchemaLoader.getCreditSchema()
        );

        System.out.println("\n Data Loading Complete, Starting Spark Jobs \n");

        Dataset<Row> top10Rated = new Top10RatedMoviesJob().run(ratingsDataset, moviesDataset);

        System.out.println(top10Rated.showString(10,0,false));

        Dataset<Row> result = new MostOftenCreditedJob().run(top10Rated, creditsDataset);

        System.out.println(result.showString(10,0,false));

        System.out.println("\n Spark Jobs Completed \n");
        spark.stop();
    }

    private static Dataset<Row> readCsvIntoDataframe(SparkSession s, String filename, StructType schema) {
        String fullPath = System.getProperty("user.dir").concat(filename);
        return s.read()
                .format("csv")
                .option("delimiter", "\t")
                .option("header", "true").schema(schema)
                .load(fullPath);
    }

}
