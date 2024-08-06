package org.spark.project;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;
import org.spark.project.config.SchemaLoader;
import org.spark.project.jobs.AverageNumberOfVotesJob;
import org.spark.project.jobs.MostOftenCreditedJob;
import org.spark.project.jobs.Top10RatedMoviesJob;

public class MovieSparkApp {

    private static final String RATINGS_CSV_FILE = "/Users/aaronregan/Files/Work/2024/tasks/toyota/title.ratings.tsv";
    private static final String MOVIES_CSV_FILE = "/Users/aaronregan/Files/Work/2024/tasks/toyota/title.basics.tsv";
    private static final String CREDITS_CSV_FILE = "/Users/aaronregan/Files/Work/2024/tasks/toyota/name.basics.tsv";
    private static final String SPARK_APP_NAME = "MoviesSparkApp";

    public static void main(String... args)  {
        new MovieSparkApp().run();
    }

    /**
     * Run and display the specified Jobs.
     */
    public void run() {
        // Build spark session
        SparkSession spark = SparkSession
                .builder()
                .config("spark.master", "local")
                .appName(SPARK_APP_NAME)
                .getOrCreate();

        Dataset<Row> ratingsDataset = readCsvIntoDataframe(
                spark, RATINGS_CSV_FILE, SchemaLoader.getRatingSchema()
        );

        Dataset<Row> moviesDataset = readCsvIntoDataframe(
                spark, MOVIES_CSV_FILE, SchemaLoader.getMovieSchema()
        );

        Dataset<Row> creditsDataset = readCsvIntoDataframe(
                spark, CREDITS_CSV_FILE, SchemaLoader.getCreditSchema()
        );

        double averageRating =  new AverageNumberOfVotesJob().run(ratingsDataset);

        Dataset<Row> top10Rated = new Top10RatedMoviesJob().run(ratingsDataset, moviesDataset, averageRating);

        Dataset<Row> result = new MostOftenCreditedJob().run(top10Rated, creditsDataset);

        System.out.println("Spark Jobs Completed");
        System.out.println("\\n");
        System.out.println(top10Rated.showString(10,0,false));
        System.out.println("\\n");
        System.out.println(result.showString(10,0,false));
    }

    private static Dataset<Row> readCsvIntoDataframe(SparkSession s, String filename, StructType schema) {
        return s.read()
                .format("csv")
                .option("delimiter", "\t")
                .option("header", "true").schema(schema)
                .load(filename);
    }

}
