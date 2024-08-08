package org.spark.project.jobs;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.spark.project.config.SchemaLoader;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class Top10RatedMoviesJobTest {

    private SparkSession spark;

    @Before
    public void setUp() {
        spark = SparkSession.builder()
                .appName("Top10RatedMoviesJobTest")
                .master("local[*]")
                .getOrCreate();
    }

    @After
    public void tearDown() {
        if (spark != null) {
            spark.stop();
        }
    }

    @Test
    public void testMoviesRankedCorrectly() {
        // Sample data
        List<Row> data = Arrays.asList(
                RowFactory.create("tt0000001", 5.7, 1000),
                RowFactory.create("tt0000002", 7.3, 1500),
                RowFactory.create("tt0000003", 8.0, 300),
                RowFactory.create("tt0000004", 6.5, 700),
                RowFactory.create("tt0000005", 9.0, 1000)
        );

        List<Row> movies = Arrays.asList(
                RowFactory.create("tt0000001", "movie", "The"),
                RowFactory.create("tt0000002", "movie", "hello"),
                RowFactory.create("tt0000003", "movie", "World"),
                RowFactory.create("tt0000004", "movie", "Test"),
                RowFactory.create("tt0000005", "movie", "Unit")

                );

        // Create a DataFrame
        Dataset<Row> ratingsDataset = spark.createDataFrame(data, SchemaLoader.getRatingSchema());
        Dataset<Row> moviesDataset = spark.createDataFrame(movies, SchemaLoader.getMovieSchema());

        // Run the job
        Top10RatedMoviesJob job = new Top10RatedMoviesJob();
        Dataset<Row> top10Movies = job.run(ratingsDataset, moviesDataset);

        // Assert expected number of rows
        assertEquals(4, top10Movies.count());

        // Convert to local collection for testing
        List<Row> result = top10Movies.collectAsList();

        // Validate results (tconst and rankingScore)
        assertEquals("tt0000002", result.get(0).getString(0));
        assertEquals("tt0000005", result.get(1).getString(0));
        assertEquals("tt0000001", result.get(2).getString(0));
        assertEquals("tt0000004", result.get(3).getString(0));

        // Validate rankingScore for the top result (tt0000005)
        assertEquals(10.43, result.get(0).getDouble(2), 0.01);
    }

    @Test
    public void testFiltersOutNonMoviesCorrectly() {
        // Sample data
        List<Row> data = Arrays.asList(
                RowFactory.create("tt0000001", 5.7, 1000),
                RowFactory.create("tt0000002", 7.3, 1500),
                RowFactory.create("tt0000003", 8.0, 300),
                RowFactory.create("tt0000004", 6.5, 700),
                RowFactory.create("tt0000005", 9.0, 1000)
        );

        List<Row> movies = Arrays.asList(
                RowFactory.create("tt0000001", "movie", "The"),
                RowFactory.create("tt0000002", "movie", "hello"),
                RowFactory.create("tt0000003", "movie", "World"),
                RowFactory.create("tt0000004", "movie", "Test"),
                RowFactory.create("tt0000005", "tv show", "Unit")

        );

        // Create a DataFrame
        Dataset<Row> ratingsDataset = spark.createDataFrame(data, SchemaLoader.getRatingSchema());
        Dataset<Row> moviesDataset = spark.createDataFrame(movies, SchemaLoader.getMovieSchema());

        // Run the job
        Top10RatedMoviesJob job = new Top10RatedMoviesJob();
        Dataset<Row> top10Movies = job.run(ratingsDataset, moviesDataset);

        // Assert expected number of rows
        assertEquals(3, top10Movies.count());

        // Convert to local collection for testing
        List<Row> result = top10Movies.collectAsList();

        // Validate results (tconst and rankingScore)
        assertEquals("tt0000002", result.get(0).getString(0));
        assertEquals("tt0000001", result.get(1).getString(0));
        assertEquals("tt0000004", result.get(2).getString(0));

        // Validate rankingScore for the top result (tt0000005)
        assertEquals(10.43, result.get(0).getDouble(2), 0.01);
    }

    @Test
    public void getAverageNumberOfVotes(){
        List<Row> data = Arrays.asList(
                RowFactory.create("tt0000001", 5.7, 1000),
                RowFactory.create("tt0000002", 7.3, 1500),
                RowFactory.create("tt0000004", 6.5, 700),
                RowFactory.create("tt0000005", 9.0, 1000)
        );

        // Create a DataFrame
        Dataset<Row> ratingsDataset = spark.createDataFrame(data, SchemaLoader.getRatingSchema());

        // Run the job
        Top10RatedMoviesJob job = new Top10RatedMoviesJob();

        double result = job.getAverageNumberOfVotes(ratingsDataset);

        double expectedAverage = 4200.0 / 4;
        assertEquals(expectedAverage, result, 0.01);
    }
}
