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

public class AverageNumberOfVotesJobTest {

    private SparkSession spark;

    @Before
    public void setUp() {
        spark = SparkSession.builder()
                .appName("AverageNumberOfVotesJobTest")
                .master("local[*]")  // Use local mode for testing
                .getOrCreate();
    }

    @After
    public void tearDown() {
        if (spark != null) {
            spark.stop();
        }
    }

    @Test
    public void testCalculateAverageNumberOfVotesCorrectly() {
        // Sample data
        List<Row> data = Arrays.asList(
                RowFactory.create("tt0000001", 5.7, 1000),
                RowFactory.create("tt0000002", 7.3, 1500),
                RowFactory.create("tt0000003", 8.0, 300),    // Should be filtered out
                RowFactory.create("tt0000004", 6.5, 700),
                RowFactory.create("tt0000005", 9.0, 1000)
        );

        // Create a DataFrame
        Dataset<Row> ratingsDataset = spark.createDataFrame(data, SchemaLoader.getRatingSchema());

        // Run the job
        AverageNumberOfVotesJob job = new AverageNumberOfVotesJob();
        double averageNumberOfVotes = job.run(ratingsDataset);

        // Assert expected average number of votes
        // Total votes = 1000 + 1500 + 700 + 1000 = 4200
        // Number of movies = 4
        double expectedAverage = 4200.0 / 4;
        assertEquals(expectedAverage, averageNumberOfVotes, 0.01);
    }
}
