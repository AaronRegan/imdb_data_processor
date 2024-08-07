package org.spark.project.jobs;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.spark.project.config.SchemaLoader;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class MostOftenCreditedJobTest {


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
    public void testRun() {
        // Sample data for top 10 movies
        List<Row> top10Data = Arrays.asList(
                RowFactory.create("tt0000001", "title1", 50),
                RowFactory.create("tt0000002", "title2", 60),
                RowFactory.create("tt0000003", "title3", 70),
                RowFactory.create("tt0000004", "title4", 80)
        );

        StructType top10Schema = new StructType()
                .add("tconst", DataTypes.StringType, false)
                .add("primaryTitle", DataTypes.StringType, false)
                .add("rankingScore", DataTypes.DoubleType, false);

        // Create DataFrame for top 10 movies
        Dataset<Row> top10Movies = spark.createDataFrame(top10Data, top10Schema);

        // Sample data for credits
        List<Row> creditsData = Arrays.asList(
                RowFactory.create("nm0000001", "Fred Astaire", 1899, 1987, "actor,miscellaneous,producer", "tt0000001,tt0000002"),
                RowFactory.create("nm0000002", "Gene Kelly", 1912, 1996, "actor,miscellaneous,producer", "tt0000004,tt0000003")
        );

        // Create DataFrame for credits
        Dataset<Row> creditsDataset = spark.createDataFrame(creditsData, SchemaLoader.getCreditSchema());

        // Run the job
        MostOftenCreditedJob job = new MostOftenCreditedJob();
        Dataset<Row> result = job.run(top10Movies, creditsDataset);

        // Convert result to a list for assertions
        List<Row> resultList = result.collectAsList();

        // Validate the result
        assertEquals(2, resultList.size()); // Expecting 2 results based on sample data

        // Verify the top credited people
        assertEquals("Fred Astaire", resultList.get(0).getString(1)); // Most credited person
        assertEquals("Gene Kelly", resultList.get(1).getString(1));
    }
}
