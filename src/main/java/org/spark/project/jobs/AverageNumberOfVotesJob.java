package org.spark.project.jobs;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import static org.apache.spark.sql.functions.*;

public class AverageNumberOfVotesJob {

    public double run(Dataset<Row> ratingsDataset) {

        // Filter movies with fewer than 500 votes
        Dataset<Row> filteredDf = ratingsDataset.filter(col("numVotes").geq(500));

        // Calculate the total number of votes and the count of movies
        Dataset<Row> aggregatedDf = filteredDf.agg(
                sum("numVotes").alias("totalVotes"),
                count("numVotes").alias("movieCount")
        );

        // Collect the results and calculate the average number of votes
        Row result = aggregatedDf.collectAsList().get(0);
        long totalVotes = result.getLong(0);
        long movieCount = result.getLong(1);
        double averageNumberOfVotes = (double) totalVotes / movieCount;

        // Print the result to the console
        System.out.println("Average Number of Votes: " + averageNumberOfVotes);

        return averageNumberOfVotes;
    }
}