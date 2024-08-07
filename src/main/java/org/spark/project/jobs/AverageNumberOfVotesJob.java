package org.spark.project.jobs;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;

import static org.apache.spark.sql.functions.*;

public class AverageNumberOfVotesJob {

    public double run(Dataset<Row> ratingsDataset) {

        // Filter movies with fewer than 500 votes
        double averageNumberOfVotes = ratingsDataset
                .filter(col("numVotes").geq(500))
                .agg(functions.avg("numVotes")).first().getDouble(0);

        // Print the result to the console
        System.out.println("Average Number of Votes: " + averageNumberOfVotes);

        return averageNumberOfVotes;
    }
}