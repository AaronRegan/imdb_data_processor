package org.spark.project.jobs;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;

import static org.apache.spark.sql.functions.*;

public class AverageNumberOfVotesJob {

    public double run(Dataset<Row> ratingsDataset) {

        System.out.println("\n Starting Average Number of Votes Job \n");

        double averageNumberOfVotes = ratingsDataset
                .filter(col("numVotes").geq(500))
                .agg(functions.avg("numVotes")).first().getDouble(0);

        System.out.println("\n Average Number of Votes: " + averageNumberOfVotes);

        return averageNumberOfVotes;
    }
}