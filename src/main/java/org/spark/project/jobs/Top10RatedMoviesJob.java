package org.spark.project.jobs;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;

import static org.apache.spark.sql.functions.bround;
import static org.apache.spark.sql.functions.col;

public class Top10RatedMoviesJob {
    public Dataset<Row> run(Dataset<Row> ratingsDataset, Dataset<Row> movieDataset, double averageNumberOfVotes) {

        System.out.println("\n Starting Top 10 Spark Job \n");

        Dataset<Row> top10Rated = ratingsDataset.filter(col("numVotes").geq(500))
                .withColumn("ranking", functions.expr("numVotes / " + averageNumberOfVotes + " * averageRating"))
                .withColumn("ranking", bround(col("ranking"), 2))
                .join(movieDataset, "tconst")
                .filter("titleType = 'movie'")
                .select("tconst", "primaryTitle", "ranking")
                .orderBy(functions.desc("ranking"))
                .limit(10);

        return top10Rated;
    }
}
