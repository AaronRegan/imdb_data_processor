package org.spark.project.jobs;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;

import static org.apache.spark.sql.functions.col;

public class Top10RatedMoviesJob {
    public Dataset<Row> run(Dataset<Row> ratingsDataset, Dataset<Row> movieDataset, double averageNumberOfVotes) {

        Dataset<Row> top10WithTitles = ratingsDataset.filter(col("numVotes").geq(500))
                .withColumn("ranking", functions.expr("numVotes / " + averageNumberOfVotes + " * averageRating"))
                .join(movieDataset, "tconst")
                .filter("titleType = 'movie'")
                .select("tconst", "primaryTitle", "ranking")
                .orderBy(functions.desc("ranking"))
                .limit(10);

        System.out.println(top10WithTitles.showString(10,0,false));

        return top10WithTitles;
    }
}
