package org.spark.project.jobs;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import static org.apache.spark.sql.functions.col;

public class Top10RatedMoviesJob {
    public Dataset<Row> run(Dataset<Row> ratingsDataset, Dataset<Row> movieDataset, double averageNumberOfVotes) {

        // Filter and calculate the ranking score
        Dataset<Row> rankedMovies = ratingsDataset
                .filter(col("numVotes").geq(500))
                .withColumn("rankingScore", col("numVotes")
                        .divide(averageNumberOfVotes)
                        .multiply(col("averageRating")));

        // Get the top 10 movies based on ranking score
        Dataset<Row> top10Movies = rankedMovies.orderBy(col("rankingScore").desc())
                .limit(10);

        Dataset<Row> top10WithTitles = top10Movies
                .join(movieDataset, top10Movies.col("tconst").equalTo(movieDataset.col("tconst")))
                .select(
                        top10Movies.col("tconst"),
                        movieDataset.col("primaryTitle"),
                        top10Movies.col("rankingScore")
                )
                .orderBy(col("rankingScore").desc());

        System.out.println(top10WithTitles.showString(10,0,false));

        return top10WithTitles;
    }
}
