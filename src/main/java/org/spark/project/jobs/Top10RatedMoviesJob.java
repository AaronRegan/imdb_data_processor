package org.spark.project.jobs;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;

import static org.apache.spark.sql.functions.bround;
import static org.apache.spark.sql.functions.col;

public class Top10RatedMoviesJob {

    private static final String NUM_VOTES = "numVotes";
    private static final String AVERAGE_RATING = "averageRating";
    private static final String RANKING = "ranking";
    private static final String TCONST = "tconst";
    private static final String PRIMARY_TITLE = "primaryTitle";
    private static final String TITLE_TYPE = "titleType";

    public Dataset<Row> run(Dataset<Row> ratingsDataset, Dataset<Row> movieDataset) {

        System.out.println("\n Starting Top 10 Spark Job \n");

        Dataset<Row> filteredMovies = ratingsDataset
                .filter(col("numVotes").geq(500));

        double averageNumberOfVotes = this.getAverageNumberOfVotes(filteredMovies);

        Dataset<Row> top10Rated = filteredMovies
                .withColumn(RANKING, functions.expr(NUM_VOTES + " / " + averageNumberOfVotes + " * " + AVERAGE_RATING))
                .withColumn(RANKING, bround(col(RANKING), 2))
                .join(movieDataset, TCONST)
                .filter(col(TITLE_TYPE).equalTo("movie"))
                .select(TCONST, PRIMARY_TITLE, RANKING)
                .orderBy(functions.desc(RANKING))
                .limit(10);

        return top10Rated;
    }

    protected double getAverageNumberOfVotes(Dataset<Row> filteredMovies) {
        return filteredMovies.agg(functions.avg("numVotes")).first().getDouble(0);
    }
}
