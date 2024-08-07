package org.spark.project.jobs;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import static org.apache.spark.sql.functions.*;

public class MostOftenCreditedJob {

    private static final String KNOWN_FOR_TITLES = "knownForTitles";
    private static final String TITLE_ID = "titleId";
    private static final String TCONST = "tconst";
    private static final String NCONST = "nconst";
    private static final String PRIMARY_NAME = "primaryName";
    private static final String PRIMARY_TITLE = "primaryTitle";
    private static final String COUNT = "count";
    private static final String MOVIE_TITLES = "movieTitles";

    public Dataset<Row> run(Dataset<Row> top10Movies, Dataset<Row> creditsDataset) {

        System.out.println("\n Starting Most Credited Spark Job \n");

        // Split the 'knownForTitles' column into individual movie IDs
        Dataset<Row> creditsExploded = creditsDataset
                .withColumn(TITLE_ID, explode(split(col(KNOWN_FOR_TITLES), ",")));

        Dataset<Row> creditsTop10 = creditsExploded
                .join(top10Movies, col(TITLE_ID).equalTo(top10Movies.col(TCONST)))
                .groupBy(NCONST, PRIMARY_NAME)
                .agg(
                        count("*").as(COUNT),  // Count the number of movies per person
                        collect_list(PRIMARY_TITLE).as(MOVIE_TITLES)
                )
                .orderBy(col(COUNT).desc())
                .limit(10);

        return creditsTop10;
    }
}
