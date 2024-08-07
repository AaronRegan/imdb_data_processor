package org.spark.project.jobs;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import static org.apache.spark.sql.functions.*;

public class MostOftenCreditedJob {
    public Dataset<Row> run(Dataset<Row> top10Movies, Dataset<Row> creditsDataset) {
        // Split the 'knownForTitles' column into individual movie IDs
        Dataset<Row> creditsExploded = creditsDataset
                .withColumn("titleId", explode(split(col("knownForTitles"), ",")));

        // Join the exploded credits with the top 10 movies dataset
        Dataset<Row> creditsInTop10 = creditsExploded
                .join(top10Movies, creditsExploded.col("titleId").equalTo(top10Movies.col("tconst")))
                .groupBy("nconst", "primaryName")
                .agg(
                        count("*").as("count"),
                        collect_list("primaryTitle").as("movieTitles")
                )
                .orderBy(col("count").desc())
                .limit(10);

        System.out.println(creditsInTop10.showString(10,0,false));

        return creditsInTop10;
    }
}
