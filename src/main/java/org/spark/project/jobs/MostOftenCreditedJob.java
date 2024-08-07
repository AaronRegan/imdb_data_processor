package org.spark.project.jobs;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import static org.apache.spark.sql.functions.*;

public class MostOftenCreditedJob {
    public Dataset<Row> run(Dataset<Row> top10Movies, Dataset<Row> creditsDataset) {

        System.out.println("\n Starting Most Credited Spark Job \n");

        // Split the 'knownForTitles' column into individual movie IDs

        return creditsDataset
                .withColumn("titleId", explode(split(col("knownForTitles"), ",")))
                .join(top10Movies, col("titleId").equalTo(top10Movies.col("tconst")))
                .groupBy("nconst", "primaryName")
                .agg(
                        count("*").as("count"),  // Count the number of movies per person
                        collect_list("primaryTitle").as("movieTitles")
                )
                .orderBy(col("count").desc())
                .limit(10);
    }
}
