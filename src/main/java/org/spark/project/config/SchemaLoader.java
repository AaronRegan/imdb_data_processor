package org.spark.project.config;

import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

public class SchemaLoader {

    private static final StructType movieSchema = new StructType()
            .add("tconst", DataTypes.StringType, false)
            .add("titleType", DataTypes.StringType, false)
            .add("primaryTitle", DataTypes.StringType, false)
            .add("originalTitle", DataTypes.StringType, true)
            .add("isAdult", DataTypes.BooleanType, false)
            .add("startYear", DataTypes.IntegerType, true)
            .add("endYear", DataTypes.IntegerType, true)
            .add("runtimeMinutes", DataTypes.IntegerType, true)
            .add("genres", DataTypes.StringType, true);

    private static final StructType ratingSchema = new StructType()
            .add("tconst", DataTypes.StringType, false)
            .add("averageRating", DataTypes.DoubleType, false)
            .add("numVotes", DataTypes.IntegerType, false);

    private static final StructType creditsSchema = new StructType()
            .add("nconst", DataTypes.StringType, false)
            .add("primaryName", DataTypes.StringType, false)
            .add("birthYear", DataTypes.IntegerType, true)
            .add("deathYear", DataTypes.IntegerType, true)
            .add("primaryProfession", DataTypes.StringType, true)
            .add("knownForTitles", DataTypes.StringType, true);

    public static StructType getRatingSchema() {
        return ratingSchema;
    }

    public static StructType getMovieSchema() {
        return movieSchema;
    }

    public static StructType getCreditSchema() {
        return creditsSchema;
    }
}
