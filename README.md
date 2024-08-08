# IMDB Spark Application

## Running solution locally

In order to run the solution locally, you first need to make sure to meet the following requirements:

- Java 11
- [**Maven 3.x**](https://maven.apache.org/download.cgi)
- [**Spark 3.5.x**](https://spark.apache.org/downloads.html)

Run `setup.sh` to download the IMDB [**Datasets**](https://datasets.imdbws.com/) required for the project

The datasets consist of three tsv files:

- `name.basics.tsv`: 13M rows containing actor id, actors names and movie titles
- `title.basics.tsv`: 10M rows containing movie identifier and primary titles
- `title.ratings.tsv`: 1M rows containing movie identifier, average ratings and number of votes

## Running the jobs

From the root folder execute:

### Java

**Compile**

`mvn package`

**Run**

`spark-submit --class org.spark.project.MovieSparkApp ./target/MovieSparkApp-1.0-SNAPSHOT.jar [logLevel=< INFO|ERROR|WARN|FINE >]`

**Jobs**

The Spark Application runs 3 Jobs;

* Calculate Average Number of Votes cast for movie ratings
* Calculate the Top 10 Rated movies according to the following formula
  * (numVotes/averageNumberOfVotes) * averageRating
* Calculate the top 10 credited people that feature in the top 10 rated titles
