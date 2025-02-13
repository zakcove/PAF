package vttp.batch5.paf.movies.repositories;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

import org.bson.Document;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.stereotype.Repository;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Accumulators;
import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Projections;
import com.mongodb.client.model.Sorts;

import jakarta.json.JsonObject;

@Repository
public class MongoMovieRepository {

    @Autowired
    private MongoTemplate mongoTemplate;

    private static final int BATCH_SIZE = 25;

    public void batchInsertMovies(List<JsonObject> movies) {
        MongoCollection<Document> collection = mongoTemplate.getCollection("imdb");

        for (int i = 0; i < movies.size(); i += BATCH_SIZE) {
            int endIndex = Math.min(i + BATCH_SIZE, movies.size());
            List<JsonObject> batch = movies.subList(i, endIndex);

            List<Document> documents = new ArrayList<>();
            for (JsonObject movie : batch) {
                Document doc = new Document()
                        .append("imdb_id", movie.getString("imdb_id", ""))
                        .append("title", movie.getString("title", ""))
                        .append("directors", movie.getString("director", ""))
                        .append("overview", movie.getString("overview", ""))
                        .append("tagline", movie.getString("tagline", ""))
                        .append("genres", movie.getString("genres", ""))
                        .append("imdb_rating", getDoubleValue(movie, "imdb_rating"))
                        .append("imdb_votes", getIntValue(movie, "imdb_votes"));

                documents.add(doc);
            }

            collection.insertMany(documents);
        }
    }

    public void logError(List<String> imdbIds, String errorMessage) {
        Document errorDoc = new Document()
                .append("imdb_ids", imdbIds)
                .append("error", errorMessage)
                .append("timestamp", new Date());

        mongoTemplate.getCollection("errors").insertOne(errorDoc);
    }

    private double getDoubleValue(JsonObject json, String key) {
        try {
            return json.getJsonNumber(key).doubleValue();
        } catch (Exception e) {
            return 0.0;
        }
    }

    private int getIntValue(JsonObject json, String key) {
        try {
            return json.getJsonNumber(key).intValue();
        } catch (Exception e) {
            return 0;
        }
    }

    /* db.imdb.aggregate([
        { $match: { directors: { $ne: "" } } },
        { $project: { directors: { $split: ["$directors", ", "] } } },
        { $unwind: "$directors" },
        { $group: { _id: "$directors", movies_count: { $sum: 1 } } },
        { $sort: { movies_count: -1 } },
        { $limit: limit }
    ]) */
    public List<Document> getProlificDirectors(int limit) {
        return mongoTemplate.getCollection("imdb")
                .aggregate(Arrays.asList(
                        Aggregates.match(Filters.ne("directors", "")),
                        Aggregates.project(
                                Projections.fields(
                                        Projections.computed("directors",
                                                new Document("$split", Arrays.asList("$directors", ", "))))),
                        Aggregates.unwind("$directors"),
                        Aggregates.group("$directors",
                                Accumulators.sum("movies_count", 1)),
                        Aggregates.sort(Sorts.descending("movies_count")),
                        Aggregates.limit(limit)))
                .into(new ArrayList<>());
    }
}
