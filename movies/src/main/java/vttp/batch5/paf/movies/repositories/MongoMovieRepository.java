package vttp.batch5.paf.movies.repositories;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.stereotype.Repository;
import vttp.batch5.paf.movies.models.Movie;
import java.util.List;
import java.util.Date;
import org.bson.Document;
import org.springframework.data.mongodb.core.aggregation.*;
import static org.springframework.data.mongodb.core.aggregation.Aggregation.*;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.domain.Sort;

@Repository
public class MongoMovieRepository {

    @Autowired
    private MongoTemplate mongoTemplate; 
    /*
     * db.imdb.insertMany([
     *   {
     *     imdb_id: "...", title: "...", directors: [...],
     *     overview: "...", tagline: "...", genres: [...],
     *     imdb_rating: 0.0, imdb_votes: 0
     *   },
     * ])
     */
    public void batchInsertMovies(List<Movie> movies) {
        List<Document> documents = movies.stream()
            .map(movie -> new Document()
                .append("imdb_id", movie.getImdbId())
                .append("title", movie.getTitle())
                .append("directors", movie.getDirectors())
                .append("overview", movie.getOverview())
                .append("tagline", movie.getTagline())
                .append("genres", movie.getGenres())
                .append("imdb_rating", movie.getImdbRating())
                .append("imdb_votes", movie.getImdbVotes()))
            .toList();
        
        mongoTemplate.getCollection("imdb").insertMany(documents);
    }

    /*
     * db.errors.insertOne({
     *   ids: [...],
     *   error: "error message",
     *   timestamp: new Date()
     * })
     */
    public void logError(List<String> ids, String errorMsg) {
        Document errorDoc = new Document()
            .append("ids", ids)
            .append("error", errorMsg)
            .append("timestamp", new Date());
        
        mongoTemplate.getCollection("errors").insertOne(errorDoc);
    }

    /*
     * db.imdb.aggregate([
     *   { $unwind: "$directors" },
     *   { $group: {
     *       _id: "$directors",
     *       movies_count: { $sum: 1 }
     *   } },
     *   { $match: { _id: { $ne: "" } } },
     *   { $sort: { movies_count: -1 } },
     *   { $limit: n }
     * ])
     */
    public List<Document> getTopDirectors(int limit) {
        AggregationOperation unwindDirectors = unwind("directors");
        AggregationOperation groupByDirector = group("directors")
            .count().as("movies_count");
        AggregationOperation matchNonEmpty = match(Criteria.where("_id").ne(""));
        AggregationOperation sortByCount = sort(Sort.Direction.DESC, "movies_count");
        AggregationOperation limitResults = limit(limit);

        Aggregation aggregation = newAggregation(
            unwindDirectors,
            groupByDirector,
            matchNonEmpty,
            sortByCount,
            limitResults
        );

        return mongoTemplate.aggregate(aggregation, "imdb", Document.class)
            .getMappedResults();
    }
}
