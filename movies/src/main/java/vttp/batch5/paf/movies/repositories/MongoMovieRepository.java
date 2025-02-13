package vttp.batch5.paf.movies.repositories;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.stereotype.Repository;
import vttp.batch5.paf.movies.models.Movie;
import java.util.List;
import java.util.Date;
import org.bson.Document;

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
}
