package vttp.batch5.paf.movies.bootstrap;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;
import vttp.batch5.paf.movies.repositories.MongoMovieRepository;
import vttp.batch5.paf.movies.repositories.MySQLMovieRepository;
import vttp.batch5.paf.movies.models.Movie;

import java.io.FileInputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.ZipInputStream;
import jakarta.json.Json;
import jakarta.json.JsonReader;
import jakarta.json.JsonObject;
import java.util.Date;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Calendar;

@Component
public class Dataloader implements CommandLineRunner {

    @Value("${data.movies.file}")
    private String dataFile;

    @Autowired
    private MongoMovieRepository mongoRepo;

    @Autowired
    private MySQLMovieRepository mysqlRepo;

    private final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");

    @Override
    public void run(String... args) throws Exception {
        Calendar cal = Calendar.getInstance();
        cal.set(2018, Calendar.JANUARY, 1); 
        Date startDate = cal.getTime();

        try (ZipInputStream zis = new ZipInputStream(new FileInputStream(dataFile))) {
            var entry = zis.getNextEntry();
            List<Movie> batch = new ArrayList<>();
            
            while (entry != null) {
                if (entry.getName().endsWith(".json")) {
                    JsonReader reader = Json.createReader(zis);
                    JsonObject movieJson = reader.readObject();
                    
                    Date releaseDate = dateFormat.parse(movieJson.getString("release_date", "1970-01-01"));
                    if (releaseDate.after(startDate)) {
                        Movie movie = parseMovie(movieJson);
                        batch.add(movie);
                        
                        if (batch.size() == 25) {
                            processBatch(batch);
                            batch.clear();
                        }
                    }
                }
                entry = zis.getNextEntry();
            }
            
            if (!batch.isEmpty()) {
                processBatch(batch);
            }
        }
    }

    private void processBatch(List<Movie> batch) {
        try {
            mysqlRepo.batchInsertMovies(batch);
            mongoRepo.batchInsertMovies(batch);
        } catch (Exception e) {
            List<String> ids = batch.stream()
                .map(Movie::getImdbId)
                .toList();
            mongoRepo.logError(ids, e.getMessage());
        }
    }

    private Movie parseMovie(JsonObject json) {
        Movie movie = new Movie();
        movie.setImdbId(json.getString("imdb_id", ""));
        movie.setTitle(json.getString("title", ""));
        movie.setOverview(json.getString("overview", ""));
        movie.setTagline(json.getString("tagline", ""));
        
        String[] directors = json.getString("director", "").split(",");
        movie.setDirectors(Arrays.asList(directors));
        
        String[] genres = json.getString("genres", "").split(",");
        movie.setGenres(Arrays.asList(genres));
        
        movie.setImdbRating(Float.parseFloat(json.getString("imdb_rating", "0")));
        movie.setImdbVotes(Integer.parseInt(json.getString("imdb_votes", "0")));
        movie.setVoteAverage(Float.parseFloat(json.getString("vote_average", "0")));
        movie.setVoteCount(Integer.parseInt(json.getString("vote_count", "0")));
        movie.setRevenue(Double.parseDouble(json.getString("revenue", "0")));
        movie.setBudget(Double.parseDouble(json.getString("budget", "1000000")));
        movie.setRuntime(Integer.parseInt(json.getString("runtime", "90")));
        
        try {
            movie.setReleaseDate(dateFormat.parse(json.getString("release_date")));
        } catch (Exception e) {
            movie.setReleaseDate(new Date(0)); 
        }
        
        return movie;
    }
}
