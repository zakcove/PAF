package vttp.batch5.paf.movies.bootstrap;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;
import vttp.batch5.paf.movies.repositories.MongoMovieRepository;
import vttp.batch5.paf.movies.repositories.MySQLMovieRepository;
import vttp.batch5.paf.movies.models.Movie;
import org.springframework.core.io.Resource;

import java.util.ArrayList;
import java.util.List;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipEntry;
import jakarta.json.Json;
import jakarta.json.JsonReader;
import jakarta.json.JsonObject;
import java.util.Date;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.io.StringReader;

@Component
public class Dataloader implements CommandLineRunner {

    @Value("${data.movies.file}")
    private Resource dataFile;

    @Autowired
    private MongoMovieRepository mongoRepo;

    @Autowired
    private MySQLMovieRepository mysqlRepo;

    private final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");

    @Override
    public void run(String... args) throws Exception {
    
        if (isDataLoaded()) {
            System.out.println("Data already loaded, skipping import");
            return;
        }

        Calendar cal = Calendar.getInstance();
        cal.set(2018, Calendar.JANUARY, 1);
        Date startDate = cal.getTime();

        try (ZipInputStream zis = new ZipInputStream(dataFile.getInputStream())) {
            ZipEntry entry;
            List<Movie> batch = new ArrayList<>();
            
            while ((entry = zis.getNextEntry()) != null) {
                if (!entry.getName().endsWith(".json")) {
                    continue;
                }

                StringBuilder jsonContent = new StringBuilder();
                byte[] buffer = new byte[1024];
                int len;
                while ((len = zis.read(buffer)) > 0) {
                    jsonContent.append(new String(buffer, 0, len));
                }

                try (JsonReader reader = Json.createReader(new StringReader(jsonContent.toString()))) {
                    JsonObject movieJson = reader.readObject();
                    try {
                        Date releaseDate = dateFormat.parse(movieJson.getString("release_date", "1970-01-01"));
                        if (releaseDate.after(startDate)) {
                            Movie movie = parseMovie(movieJson);
                            batch.add(movie);
                            
                            if (batch.size() == 25) {
                                processBatch(batch);
                                batch.clear();
                            }
                        }
                    } catch (Exception e) {
                        System.err.println("Error processing movie: " + e.getMessage());
                    }
                }
            }
            
            if (!batch.isEmpty()) {
                processBatch(batch);
            }
        }
    }

    private boolean isDataLoaded() {
        
        return mongoRepo.hasData() && mysqlRepo.hasData();
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
        
        List<String> directors = new ArrayList<>();
        if (json.containsKey("directors")) {
            String directorsStr = json.getString("directors", "[]");
            try {
                JsonReader reader = Json.createReader(new StringReader(directorsStr));
                directors = reader.readArray().stream()
                    .map(v -> v.toString().replaceAll("\"", "").trim())
                    .filter(d -> !d.isEmpty())
                    .toList();
            } catch (Exception e) {
                System.err.println("Error parsing directors: " + e.getMessage());
            }
        }
        movie.setDirectors(directors);
        
        List<String> genres = new ArrayList<>();
        if (json.containsKey("genres")) {
            String genresStr = json.getString("genres", "[]");
            try {
                JsonReader reader = Json.createReader(new StringReader(genresStr));
                genres = reader.readArray().stream()
                    .map(v -> v.toString().replaceAll("\"", "").trim())
                    .filter(g -> !g.isEmpty())
                    .toList();
            } catch (Exception e) {
                System.err.println("Error parsing genres: " + e.getMessage());
            }
        }
        movie.setGenres(genres);
        
        if (json.containsKey("imdb_rating")) {
            try {
                movie.setImdbRating((float) json.getJsonNumber("imdb_rating").doubleValue());
            } catch (Exception e) {
                
            }
        }
        
        if (json.containsKey("imdb_votes")) {
            try {
                movie.setImdbVotes(json.getInt("imdb_votes"));
            } catch (Exception e) {
                
            }
        }
        
        try {
            movie.setVoteAverage(json.containsKey("vote_average") ? 
                (float) json.getJsonNumber("vote_average").doubleValue() : 0f);
        } catch (Exception e) {
            movie.setVoteAverage(0f);
        }
        
        try {
            movie.setVoteCount(json.containsKey("vote_count") ? 
                json.getInt("vote_count") : 0);
        } catch (Exception e) {
            movie.setVoteCount(0);
        }
        
        try {
            movie.setRevenue(json.containsKey("revenue") ? 
                json.getJsonNumber("revenue").doubleValue() : 1000000.0);
        } catch (Exception e) {
            movie.setRevenue(1000000.0);
        }
        
        try {
            movie.setBudget(json.containsKey("budget") ? 
                json.getJsonNumber("budget").doubleValue() : 1000000.0);
        } catch (Exception e) {
            movie.setBudget(1000000.0);
        }
        
        try {
            movie.setRuntime(json.containsKey("runtime") ? 
                json.getInt("runtime") : 90);
        } catch (Exception e) {
            movie.setRuntime(90);
        }
        
        if (json.containsKey("release_date")) {
            try {
                movie.setReleaseDate(dateFormat.parse(json.getString("release_date")));
            } catch (Exception e) {
                
            }
        }
        
        return movie;
    }
}