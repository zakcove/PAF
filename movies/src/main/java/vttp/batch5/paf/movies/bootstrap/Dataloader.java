package vttp.batch5.paf.movies.bootstrap;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.StringReader;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import java.util.stream.Collectors;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.core.io.Resource;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.stereotype.Component;

import jakarta.json.Json;
import jakarta.json.JsonObject;
import jakarta.json.JsonReader;
import jakarta.json.JsonValue;
import vttp.batch5.paf.movies.repositories.MySQLMovieRepository;
import vttp.batch5.paf.movies.repositories.MongoMovieRepository;

@Component
public class Dataloader implements CommandLineRunner {

    @Autowired
    private MySQLMovieRepository mysqlRepo;

    @Autowired
    private MongoMovieRepository mongoRepo;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Autowired
    private MongoTemplate mongoTemplate;

    @Value("${data.movies.file}")
    private Resource moviesZip;

    private static final LocalDate CUT_OFF_DATE = LocalDate.of(2018, 1, 1);
    private static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd");

    @SuppressWarnings("null")
    @Override
    public void run(String... args) throws Exception {
        jdbcTemplate.execute("""
            CREATE TABLE IF NOT EXISTS imdb (
                imdb_id VARCHAR(16) PRIMARY KEY,
                vote_average FLOAT,
                vote_count INT,
                release_date DATE,
                revenue DECIMAL(15, 2),
                budget DECIMAL(15, 2),
                runtime INT
            )
        """);

        Long mysqlCount = jdbcTemplate.queryForObject(
            "SELECT COUNT(*) FROM imdb", Long.class);
    
        Long mongoCount = mongoTemplate.getCollection("imdb").countDocuments();

        if (mysqlCount == 0 || mongoCount == 0) {
            System.out.println("Loading movies data into databases...");
            loadMoviesData();
        } else {
            System.out.println("Movies data already loaded in databases");
        }
    }

    private void loadMoviesData() throws Exception {
        List<JsonObject> filteredMovies = new ArrayList<>();
        System.out.println("Reading movies from ZIP file...");

        try (InputStream is = moviesZip.getInputStream();
             ZipInputStream zis = new ZipInputStream(is)) {
            
            ZipEntry entry;
            while ((entry = zis.getNextEntry()) != null) {
                if (entry.getName().endsWith(".json")) {
                    BufferedReader reader = new BufferedReader(new InputStreamReader(zis));
                    String line;
                    while ((line = reader.readLine()) != null) {
                        try (JsonReader jsonReader = Json.createReader(new StringReader(line))) {
                            JsonObject movieJson = jsonReader.readObject();
                            JsonObject imputedMovie = imputeMissingValues(movieJson);
                    
                            if (isMovieAfter2018(imputedMovie)) {
                                filteredMovies.add(imputedMovie);
                            }
                        } catch (Exception e) {
                            System.err.println("Error reading JSON line: " + e.getMessage());
                            continue; 
                        }
                    }
                }
                zis.closeEntry();
            }
        }

        System.out.println("Found " + filteredMovies.size() + " movies after 2010");
        System.out.println("Inserting movies into databases...");

        int totalBatches = (filteredMovies.size() + 24) / 25;
        int currentBatch = 0;

        for (int i = 0; i < filteredMovies.size(); i += 25) {
            currentBatch++;
            int endIndex = Math.min(i + 25, filteredMovies.size());
            List<JsonObject> batch = filteredMovies.subList(i, endIndex);
            
            try {
                mysqlRepo.batchInsertMovies(batch);
                mongoRepo.batchInsertMovies(batch);
                System.out.printf("Processed batch %d/%d%n", currentBatch, totalBatches);
            } catch (Exception e) {
                List<String> failedIds = batch.stream()
                    .map(movie -> movie.getString("imdb_id", ""))
                    .collect(Collectors.toList());
                
                mongoRepo.logError(failedIds, e.getMessage());
                System.out.printf("Error in batch %d/%d: %s%n", currentBatch, totalBatches, e.getMessage());
                continue;
            }
        }

        System.out.println("Database loading completed successfully!");
    }

    private boolean isMovieAfter2018(JsonObject movie) {
        try {
            String releaseDateStr = movie.getString("release_date", "");
            if (releaseDateStr.isEmpty()) {
                return false;
            }
            LocalDate releaseDate = LocalDate.parse(releaseDateStr, DATE_FORMATTER);
            return !releaseDate.isBefore(CUT_OFF_DATE);
        } catch (DateTimeParseException e) {
            return false;
        }
    }

    private JsonObject imputeMissingValues(JsonObject original) {
        var builder = Json.createObjectBuilder();
        
        for (var entry : original.entrySet()) {
            String key = entry.getKey();
            JsonValue value = entry.getValue();
            
            if (value == JsonValue.NULL) {
           
                switch (key) {
                    case "vote_average":
                    case "vote_count":
                    case "revenue":
                    case "budget":
                    case "runtime":
                    case "popularity":
                    case "imdb_rating":
                    case "imdb_votes":
                        builder.add(key, 0);
                        break;
                    
                    case "title":
                    case "status":
                    case "release_date":
                    case "imdb_id":
                    case "original_language":
                    case "overview":
                    case "tagline":
                    case "genres":
                    case "spoken_languages":
                    case "casts":
                    case "director":
                    case "poster_path":
                        builder.add(key, "");
                        break;
                    
                    default:
                        builder.addNull(key);
                }
            } else {
                builder.add(key, value);
            }
        }
        
        return builder.build();
    }
}