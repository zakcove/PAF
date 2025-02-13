package vttp.batch5.paf.movies.repositories;

import java.util.List;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

import jakarta.json.JsonObject;

@Repository
public class MySQLMovieRepository {

    @Autowired
    private JdbcTemplate jdbcTemplate;

    private static final int BATCH_SIZE = 25;
    private static final String INSERT_MOVIE_SQL = """
        INSERT INTO imdb (imdb_id, vote_average, vote_count, release_date, 
                        revenue, budget, runtime) 
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """;

    @Transactional
    public void batchInsertMovies(List<JsonObject> movies) {
        for (int i = 0; i < movies.size(); i += BATCH_SIZE) {
            int endIndex = Math.min(i + BATCH_SIZE, movies.size());
            List<JsonObject> batch = movies.subList(i, endIndex);
            
            List<Object[]> batchParams = batch.stream()
                .map(movie -> new Object[] {
                    movie.getString("imdb_id", ""),
                    getDoubleValue(movie, "vote_average"),
                    getIntValue(movie, "vote_count"),
                    movie.getString("release_date", ""),
                    getLongValue(movie, "revenue"),
                    getLongValue(movie, "budget"),
                    getIntValue(movie, "runtime")
                })
                .toList();

            jdbcTemplate.batchUpdate(INSERT_MOVIE_SQL, batchParams);
        }
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

    private long getLongValue(JsonObject json, String key) {
        try {
            return json.getJsonNumber(key).longValue();
        } catch (Exception e) {
            return 0L;
        }
    }
}
