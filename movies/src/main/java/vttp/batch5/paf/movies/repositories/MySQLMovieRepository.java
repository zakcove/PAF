package vttp.batch5.paf.movies.repositories;

import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

import jakarta.json.JsonObject;

@Repository
public class MySQLMovieRepository {

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Autowired
    private NamedParameterJdbcTemplate namedParameterJdbcTemplate;

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

    public List<Map<String, Object>> getDirectorsFinancials(List<String> directorNames) {
    String sql = """
        SELECT d.director_name,
               SUM(m.revenue) as total_revenue,
               SUM(m.budget) as total_budget
        FROM (
            SELECT DISTINCT TRIM(SUBSTRING_INDEX(SUBSTRING_INDEX(t.directors, ',', n.n), ',', -1)) as director_name
            FROM imdb t
            CROSS JOIN (
                SELECT a.N + b.N * 10 + 1 n
                FROM (SELECT 0 AS N UNION SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4 UNION SELECT 5 UNION SELECT 6 UNION SELECT 7 UNION SELECT 8 UNION SELECT 9) a,
                     (SELECT 0 AS N UNION SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4 UNION SELECT 5 UNION SELECT 6 UNION SELECT 7 UNION SELECT 8 UNION SELECT 9) b
                ORDER BY n
            ) n
            WHERE n.n <= 1 + (LENGTH(t.directors) - LENGTH(REPLACE(t.directors, ',', '')))
        ) d
        JOIN imdb m ON FIND_IN_SET(d.director_name, m.directors) > 0
        WHERE d.director_name IN (:directors)
        GROUP BY d.director_name
    """;

    MapSqlParameterSource params = new MapSqlParameterSource();
    params.addValue("directors", directorNames);

    return namedParameterJdbcTemplate.queryForList(sql, params);
}
}


