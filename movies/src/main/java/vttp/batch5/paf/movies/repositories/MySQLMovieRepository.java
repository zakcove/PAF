package vttp.batch5.paf.movies.repositories;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;
import vttp.batch5.paf.movies.models.Movie;
import java.util.List;
import java.util.Map;
import java.util.HashMap;

@Repository
public class MySQLMovieRepository {

    @Autowired
    private JdbcTemplate jdbcTemplate;

    private static final String SQL_INSERT_MOVIE = """
        INSERT INTO imdb (imdb_id, vote_average, vote_count, release_date, 
                         revenue, budget, runtime) 
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """;

    private static final String SQL_GET_DIRECTOR_FINANCIALS = """
        SELECT m.* FROM imdb m 
        JOIN movie_directors md ON m.imdb_id = md.imdb_id 
        WHERE md.director_name = ?
        """;

    public void batchInsertMovies(List<Movie> movies) {
        List<Object[]> batchArgs = movies.stream()
            .map(movie -> new Object[] {
                movie.getImdbId(),
                movie.getVoteAverage(),
                movie.getVoteCount(),
                movie.getReleaseDate(),
                movie.getRevenue(),
                movie.getBudget(),
                movie.getRuntime()
            })
            .toList();

        jdbcTemplate.batchUpdate(SQL_INSERT_MOVIE, batchArgs);
    }

    public Map<String, Object> getDirectorFinancials(String directorName) {
        List<Map<String, Object>> results = jdbcTemplate.queryForList(SQL_GET_DIRECTOR_FINANCIALS, directorName);
        
        double totalRevenue = 0.0;
        double totalBudget = 0.0;
        
        for (Map<String, Object> row : results) {
            totalRevenue += ((Number) row.get("revenue")).doubleValue();
            totalBudget += ((Number) row.get("budget")).doubleValue();
        }

        Map<String, Object> financials = new HashMap<>();
        financials.put("total_revenue", totalRevenue);
        financials.put("total_budget", totalBudget);
        return financials;
    }
}
