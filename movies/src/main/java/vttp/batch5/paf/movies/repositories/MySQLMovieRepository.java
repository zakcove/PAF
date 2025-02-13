package vttp.batch5.paf.movies.repositories;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.ConnectionCallback;
import org.springframework.stereotype.Repository;
import vttp.batch5.paf.movies.models.Movie;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.sql.PreparedStatement;
import java.sql.SQLException;

@Repository
public class MySQLMovieRepository {

    @Autowired
    private JdbcTemplate jdbcTemplate;

    private static final String SQL_INSERT_MOVIE = """
        INSERT INTO imdb (imdb_id, vote_average, vote_count, release_date, revenue, budget, runtime) 
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """;

    private static final String SQL_INSERT_DIRECTOR = """
        INSERT INTO movie_directors (imdb_id, director_name) 
        VALUES (?, ?)
        """;

    private static final String SQL_GET_DIRECTOR_FINANCIALS = """
        SELECT m.* FROM imdb m 
        JOIN movie_directors md ON m.imdb_id = md.imdb_id 
        WHERE md.director_name = ?
        """;

    public void batchInsertMovies(List<Movie> movies) {
        jdbcTemplate.execute((ConnectionCallback<Object>) connection -> {
            try {
                connection.setAutoCommit(false);
                
                try (PreparedStatement movieStmt = connection.prepareStatement(SQL_INSERT_MOVIE);
                     PreparedStatement directorStmt = connection.prepareStatement(SQL_INSERT_DIRECTOR)) {
                    
                    for (Movie movie : movies) {
                        movieStmt.setString(1, movie.getImdbId());
                        movieStmt.setFloat(2, movie.getVoteAverage());
                        movieStmt.setInt(3, movie.getVoteCount());
                        movieStmt.setDate(4, new java.sql.Date(movie.getReleaseDate().getTime()));
                        movieStmt.setDouble(5, movie.getRevenue());
                        movieStmt.setDouble(6, movie.getBudget());
                        movieStmt.setInt(7, movie.getRuntime());
                        movieStmt.addBatch();
                    }
                    movieStmt.executeBatch();
                    
                    for (Movie movie : movies) {
                        for (String director : movie.getDirectors()) {
                            directorStmt.setString(1, movie.getImdbId());
                            directorStmt.setString(2, director.trim());
                            directorStmt.addBatch();
                        }
                    }
                    directorStmt.executeBatch();
                    
                    connection.commit();
                } catch (SQLException e) {
                    connection.rollback();
                    throw e;
                }
            } finally {
                connection.setAutoCommit(true);
            }
            return null;
        });
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

    public boolean hasData() {
        Integer count = jdbcTemplate.queryForObject("SELECT COUNT(*) FROM imdb", Integer.class);
        return count != null && count > 0;
    }
}
