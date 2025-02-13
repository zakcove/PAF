package vttp.batch5.paf.movies.repositories;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;
import vttp.batch5.paf.movies.models.Movie;
import java.util.List;

@Repository
public class MySQLMovieRepository {

    @Autowired
    private JdbcTemplate jdbcTemplate;

    private static final String SQL_INSERT_MOVIE = """
        INSERT INTO imdb (imdb_id, vote_average, vote_count, release_date, 
                         revenue, budget, runtime) 
        VALUES (?, ?, ?, ?, ?, ?, ?)
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
}
