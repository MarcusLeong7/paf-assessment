package vttp.batch5.paf.movies.repositories;


import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;
import vttp.batch5.paf.movies.models.Movie;

import java.util.List;
import java.util.stream.Collectors;

import static vttp.batch5.paf.movies.repositories.Queries.*;

@Repository
public class MySQLMovieRepository {

    @Autowired
    private JdbcTemplate template;

    // TODO: Task 2.3
    // You can add any number of parameters and return any type from the method
    public int[] batchInsertMovies(List<Movie> movies) {
        List<Object[]> params = movies.stream()
                .map(movie -> new Object[]{movie.getImdbId(), movie.getVoteAverage(), movie.getVoteCount(),
                        movie.getReleaseDate(), movie.getRevenue(), movie.getBudget(), movie.getRuntime()})
                .collect(Collectors.toList());

        return template.batchUpdate(SQL_INSERT_MOVIE, params);
    }

    // TODO: Task 3


}
