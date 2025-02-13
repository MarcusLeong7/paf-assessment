package vttp.batch5.paf.movies.repositories;


import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.support.rowset.SqlRowSet;
import org.springframework.stereotype.Repository;
import vttp.batch5.paf.movies.models.Movie;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;

import static vttp.batch5.paf.movies.repositories.Queries.*;

@Repository
public class MySQLMovieRepository {

    @Autowired
    private JdbcTemplate template;

    // TODO: Task 2.3
    // You can add any number of parameters and return any type from the method
    public boolean batchInsertMovies(List<Movie> movies) {
        List<Object[]> params = movies.stream()
                .map(movie -> new Object[]{movie.getImdbId(), movie.getVoteAverage(), movie.getVoteCount(),
                        movie.getReleaseDate(), movie.getRevenue(), movie.getBudget(), movie.getRuntime()})
                .collect(Collectors.toList());

        int added[] = template.batchUpdate(SQL_INSERT_MOVIE, params);
        return added.length == movies.size();

    }

    // TODO: Task 3
    public List<Movie> getAllMovies() {
        SqlRowSet rs = template.queryForRowSet(SQL_GET_ALL_MOVIES);
        List<Movie> movies = new LinkedList<>();
        int revenue = rs.getInt("revenue");
        int budget = rs.getInt("budget");
        int earnings = revenue - budget;
        while (rs.next()) {
            Movie movie = new Movie();
            movie.setImdbId(rs.getString("title"));
            movie.setRevenue(rs.getInt("revenue"));
            movie.setBudget(rs.getInt("budget"));
            movie.setEarnings(earnings);
            movies.add(movie);
        }
        return movies;
    }
    
}
