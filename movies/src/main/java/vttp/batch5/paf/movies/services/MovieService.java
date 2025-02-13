package vttp.batch5.paf.movies.services;

import jakarta.json.Json;
import jakarta.json.JsonArray;
import jakarta.json.JsonArrayBuilder;
import org.bson.Document;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.MongoTransactionManager;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import vttp.batch5.paf.movies.models.Movie;
import vttp.batch5.paf.movies.repositories.MongoMovieRepository;
import vttp.batch5.paf.movies.repositories.MySQLMovieRepository;

import java.util.List;

@Service
public class MovieService {

    @Autowired
    private MySQLMovieRepository sqlRepo;

    @Autowired
    private MongoMovieRepository mongoRepo;

    @Autowired
    private MongoTransactionManager transactionManager;

    // TODO: Task 2
    @Transactional
    public void createMovie(List<Movie> movie, List<Document> docs) {

      try{ while(movie.size() <= 25){
           sqlRepo.batchInsertMovies(movie);
       }
         mongoRepo.batchInsertMovies(docs,"imdb");
            System.out.println("Movie inserted successfully!");
        } catch (Exception e) {
            throw new RuntimeException("Error inserting movie: " + e.getMessage());
        }
    }


    // TODO: Task 3
    // You may change the signature of this method by passing any number of parameters
    // and returning any type
    public JsonArray getProlificDirectors(int limit) {

        List<Document> result = mongoRepo.getMovies(limit);

        JsonArrayBuilder gamesArray = Json.createArrayBuilder();
        for (Document movies : result) {
            gamesArray.add(Json.createObjectBuilder()
                    .add("director_name", movies.getInteger("directors"))
                    .add("movies_count", movies.getString("movieCount"))
                    .add("revenue", movies.getInteger("revenue"))
                    .add("budget", movies.getString("budget"))
                    .add("earnings", "profit") // or loss
                    .build());
        }
        JsonArray movies = gamesArray.build();
        return movies;
    }


    // TODO: Task 4
    // You may change the signature of this method by passing any number of parameters
    // and returning any type
    public void generatePDFReport() {

    }

}
