package vttp.batch5.paf.movies.services;

import org.bson.Document;
import org.springframework.beans.factory.annotation.Autowired;
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

  // TODO: Task 2
  @Transactional
  public void createMovie(List<Movie> movie,List<Document> docs) {

    try{
      while (docs.size() <= 25){
        mongoRepo.batchInsertMovies(docs,"imdb");
      }
      sqlRepo.batchInsertMovies(movie);

      System.out.println("Movie inserted successfully!");
    } catch (Exception e) {
      throw new RuntimeException("Error inserting movie: " + e.getMessage());
    }
  }
  

  // TODO: Task 3
  // You may change the signature of this method by passing any number of parameters
  // and returning any type
  public void getProlificDirectors() {
  }


  // TODO: Task 4
  // You may change the signature of this method by passing any number of parameters
  // and returning any type
  public void generatePDFReport() {

  }

}
