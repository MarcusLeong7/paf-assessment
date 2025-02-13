package vttp.batch5.paf.movies.controllers;

import jakarta.json.JsonArray;
import org.bson.Document;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import vttp.batch5.paf.movies.repositories.MongoMovieRepository;
import vttp.batch5.paf.movies.repositories.MySQLMovieRepository;
import vttp.batch5.paf.movies.services.MovieService;

@Controller
@RequestMapping
public class MainController {


    @Autowired
    private MovieService movieSvc;

  // TODO: Task 3
    @GetMapping
    @ResponseBody
    public ResponseEntity<String> getProlificDirectors(int limit) {

        // Fetch details
        JsonArray resp = movieSvc.getProlificDirectors(limit);

        if (resp.isEmpty()) {
            return ResponseEntity.status(HttpStatus.NOT_FOUND)
                    .body("{\"error\": \"Game not found\"}");
        }

        return ResponseEntity.ok(resp.toString());
    }



  
  // TODO: Task 4


}
