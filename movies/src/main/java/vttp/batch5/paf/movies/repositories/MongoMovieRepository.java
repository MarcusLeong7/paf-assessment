package vttp.batch5.paf.movies.repositories;

import org.bson.Document;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public class MongoMovieRepository {

 @Autowired
 private MongoTemplate template;

 // TODO: Task 2.3
 // You can add any number of parameters and return any type from the method
 // You can throw any checked exceptions from the method
 // Write the native Mongo query you implement in the method in the comments
 //
 //    native MongoDB query here
 // db.movies.insertMany( [
 //      { imdb_id:"",title:"",director:"",overview:"",
 //            tagline:"",genres:"",imdb_rating:10,imdb_votes:10000 },
 //      { imdb_id:"",title:"",director:"",overview:"",
 //            tagline:"",genres:"",imdb_rating:10,imdb_votes:10000 },
 //      { imdb_id:"",title:"",director:"",overview:"",
 //            tagline:"",genres:"",imdb_rating:10,imdb_votes:10000 } ])
 public void batchInsertMovies(List<Document> docs, String colName) {
  if (docs == null || docs.isEmpty()) {
   return; // To avoid inserting empty data
  }
  template.insert(docs, colName);
 }

 // TODO: Task 2.4
 // You can add any number of parameters and return any type from the method
 // You can throw any checked exceptions from the method
 // Write the native Mongo query you implement in the method in the comments
 //
 //    native MongoDB query here
 //
 public void logError() {

 }

 // TODO: Task 3
 // Write the native Mongo query you implement in the method in the comments
 //
 //    native MongoDB query here
 //


}
