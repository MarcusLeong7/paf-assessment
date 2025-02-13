package vttp.batch5.paf.movies.repositories;

import com.mongodb.client.result.UpdateResult;
import jakarta.json.JsonObject;
import org.bson.Document;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.aggregation.*;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.stereotype.Repository;
import vttp.batch5.paf.movies.models.Movie;
import vttp.batch5.paf.movies.models.MovieMongo;

import javax.print.attribute.standard.DocumentName;
import java.util.List;

@Repository
public class MongoMovieRepository {

 @Autowired
 private MongoTemplate template;

 @Autowired
 private MySQLMovieRepository sqlRepo;

 // TODO: Task 2.3
 // You can add any number of parameters and return any type from the method
 // You can throw any checked exceptions from the method
 // Write the native Mongo query you implement in the method in the comments
 //
 //    native MongoDB query here
 // db.imdb.updateMany(
 //    { imdb_id:123 },  // Filter criteria
 //    {
 //        $set: {
 //            imdb_id: "",
 //            title: "",
 //            director: "",
 //            overview: "",
 //            tagline: "",
 //            genres: "" ,
 //            imdb_rating:"",
 //            imdb_votes:"",
 //        }
 //    },
 //    { upsert: true }
 //);
 public void batchInsertMovies(Document doc) {

  Query query = Query.query(Criteria.where("imdb_id").is(doc.getString("imdb_id")));

  Update update = new Update()
          .set("title", doc.getString("title"))
          .set("director", doc.getString("director"))
          .set("overview", doc.getString("overview"))
          .set("tagline", doc.getString("tagline"))
          .set("genres", doc.getString("genres"))
          .set("imdb_rating", doc.getInteger("imdb_rating"))
          .set("imdb_votes", doc.getInteger("imdb_votes"));

  UpdateResult updateResult = template.updateFirst(query, update, "imdb");

 }

 public boolean batchInsertMovies(List<Document> docs, String colName) {
  if (docs == null || docs.isEmpty()) {
   return false; // To avoid inserting empty data
  }
  template.save(docs, colName);
  return true;

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
 //db.imdb.aggregate([
 //    {
 //        $addFields: {
 //            directorsArray: { $split: ["$directors", ", "] } }
 //    },
 //    {
 //        $unwind: "$directorsArray"
 //    },
 //    {
 //        $group: {
 //            _id: "$directorsArray", // Group by director name
 //            movieCount: { $sum: 1 } // Count occurrences
 //        }
 //    },
 //    {
 //        $sort: { movieCount: -1 }
 //    },
 //    {
 //        $limit: 5
 //    }
 //]);
 public List<Document> getMovies(int limit) {

  ProjectionOperation projectFields = Aggregation.project()
          .andExpression("split(directors, ', ')").as("directorsArray");

  UnwindOperation unwindDirectors = Aggregation.unwind("directorsArray");

  GroupOperation groupFields = Aggregation.group("directorsArray")
          .count().as("movieCount");

  SortOperation sortCount = Aggregation.sort(Sort.by(Sort.Direction.DESC, "movieCount"));

  LimitOperation limitOperation = Aggregation.limit(limit);

  // Assemble a pipeline
  Aggregation pipeline = Aggregation.newAggregation(projectFields,unwindDirectors,
          groupFields,sortCount,limitOperation);

  //Execute the query
  return template.aggregate(pipeline,"imdb", Document.class).getMappedResults();


 }


}
