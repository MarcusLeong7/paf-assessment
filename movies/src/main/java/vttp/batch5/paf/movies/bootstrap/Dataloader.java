package vttp.batch5.paf.movies.bootstrap;

import jakarta.json.*;
import org.bson.Document;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.stereotype.Component;
import vttp.batch5.paf.movies.models.Movie;
import vttp.batch5.paf.movies.models.MovieMongo;
import vttp.batch5.paf.movies.repositories.MongoMovieRepository;
import vttp.batch5.paf.movies.repositories.MySQLMovieRepository;
import vttp.batch5.paf.movies.services.MovieService;

import java.io.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.logging.Logger;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

@Component
public class Dataloader implements CommandLineRunner {

    @Autowired
    private MovieService movieSvc;

    @Autowired
    private MongoMovieRepository mongoRepo;

    @Autowired
    private MySQLMovieRepository sqlRepo;

    private final Logger logger = Logger.getLogger(Dataloader.class.getName());

    //TODO: Task 2

    private static final String filePath =
            "C:\\Users\\marcu\\OneDrive\\Desktop\\PAF\\paf_b5_assessment\\data\\movies_post_2010.zip";

    @Override
    public void run(String... args) throws Exception {
        File zip = new File(filePath);

        if (!zip.exists()) {
            System.out.println("ZIP file not found: " + filePath);
            return;
        }

        System.out.println("Processing ZIP file:" + filePath);

        try (FileInputStream fis = new FileInputStream(zip);
             ZipInputStream zis = new ZipInputStream(fis)) {
            ZipEntry zipEntry;
            while ((zipEntry = zis.getNextEntry()) != null) {
                System.out.println("Found: " + zipEntry.getName() + (zipEntry.isDirectory() ? " [Directory]" : " [File]"));

                if (!zipEntry.isDirectory()) {
                    // Read file contents with helper method
                    readZip(zis);
                }
                zis.closeEntry();
            }
        }

    }

    private void readZip(ZipInputStream zis) throws IOException {
        InputStreamReader inputStream = new InputStreamReader(zis);
        BufferedReader br = new BufferedReader(inputStream);
        List<Document> filteredDocuments = new ArrayList<>();
        List<Movie> movieList = new ArrayList<>();
        JsonArrayBuilder jsonArrayBuilder = Json.createArrayBuilder();

        logger.info("### Reading JSON file:");
        String line;
        System.out.println("Contents:");
        while ((line = br.readLine()) != null) {
            line = line.trim();
            int count = 0;
            if (line.startsWith("{") && line.endsWith("}")) {
                try (JsonReader jsonReader = Json.createReader(new StringReader(line))) {
                    JsonObject jsonObject = jsonReader.readObject();

                    String imdbID = jsonObject.getString("imdb_id");
                    String title = jsonObject.getString("title");
                    String directors = jsonObject.getString("director");
                    String overview = jsonObject.getString("overview");
                    String tagline = jsonObject.getString("tagline");
                    String genres = jsonObject.getString("genres");
                    int imdbRating = jsonObject.containsKey("imdb_rating")
                                     && jsonObject.getValueType() == JsonValue.ValueType.NUMBER
                            ? jsonObject.getInt("imdb_rating") : 0;
                    int imdbVotes = jsonObject.containsKey("imdb_votes") && jsonObject.getValueType() == JsonValue.ValueType.NUMBER
                            ? jsonObject.getInt("imdb_votes") : 0;

                    JsonObject obj = Json.createObjectBuilder()
                            .add("imdb_id", imdbID)
                            .add("title", title)
                            .add("directors", directors)
                            .add("overview", overview)
                            .add("tagline", tagline)
                            .add("genres", genres)
                            .add("imdb_rating", imdbRating)
                            .add("imdb_votes", imdbVotes)
                            .build();

                    Document document = Document.parse(obj.toString());
                    filteredDocuments.add(document);
                    for (Document doc : filteredDocuments) {
                        mongoRepo.batchInsertMovies(doc);
                    }


                    /*System.out.println(filteredDocuments.size());*/

                    int voteAverage = jsonObject.containsKey("vote_average")
                                      && jsonObject.getValueType() == JsonValue.ValueType.NUMBER
                            ? jsonObject.getInt("vote_average") : 0;
                    int voteCount = jsonObject.containsKey("vote_count")
                                    && jsonObject.getValueType() == JsonValue.ValueType.NUMBER
                            ? jsonObject.getInt("vote_count") : 0;
                    int revenue = jsonObject.containsKey("revenue")
                                  && jsonObject.getValueType() == JsonValue.ValueType.NUMBER
                            ? jsonObject.getInt("revenue") : 0;
                    int budget = jsonObject.containsKey("budget")
                                  && jsonObject.getValueType() == JsonValue.ValueType.NUMBER
                            ? jsonObject.getInt("budget") : 0;
                    int runtime = jsonObject.containsKey("runtime")
                                 && jsonObject.getValueType() == JsonValue.ValueType.NUMBER
                            ? jsonObject.getInt("runtime") : 0;
                    String datestring = jsonObject.getString("release_date");
                    SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
                    Date releaseDate = formatter.parse(datestring);

                    Movie movie = new Movie();
                    movie.setImdbId(imdbID);
                    movie.setVoteAverage(voteAverage);
                    movie.setVoteCount(voteCount);
                    movie.setReleaseDate(releaseDate);
                    movie.setRevenue(revenue);
                    movie.setBudget(budget);
                    movie.setRuntime(runtime);
                    movieList.add(movie);

                } catch (ParseException e) {
                    throw new RuntimeException(e);
                }

                /*System.out.println(line);*/
            }
            if ( !movieList.isEmpty()) {
                sqlRepo.batchInsertMovies(movieList);
               /* movieSvc.createMovie(movieList,filteredDocuments);*/
            }


          /*  JsonArray arr = jsonArrayBuilder.build();
            if (!arr.isEmpty()) {
                List<Document> documentList = arr.stream()
                        .map(j -> Document.parse(j.toString()))
                        .toList();
                mongoRepo.batchInsertMovies(documentList, "imdb");
            }*/
        }
        br.close();
        System.out.println(">>> END OF FILE <<<");


    }
}

