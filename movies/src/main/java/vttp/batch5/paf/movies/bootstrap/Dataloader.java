package vttp.batch5.paf.movies.bootstrap;

import jakarta.json.*;
import org.bson.Document;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.stereotype.Component;
import vttp.batch5.paf.movies.repositories.MongoMovieRepository;

import java.io.*;
import java.util.List;
import java.util.logging.Logger;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

@Component
public class Dataloader implements CommandLineRunner {

    @Autowired
    private MongoMovieRepository mongoRepo;

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
        JsonArrayBuilder jsonArrayBuilder = Json.createArrayBuilder();

        logger.info("### Reading JSON file:");
        String line;
        System.out.println("Contents:");
        while ((line = br.readLine()) != null) {
            line = line.trim();
            if (line.startsWith("{") && line.endsWith("}")) {
                try (JsonReader jsonReader = Json.createReader(new StringReader(line))) {
                    JsonObject jsonObject = jsonReader.readObject();
                    jsonArrayBuilder.add(jsonObject);
                }
                /*System.out.println(line);*/
            }
            JsonArray arr = jsonArrayBuilder.build();
            if (!arr.isEmpty()) {
                List<Document> documentList = arr.stream()
                        .map(j -> Document.parse(j.toString()))
                        .toList();
                mongoRepo.batchInsertMovies(documentList, "imdb");

            }
        }
        br.close();
        System.out.println(">>> END OF FILE <<<");


    }
}

