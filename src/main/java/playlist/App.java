package playlist;

import java.net.http.HttpClient;
import java.util.logging.Logger;

import com.fasterxml.jackson.databind.ObjectMapper;

public class App {

    private static final Logger logger = Logger.getLogger(App.class.getName());
    private static final ObjectMapper mapper = new ObjectMapper();

    private static final String API_KEY = System.getenv("API_KEY");
    private static final String PLAYLIST_ID = System.getenv("PLAYLIST_ID");

    private static final String SCHEMA_REGISTRY_URL = System.getenv()
            .getOrDefault("SCHEMA_REGISTRY_URL", "http://localhost:8081");

    private static final String BOOTSTRAP_SERVERS = System.getenv()
            .getOrDefault("KAFKA_BOOTSTRAP_SERVERS", "localhost:9102,localhost:9095,localhost:9097");

    private static final String TOPIC = "playlist.alert";

    private static final HttpClient httpClient = HttpClient.newHttpClient();

}