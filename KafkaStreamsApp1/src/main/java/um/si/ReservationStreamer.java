package um.si;

import io.confluent.kafka.streams.serdes.avro.GenericAvroSerde;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import um.si.movie.Movie;

import java.util.*;
import java.util.stream.Collectors;

public class ReservationStreamer {

    private static Properties setupApp() {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "movie-title-sort-app");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        return props;
    }

    public static void main(String[] args) throws Exception {
        final Map<String, String> serdeConfig = Collections.singletonMap("schema.registry.url", "http://0.0.0.0:8081");

        final Serde<GenericRecord> valueGenericAvroSerde = new GenericAvroSerde();
        valueGenericAvroSerde.configure(serdeConfig, false);

        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, GenericRecord> inputStream = builder.stream("movie_topic_3", Consumed.with(Serdes.String(), valueGenericAvroSerde));
//        inputStream.foreach((key, value) -> System.out.println("key: " + key + " value: " + value));

        KStream<String, Movie> movieStream = inputStream.mapValues(value -> {
            String title = value.get("title").toString();
            double rating = Double.parseDouble(value.get("rating").toString());
            return new Movie(title, rating);
        });

        KStream<String, Movie> filteredMoviesStream = movieStream.filter((key, movie) -> movie.getRating() > 8);
        List<Movie> movies = Collections.synchronizedList(new ArrayList<>());
        filteredMoviesStream.foreach((key, movie) -> movies.add(movie));

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            List<Movie> sortedMovies = movies.stream().sorted(Comparator.comparingDouble(Movie::getRating).reversed()).limit(5).collect(Collectors.toList());
            System.out.println("------------------- FILTERED MOVIES ----------------");
            sortedMovies.forEach(movie -> System.out.println(movie.getTitle() + " " + movie.getRating()));

        }));

        Topology topology = builder.build();
        final KafkaStreams streams = new KafkaStreams(topology, setupApp());
        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}