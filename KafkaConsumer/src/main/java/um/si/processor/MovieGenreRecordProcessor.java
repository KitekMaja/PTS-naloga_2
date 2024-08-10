package um.si.processor;

import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.logging.Logger;

public class MovieGenreRecordProcessor {
    private static final Logger LOG = Logger.getLogger(String.valueOf(MovieGenreRecordProcessor.class));
    private static final StringBuilder SB = new StringBuilder("Processing movie genre mapping record: ");

    public static void processMovieGenreRecord(ConsumerRecord<String, GenericRecord> record) {
        GenericRecord movieGenreRecord = record.value();
        String id = movieGenreRecord.get("id").toString();
        String movie_id = movieGenreRecord.get("movie_id").toString();
        String genre_id = movieGenreRecord.get("genre_id").toString();

//        LOG.info(SB.append("id=").append(id).append(", ").append("movie_id=").append(movie_id).append(", ").append("genre_id=").append(genre_id).toString());

    }
}
