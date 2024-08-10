package um.si.processor;

import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.logging.Logger;

public class MovieRecordProcessor {
    private static final Logger LOG = Logger.getLogger(String.valueOf(MovieRecordProcessor.class));
    private static final StringBuilder SB = new StringBuilder("Processing movie record: ");

    public static void processMovieRecord(ConsumerRecord<String, GenericRecord> record) {
        GenericRecord movieRecord = record.value();
        String movie_id = movieRecord.get("movie_id").toString();
        String title = movieRecord.get("title").toString();
        String length = movieRecord.get("length").toString();
        double rating = (double) movieRecord.get("rating");
        String summary = movieRecord.get("summary").toString();

//        LOG.info(SB.append("movie_id=").append(movie_id).append(", ").append("title=").append(title).append(", ").append("length=").append(length).append(", ").append("rating=").append(rating).append(", ").append("summary=").append(summary).toString());

    }
}
