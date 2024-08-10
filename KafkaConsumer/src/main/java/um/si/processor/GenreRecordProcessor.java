package um.si.processor;

import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.logging.Logger;

public class GenreRecordProcessor {
    private static final Logger LOG = Logger.getLogger(String.valueOf(GenreRecordProcessor.class));
    private static final StringBuilder SB = new StringBuilder("Processing genre record: ");

    public static void processGenreRecord(ConsumerRecord<String, GenericRecord> record) {
        GenericRecord genreRecord = record.value();
        String genre_id = genreRecord.get("genre_id").toString();
        String genre = genreRecord.get("genre").toString();

//        LOG.info(SB.append("genre_id=").append(genre_id).append(", ").append("genre=").append(genre).toString());
       }
}
