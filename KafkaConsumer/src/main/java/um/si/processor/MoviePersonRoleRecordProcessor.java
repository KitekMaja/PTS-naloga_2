package um.si.processor;

import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.logging.Logger;

public class MoviePersonRoleRecordProcessor {
    private static final Logger LOG = Logger.getLogger(String.valueOf(MoviePersonRoleRecordProcessor.class));
    private static final StringBuilder SB = new StringBuilder("Processing movie role personnel mapping record: ");

    public static void processMoviePersonRoleRecord(ConsumerRecord<String, GenericRecord> record) {
        GenericRecord addressRecord = record.value();
        String id = addressRecord.get("id").toString();
        String movie_id = addressRecord.get("movie_id").toString();
        String person_id = addressRecord.get("person_id").toString();
        String role_id = addressRecord.get("role_id").toString();

//        LOG.info(SB.append("id=").append(id).append(", ").append("movie_id=").append(movie_id).append(", ").append("person_id=").append(person_id).append(", ").append("role_id=").append(role_id).toString());

    }
}
