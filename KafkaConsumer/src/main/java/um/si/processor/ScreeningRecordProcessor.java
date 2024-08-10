package um.si.processor;

import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.logging.Logger;

public class ScreeningRecordProcessor {
    private static final Logger LOG = Logger.getLogger(String.valueOf(ScreeningRecordProcessor.class));
    private static final StringBuilder SB = new StringBuilder("Processing screening record: ");

    public static void processScreeningRecord(ConsumerRecord<String, GenericRecord> record) {
        GenericRecord screeningRecord = record.value();
        String screening_id = screeningRecord.get("screening_id").toString();
        String date_of_screening = screeningRecord.get("date_of_screening").toString();
        String technology = screeningRecord.get("technology").toString();
        String movie_id = screeningRecord.get("movie_id").toString();
        String price_id = screeningRecord.get("price_id").toString();
        String cinema_hall_id = screeningRecord.get("cinema_hall_id").toString();

//        LOG.info(SB.append("screening_id=").append(screening_id).append(", ").append("date_of_screening=").append(date_of_screening).append(", ").append("technology=").append(technology).append(", ").append("movie_id=").append(movie_id).append(", ").append("price_id=").append(price_id).append(", ").append("cinema_hall_id=").append(cinema_hall_id).toString());
      }
}
