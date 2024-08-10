package um.si.processor;

import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.logging.Logger;

public class ReservedSeatProcessor {
    private static final Logger LOG = Logger.getLogger(String.valueOf(ReservedSeatProcessor.class));
    private static final StringBuilder SB = new StringBuilder("Processing reserved seats record: ");

    public static void processSeatReservationRecord(ConsumerRecord<String, GenericRecord> record) {
        GenericRecord seatsRecord = record.value();
        String id = seatsRecord.get("id").toString();
        String reservation_id = seatsRecord.get("reservation_id").toString();
        String person_id = seatsRecord.get("person_id").toString();
        String screening_id = seatsRecord.get("screening_id").toString();

//        LOG.info(SB.append("id=").append(id).append(", ").append("reservation_id=").append(reservation_id).append(", ").append("person_id=").append(person_id).append(", ").append("screening_id=").append(screening_id).toString());

    }
}
