package um.si.processor;

import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.logging.Logger;

public class ReservationRecordProcessor {
    private static final Logger LOG = Logger.getLogger(String.valueOf(ReservationRecordProcessor.class));
    private static final StringBuilder SB = new StringBuilder("Processing reservation record: ");

    public static void processReservationRecord(ConsumerRecord<String, GenericRecord> record) {
        GenericRecord reservationRecord = record.value();
        String reservation_id = reservationRecord.get("reservation_id").toString();
        String number_of_tickets = reservationRecord.get("number_of_tickets").toString();
        String date_of_reservation = reservationRecord.get("date_of_reservation").toString();
        Double discount = (Double) reservationRecord.get("discount");
        String screening_id = reservationRecord.get("screening_id").toString();
        String person_id = reservationRecord.get("person_id").toString();

//        LOG.info(SB.append("reservation_id=").append(reservation_id).append(", ").append("number_of_tickets=").append(number_of_tickets).append(", ").append("date_of_reservation=").append(date_of_reservation).append(", ").append("discount=").append(discount).append(", ").append("screening_id=").append(screening_id).append(", ").append("person_id=").append(person_id).toString());
    }
}
