package um.si.recordProducers;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Random;

public class ReservationRecordProducer {

    public static ProducerRecord<Object, Object> generateReservationRecord(Schema schema, String screeningId, String memberId) {
        Random rand = new Random();
        GenericRecord avroRecord = new GenericData.Record(schema);

        LocalDateTime myDateObj = LocalDateTime.now();
        DateTimeFormatter myFormatObj = DateTimeFormatter.ofPattern("dd-MM-yyyy HH:mm:ss");
        String formattedDate = myDateObj.format(myFormatObj);

        String reservationId = "r_" + System.currentTimeMillis();
        avroRecord.put("reservation_id", reservationId);
        avroRecord.put("number_of_tickets", Integer.valueOf(rand.nextInt(5) + 1));
        avroRecord.put("date_of_reservation", formattedDate);
        avroRecord.put("discount", Double.valueOf(0.5 + (2.5 - 0.5) * rand.nextDouble()));
        avroRecord.put("screening_id", screeningId);
        avroRecord.put("person_id", memberId);

        return new ProducerRecord<>("reservation_topic_2", reservationId, avroRecord);
    }
}
