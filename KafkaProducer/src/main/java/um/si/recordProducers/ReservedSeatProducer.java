package um.si.recordProducers;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.Set;

public class ReservedSeatProducer {

    private static final Map<String, Map<Integer, Integer>> screeningSeatMap = new HashMap<>();
    public static ProducerRecord<Object, Object> generateReservedSeatRecord(Schema schema, String reservationId, String personId, String screeningId) {
        GenericRecord avroRecord = new GenericData.Record(schema);
        Random rand = new Random();

            int row, column;
            do {
                row = rand.nextInt(15) + 1;
                column = rand.nextInt(20) + 1;
            } while (!isSeatAvailable(screeningId, row, column));

            reserveSeat(screeningId, row, column);
            String seatReservationId = "sr_" + System.currentTimeMillis();
            avroRecord.put("id", seatReservationId);
            avroRecord.put("reservation_id", reservationId);
            avroRecord.put("person_id", personId);
            avroRecord.put("screening_id", screeningId);

            return new ProducerRecord<>("seat_reservation_topic", seatReservationId, avroRecord);

    }
    private static boolean isSeatAvailable(String screeningId, int row, int column) {
        Map<Integer, Integer> reservedSeats = screeningSeatMap.getOrDefault(screeningId, new HashMap<>());
        return !reservedSeats.containsKey(row) || reservedSeats.get(row) != column;
    }

    private static void reserveSeat(String screeningId, int row, int column) {
        Map<Integer, Integer> reservedSeats = screeningSeatMap.computeIfAbsent(screeningId, k -> new HashMap<>());
        reservedSeats.put(row, column);
    }
}
