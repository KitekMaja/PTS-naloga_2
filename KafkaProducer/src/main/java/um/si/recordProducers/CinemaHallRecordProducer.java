package um.si.recordProducers;

import com.github.javafaker.Faker;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Random;

public class CinemaHallRecordProducer {
    public static ProducerRecord<Object, Object> generateCinemaHallRecord(Schema schema, int floorNumber, int roomNumber) {
        GenericRecord avroRecord = new GenericData.Record(schema);
        Faker faker = new Faker();

        String cinemaHallId = "ch_" + System.currentTimeMillis();
        avroRecord.put("cinema_hall_id", cinemaHallId);
        avroRecord.put("hall_name", faker.artist().name());
        avroRecord.put("floor_number", Integer.valueOf(floorNumber));
        avroRecord.put("room_number", Integer.valueOf(roomNumber));

        return new ProducerRecord<>("cinema_hall_topic", cinemaHallId, avroRecord);
    }
}
