package um.si.recordProducers;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Arrays;
import java.util.List;
import java.util.Random;

public class EquipmentRecordProducer {
    public static ProducerRecord<Object, Object> generateEquipmentRecord(Schema schema, String cinemaHallId) {
        Random rand = new Random();
        GenericRecord avroRecord = new GenericData.Record(schema);

        List<String> equipment = Arrays.asList("Projector", "Sound System", "Screen", "Projection Stand", "Projector", "Lightning");

        String equipmentId = "e_" + System.currentTimeMillis();
        avroRecord.put("equipment_serial_number", equipmentId);
        avroRecord.put("equipment_name", equipment.get(rand.nextInt(equipment.size())));
        avroRecord.put("cinema_hall_id", cinemaHallId);

        return new ProducerRecord<>("equipment_topic", equipmentId, avroRecord);
    }
}
