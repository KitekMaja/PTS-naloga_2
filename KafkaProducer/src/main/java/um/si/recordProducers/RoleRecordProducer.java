package um.si.recordProducers;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Arrays;
import java.util.List;
import java.util.Random;

public class RoleRecordProducer {
    public static ProducerRecord<Object, Object> generateRoleRecord(Schema schema) {
        GenericRecord avroRecord = new GenericData.Record(schema);

        List<String> role = Arrays.asList("Actor", "Actress", "Director", "Producer", "Screenwriter", "Cinematographer",
                "Editor", "Production Designer", "Composer", "Sound Designer");

        String roleId = "role_" + System.currentTimeMillis();
        avroRecord.put("role_id", roleId);
        avroRecord.put("role", String.valueOf(role.get(new Random().nextInt(role.size()))));

        return new ProducerRecord<>("role_topic", roleId, avroRecord);
    }
}
