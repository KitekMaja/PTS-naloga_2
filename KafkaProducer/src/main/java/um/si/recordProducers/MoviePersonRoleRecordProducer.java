package um.si.recordProducers;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.ProducerRecord;

public class MoviePersonRoleRecordProducer {
    public static ProducerRecord<Object, Object> generateMoviePersonRoleRecord(Schema schema, String movieId, String personId, String roleId) {
        GenericRecord avroRecord = new GenericData.Record(schema);

        String moviePersonRoleMappingId = "mppm_" + System.currentTimeMillis();
        avroRecord.put("id", moviePersonRoleMappingId);
        avroRecord.put("movie_id", movieId);
        avroRecord.put("person_id", personId);
        avroRecord.put("role_id", roleId);

        return new ProducerRecord<>("movie_person_role_mapping_topic", moviePersonRoleMappingId, avroRecord);
    }
}
