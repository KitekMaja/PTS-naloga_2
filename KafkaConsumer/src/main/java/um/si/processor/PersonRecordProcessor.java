package um.si.processor;

import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.logging.Logger;

public class PersonRecordProcessor {
    private static final Logger LOG = Logger.getLogger(String.valueOf(PersonRecordProcessor.class));
    private static final StringBuilder SB = new StringBuilder("Processing person record: ");

    public static void processPersonRecord(ConsumerRecord<String, GenericRecord> record) {
        GenericRecord personRecord = record.value();
        String person_id = personRecord.get("person_id").toString();
        String name = personRecord.get("name").toString();
        String surname = personRecord.get("surname").toString();
        String dateOfBirth = personRecord.get("dateOfBirth").toString();

        StringBuilder person = SB.append("person_id=").append(person_id).append(", ").append("name=").append(name).append(", ").append("surname=").append(surname).append(", ").append("dateOfBirth=").append(dateOfBirth);
//        LOG.info(person.toString());
        }
}
