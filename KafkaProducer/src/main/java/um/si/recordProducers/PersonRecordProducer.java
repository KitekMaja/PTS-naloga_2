package um.si.recordProducers;

import com.github.javafaker.Faker;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.Random;

public class PersonRecordProducer {
    public static ProducerRecord<Object, Object> generatePersonRecord(Schema schema, boolean role) {
        Random rand = new Random();
        GenericRecord avroRecord = new GenericData.Record(schema);
        Faker faker = new Faker();

        LocalDate myDateObj = LocalDate.now();
        DateTimeFormatter myFormatObj = DateTimeFormatter.ofPattern("dd-MM-yyyy");

        String memberId = "m_" + System.currentTimeMillis();
        avroRecord.put("person_id", memberId);
        avroRecord.put("name", String.valueOf(faker.address().firstName()));
        avroRecord.put("surname", String.valueOf(faker.address().lastName()));
        avroRecord.put("dateOfBirth", myDateObj.minusYears(rand.nextInt(60) + 18).format(myFormatObj));
        if (!role) {
            avroRecord.put("email", String.valueOf(faker.internet().emailAddress()));
        }
        return new ProducerRecord<>("person_topic", memberId, avroRecord);
    }
}
