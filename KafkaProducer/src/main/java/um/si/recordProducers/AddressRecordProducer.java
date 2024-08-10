package um.si.recordProducers;

import com.github.javafaker.Faker;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.ProducerRecord;

public class AddressRecordProducer {
    public static ProducerRecord<Object, Object> generateAddressRecord(Schema schema, String memberId) {
        GenericRecord avroRecord = new GenericData.Record(schema);
        Faker faker = new Faker();

        String addressId = "a_" + System.currentTimeMillis();
        avroRecord.put("address_id", addressId);
        avroRecord.put("street", String.valueOf(faker.address().streetAddress()));
        avroRecord.put("city", String.valueOf(faker.address().cityName()));
        avroRecord.put("postalCode", String.valueOf(faker.address().zipCode()));
        avroRecord.put("countryCode", String.valueOf(faker.address().countryCode()));
        avroRecord.put("person_id", memberId);

        return new ProducerRecord<>("address_topic", addressId, avroRecord);
    }
}
