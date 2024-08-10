package um.si.processor;

import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.logging.Logger;

public class AddressRecordProcessor {
    private static final Logger LOG = Logger.getLogger(String.valueOf(AddressRecordProcessor.class));
    private static final StringBuilder SB = new StringBuilder("Processing address record: ");

    public static void processAddressRecord(ConsumerRecord<String, GenericRecord> record) {
        GenericRecord addressRecord = record.value();
        String addressId = addressRecord.get("address_id").toString();
        String street = addressRecord.get("street").toString();
        String city = addressRecord.get("city").toString();
        String zipCode = addressRecord.get("postalCode").toString();
        String countryCode = addressRecord.get("countryCode").toString();
        String personId = addressRecord.get("person_id").toString();

//        LOG.info(SB.append("addressId=").append(addressId).append(", ").append("street=").append(street).append(", ").append("city=").append(city).append(", ").append("zipCode=").append(zipCode).append(", ").append("countryCode=").append(countryCode).append(", ").append("personId=").append(personId).toString());
    }
}
