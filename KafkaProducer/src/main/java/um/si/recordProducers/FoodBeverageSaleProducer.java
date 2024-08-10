package um.si.recordProducers;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Random;

public class FoodBeverageSaleProducer {
    public static ProducerRecord<Object, Object> generateSnackSaleProducer(Schema schema, String snackId) {
        GenericRecord avroRecord = new GenericData.Record(schema);
        Random rand = new Random();

        String foodAndBeverageSaleId = "fab_sale_" + System.currentTimeMillis();
        avroRecord.put("snack_sale_id", foodAndBeverageSaleId);
        avroRecord.put("amt", rand.nextInt(3) + 1);
        avroRecord.put("snack_id", snackId);

        return new ProducerRecord<>("snacks_sale_topic_3", foodAndBeverageSaleId, avroRecord);
    }

}
