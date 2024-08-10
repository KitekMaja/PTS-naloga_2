package um.si.recordProducers;

import com.github.javafaker.Faker;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.ProducerRecord;

public class FoodAndBeverageProducer {
    public static ProducerRecord<Object, Object> generateFoodAndBeverageRecord(Schema schema, String priceId) {
        GenericRecord avroRecord = new GenericData.Record(schema);
        Faker dataFaker = new Faker();

        String foodAndBeverageId = "fab_" + System.currentTimeMillis();
        avroRecord.put("snack_id", foodAndBeverageId);
        avroRecord.put("name", dataFaker.food().dish());
        avroRecord.put("amount", dataFaker.food().measurement());
        avroRecord.put("price_id", priceId);

        return new ProducerRecord<>("snacks_topic_3", foodAndBeverageId, avroRecord);
    }
}
