package um.si.processor;

import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.logging.Logger;

public class SnacksRecordProcessor {
    private static final Logger LOG = Logger.getLogger(String.valueOf(SnacksRecordProcessor.class));
    private static final StringBuilder SB = new StringBuilder("Processing snacks record: ");

    public static void processSnacksRecord(ConsumerRecord<String, GenericRecord> record) {
        GenericRecord snacksRecord = record.value();
        String snack_id = snacksRecord.get("snack_id").toString();
        String name = snacksRecord.get("name").toString();
        String amount = snacksRecord.get("amount").toString();
        String price_id = snacksRecord.get("price_id").toString();

//        LOG.info(SB.append("snack_id=").append(snack_id).append(", ").append("name=").append(name).append(", ").append("amount=").append(amount).append(", ").append("price_id=").append(price_id).toString());
    }
}
