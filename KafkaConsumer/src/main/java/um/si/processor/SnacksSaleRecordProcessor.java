package um.si.processor;

import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.logging.Logger;

public class SnacksSaleRecordProcessor {
    private static final Logger LOG = Logger.getLogger(String.valueOf(SnacksSaleRecordProcessor.class));
    private static final StringBuilder SB = new StringBuilder("Processing snack sale record: ");

    public static void processSnackSaleRecord(ConsumerRecord<String, GenericRecord> record) {
        GenericRecord snackSaleRecord = record.value();
        String snack_sale_id = snackSaleRecord.get("snack_sale_id").toString();
        int amt = (int) snackSaleRecord.get("amt");
        String snack_id = snackSaleRecord.get("snack_id").toString();

       LOG.info(SB.append("snack_sale_id=").append(snack_sale_id).append(", ").append("amount=").append(amt).append(", ").append("snack_id=").append(snack_id).toString());
       }
}
