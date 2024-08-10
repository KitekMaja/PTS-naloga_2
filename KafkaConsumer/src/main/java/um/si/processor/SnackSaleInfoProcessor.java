package um.si.processor;

import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.logging.Logger;

public class SnackSaleInfoProcessor {
    private static final Logger LOG = Logger.getLogger(String.valueOf(SnackSaleInfoProcessor.class));
    private static final StringBuilder SB = new StringBuilder("Processing address record: ");

    public static void processSnackSaleInfoRecord(ConsumerRecord<String, GenericRecord> record) {
        GenericRecord addressRecord = record.value();
        String snackSaleId = addressRecord.get("snackSaleId").toString();
        String amt = addressRecord.get("amt").toString();
        String snackName = addressRecord.get("snackName").toString();

//        LOG.info(SB.append("snackSaleId=").append(snackSaleId).append(", ")
//                .append("amt=").append(amt).append(", ")
//                .append("snackName=").append(snackName).append(", ").toString());
    }
}
