package um.si.processor;

import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.logging.Logger;

public class PriceRecordProcessor {
    private static final Logger LOG = Logger.getLogger(String.valueOf(PriceRecordProcessor.class));
    private static final StringBuilder SB = new StringBuilder("Processing price record: ");

    public static void processPriceRecord(ConsumerRecord<String, GenericRecord> record) {
        GenericRecord priceRecord = record.value();
        String price_id = priceRecord.get("price_id").toString();
        String dateFrom = priceRecord.get("dateFrom").toString();
        String dateTo = priceRecord.get("dateTo").toString();
        double price = (double) priceRecord.get("price");

//        LOG.info(SB.append("price_id=").append(price_id).append(", ").append("dateFrom=").append(dateFrom).append(", ").append("dateTo=").append(dateTo).append(", ").append("price=").append(price).toString());

    }
}
