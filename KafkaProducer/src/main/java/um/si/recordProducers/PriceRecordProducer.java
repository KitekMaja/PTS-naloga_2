package um.si.recordProducers;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.Random;

public class PriceRecordProducer {
    private static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd");
    private static final Random rand = new Random();

    public static ProducerRecord<Object, Object> generatePriceRecord(Schema schema) {
        GenericRecord avroRecord = new GenericData.Record(schema);
        LocalDate dateFrom = generateRandomDate();
        LocalDate dateTo = generateRandomDateAfter(dateFrom);

        String priceId = "p_" + System.currentTimeMillis();
        avroRecord.put("price_id", priceId);
        avroRecord.put("dateFrom", dateFrom.format(DATE_FORMATTER));
        avroRecord.put("dateTo", dateTo.format(DATE_FORMATTER));
        avroRecord.put("price", Double.valueOf(generateRandomPrice()));

        return new ProducerRecord<>("price_topic", priceId, avroRecord);
    }

    private static LocalDate generateRandomDate() {
        LocalDate startDate = LocalDate.now().minusMonths(1);
        LocalDate endDate = LocalDate.now().plusMonths(12);
        long daysBetween = endDate.toEpochDay() - startDate.toEpochDay();
        int randomDay = rand.nextInt((int) daysBetween + 1);
        return startDate.plusDays(randomDay);
    }

    private static LocalDate generateRandomDateAfter(LocalDate dateFrom) {
        LocalDate startDate = dateFrom.plusDays(1); // Ensure dateTo is after dateFrom
        LocalDate endDate = LocalDate.now().plusMonths(12); // 12 months from now

        // Ensure startDate is before endDate
        if (startDate.isAfter(endDate)) {
            // Adjust startDate to be the same as endDate to avoid negative daysBetween
            startDate = endDate.plusDays(3);
        }

        // Calculate days between startDate and endDate
        long daysBetween = endDate.toEpochDay() - startDate.toEpochDay();

        // Generate a random number of days within the valid range
        int randomDay = rand.nextInt((int) daysBetween + 1); // +1 to include endDate
        return startDate.plusDays(randomDay);
    }

    private static double generateRandomPrice() {
        // Random price between 10 and 100
        return 10 + (90 * rand.nextDouble());
    }
}
