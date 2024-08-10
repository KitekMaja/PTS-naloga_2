package um.si.recordProducers;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;

public class ScreeningRecordProducer {

    private static final Map<String, List<LocalDateTime>> SCREENINGS = new HashMap<>();
    private static final DateTimeFormatter FORMATTER = DateTimeFormatter.ofPattern("dd-MM-yyyy HH:mm:ss");

    public static ProducerRecord<Object, Object> generateScreeningRecord(Schema schema, String movieId, String priceId, String cinemaHallId) {
        Random rand = new Random();
        GenericRecord avroRecord = new GenericData.Record(schema);
        List<String> technologies = Arrays.asList("2D", "3D", "IMAX");

        String screeningId = "s_" + System.currentTimeMillis();
        avroRecord.put("screening_id", screeningId);
        avroRecord.put("date_of_screening", generateUniqueScreeningDate(cinemaHallId));
        avroRecord.put("technology", technologies.get(rand.nextInt(technologies.size())));
        avroRecord.put("movie_id", movieId);
        avroRecord.put("price_id", priceId);
        avroRecord.put("cinema_hall_id", cinemaHallId);

        return new ProducerRecord<>("screening_topic", screeningId, avroRecord);
    }

    public static String generateUniqueScreeningDate(String cinemaHallId) {
        LocalDateTime newScreeningDate;
        boolean dateIsUnique;

        // Initialize the list for the hall if it doesn't exist
        SCREENINGS.putIfAbsent(cinemaHallId, new ArrayList<>());

        do {
            newScreeningDate = generateRandomDateTime();
            dateIsUnique = isDateUnique(cinemaHallId, newScreeningDate);
        } while (!dateIsUnique);

        SCREENINGS.get(cinemaHallId).add(newScreeningDate);
        return newScreeningDate.format(FORMATTER);
    }

    private static LocalDateTime generateRandomDateTime() {
        LocalDateTime now = LocalDateTime.now();
        LocalDateTime futureDate = now.plusDays(10); // 30 days in the future
        long minutesBetween = java.time.Duration.between(now, futureDate).toMinutes();
        long randomMinutes = (long) (Math.random() * minutesBetween);
        return now.plusMinutes(randomMinutes);
    }

    // Check if the date-time is unique for the given cinema hall
    private static boolean isDateUnique(String cinemaHallId, LocalDateTime dateTime) {
        List<LocalDateTime> existingDates = SCREENINGS.get(cinemaHallId);
        return existingDates.stream().noneMatch(existingDate ->
                Math.abs(java.time.Duration.between(existingDate, dateTime).toMinutes()) < 30); // 30-minute overlap check
    }
}
