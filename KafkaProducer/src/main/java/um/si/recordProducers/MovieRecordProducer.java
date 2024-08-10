package um.si.recordProducers;

import com.github.javafaker.Faker;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Random;

public class MovieRecordProducer {

    private static final DateTimeFormatter FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm");

    public static ProducerRecord<Object, Object> generateMovieRecord(Schema schema) {
        GenericRecord avroRecord = new GenericData.Record(schema);
        Faker faker = new Faker();

        String movieId = "movie_" + System.currentTimeMillis();
        avroRecord.put("movie_id", movieId);
        avroRecord.put("title", String.valueOf(faker.hitchhikersGuideToTheGalaxy().planet()));
        avroRecord.put("length", String.valueOf(generateMovieLength()));
        avroRecord.put("rating", Double.valueOf(faker.number().randomDouble(2, 0, 10)));
        avroRecord.put("summary", String.valueOf(faker.hitchhikersGuideToTheGalaxy().marvinQuote()));

        return new ProducerRecord<>("movie_topic_3", movieId, avroRecord);
    }

    private static String generateMovieLength() {
        String start = generateRandomDateTime();
        String end = generateRandomDateTime();

        LocalDateTime startTime = LocalDateTime.parse(start, FORMATTER);
        LocalDateTime endTime = LocalDateTime.parse(end, FORMATTER);
        if (endTime.isBefore(startTime)) {
            endTime = startTime.plusHours(1).plusMinutes(30); // Ensure end time is at least 1.5 hours after start time
        }

        String adjustedEnd = endTime.format(FORMATTER);
        String length = calculateMovieLength(start, adjustedEnd);
        return length;
    }

    public static String calculateMovieLength(String start, String end) {
        LocalDateTime startTime = LocalDateTime.parse(start, FORMATTER);
        LocalDateTime endTime = LocalDateTime.parse(end, FORMATTER);

        Duration duration = Duration.between(startTime, endTime);

        long hours = duration.toHours();
        long minutes = duration.toMinutes() % 60;

        return String.format("%d hours %d minutes", hours, minutes);
    }

    public static String generateRandomDateTime() {
        Random random = new Random();
        int year = 2024; // Fixed year for simplicity
        int month = random.nextInt(12) + 1;
        int day = random.nextInt(28) + 1; // Ensuring valid days in month
        int hour = random.nextInt(24);
        int minute = random.nextInt(60);

        LocalDateTime dateTime = LocalDateTime.of(year, month, day, hour, minute);
        return dateTime.format(FORMATTER);
    }
}
