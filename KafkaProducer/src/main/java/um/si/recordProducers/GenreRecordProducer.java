package um.si.recordProducers;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Arrays;
import java.util.List;
import java.util.Random;

public class GenreRecordProducer {
    public static ProducerRecord<Object, Object> generateGenreRecord(Schema schema) {
        GenericRecord avroRecord = new GenericData.Record(schema);

        List<String> genre = Arrays.asList("Action", "Adventure", "Comedy", "Drama", "Fantasy", "Horror", "Mistery", "Romance", "Sci-fi", "Thriller", "Animation", "Crime", "Family", "Documentary", "War");

        String genreId = "g_" + System.currentTimeMillis();
        avroRecord.put("genre_id", genreId);
        avroRecord.put("genre", genre.get(new Random().nextInt(genre.size())));

        return new ProducerRecord<>("genre_topic", genreId, avroRecord);
    }
}
