package um.si.recordProducers;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.ProducerRecord;

public class MovieGenreRecordProducer {
    public static ProducerRecord<Object, Object> generateMovieGenre(Schema schema, String movieId, String genreId) {
        GenericRecord avroRecord = new GenericData.Record(schema);

        String movieGenreMappingId = "mgm_" + System.currentTimeMillis();
        avroRecord.put("id", movieGenreMappingId);
        avroRecord.put("movie_id", movieId);
        avroRecord.put("genre_id", genreId);

        return new ProducerRecord<>("movie_genre_mapping_topic", movieGenreMappingId, avroRecord);
    }
}
