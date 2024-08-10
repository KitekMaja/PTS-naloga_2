package um.si;


import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import um.si.processor.*;

import java.util.Arrays;
import java.util.Properties;
import java.util.logging.Logger;

public class CinemaConsumer {

    public static final String MOVIE_TOPIC_2 = "movie_topic_2";
    public static final String ADDRESS_TOPIC = "address_topic";
    public static final String CINEMA_HALL_TOPIC = "cinema_hall_topic";
    public static final String EQUIPMENT_TOPIC = "equipment_topic";
    public static final String SNACKS_TOPIC_2 = "snacks_topic_3";
    public static final String SNACKS_SALE_TOPIC = "snacks_sale_topic_3";
    public static final String GENRE_TOPIC = "genre_topic";
    public static final String MOVIE_GENRE_MAPPING_TOPIC = "movie_genre_mapping_topic";
    public static final String MOVIE_PERSON_ROLE_MAPPING_TOPIC = "movie_person_role_mapping_topic";
    public static final String PERSON_TOPIC = "person_topic";
    public static final String PRICE_TOPIC = "price_topic";
    public static final String RESERVATION_TOPIC_2 = "reservation_topic_2";
    public static final String SEAT_RESERVATION_TOPIC = "seat_reservation_topic";
    public static final String ROLE_TOPIC = "role_topic";
    public static final String SCREENING_TOPIC = "screening_topic";
    public static final String SNACK_SALE_INFO_TOPIC = "snack_sale_info_topic_2";
    private static final Logger LOG = Logger.getLogger(String.valueOf(CinemaConsumer.class));

    private static KafkaConsumer<String, GenericRecord> createConsumer() {

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, "AvroConsumer");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class.getName());
        props.put(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "consumer-cinema");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        return new KafkaConsumer<>(props);
    }

    public static void main(String[] args) throws Exception {
        KafkaConsumer<String, GenericRecord> consumer = createConsumer();

        consumer.subscribe(Arrays.asList(ADDRESS_TOPIC, CINEMA_HALL_TOPIC, EQUIPMENT_TOPIC,
                SNACKS_TOPIC_2, SNACKS_SALE_TOPIC, GENRE_TOPIC, MOVIE_GENRE_MAPPING_TOPIC,
                MOVIE_PERSON_ROLE_MAPPING_TOPIC, MOVIE_TOPIC_2, PERSON_TOPIC, PRICE_TOPIC,
                RESERVATION_TOPIC_2, SEAT_RESERVATION_TOPIC, ROLE_TOPIC, SCREENING_TOPIC,
                SNACK_SALE_INFO_TOPIC));

        try {
            while (true) {
                ConsumerRecords<String, GenericRecord> records = consumer.poll(20000);
                for (ConsumerRecord<String, GenericRecord> record : records) {
                    String topic = record.topic();
                    switch (topic) {
                        case ADDRESS_TOPIC:
                            AddressRecordProcessor.processAddressRecord(record);
                            break;
                        case CINEMA_HALL_TOPIC:
                            CinemaHallRecordProcessor.processCinemaHallRecord(record);
                            break;
                        case EQUIPMENT_TOPIC:
                            EquipmentRecordProcessor.processHallEquipmentRecord(record);
                            break;
                        case SNACKS_TOPIC_2:
                            SnacksRecordProcessor.processSnacksRecord(record);
                            break;
                        case SNACKS_SALE_TOPIC:
                            SnacksSaleRecordProcessor.processSnackSaleRecord(record);
                            break;
                        case GENRE_TOPIC:
                            GenreRecordProcessor.processGenreRecord(record);
                            break;
                        case MOVIE_GENRE_MAPPING_TOPIC:
                            MovieGenreRecordProcessor.processMovieGenreRecord(record);
                            break;
                        case MOVIE_PERSON_ROLE_MAPPING_TOPIC:
                            MoviePersonRoleRecordProcessor.processMoviePersonRoleRecord(record);
                            break;
                        case MOVIE_TOPIC_2:
                            MovieRecordProcessor.processMovieRecord(record);
                            break;
                        case PERSON_TOPIC:
                            PersonRecordProcessor.processPersonRecord(record);
                            break;
                        case PRICE_TOPIC:
                            PriceRecordProcessor.processPriceRecord(record);
                            break;
                        case RESERVATION_TOPIC_2:
                            ReservationRecordProcessor.processReservationRecord(record);
                            break;
                        case SEAT_RESERVATION_TOPIC:
                            ReservedSeatProcessor.processSeatReservationRecord(record);
                            break;
                        case ROLE_TOPIC:
                            RoleRecordProcessor.processRoleRecord(record);
                            break;
                        case SCREENING_TOPIC:
                            ScreeningRecordProcessor.processScreeningRecord(record);
                            break;
                        case SNACK_SALE_INFO_TOPIC:
                            SnackSaleInfoProcessor.processSnackSaleInfoRecord(record);
                            break;
                        default:
                            System.out.printf("Received record from unknown topic: %s%n", topic);
                    }
                }
            }
        } finally {
            consumer.close();
        }
    }

}