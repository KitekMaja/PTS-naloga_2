package um.si;

import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import um.si.recordProducers.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.logging.Logger;

import static um.si.schema.SchemaCreator.*;

public class CinemaProducer {
    private static final Schema S_ADDRESS = getAddressSchema();
    private static final Schema S_PERSON = getPersonSchema();
    private static final Schema S_CINEMA_HALL = getCinemaHallSchema();
    private static final Schema S_EQUIPMENT = getHallEquipmentSchema();
    private static final Schema S_FOOD_BEVERAGE = getFoodAndBeverageSchema();
    private static final Schema S_FOOD_BEVERAGE_SALE = getFoodAndBeverageSaleSchema();
    private static final Schema S_GENRE = getGenreSchema();
    private static final Schema S_MOVIE_GENRE = getMovieGenreSchema();
    private static final Schema S_MOVIE_PERSONNEL_ROLE = getMoviePersonRoleSchema();
    private static final Schema S_MOVIE = getMovieSchema();
    private static final Schema S_ROLE = getRoleSchema();
    private static final Schema S_SCREENING = getScreeningSchema();
    private static final Schema S_PRICE = getPriceSchema();
    private static final Schema S_RESERVATION = getReservationSchema();
    private static final Schema S_RESERVED_SEATS = getReservedSeatsSchema();
    private static final Random RANDOM = new Random();
    private static final Logger LOG = Logger.getLogger(String.valueOf(CinemaProducer.class));

    public static void main(String[] args) throws Exception {

        Random rand = new Random();
        KafkaProducer producer = createProducer();
        List<String> cinemaHallRecordIDs = createCinemaHalls(producer);
        List<String> movieIDs = createMovies(producer);
        List<String> personIDs = createPersons(producer);
        List<String> moviePriceIDs = createMoviePrices(producer);
        List<String> snackIDs = createSnacks(producer);
        while (true) {
            int cinemaHallRecordsSize = cinemaHallRecordIDs.size();
            String random_hall_id = cinemaHallRecordIDs.get(rand.nextInt(cinemaHallRecordsSize));
            produceEquipmentForCinemaHall(random_hall_id, producer);


            for (int i = 0; i < rand.nextInt(15); i++) {
                String screening_id = produceScreening(movieIDs.get(rand.nextInt(movieIDs.size())), moviePriceIDs.get(rand.nextInt(moviePriceIDs.size())), random_hall_id, producer);
                String person_id = personIDs.get(rand.nextInt(personIDs.size()));
                ProducerRecord reservationRecord = ReservationRecordProducer.generateReservationRecord(S_RESERVATION, screening_id, person_id);
                producer.send(reservationRecord);
                LOG.info("[RESERVATION] Sent a new reservation object.");
                GenericRecord reservationValue = (GenericRecord) reservationRecord.value();
                String reservation_id = String.valueOf(reservationValue.get("reservation_id"));
                int number_of_tickets = Integer.parseInt((String.valueOf(reservationValue.get("number_of_tickets"))));

                produceReservedSeats(number_of_tickets, reservation_id, person_id, screening_id, producer);
                for (int j = 0; j < rand.nextInt(number_of_tickets) + 1; j++) {
                    String snack_id = snackIDs.get(rand.nextInt(snackIDs.size()));
                    produceSnackSale(snack_id, producer);
                }
            }
            Thread.sleep(20000);
        }
    }

    private static void produceSnackSale(String snack_id, KafkaProducer producer) {
        ProducerRecord snackSaleRecord = FoodBeverageSaleProducer.generateSnackSaleProducer(S_FOOD_BEVERAGE_SALE, snack_id);
        producer.send(snackSaleRecord);
        LOG.info("[SALE] Sent a new snack sale object.");
    }

    private static String produceSnack(String snack_price_id, KafkaProducer producer) {
        ProducerRecord snackRecord = FoodAndBeverageProducer.generateFoodAndBeverageRecord(S_FOOD_BEVERAGE, snack_price_id);
        producer.send(snackRecord);
        LOG.info("[SNACK] Sent a new snack object.");
        GenericRecord snackValue = (GenericRecord) snackRecord.value();
        return String.valueOf(snackValue.get("snack_id"));
    }

    private static void produceReservedSeats(int number_of_tickets, String reservation_id, String person_id, String screening_id, KafkaProducer producer) {
        for (int i = 0; i < number_of_tickets; i++) {
            ProducerRecord reservedSeatRecords = ReservedSeatProducer.generateReservedSeatRecord(S_RESERVED_SEATS, reservation_id, person_id, screening_id);
            producer.send(reservedSeatRecords);
            LOG.info("[RESERVED SEAT] Sent a new reserved seat object.");
        }
    }

    private static String produceScreening(String movie_id, String movie_price_id, String random_hall_id, KafkaProducer producer) {
        ProducerRecord screeningRecord = ScreeningRecordProducer.generateScreeningRecord(S_SCREENING, movie_id, movie_price_id, random_hall_id);
        producer.send(screeningRecord);
        LOG.info("[SCREENING] Sent a new screening object.");
        GenericRecord screeningValue = (GenericRecord) screeningRecord.value();
        return String.valueOf(screeningValue.get("screening_id"));
    }

    private static List<String> createCinemaHalls(KafkaProducer producer) {
        List<String> cinemaHallRecordIDs = new ArrayList<>();
        for (int i = 1; i <= 5; i++) {
            for (int j = 1; j <= 6; j++) {
                String cinemaHall = produceCinemaHall(producer, i, j);
                cinemaHallRecordIDs.add(cinemaHall);
            }
        }
        return cinemaHallRecordIDs;
    }

    private static List<String> createMovies(KafkaProducer producer) {
        List<String> movieIDs = new ArrayList<>();
        for (int i = 0; i < 30; i++) {
            String movie_id = produceMovie(producer);
            createGenreMovieMapping(producer, movie_id);
            createMoviePersonnelRoleMapping(producer, movie_id);
            movieIDs.add(movie_id);
        }
        return movieIDs;
    }

    private static List<String> createPersons(KafkaProducer producer) {
        List<String> personIds = new ArrayList<>();
        for (int i = 0; i < 15; i++) {
            String person_id = producePerson(producer, false);
            produceAddressForPerson(person_id, producer);
            personIds.add(person_id);
        }
        return personIds;
    }

    private  static List<String> createMoviePrices(KafkaProducer producer) {
        List<String> moviePriceIDs = new ArrayList<>();
        for (int i = 0; i < 30; i++) {
            String movie_price_id = producePrice(producer);
            moviePriceIDs.add(movie_price_id);
        }
        return moviePriceIDs;
    }

    private static List<String> createSnacks(KafkaProducer producer) {
        List<String> snackIDs = new ArrayList<>();
        for (int i = 0; i < 50; i++) {
            String snack_price_id = producePrice(producer);
            String snack_id = produceSnack(snack_price_id, producer);
            snackIDs.add(snack_id);
        }
        return snackIDs;
    }

    private static String producePrice(KafkaProducer producer) {
        ProducerRecord priceRecord = PriceRecordProducer.generatePriceRecord(S_PRICE);
        producer.send(priceRecord);
        LOG.info("[PRICE] Sent a new price object.");
        GenericRecord priceValue = (GenericRecord) priceRecord.value();
        return String.valueOf(priceValue.get("price_id"));
    }

    private static String produceCinemaHall(KafkaProducer producer, int floor, int room) {
        ProducerRecord cinemaHallRecord = CinemaHallRecordProducer.generateCinemaHallRecord(S_CINEMA_HALL, floor, room);
        producer.send(cinemaHallRecord);
        LOG.info("[CINEMA_HALL] Sent a new cinema hall object.");
        GenericRecord cinemaHallValue = (GenericRecord) cinemaHallRecord.value();
        return String.valueOf(cinemaHallValue.get("cinema_hall_id"));
    }

    private static void produceEquipmentForCinemaHall(String cinema_hall_id, KafkaProducer producer) {
        for (int i = 0; i < RANDOM.nextInt(6) + 1; i++) {
            ProducerRecord equipmentRecord = EquipmentRecordProducer.generateEquipmentRecord(S_EQUIPMENT, cinema_hall_id);
            producer.send(equipmentRecord);
            LOG.info("[EQUIPMENT] Sent a new equipment object.");
        }
    }

    private static KafkaProducer createProducer() {

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "AvroProducer");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
        props.put(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081");

        return new KafkaProducer(props);
    }

    private static String producePerson(KafkaProducer producer, boolean role) {
        ProducerRecord memberRecord = PersonRecordProducer.generatePersonRecord(S_PERSON, role);
        producer.send(memberRecord);
        LOG.info("[PERSON] Sent a new person object.");
        LOG.info(memberRecord.toString());
        GenericRecord memberValue = (GenericRecord) memberRecord.value();
        return String.valueOf(memberValue.get("person_id"));
    }

    private static void produceAddressForPerson(String person_id, KafkaProducer producer) {
        for (int i = 0; i < RANDOM.nextInt(2) + 1; i++) {
            ProducerRecord addressRecord = AddressRecordProducer.generateAddressRecord(S_ADDRESS, person_id);
            producer.send(addressRecord);
            LOG.info("[ADDRESS] Sent a new address object.");
        }
    }


    private static void createGenreMovieMapping(KafkaProducer producer, String movie_id) {
        for (int i = 0; i < RANDOM.nextInt(5) + 1; i++) {
            ProducerRecord genreRecord = GenreRecordProducer.generateGenreRecord(S_GENRE);
            producer.send(genreRecord);
            LOG.info("[GENRE] Sent a new genre object.");
            GenericRecord genreValue = (GenericRecord) genreRecord.value();
            String genre_id = String.valueOf(genreValue.get("genre_id"));

            ProducerRecord movieGenreRecord = MovieGenreRecordProducer.generateMovieGenre(S_MOVIE_GENRE, movie_id, genre_id);
            producer.send(movieGenreRecord);
            LOG.info("[MOVIE_GENRE] Sent a new movie-genre mapping object.");
        }
    }

    private static void createMoviePersonnelRoleMapping(KafkaProducer producer, String movie_id) {
        String personnel_id = producePerson(producer, true);
        for (int i = 0; i < RANDOM.nextInt(2) + 1; i++) {
            ProducerRecord roleRecord = RoleRecordProducer.generateRoleRecord(S_ROLE);
            producer.send(roleRecord);
            LOG.info("[ROLE] Sent a new role object.");
            GenericRecord roleValue = (GenericRecord) roleRecord.value();
            String role_id = String.valueOf(roleValue.get("role_id"));

            ProducerRecord moviePersonnelRoleRecord = MoviePersonRoleRecordProducer.generateMoviePersonRoleRecord(S_MOVIE_PERSONNEL_ROLE, movie_id, personnel_id, role_id);
            producer.send(moviePersonnelRoleRecord);
            LOG.info("[MOVIE_ROLE_PERSONNEL] Sent a new movie-role-personnel mapping object.");
        }
    }

    private static String produceMovie(KafkaProducer producer) {
        ProducerRecord movieRecord = MovieRecordProducer.generateMovieRecord(S_MOVIE);
        producer.send(movieRecord);
        LOG.info("[MOVIE] Sent a new movie object.");
        GenericRecord movieValue = (GenericRecord) movieRecord.value();
        return String.valueOf(movieValue.get("movie_id"));
    }
}
