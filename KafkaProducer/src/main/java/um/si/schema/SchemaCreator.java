package um.si.schema;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;

public class SchemaCreator {


    public static Schema getRoleSchema() {
        return SchemaBuilder.record("Role").fields()
                .requiredString("role_id")
                .requiredString("role").endRecord();
    }

    public static Schema getFoodAndBeverageSchema() {
        return SchemaBuilder.record("FoodAndBeverage").fields()
                .requiredString("snack_id")
                .requiredString("name")
                .requiredString("amount")
                .requiredString("price_id").endRecord();
    }

    public static Schema getFoodAndBeverageSaleSchema() {
        return SchemaBuilder.record("FoodAndBeverageSale").fields()
                .requiredString("snack_sale_id")
                .requiredInt("amt")
                .requiredString("snack_id").endRecord();
    }

    public static Schema getGenreSchema() {
        return SchemaBuilder.record("Genre").fields()
                .requiredString("genre_id")
                .requiredString("genre").endRecord();
    }

    public static Schema getMovieGenreSchema() {
        return SchemaBuilder.record("MovieGenre").fields()
                .requiredString("id")
                .requiredString("movie_id")
                .requiredString("genre_id").endRecord();
    }

    public static Schema getMoviePersonRoleSchema() {
        return SchemaBuilder.record("MoviePersonRole").fields()
                .requiredString("id")
                .requiredString("movie_id")
                .requiredString("person_id")
                .requiredString("role_id").endRecord();
    }

    public static Schema getMovieSchema() {
        return SchemaBuilder.record("Movie").fields()
                .requiredString("movie_id")
                .requiredString("title")
                .requiredString("length")
                .requiredDouble("rating")
                .requiredString("summary").endRecord();
    }

    public static Schema getPriceSchema() {
        return SchemaBuilder.record("Price").fields()
                .requiredString("price_id")
                .requiredString("dateFrom")
                .requiredString("dateTo")
                .requiredDouble("price").endRecord();
    }

    public static Schema getReservedSeatsSchema() {
        return SchemaBuilder.record("ReservedSeats").fields()
                .requiredString("id")
                .requiredString("reservation_id")
                .requiredString("person_id")
                .requiredString("screening_id").endRecord();
    }

    public static Schema getScreeningSchema() {
        return SchemaBuilder.record("Screening").fields()
                .requiredString("screening_id")
                .requiredString("date_of_screening")
                .requiredString("technology")
                .requiredString("movie_id")
                .requiredString("price_id")
                .requiredString("cinema_hall_id").endRecord();
    }
    public static Schema getReservationSchema() {
        return SchemaBuilder.record("Reservation").fields()
                .requiredString("reservation_id")
                .requiredInt("number_of_tickets")
                .requiredString("date_of_reservation")
                .requiredDouble("discount")
                .requiredString("screening_id")
                .requiredString("person_id").endRecord();
    }

    public static Schema getHallEquipmentSchema() {
        return SchemaBuilder.record("Equipment").fields()
                .requiredString("equipment_serial_number")
                .requiredString("equipment_name")
                .requiredString("cinema_hall_id").endRecord();
    }

    public static Schema getCinemaHallSchema() {
        return SchemaBuilder.record("CinemaHall").fields()
                .requiredString("cinema_hall_id")
                .requiredString("hall_name")
                .requiredInt("floor_number")
                .requiredInt("room_number").endRecord();
    }

    public static Schema getPersonSchema() {
        return SchemaBuilder.record("Person").fields()
                .requiredString("person_id")
                .requiredString("name")
                .requiredString("surname")
                .requiredString("dateOfBirth")
                .nullableString("email", "").endRecord();
    }

    public static Schema getAddressSchema() {
        return SchemaBuilder.record("Address").fields()
                .requiredString("address_id")
                .requiredString("street")
                .requiredString("city")
                .requiredString("postalCode")
                .requiredString("countryCode")
                .requiredString("person_id").endRecord();
    }
}
