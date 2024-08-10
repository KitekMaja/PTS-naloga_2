package um.si.processor;

import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.logging.Logger;

public class CinemaHallRecordProcessor {
    private static final Logger LOG = Logger.getLogger(String.valueOf(CinemaHallRecordProcessor.class));
    private static final StringBuilder SB = new StringBuilder("Processing cinema hall record: ");

    public static void processCinemaHallRecord(ConsumerRecord<String, GenericRecord> record) {
        GenericRecord cinemaHallRecord = record.value();
        String cinemaHallId = cinemaHallRecord.get("cinema_hall_id").toString();
        String hallName = cinemaHallRecord.get("hall_name").toString();
        int floorNumber = (int) cinemaHallRecord.get("floor_number");
        int roomNumber = (int) cinemaHallRecord.get("room_number");

//        LOG.info(SB.append("cinemaHallId=").append(cinemaHallId).append(", ").append("hallName=").append(hallName).append(", ").append("floorNumber=").append(floorNumber).append(", ").append("roomNumber=").append(roomNumber).toString());

    }
}
