package um.si.processor;

import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.logging.Logger;

public class EquipmentRecordProcessor {
    private static final Logger LOG = Logger.getLogger(String.valueOf(EquipmentRecordProcessor.class));
    private static final StringBuilder SB = new StringBuilder("Processing equipment record: ");

    public static void processHallEquipmentRecord(ConsumerRecord<String, GenericRecord> record) {
        GenericRecord equipmentRecord = record.value();
        String equipment_serial_number = equipmentRecord.get("equipment_serial_number").toString();
        String equipment_name = equipmentRecord.get("equipment_name").toString();
        String cinema_hall_id = equipmentRecord.get("cinema_hall_id").toString();

//        LOG.info(SB.append("equipment_serial_number=").append(equipment_serial_number).append(", ").append("equipment_name=").append(equipment_name).append(", ").append("cinema_hall_id=").append(cinema_hall_id).toString());
   }
}
