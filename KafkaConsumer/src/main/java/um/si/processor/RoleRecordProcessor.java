package um.si.processor;

import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.logging.Logger;

public class RoleRecordProcessor {
    private static final Logger LOG = Logger.getLogger(String.valueOf(RoleRecordProcessor.class));
    private static final StringBuilder SB = new StringBuilder("Processing role record: ");

    public static void processRoleRecord(ConsumerRecord<String, GenericRecord> record) {
        GenericRecord roleRecord = record.value();
        String roleId = roleRecord.get("role_id").toString();
        String role = roleRecord.get("role").toString();

//        LOG.info(SB.append("roleId=").append(roleId).append(", ").append("role=").append(role).toString());

    }
}
