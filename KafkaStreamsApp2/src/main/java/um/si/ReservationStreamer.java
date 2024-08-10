package um.si;

import io.confluent.kafka.streams.serdes.avro.GenericAvroSerde;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;

import java.util.Collections;
import java.util.Map;
import java.util.Properties;

public class ReservationStreamer {

    public static void main(String[] args) {
        Properties props = setupApp();
        final Map<String, String> serdeConfig = Collections.singletonMap("schema.registry.url", "http://localhost:8081");
        final GenericAvroSerde valueGenericAvroSerde = new GenericAvroSerde();
        valueGenericAvroSerde.configure(serdeConfig, false);

        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, GenericRecord> salesStream = builder.stream("snacks_sale_topic_3", Consumed.with(Serdes.String(), valueGenericAvroSerde));
        KStream<String, GenericRecord> salesStreamBySnackId = salesStream.selectKey((key, sale) -> sale.get("snack_id").toString());

        KTable<String, GenericRecord> snacksTable = builder.table("snacks_topic_3", Consumed.with(Serdes.String(), valueGenericAvroSerde));
        KStream<String, GenericRecord> joinedSnackInfoStream = salesStreamBySnackId.join(snacksTable, (sale, snack) -> {

            String snackSaleId = sale.get("snack_sale_id").toString();
            int amt = Integer.parseInt(sale.get("amt").toString());
            String snackName = snack.get("name").toString();

            Schema schema = createSnackSaleInformationSchema();
            GenericRecord result = new GenericData.Record(schema);
            result.put("snackSaleId", snackSaleId);
            result.put("amount", amt);
            result.put("snackName", snackName);
            return result;
        });

//        joinedSnackInfoStream.foreach((key, value) -> {
//            System.out.println("Key: " + key + ", Value: " + value);
//        });

        KTable<String, Integer> aggregateSalesBySnackName = joinedSnackInfoStream.groupBy((key, value) -> value.get("snackName").toString(), Grouped.with(Serdes.String(), valueGenericAvroSerde)).aggregate(() -> 0, (key, value, aggregate) -> aggregate + (int) value.get("amount"), Materialized.with(Serdes.String(), Serdes.Integer()));
        aggregateSalesBySnackName.toStream().foreach((key, value) -> {
            System.out.println("-------------------------------------------------------------------");
            System.out.println(key + ", total amount sold = " + value);
        });

        Topology topology = builder.build();
        KafkaStreams streams = new KafkaStreams(topology, props);
        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }


    private static Schema createSnackSaleInformationSchema() {
        return SchemaBuilder.record("SnackSaleInfo").fields().requiredString("snackSaleId").requiredInt("amount").requiredString("snackName").endRecord();
    }

    private static Properties setupApp() {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "snack-sales-app");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, GenericAvroSerde.class.getName());
        props.put("schema.registry.url", "http://localhost:8081");
        return props;
    }
}