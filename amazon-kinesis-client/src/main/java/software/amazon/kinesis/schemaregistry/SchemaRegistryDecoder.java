package software.amazon.kinesis.schemaregistry;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import com.amazonaws.services.schemaregistry.common.Schema;
import com.amazonaws.services.schemaregistry.deserializers.GlueSchemaRegistryDeserializer;
import lombok.extern.slf4j.Slf4j;
import software.amazon.kinesis.retrieval.KinesisClientRecord;

/**
 *  Identifies and decodes Glue Schema Registry data from incoming KinesisClientRecords.
 */
@Slf4j
public class SchemaRegistryDecoder {
    private static final String USER_AGENT_APP_NAME = "kcl" + "-" + "3.0.0";
    private final GlueSchemaRegistryDeserializer glueSchemaRegistryDeserializer;

    public SchemaRegistryDecoder(GlueSchemaRegistryDeserializer glueSchemaRegistryDeserializer) {
        this.glueSchemaRegistryDeserializer = glueSchemaRegistryDeserializer;
        this.glueSchemaRegistryDeserializer.overrideUserAgentApp(USER_AGENT_APP_NAME);
    }

    /**
     * Process the list records and return records with schema and decoded data set.
     * @param records List<KinesisClientRecord>
     * @return List<KinesisClientRecord>
     */
    public List<KinesisClientRecord> decode(final List<KinesisClientRecord> records) {
        final List<KinesisClientRecord> decodedRecords = new ArrayList<>();

        for (final KinesisClientRecord record : records) {
            final KinesisClientRecord decodedRecord = decodeRecord(record);
            decodedRecords.add(decodedRecord);
        }

        return decodedRecords;
    }

    private KinesisClientRecord decodeRecord(final KinesisClientRecord record) {
        if (record.data() == null) {
            return record;
        }

        int length = record.data().remaining();
        byte[] data = new byte[length];
        record.data().get(data, 0, length);

        try {
            if (!isSchemaEncoded(data)) {
                return record;
            }

            final Schema schema = glueSchemaRegistryDeserializer.getSchema(data);
            final ByteBuffer recordData = ByteBuffer.wrap(glueSchemaRegistryDeserializer.getData(data));

            return record.toBuilder().schema(schema).data(recordData).build();
        } catch (Exception e) {
            log.warn("Unable to decode Glue Schema Registry information from record {}: ", record.sequenceNumber(), e);
            // We ignore Glue Schema Registry failures and return the record.
            return record;
        }
    }

    private boolean isSchemaEncoded(byte[] data) {
        return glueSchemaRegistryDeserializer.canDeserialize(data);
    }
}
