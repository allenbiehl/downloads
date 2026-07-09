package cdet.kafka.connect.transforms;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.errors.DataException;

import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TimestampConverter<R extends ConnectRecord<R>> implements Transformation<R> {
    
    private static final String FORMATS_CONFIG = "date.formats";
    private static final String FIELD_CONFIG = "date.field";
    
    private static final ConfigDef CONFIG_DEF = new ConfigDef()
        .define(FIELD_CONFIG, ConfigDef.Type.STRING, "event_time", ConfigDef.Importance.HIGH, "Target date field")
        .define(FORMATS_CONFIG, ConfigDef.Type.LIST, ConfigDef.Importance.HIGH, "Comma-separated list of date formats");

    private static final DateTimeFormatter ISO_DATE_FORMAT =
        DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
    
    private String fieldName;
    private DateTimeFormatter dateTimeParser;

    @Override
    public void configure(Map<String, ?> configs) {
        Map<String, Object> parsedConfig = CONFIG_DEF.parse(configs);
        this.fieldName = (String) parsedConfig.get(FIELD_CONFIG);
        List<String> formats = (List<String>) parsedConfig.get(FORMATS_CONFIG);

        DateTimeFormatterBuilder builder = new DateTimeFormatterBuilder();
        
        for (String format : formats) {
            if ("ISO".equalsIgnoreCase(format.trim())) {
                builder.appendOptional(DateTimeFormatter.ISO_DATE_TIME);
            } else {
                builder.appendOptional(DateTimeFormatter.ofPattern(format.trim()));
            }
        }

        this.dateTimeParser = builder
            .parseDefaulting(ChronoField.HOUR_OF_DAY, 0)
            .parseDefaulting(ChronoField.MINUTE_OF_HOUR, 0)
            .parseDefaulting(ChronoField.SECOND_OF_MINUTE, 0)
            .toFormatter()
            .withZone(java.time.ZoneOffset.UTC);
    }

    @Override
    public R apply(R record) {
        if (record.value() == null || !(record.value() instanceof Map)) return record;

        Map<?, ?> rawMap = (Map<?, ?>) record.value();
        Object rawField = rawMap.get(this.fieldName);
        if (rawField == null) return record;

        try {
            java.time.temporal.TemporalAccessor temporal = this.dateTimeParser.parse(rawField.toString().trim());
            ZonedDateTime dt = ZonedDateTime.from(temporal);

            // Reconstruct a type-safe String/Object map for the modification
            Map<String, Object> valueMap = new HashMap<>();
            for (Map.Entry<?, ?> entry : rawMap.entrySet()) {
                if (entry.getKey() != null) {
                    valueMap.put(entry.getKey().toString(), entry.getValue());
                }
            }
            
            // Inject the cleaned ISO timestamp string
            valueMap.put(this.fieldName, dt.format(ISO_DATE_FORMAT));

            return record.newRecord(
                record.topic(), 
                record.kafkaPartition(), 
                record.keySchema(), 
                record.key(), 
                record.valueSchema(), 
                valueMap, 
                dt.toInstant().toEpochMilli()
            );
                
        } catch (Exception e) {
            throw new DataException("Failed to parse field '" + this.fieldName + "' value: " + rawField, e);
        }
    }

    @Override public void close() {}
    @Override public ConfigDef config() { return CONFIG_DEF; }
}
