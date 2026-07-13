package cdet.kafka.connect.transforms;

import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.errors.DataException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

public class TimestampConverterTest {

    private TimestampConverter<SourceRecord> xform;

    @BeforeEach
    public void setUp() {
        xform = new TimestampConverter<>();
        
        // Emulate your production Kafka Connect configuration properties
        Map<String, String> props = new HashMap<>();
        props.put("date.field", "event_time");
        props.put("date.formats", "ISO, yyyy-MM-dd HH:mm:ss, yyyy-MM-dd");
        
        xform.configure(props);
    }

    private SourceRecord createTestRecord(String dateValue) {
        Map<String, Object> valueMap = new HashMap<>();
        valueMap.put("event_time", dateValue);
        valueMap.put("message", "test-payload");

        // Simple mock Kafka record shell
        return new SourceRecord(
            null, null, "test-topic", 0,
            null, null, null, valueMap
        );
    }

    @Test
    public void testIsoFormatParsing() {
        SourceRecord record = createTestRecord("2026-10-01T00:00:00Z");
        SourceRecord transformed = xform.apply(record);
        
        Map<?, ?> updatedValue = (Map<?, ?>) transformed.value();
        assertEquals("2026-10-01T00:00:00.000Z", updatedValue.get("event_time"));
        assertNotNull(transformed.timestamp());
    }

    @Test
    public void testSpaceSeparatedFormatParsing() {
        SourceRecord record = createTestRecord("2026-10-01 00:00:00");
        SourceRecord transformed = xform.apply(record);
        
        Map<?, ?> updatedValue = (Map<?, ?>) transformed.value();
        assertEquals("2026-10-01T00:00:00.000Z", updatedValue.get("event_time"));
        assertNotNull(transformed.timestamp());
    }

    @Test
    public void testPureDateFormatParsing() {
        SourceRecord record = createTestRecord("2026-10-01");
        SourceRecord transformed = xform.apply(record);
        
        Map<?, ?> updatedValue = (Map<?, ?>) transformed.value();
        assertEquals("2026-10-01T00:00:00.000Z", updatedValue.get("event_time"));
        assertNotNull(transformed.timestamp());
    }

    @Test
    public void testInvalidDateFormatThrowsDataException() {
        // This format is not registered in our configurations
        SourceRecord record = createTestRecord("10/01/2026"); 
        
        DataException exception = assertThrows(DataException.class, () -> {
            xform.apply(record);
        });
        
        assertTrue(exception.getMessage().contains("Failed to parse field 'event_time'"));
    }
}
