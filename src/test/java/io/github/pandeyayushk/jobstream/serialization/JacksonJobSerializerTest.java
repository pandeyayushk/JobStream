package io.github.pandeyayushk.jobstream.serialization;

import io.github.pandeyayushk.jobstream.job.Job;
import io.github.pandeyayushk.jobstream.job.JobId;
import io.github.pandeyayushk.jobstream.job.JobStatus;
import io.github.pandeyayushk.jobstream.payload.Payload;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrowsExactly;

public class JacksonJobSerializerTest {

    private final JacksonJobSerializer serializer=new JacksonJobSerializer();

    @Test
    public void serializeAndDeserializePreservesJobFields(){
        Payload payload = Payload.of(Map.of(
                "email", "test@example.com",
                "attempts", 3,
                "urgent", true
        ));
        Job original= Job.create("Email",payload);
        String json=serializer.serialize(original);
        Job deserialized=serializer.deserialize(json);

        assertEquals(original.id(),deserialized.id());
        assertEquals(original.type(),deserialized.type());
        assertEquals(original.status(),deserialized.status());
        assertEquals(original.payload().asMap(),deserialized.payload().asMap());
        assertEquals(original.createdAt(),deserialized.createdAt());
        assertEquals(original.updatedAt(),deserialized.updatedAt());
        assertEquals(original.metadata(),deserialized.metadata());
    }

    @Test
    public void timestampsPreservePrecision(){
        Instant createdAt = Instant.parse("2026-09-20T10:00:00.123456789Z");
        Instant updatedAt = Instant.parse("2026-09-21T18:45:30.987654321Z");
        JobId id=JobId.generate();
        Payload payload = Payload.of(Map.of(
                "email", "test@example.com",
                "attempts", 3,
                "urgent", true
        ));
        Job job=Job.reconstitute(id,"Email", JobStatus.PENDING,payload,createdAt,updatedAt,Map.of());
        String json=serializer.serialize(job);
        Job deserialized=serializer.deserialize(json);
        assertEquals(job.createdAt(),deserialized.createdAt());
        assertEquals(job.updatedAt(),deserialized.updatedAt());
    }

    @Test
    public void metadataIsPreserved(){
        JobId id=JobId.generate();
        Payload payload=Payload.empty();
        Instant now=Instant.now();
        Map<String, String> originalMetadata = new HashMap<>(Map.of());
        originalMetadata.put("source", "api");
        Job job=Job.reconstitute(id,"Email",JobStatus.PENDING,payload,now,now,originalMetadata);
        String json=serializer.serialize(job);
        Job deserialized =serializer.deserialize(json);
        assertEquals(originalMetadata,deserialized.metadata());
    }

    @Test
    public void malformedJsonThrowsSerializationException(){
        String json="Malformed";
        assertThrowsExactly(SerializationException.class,()->
                serializer.deserialize(json)
        );
    }

    @Test
    public void invalidJobIdThrowsSerializationException(){
        String json = """
        {
          "id": "not-a-valid-uuid",
          "type": "Email",
          "status": "PENDING",
          "payload": {},
          "createdAt": "2026-09-20T10:00:00.123456789Z",
          "updatedAt": "2026-09-21T18:45:30.987654321Z",
          "metadata": {}
        }
        """;

        assertThrowsExactly(SerializationException.class,()->
                serializer.deserialize(json)
        );
    }
}
