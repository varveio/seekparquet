// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package io.varve.seekparquet;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Base64;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;
import org.junit.jupiter.api.Test;

class RefusalSerializationTest {

    // Written with RefusalException at 82dd154, before location fields existed, using
    // ObjectOutputStream.writeObject on MISSING_PART("part.parquet", "missing") then
    // NO_MANIFEST(null, "missing"), both with empty stack traces. Keep this fixture from
    // the old class: serializing the current class cannot test missing-field defaults.
    private static final String LEGACY = """
            rO0ABXNyACVpby52YXJ2ZS5zZWVrcGFycXVldC5SZWZ1c2FsRXhjZXB0aW9uAAAAAAAAAAECAAJMAARjb2RldAAiTGlvL3ZhcnZl
            L3NlZWtwYXJxdWV0L1JlZnVzYWxDb2RlO0wABHBhcnR0ABJMamF2YS9sYW5nL1N0cmluZzt4cgApaW8udmFydmUuc2Vla3BhcnF1
            ZXQuU2Vla1BhcnF1ZXRFeGNlcHRpb24AAAAAAAAAAQIAAHhyABpqYXZhLmxhbmcuUnVudGltZUV4Y2VwdGlvbp5fBkcKNIPlAgAA
            eHIAE2phdmEubGFuZy5FeGNlcHRpb27Q/R8+GjscxAIAAHhyABNqYXZhLmxhbmcuVGhyb3dhYmxl1cY1Jzl3uMsDAARMAAVjYXVz
            ZXQAFUxqYXZhL2xhbmcvVGhyb3dhYmxlO0wADWRldGFpbE1lc3NhZ2VxAH4AAlsACnN0YWNrVHJhY2V0AB5bTGphdmEvbGFuZy9T
            dGFja1RyYWNlRWxlbWVudDtMABRzdXBwcmVzc2VkRXhjZXB0aW9uc3QAEExqYXZhL3V0aWwvTGlzdDt4cHB0ACptaXNzaW5nX3Bh
            cnQgaW4gcGFydCBwYXJ0LnBhcnF1ZXQ6IG1pc3Npbmd1cgAeW0xqYXZhLmxhbmcuU3RhY2tUcmFjZUVsZW1lbnQ7AkYqPDz9IjkC
            AAB4cAAAAABzcgAfamF2YS51dGlsLkNvbGxlY3Rpb25zJEVtcHR5TGlzdHq4F7Q8p57eAgAAeHB4fnIAIGlvLnZhcnZlLnNlZWtw
            YXJxdWV0LlJlZnVzYWxDb2RlAAAAAAAAAAASAAB4cgAOamF2YS5sYW5nLkVudW0AAAAAAAAAABIAAHhwdAAMTUlTU0lOR19QQVJU
            dAAMcGFydC5wYXJxdWV0c3EAfgAAcHQAFG5vX21hbmlmZXN0OiBtaXNzaW5ndXEAfgAMAAAAAHEAfgAPeH5xAH4AEHQAC05PX01B
            TklGRVNUcA==
            """;

    @Test
    void locatedRefusalsRoundTripWithoutLosingOrSharingTheKey() throws Exception {
        byte[] key = {0, (byte) 0xff, 42};
        RefusalException original = new RefusalException(RefusalCode.KEY_ORDER, "part.parquet", "unordered", 0, 0, key);
        key[0] = 99;
        RefusalException restored = roundTrip(original);
        assertEquals(original.code(), restored.code());
        assertEquals(original.getMessage(), restored.getMessage());
        assertEquals(original.part(), restored.part());
        assertEquals(OptionalInt.of(0), restored.rowGroup());
        assertEquals(OptionalLong.of(0), restored.row());
        assertEquals(Optional.of(Bytes.of((byte) 0, (byte) 0xff, (byte) 42)), restored.key());
        byte[] copy = restored.key().orElseThrow().toByteArray();
        copy[1] = 0;
        assertEquals(original.key(), restored.key());
    }

    @Test
    void partialLocationsRoundTrip() throws Exception {
        RefusalException original = new RefusalException(RefusalCode.MISSING_PAGE_INDEX, "part.parquet", "missing", 3, -1, null);
        RefusalException restored = roundTrip(original);
        assertEquals(original.code(), restored.code());
        assertEquals(original.part(), restored.part());
        assertEquals(OptionalInt.of(3), restored.rowGroup());
        assertEquals(OptionalLong.empty(), restored.row());
        assertEquals(Optional.empty(), restored.key());
    }

    @Test
    void unlocatedRefusalsRoundTrip() throws Exception {
        for (RefusalException original : new RefusalException[] {
                new RefusalException(RefusalCode.MISSING_PART, "part.parquet", "missing"),
                new RefusalException(RefusalCode.NO_MANIFEST, null, "missing")}) {
            RefusalException restored = roundTrip(original);
            assertEquals(original.code(), restored.code());
            assertEquals(original.getMessage(), restored.getMessage());
            assertEquals(original.part(), restored.part());
            assertNoLocation(restored);
        }
    }

    @Test
    void oldStreamsDoNotInventLocations() throws Exception {
        try (ObjectInputStream in = new ObjectInputStream(new ByteArrayInputStream(Base64.getMimeDecoder().decode(LEGACY)))) {
            RefusalException part = (RefusalException) in.readObject();
            assertEquals(RefusalCode.MISSING_PART, part.code());
            assertEquals(Optional.of("part.parquet"), part.part());
            assertEquals("missing_part in part part.parquet: missing", part.getMessage());
            assertNoLocation(part);
            RefusalException manifest = (RefusalException) in.readObject();
            assertEquals(RefusalCode.NO_MANIFEST, manifest.code());
            assertEquals(Optional.empty(), manifest.part());
            assertEquals("no_manifest: missing", manifest.getMessage());
            assertNoLocation(manifest);
        }
    }

    private static void assertNoLocation(RefusalException refusal) {
        assertEquals(OptionalInt.empty(), refusal.rowGroup());
        assertEquals(OptionalLong.empty(), refusal.row());
        assertEquals(Optional.empty(), refusal.key());
    }

    private static RefusalException roundTrip(RefusalException refusal) throws Exception {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (ObjectOutputStream out = new ObjectOutputStream(bytes)) {
            out.writeObject(refusal);
        }
        try (ObjectInputStream in = new ObjectInputStream(new ByteArrayInputStream(bytes.toByteArray()))) {
            return (RefusalException) in.readObject();
        }
    }
}
