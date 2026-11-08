// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package io.varve.seekparquet;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.junit.jupiter.api.Test;

class ManifestTest {

    private static final String PART = "{\"path\":\"data/p.parquet\",\"rows\":3,\"bytes\":100,\"min_key_base64\":\"YQ==\",\"max_key_base64\":\"Yw==\"}";

    private static String manifest(String parts, String extra) {
        return "{\"format\":\"seekparquet\",\"format_version\":0,\"draft_revision\":1,\"schema_id\":\"s/v1\",\"key\":{\"column\":\"key\",\"unique\":true},\"parts\":[" + parts + "]" + extra + "}";
    }

    private static Manifest parse(String json) {
        return Manifest.parse(json.getBytes(StandardCharsets.UTF_8));
    }

    private static RefusalCode refusal(String json) {
        return assertThrows(RefusalException.class, () -> parse(json)).code();
    }

    @Test
    void parsesAValidManifestAndKeepsExtensionsAsWritten() {
        Manifest m = parse(manifest(PART, ",\"extensions\":{\"tool\": {\"n\": 1.50e3, \"list\":[1, 2]}}"));
        assertEquals(FormatVersion.DRAFT_1, m.version());
        assertEquals("s/v1", m.schemaId());
        assertEquals(List.of(new ManifestPart("data/p.parquet", 3, 100, Bytes.utf8("a"), Bytes.utf8("c"), Optional.empty())), m.parts());
        assertEquals(Map.of("tool", "{\"n\": 1.50e3, \"list\":[1, 2]}"), m.extensions());
        assertEquals(3, m.rows());
    }

    @Test
    void anEmptyPartListIsAnEmptyDataset() {
        assertEquals(List.of(), parse(manifest("", "")).parts());
    }

    @Test
    void theVersionIsDeterminedFirst() {
        // An unknown member in a manifest of an unsupported revision is unsupported, not invalid (MAN-7).
        assertEquals(RefusalCode.UNSUPPORTED_VERSION,
                refusal("{\"format\":\"seekparquet\",\"format_version\":0,\"draft_revision\":2,\"zzz\":1}"));
        assertEquals(RefusalCode.UNSUPPORTED_VERSION, refusal("{\"format\":\"seekparquet\",\"format_version\":1}"));
        assertEquals(RefusalCode.INVALID_MANIFEST, refusal("{\"format\":\"seekparquet\",\"format_version\":1,\"draft_revision\":1}"));
        assertEquals(RefusalCode.INVALID_MANIFEST, refusal("{\"format\":\"seekparquet\",\"format_version\":0}"));
        assertEquals(RefusalCode.INVALID_MANIFEST, refusal("{\"format\":\"other\",\"format_version\":0,\"draft_revision\":1}"));
        assertEquals(RefusalCode.INVALID_MANIFEST, refusal("{\"format\":\"seekparquet\",\"format_version\":\"0\",\"draft_revision\":1}"));
    }

    @ParameterizedTest
    @ValueSource(strings = {"3.0", "3e0", "03", "-3", "\"3\"", "0", "9223372036854775808", "true", "null"})
    void rowsMustBeAnExactPositiveInteger(String rows) {
        assertEquals(RefusalCode.INVALID_MANIFEST, refusal(manifest(PART.replace("\"rows\":3", "\"rows\":" + rows), "")));
    }

    @ParameterizedTest
    @ValueSource(strings = {"YQ", "YR==", "Y-==", " YQ==", "", "====", "YQ==YQ=="})
    void keysMustBeCanonicalBase64(String key) {
        assertEquals(RefusalCode.INVALID_MANIFEST, refusal(manifest(PART.replace("YQ==", key), "")));
    }

    @ParameterizedTest
    @ValueSource(strings = {"/abs", "a//b", "a/./b", "../x", "a/..", "a\\\\b", "a\\u0001b", "", "a/"})
    void pathsMustBePlainRelativePaths(String path) {
        assertEquals(RefusalCode.INVALID_MANIFEST, refusal(manifest(PART.replace("data/p.parquet", path), "")));
    }

    @Test
    void duplicatesAndUndefinedMembersAreRefusedAtAnyDepth() {
        assertEquals(RefusalCode.INVALID_MANIFEST, refusal(manifest(PART, ",\"schema_id\":\"x\"")));
        assertEquals(RefusalCode.INVALID_MANIFEST, refusal(manifest(PART, ",\"extensions\":{\"t\":{\"a\":1,\"\\u0061\":2}}")));
        assertEquals(RefusalCode.INVALID_MANIFEST, refusal(manifest(PART.replace("\"rows\":3", "\"rows\":3,\"rows\":3"), "")));
        assertEquals(RefusalCode.INVALID_MANIFEST, refusal(manifest(PART, ",\"other\":1")));
        assertEquals(RefusalCode.INVALID_MANIFEST, refusal(manifest(PART.replace("\"rows\":3", "\"rows\":3,\"x\":1"), "")));
        assertEquals(RefusalCode.INVALID_MANIFEST, refusal(manifest(PART, ",\"extensions\":[]")));
        assertEquals(RefusalCode.INVALID_MANIFEST, refusal(manifest(PART, ",\"extensions\":{\"t\":\"\\ud800\"}")));
    }

    @Test
    void partBoundsMustAscendAndBeDisjoint() {
        String second = PART.replace("data/p.parquet", "data/q.parquet");
        assertEquals(RefusalCode.INVALID_MANIFEST, refusal(manifest(PART + "," + second, "")));
        String later = second.replace("YQ==", "ZA==").replace("Yw==", "ZQ==");
        assertEquals(2, parse(manifest(PART + "," + later, "")).parts().size());
        assertEquals(RefusalCode.INVALID_MANIFEST, refusal(manifest(PART.replace("YQ==", "ZA=="), "")));
        assertEquals(RefusalCode.INVALID_MANIFEST, refusal(manifest(PART + "," + PART, "")));
    }

    @ParameterizedTest
    @ValueSource(strings = {"null", "{}", "{\"column\":\"key\"}", "{\"column\":\"\",\"unique\":false}",
            "{\"column\":\"key\",\"unique\":1}", "{\"column\":\"key\",\"unique\":true,\"extra\":0}",
            "{\"column\":\"a\\u007fb\",\"unique\":true}"})
    void keyDeclarationIsRequiredAndStrict(String key) {
        String json = manifest(PART, "").replace("{\"column\":\"key\",\"unique\":true}", key);
        assertEquals(RefusalCode.INVALID_MANIFEST, refusal(json));
    }

    @Test
    void syntaxErrorsAndByteOrderMarksAreInvalid() {
        assertEquals(RefusalCode.INVALID_MANIFEST, refusal("[]"));
        assertEquals(RefusalCode.INVALID_MANIFEST, refusal(manifest(PART, "") + "x"));
        assertEquals(RefusalCode.INVALID_MANIFEST, refusal("\uFEFF" + manifest(PART, "")));
        byte[] bad = manifest(PART, ",\"extensions\":{\"t\":\"\u00e9\"}").getBytes(StandardCharsets.UTF_8);
        bad[bad.length - 5] = (byte) 0xC0;
        assertEquals(RefusalCode.INVALID_MANIFEST, assertThrows(RefusalException.class, () -> Manifest.parse(bad)).code());
    }
}
