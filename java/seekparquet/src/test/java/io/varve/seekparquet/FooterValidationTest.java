// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package io.varve.seekparquet;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;
import org.apache.parquet.format.FileMetaData;
import org.apache.parquet.format.KeyValue;
import org.apache.parquet.format.Util;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class FooterValidationTest {
    @TempDir Path tmp;

    private void check(String name, Consumer<FileMetaData> change, RefusalCode code) throws Exception {
        Path root = TestDatasets.write(tmp.resolve(name), new TestDatasets.Spec(),
                List.of(List.of(TestDatasets.TestRow.of("a", 1))));
        Path file = root.resolve("data/part-00000.parquet");
        byte[] raw = Files.readAllBytes(file);
        int length = ByteBuffer.wrap(raw).order(ByteOrder.LITTLE_ENDIAN).getInt(raw.length - 8);
        int start = raw.length - 8 - length;
        FileMetaData footer = Thrift.fileMetaData(java.util.Arrays.copyOfRange(raw, start, start + length));
        change.accept(footer);
        ByteArrayOutputStream encoded = new ByteArrayOutputStream();
        Util.writeFileMetaData(footer, encoded);
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        out.write(raw, 0, start);
        out.write(encoded.toByteArray());
        out.write(ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN).putInt(encoded.size()).array());
        out.write(new byte[] {'P', 'A', 'R', '1'});
        Files.write(file, out.toByteArray());
        Path manifest = root.resolve("manifest.json");
        Files.writeString(manifest, Files.readString(manifest).replace("\"bytes\":" + raw.length, "\"bytes\":" + out.size()));
        IndexedAccessTest.CountingStorage storage = new IndexedAccessTest.CountingStorage(root);
        assertEquals(code, assertThrows(RefusalException.class, () -> Dataset.open(storage)).code());
        assertEquals(1, storage.reads.get(), "invalid footer must fail before reading column data or indexes");
    }

    @Test
    void referencesSchemaPathsAndCountsAreCheckedBeforeDataReads() throws Exception {
        check("external", f -> f.getRow_groups().getFirst().getColumns().getFirst().setFile_path(""), RefusalCode.INVALID_FOOTER);
        check("path", f -> f.getRow_groups().getFirst().getColumns().getFirst().getMeta_data().setPath_in_schema(List.of("elsewhere")),
                RefusalCode.INVALID_FOOTER);
        check("count", f -> f.setNum_rows(9), RefusalCode.INVALID_FOOTER);
        check("root", f -> f.getSchema().getFirst().setType(org.apache.parquet.format.Type.INT64), RefusalCode.INVALID_FOOTER);
    }

    @Test
    void keyStampsAreExactAndAppearOnce() throws Exception {
        check("duplicate", f -> {
            List<KeyValue> stamps = new ArrayList<>(f.getKey_value_metadata());
            stamps.add(new KeyValue("seekparquet.key_unique").setValue("true"));
            f.setKey_value_metadata(stamps);
        }, RefusalCode.STAMP_MISMATCH);
        check("case", f -> f.getKey_value_metadata().stream().filter(k -> k.getKey().equals("seekparquet.key_unique"))
                .findFirst().orElseThrow().setValue("True"), RefusalCode.STAMP_MISMATCH);
        check("column", f -> f.getKey_value_metadata().stream().filter(k -> k.getKey().equals("seekparquet.key_column"))
                .findFirst().orElseThrow().setValue("other"), RefusalCode.STAMP_MISMATCH);
    }

    @Test
    void aFooterCannotHideTrailingBytes() throws IOException {
        byte[] raw = TestDatasets.part(new TestDatasets.Spec(), List.of(TestDatasets.TestRow.of("a", 1)));
        int length = ByteBuffer.wrap(raw).order(ByteOrder.LITTLE_ENDIAN).getInt(raw.length - 8);
        byte[] footer = java.util.Arrays.copyOfRange(raw, raw.length - 8 - length, raw.length - 7);
        assertThrows(DataException.class, () -> Thrift.fileMetaData(footer));
    }
}
