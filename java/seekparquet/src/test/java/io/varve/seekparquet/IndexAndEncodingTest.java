// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package io.varve.seekparquet;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.nio.ByteBuffer;
import java.util.List;
import org.apache.parquet.format.BoundaryOrder;
import org.apache.parquet.format.ColumnIndex;
import org.apache.parquet.format.OffsetIndex;
import org.apache.parquet.format.PageLocation;
import org.junit.jupiter.api.Test;

class IndexAndEncodingTest {
    @Test
    void routingUsesTheEarliestCandidateEvenWhenBoundsAreNonMonotone() throws DataException {
        ColumnIndex columns = new ColumnIndex(List.of(false, false, false, false),
                List.of(bytes("a"), bytes("a"), bytes("a"), bytes("a")),
                List.of(bytes("z"), bytes("b"), bytes("c"), bytes("d")), BoundaryOrder.UNORDERED);
        OffsetIndex offsets = new OffsetIndex(List.of(new PageLocation(10, 10, 0), new PageLocation(20, 10, 1),
                new PageLocation(30, 10, 2), new PageLocation(40, 10, 3)));
        KeyIndex index = KeyIndex.check(columns, offsets, 4, 10, 50, true);
        assertEquals(0, index.pageFor(0, new byte[] {'c'}, false));
        assertEquals(2, index.pageFor(1, new byte[] {'c'}, false));
        assertEquals(3, index.pageFor(1, new byte[] {'c'}, true));
        assertEquals(4, index.pageFor(0, new byte[] {(byte) 255}, false));
    }

    @Test
    void hugeRunCountsAndOverflowingVarintsFailBeforeDecoding() {
        byte[] aboveLong = {(byte) 128, (byte) 128, (byte) 128, (byte) 128, (byte) 128,
                (byte) 128, (byte) 128, (byte) 128, (byte) 128, 2};
        assertThrows(DataException.class, () -> EncodingChecks.uleb(aboveLong, new int[] {0}, aboveLong.length));
        byte[] hugeRun = {(byte) 255, (byte) 255, (byte) 255, (byte) 255, 15};
        assertThrows(DataException.class, () -> EncodingChecks.rleHybrid(hugeRun, 0, hugeRun.length, 32, 2));
    }

    private static ByteBuffer bytes(String s) {
        return ByteBuffer.wrap(s.getBytes(java.nio.charset.StandardCharsets.UTF_8));
    }
}
