// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package io.varve.seekparquet;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assumptions.assumeTrue;
import java.nio.file.Path;
import java.nio.file.Files;
import java.util.*;
import java.util.concurrent.CancellationException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class ListIndexResourcesTest {
    @TempDir Path temp;

    private static long retained(Dataset d) throws Exception {
        var f=Parts.class.getDeclaredField("indexMemory"); f.setAccessible(true);
        Object memory=f.get(d.parts());
        var used=IndexMemory.class.getDeclaredField("retained");used.setAccessible(true);
        return used.getLong(memory);
    }

    @Test void sharedMemoryIsolation() throws Exception {
        List<TestDatasets.TestRow> rows=new ArrayList<>();
        for(int i=0;i<4000;i++)rows.add(TestDatasets.TestRow.of(String.format("d%04d/f",i),i));
        var spec=new TestDatasets.Spec();spec.rowsPerGroup=200;spec.rowsPerPage=20;
        Path root=TestDatasets.write(temp.resolve("memory"),spec,List.of(rows));
        long memory;
        try(Dataset probe=Dataset.open(root)){probe.verify();memory=retained(probe);}
        var options=OpenOptions.DEFAULT.withLimits(Limits.DEFAULT.toBuilder().indexMemory(memory/2).build());
        var q=ListQuery.builder((byte)'/').prefix(Bytes.utf8("d3999/")).limit(10).build();
        List<ListEntry> expected;
        try(Dataset plain=Dataset.open(io.varve.seekparquet.storage.LocalStorage.of(root),options,Cancellation.none())){expected=plain.list(q);}
        for(long budget:new long[]{1,0})for(boolean warm:new boolean[]{false,true}) {
            try(Dataset d=Dataset.open(io.varve.seekparquet.storage.LocalStorage.of(root),options,Cancellation.none())) {
                if(warm)d.list(ListQuery.builder((byte)'/').prefix(Bytes.utf8("d0000/")).limit(1).build());
                long before=retained(d);
                var stats=d.buildListIndex(new ListIndexOptions(budget,3,0),Cancellation.none());
                assertEquals(before,retained(d));assertEquals(0,stats.groupsFailed());
                assertEquals(expected,d.list(q));
            }
        }
    }

    @Test void logarithmicPaginationAndCancellation() throws Exception {
        List<TestDatasets.TestRow> rows=new ArrayList<>();
        for(int i=0;i<20000;i++)rows.add(TestDatasets.TestRow.of(String.format("a/%05d/f",i),i));
        rows.add(TestDatasets.TestRow.of("b/f",20000));
        for(int groupRows:new int[]{30000,20}) {
            var spec=new TestDatasets.Spec();spec.rowsPerGroup=groupRows;spec.rowsPerPage=2;
            Path root=TestDatasets.write(temp.resolve("walk"+groupRows),spec,List.of(rows));
            try(Dataset d=Dataset.open(root)) {
                d.buildListIndex();
                var first=d.list(ListQuery.builder((byte)'/').limit(1).build());
                assertEquals(Bytes.utf8("a/"),((ListEntry.CommonPrefix)first.getFirst()).prefix());
                for(int i=0;i<3;i++) {
                    var next=d.list(Reads.listQuery(Bytes.empty(), (byte)'/', Optional.empty(), 1, List.of(), Optional.of(new Cursor.CommonPrefix(Bytes.utf8("a/"))), 0));
                    assertEquals(Bytes.utf8("b/"),((ListEntry.CommonPrefix)next.getFirst()).prefix());
                }
                var g=d.parts().listIndex.groups[0];
                int[] checks={0};
                g.firstOutside(0,Bytes.utf8("a/").unsafeArray(),()->checks[0]++);
                assertTrue(checks[0]<=20,"steps="+checks[0]);
                var cancel=Cancellation.create();checks[0]=0;
                assertThrows(CancellationException.class,()->g.firstOutside(0,Bytes.utf8("a/").unsafeArray(),()->{
                    if(++checks[0]==3)cancel.cancel();cancel.throwIfCancelled();
                }));
            }
        }
    }

    @Test void emptyCacheAndEffectiveOptions() throws Exception {
        for(boolean flat:new boolean[]{false,true}) {
            var spec=new TestDatasets.Spec();
            Path root=TestDatasets.write(temp.resolve("empty"+flat),spec,List.of(List.of(
                    TestDatasets.TestRow.of(flat?"a":"a/x",0),TestDatasets.TestRow.of(flat?"b":"b/y",1))));
            try(Dataset d=Dataset.open(root)) {
                var tiny=new ListIndexOptions(1,1,0);
                var stats=d.buildListIndex(tiny,Cancellation.none());
                assertFalse(stats.hasIndex());assertNotNull(d.parts().listIndex);
                assertSame(stats,d.buildListIndex(tiny,Cancellation.none()));
                var full=d.buildListIndex();assertEquals(ListIndexOptions.DEFAULT,full.options());
                if(full.hasIndex()) {
                    var reused=d.buildListIndex(tiny,Cancellation.none());
                    assertTrue(reused.optionsMismatch());assertEquals(full,reused.forOptions(full.options()));assertNotEquals(tiny,reused.options());
                }
            }
        }
    }

    @Test void sharedFixtureAccountingParity() throws Exception {
        String input=System.getenv("SEEKPARQUET_LIST_INDEX_PARITY");
        assumeTrue(input!=null,"set SEEKPARQUET_LIST_INDEX_PARITY to the Go parity test's export");
        int checked=0;
        for(String line:Files.readAllLines(Path.of(input))) {
            String[] f=line.split("\t");
            try(Dataset d=Dataset.open(Path.of(f[0]))) {
                var st=d.buildListIndex(new ListIndexOptions(Long.parseLong(f[1]),2,Integer.parseInt(f[2])),Cancellation.none());
                assertEquals(Long.parseLong(f[3]),st.groupsIndexed(),line);
                assertEquals(Long.parseLong(f[4]),st.entries(),line);
                assertEquals(Long.parseLong(f[5]),st.bytes(),line);
                assertEquals(Boolean.parseBoolean(f[6]),st.budgetExhausted(),line);checked++;
            }
        }
        assertEquals(96,checked);
    }
}
