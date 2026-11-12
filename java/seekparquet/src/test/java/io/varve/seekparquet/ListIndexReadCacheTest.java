// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package io.varve.seekparquet;

import static org.junit.jupiter.api.Assertions.*;
import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.parquet.format.CompressionCodec;
import org.apache.parquet.format.Util;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class ListIndexReadCacheTest {
    @TempDir Path temp;

    private static void unsupported(Path root, int part, int group) throws Exception {
        Path file = root.resolve(String.format("data/part-%05d.parquet", part));
        byte[] raw = Files.readAllBytes(file);
        int length = ByteBuffer.wrap(raw).order(ByteOrder.LITTLE_ENDIAN).getInt(raw.length - 8);
        int start = raw.length - 8 - length;
        var footer = Thrift.fileMetaData(Arrays.copyOfRange(raw, start, start + length));
        footer.getRow_groups().get(group).getColumns().getFirst().getMeta_data().setCodec(CompressionCodec.LZO);
        var encoded = new ByteArrayOutputStream(); Util.writeFileMetaData(footer, encoded);
        var out = new ByteArrayOutputStream(); out.write(raw, 0, start); out.write(encoded.toByteArray());
        out.write(ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN).putInt(encoded.size()).array());
        out.write(new byte[] {'P','A','R','1'}); Files.write(file, out.toByteArray());
        Path manifest = root.resolve("manifest.json");
        Files.writeString(manifest, Files.readString(manifest).replace("\"bytes\":" + raw.length, "\"bytes\":" + out.size()));
    }

    private record Read(Object entries, ReadStats stats) {}
    private static Read read(Dataset d, Object q) {
        AtomicReference<ReadStats> stats = new AtomicReference<>();
        Object result = q instanceof RangeQuery range ? d.range(range, Cancellation.none(), stats::set)
                : d.list((ListQuery) q, Cancellation.none(), stats::set);
        return new Read(result, stats.get());
    }

    @Test void warmReadCost() throws Exception {
        List<TestDatasets.TestRow> rows = new ArrayList<>();
        for (int i=0;i<500;i++) rows.add(TestDatasets.TestRow.of(String.format("a/one/%06d",i),i));
        for (int i=0;i<20000;i++) rows.add(TestDatasets.TestRow.of(String.format("f%06d",i),i));
        for (int i=0;i<500;i++) rows.add(TestDatasets.TestRow.of(String.format("g/d%06d/x",i),i));
        for (int i=0;i<20;i++) {
            rows.add(TestDatasets.TestRow.of(String.format("z/%02d/",i),i));
            for (int j=0;j<50;j++) rows.add(TestDatasets.TestRow.of(String.format("z/%02d/%03d",i,j),j));
        }
        var spec = new TestDatasets.Spec(); spec.rowsPerGroup=500; spec.rowsPerPage=150;
        Path root=TestDatasets.write(temp.resolve("cost"),spec,List.of(rows,List.of(TestDatasets.TestRow.of("zz/x",0),TestDatasets.TestRow.of("zz/y",1))));
        unsupported(root,1,0);
        long firstCost;
        try (Dataset probe=Dataset.open(root)) {
            var st=probe.buildListIndex(); assertEquals(1,st.groupsFailed()); assertTrue(st.groupsSkipped()>=40);
            firstCost=256+probe.parts().listIndex.groups[0].memoryBytes();
        }
        for (long budget:new long[]{0,firstCost}) {
            try (Dataset plain=Dataset.open(root); Dataset indexed=Dataset.open(root)) {
                var st=indexed.buildListIndex(new ListIndexOptions(budget,1,0),Cancellation.none());
                assertTrue(st.hasIndex()); if(budget>0) assertTrue(st.budgetExhausted());
                for (Object q:List.of(RangeQuery.builder().start(Bytes.utf8("f010000")).limit(3).build(),
                        new ListQuery(Bytes.utf8("z/"),(byte)'/',Optional.empty(),20,List.of("size"),Optional.empty()),
                        new ListQuery(Bytes.utf8("z/"),(byte)'/',Optional.empty(),20,List.of("size"),Optional.of(new Cursor.CommonPrefix(Bytes.utf8("z/00/")))),
                        ListQuery.builder((byte)'/').prefix(Bytes.utf8("f01")).limit(3).build(),
                        ListQuery.builder((byte)'/').prefix(Bytes.utf8("g/")).limit(20).build())) {
                    for(int round=0;round<3;round++) {
                        var want=read(plain,q); var got=read(indexed,q); assertEquals(want.entries(),got.entries());
                        if(round>0) {
                            assertTrue(got.stats().sourceReads()<=want.stats().sourceReads(),q+" "+want.stats()+" / "+got.stats());
                            assertTrue(got.stats().keyPagesDecoded()<=want.stats().keyPagesDecoded(),q+" "+want.stats()+" / "+got.stats());
                            assertEquals(0,got.stats().indexLoads());
                        }
                    }
                }
            }
        }
    }

    @Test void unsupportedGroupIsOmitted() throws Exception {
        var spec=new TestDatasets.Spec(); spec.rowsPerGroup=2;
        Path root=TestDatasets.write(temp.resolve("unsupported"),spec,List.of(List.of(TestDatasets.TestRow.of("a/x",0),
                TestDatasets.TestRow.of("a/y",1),TestDatasets.TestRow.of("b/x",2),TestDatasets.TestRow.of("b/y",3))));
        unsupported(root,0,1);
        try(Dataset plain=Dataset.open(root); Dataset indexed=Dataset.open(root)) {
            var st=indexed.buildListIndex(); assertEquals(1,st.groupsFailed()); assertEquals(1,st.groupsIndexed());
            var q=ListQuery.builder((byte)'/').prefix(Bytes.utf8("a/")).limit(1).build();
            assertEquals(plain.list(q),indexed.list(q));
            for(Dataset d:List.of(plain,indexed)) {
                var e=assertThrows(UnsupportedFeatureException.class,()->d.list(ListQuery.builder((byte)'/').prefix(Bytes.utf8("b/")).limit(10).build()));
                assertEquals(UnsupportedCode.UNSUPPORTED_ENCODING,e.code());
            }
        }
    }

    @Test void ordinaryOrderChecksAfterBuild() throws Exception {
        var spec=new TestDatasets.Spec(); spec.rowsPerGroup=2;
        List<TestDatasets.TestRow> rows=new ArrayList<>();
        for(String key:List.of("a/0","a/1","c/0","c/1","b/0","d/0"))rows.add(TestDatasets.TestRow.of(key,0));
        Path root=TestDatasets.write(temp.resolve("order"),spec,List.of(rows));
        for(boolean indexed:new boolean[]{false,true}) for(boolean list:new boolean[]{false,true}) {
            try(Dataset d=Dataset.open(root)) {
                if(indexed)d.buildListIndex();
                var e=assertThrows(RefusalException.class,()->{
                    if(list)d.list(ListQuery.builder((byte)'/').limit(100).build());else d.range(RangeQuery.all(100));
                });
                assertEquals(RefusalCode.KEY_ORDER,e.code());
            }
        }
    }

    @Test void emptyDeclinesAndTransientStats() throws Exception {
        for(boolean flat:new boolean[]{false,true}) {
            Path root=TestDatasets.write(temp.resolve("empty"+flat),new TestDatasets.Spec(),List.of(List.of(
                    TestDatasets.TestRow.of(flat?"a":"a/x",0),TestDatasets.TestRow.of(flat?"b":"b/y",1))));
            try(Dataset plain=Dataset.open(root); Dataset indexed=Dataset.open(root)) {
                assertFalse(indexed.buildListIndex(new ListIndexOptions(1,1,0),Cancellation.none()).hasIndex());
                for(String cursor:List.of("a/","a","missing/")) {
                    var q=new ListQuery(Bytes.empty(),(byte)'/',Optional.empty(),10,List.of(),Optional.of(new Cursor.CommonPrefix(Bytes.utf8(cursor))));
                    var got=read(indexed,q); assertEquals(plain.list(q),got.entries()); var st=got.stats();
                    assertEquals(0,st.listIndexSkips()+st.listIndexDelimiter()+st.listIndexMissingGroup()+st.listIndexDensePage()+st.listIndexUnsafeRun());
                }
            }
            try(Dataset d=Dataset.open(root); var request=d.begin(Cancellation.none())) {
                var part=d.parts().get(0,request.ctx);
                part.keyIndex(0,request.ctx,false);
                assertEquals(1,request.ctx.stats.indexLoads); assertTrue(request.ctx.stats.sourceReads>0);
                part.keyIndex(0,request.ctx,false); assertEquals(2,request.ctx.stats.indexLoads);
            }
        }
    }
}
