package com.example.demo.event;

import lombok.Builder;
import lombok.Data;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.store.Directory;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

@Builder
@Data
public class WriteEvent {
    private Directory directory;
    private Analyzer analyzer;
    private IndexWriter indexWriter;
    private List<String[]> partition;
    private Integer index;
    private Integer total;
    private AtomicInteger count;
}
