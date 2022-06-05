package com.example.demo.event;

import lombok.Builder;
import lombok.Data;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.store.Directory;

@Builder
@Data
public class IndexPushEvent {
    private Directory directory;
    private Analyzer analyzer;
}
