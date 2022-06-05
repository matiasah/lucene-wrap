package com.example.demo.entity;

import lombok.Builder;
import lombok.Data;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.store.Directory;

@Builder
@Data
public class SearchIndex {
    private Directory directory;
    private Analyzer analyzer;
    private IndexReader indexReader;
}
