package com.example.demo.listener;

import com.example.demo.lucene.Base64WrappingDecoder;
import com.example.demo.lucene.Base64WrappingEncoder;
import com.example.demo.lucene.WrappingDirectory;
import com.example.demo.util.CsvLoader;
import com.example.demo.event.WriteEvent;
import com.google.common.collect.Lists;
import lombok.extern.slf4j.Slf4j;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.MMapDirectory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
@Component
public class IndexBuilder {

    @Autowired
    private CsvLoader csvLoader;

    @Autowired
    private ApplicationEventPublisher publisher;

    @Async
    @EventListener
    public void handle(ContextRefreshedEvent event) throws IOException {
        log.info("Loading CSV");

        // Read file
        List<String[]> rows = csvLoader.loadObjectList("urbandict-word-defs.csv");

        log.info("Loaded CSV {}", rows.size());

        // Create directory
        final Directory directory = new WrappingDirectory(
                new MMapDirectory(Path.of("directory")),
                new Base64WrappingEncoder(),
                new Base64WrappingDecoder()
        );

        // Create analyzer
        final StandardAnalyzer analyzer =  new StandardAnalyzer();

        // Create index writer
        final IndexWriterConfig indexWriterConfig = new IndexWriterConfig(analyzer);
        final IndexWriter writter = new IndexWriter(directory, indexWriterConfig);

        // Partition list
        final List<List<String[]>> partitions = Lists.partition(rows, 10000);

        // Initialize counters
        int i = 0;
        int total = partitions.size();
        AtomicInteger counter = new AtomicInteger(total);

        // For every partition list
        for (List<String[]> partition : partitions) {

            // Create a write event
            final WriteEvent writeEvent =  WriteEvent.builder()
                    .directory(directory)
                    .analyzer(analyzer)
                    .indexWriter(writter)
                    .partition(partition)
                    .index(++i)
                    .total(total)
                    .count(counter)
                    .build();

            // Publish event
            publisher.publishEvent(writeEvent);
        }

    }

}
