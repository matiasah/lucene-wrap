package com.example.demo.listener;

import com.example.demo.event.IndexPushEvent;
import com.example.demo.event.WriteEvent;
import lombok.extern.slf4j.Slf4j;
import org.apache.lucene.document.Document;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;

import java.io.IOException;

@Slf4j
@Component
public class WriteEventListener {

    @Autowired
    private ApplicationEventPublisher publisher;

    @Async
    @EventListener
    public void handle(WriteEvent event) throws IOException {
        // Log index
        log.info("Processing event number {} of {} with {} rows", event.getIndex(), event.getTotal(), event.getPartition().size());

        // For every row
        for (String [] row : event.getPartition()) {

            // Create document
            Document document = new Document();

            // Set word id
            document.add(new org.apache.lucene.document.LongPoint("id", Long.parseLong(row[0])));

            // Set word
            document.add(new org.apache.lucene.document.TextField("word", row[1], org.apache.lucene.document.Field.Store.YES));

            // Set author
            document.add(new org.apache.lucene.document.TextField("author", row[4], org.apache.lucene.document.Field.Store.YES));

            // Set definition
            document.add(new org.apache.lucene.document.TextField("definition", row[5], org.apache.lucene.document.Field.Store.YES));

            // Write document
            event.getIndexWriter().addDocument(document);
        }

        // Log commit
        log.info("Finished building index for event number {} of {} ({} remaining)", event.getIndex(), event.getTotal(), event.getCount().get());

        // If counter reached zero
        if (event.getCount().decrementAndGet() == 0) {

            // Log write
            log.info("Writing index");

            // Close index writer
            event.getIndexWriter().close();

            // Log finished
            log.info("Finished writing index");

            // Create index push event
            IndexPushEvent indexPushEvent = IndexPushEvent.builder()
                    .directory(event.getDirectory())
                    .analyzer(event.getAnalyzer())
                    .build();

            // Log publish
            log.info("Publishing index push event");

            // Publish index push event
            publisher.publishEvent(indexPushEvent);
        }

    }

}
