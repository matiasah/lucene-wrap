package com.example.demo.listener;

import com.example.demo.entity.SearchIndex;
import com.example.demo.event.IndexPushEvent;
import lombok.extern.slf4j.Slf4j;
import org.apache.lucene.index.DirectoryReader;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.beans.factory.support.BeanDefinitionBuilder;
import org.springframework.beans.factory.support.DefaultListableBeanFactory;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;

import java.io.IOException;

@Slf4j
@Component
public class IndexPushEventListener {

    @Autowired
    private DefaultListableBeanFactory beanFactory;

    @Async
    @EventListener
    public void handle(IndexPushEvent event) throws IOException {

        // Log creation
        log.info("Creating index bean");

        // Create SearchIndex
        SearchIndex searchIndex = SearchIndex.builder()
                .directory(event.getDirectory())
                .analyzer(event.getAnalyzer())
                .indexReader(DirectoryReader.open(event.getDirectory()))
                .build();

        // Create BeanDefinition
        BeanDefinition beanDefinition = BeanDefinitionBuilder
                .rootBeanDefinition(SearchIndex.class, () -> searchIndex)
                .getBeanDefinition();

        // Register BeanDefinition
        beanFactory.registerBeanDefinition("searchIndex", beanDefinition);

        // Log registration
        log.info("Index bean registered");
    }

}
