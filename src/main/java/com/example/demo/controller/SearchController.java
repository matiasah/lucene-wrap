package com.example.demo.controller;

import com.example.demo.entity.SearchIndex;
import com.example.demo.pojo.DictionaryWord;
import org.apache.lucene.document.Document;
import org.apache.lucene.queryparser.classic.MultiFieldQueryParser;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.TopDocs;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Lazy;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

@RestController
public class SearchController {

    @Lazy
    @Autowired
    private SearchIndex searchIndex;

    @PostMapping("/search")
    public List<DictionaryWord> search(@RequestBody String queryString) throws IOException, ParseException {
        // Create searcher
        IndexSearcher searcher = new IndexSearcher(searchIndex.getIndexReader());

        // Create query
        MultiFieldQueryParser parser = new MultiFieldQueryParser(
                new String[]{"word", "author", "definition"},
                searchIndex.getAnalyzer()
        );

        // Search
        TopDocs topDocs = searcher.search(parser.parse(queryString), 10);

        // Create list of results
        List<DictionaryWord> results = new ArrayList<>(topDocs.scoreDocs.length);

        // For every top doc
        for (int i = 0; i < topDocs.scoreDocs.length; i++) {

            // Get document
            Document document = searcher.doc(topDocs.scoreDocs[i].doc);

            // Create dictionary word
            DictionaryWord dictionaryWord = DictionaryWord.builder()
                    .word(document.get("word"))
                    .author(document.get("author"))
                    .definition(document.get("definition"))
                    .build();

            // Add to list
            results.add(dictionaryWord);
        }

        // Return results
        return results;
    }

}
