package com.example.demo.util;

import com.fasterxml.jackson.databind.MappingIterator;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvParser;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;
import lombok.extern.slf4j.Slf4j;
import org.springframework.core.io.ClassPathResource;
import org.springframework.stereotype.Component;

import java.io.File;
import java.util.Collections;
import java.util.List;

@Slf4j
@Component
public class CsvLoader {

    public List<String[]> loadObjectList(String fileName) {
        try {
            final CsvMapper mapper = new CsvMapper();
            final CsvSchema bootstrapSchema = CsvSchema.emptySchema().withSkipFirstDataRow(true);
            mapper.enable(CsvParser.Feature.WRAP_AS_ARRAY);
            final File file = new ClassPathResource(fileName).getFile();
            final MappingIterator<String[]> readValues = mapper.readerFor(String[].class).with(bootstrapSchema).readValues(file);
            return readValues.readAll();
        } catch (Exception e) {
            log.error("Error occurred while loading object list from file " + fileName, e);
            return Collections.emptyList();
        }
    }

}
