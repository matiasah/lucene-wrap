package com.example.demo.pojo;

import lombok.Builder;
import lombok.Data;

@Builder
@Data
public class DictionaryWord {
    private String word;
    private String author;
    private String definition;
}
