package com.example.demo.lucene;

import lombok.Builder;
import lombok.Data;

@Builder
@Data
public class BufferRange {
    private long encodedPosition;
    private long decodedPosition;
    private int encodedLength;
}
