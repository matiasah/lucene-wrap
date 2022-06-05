package com.example.demo.lucene;

import org.apache.tomcat.util.codec.binary.Base64;

public class Base64WrappingDecoder implements WrappingDecoder {

    @Override
    public byte[] decode(byte[] encoded) {
        return Base64.decodeBase64(encoded);
    }

}
