package com.example.demo.lucene;

import org.apache.tomcat.util.codec.binary.Base64;

public class Base64WrappingEncoder implements WrappingEncoder {

    @Override
    public byte[] encode(byte[] decoded) {
        return Base64.encodeBase64(decoded);
    }

}
