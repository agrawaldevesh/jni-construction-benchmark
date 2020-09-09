package com.evolvedbinary.jnibench.common;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Random;

public class StringProvider {
    private final List<String> strings;
    private final List<UTF8String> utf8Strings;
    private final List<byte[]> bytes;
    private final long interesting;
    private final boolean postProcess;

    private final static String Z = "Z";
    private final static String z = "z";
    private final static String A = "A";
    private final static String a = "a";

    private final static UTF8String ua = UTF8String.fromString(a);
    private final static UTF8String uz = UTF8String.fromString(z);
    private final static UTF8String uA = UTF8String.fromString(A);
    private final static UTF8String uZ = UTF8String.fromString(Z);

    private final static byte ba = ua.getBytes()[0];
    private final static byte bz = uz.getBytes()[0];
    private final static byte bA = uA.getBytes()[0];
    private final static byte bZ = uZ.getBytes()[0];

    private String createString(Random random, int targetStringLength) {
        int leftLimit = 97; // letter 'a'
        int rightLimit = 122; // letter 'z'

        return random.ints(leftLimit, rightLimit + 1)
                .limit(targetStringLength)
                .collect(StringBuilder::new, StringBuilder::appendCodePoint, StringBuilder::append)
                .toString();
    }

    public StringProvider(int stringLength, int numStrings, boolean postProcess) {
        strings = new ArrayList<>(numStrings);
        utf8Strings = new ArrayList<>(numStrings);
        bytes = new ArrayList<>(numStrings);
        Random random = new Random();
        for (int i = 0; i < numStrings; ++i) {
            String s = createString(random, stringLength);
            UTF8String us = UTF8String.fromString(s);
            strings.add(s);
            utf8Strings.add(us);
            bytes.add(us.copy().getBytes());
        }
        interesting = strings.stream().filter((l) -> l.contains("abc")).count();
        this.postProcess = postProcess;
    }

    public long getInteresting() {
        return interesting;
    }

    private String postProcess(String input) {
        if (postProcess) {
            return input.toUpperCase(Locale.ENGLISH).replace(z, Z).replace(a, A);
        } else {
            return input;
        }
    }

    private UTF8String postProcess(UTF8String input) {
        if (postProcess) {
            return input.toUpperCase().replace(uz, uZ).replace(ua, uA);
        } else {
            return input;
        }
    }

    private byte[] toUpperCase(byte[] input) {
        byte[] output = new byte[input.length];
        for (int i = 0; i < input.length; ++i) {
            output[i] = (byte) Character.toUpperCase((int) input[i]);
        }
        return output;
    }

    private byte[] postProcess(byte[] input) {
        if (postProcess) {
            return replace(replace(toUpperCase(input), bz, bZ), ba, bA);
        } else {
            return input;
        }
    }

    private byte[] replace(byte[] input, byte source, byte target) {
        if (source == target || input.length == 0) {
            return input;
        }
        byte[] output = new byte[input.length];
        for (int i = 0; i < input.length; ++i) {
           byte b = input[i];
           output[i] = (b == source) ? target : b;
        }
        return output;
    }

    public String getString(int index) {
        int listsSize = strings.size();
        if (listsSize == 0) {
            return null;
        }
        return postProcess(strings.get(index % listsSize));
    }

    public UTF8String getUTF8String(int index) {
        int listsSize = utf8Strings.size();
        if (listsSize == 0) {
            return null;
        }
        return postProcess(utf8Strings.get(index % listsSize));
    }

    public byte[] getByteString(int index) {
        int listsSize = bytes.size();
        if (listsSize == 0) {
            return null;
        }
        return postProcess(bytes.get(index % listsSize));
    }
}
