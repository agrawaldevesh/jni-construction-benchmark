package com.evolvedbinary.jnibench.common;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Random;

public class StringProvider {
    private final List<String> lists;
    private final long interesting;
    private final boolean postProcess;

    private String createString(Random random, int targetStringLength) {
        int leftLimit = 97; // letter 'a'
        int rightLimit = 122; // letter 'z'

        return random.ints(leftLimit, rightLimit + 1)
                .limit(targetStringLength)
                .collect(StringBuilder::new, StringBuilder::appendCodePoint, StringBuilder::append)
                .toString();
    }

    public StringProvider(int stringLength, int numStrings, boolean postProcess) {
        lists = new ArrayList<>(numStrings);
        Random random = new Random();
        for (int i = 0; i < numStrings; ++i) {
            lists.add(createString(random, stringLength));
        }
        interesting = lists.stream().filter((l) -> l.contains("abc")).count();
        this.postProcess = postProcess;
    }

    public long getInteresting() {
        return interesting;
    }

    private String postProcess(String input) {
        if (postProcess) {
            return input.toUpperCase(Locale.ENGLISH).replace('Z', 'z').replace('A', 'a');
        } else {
            return input;
        }
    }

    public String getString(int index) {
        int listsSize = lists.size();
        if (listsSize == 0) {
            return null;
        }
        return postProcess(lists.get(index % listsSize));
    }
}
