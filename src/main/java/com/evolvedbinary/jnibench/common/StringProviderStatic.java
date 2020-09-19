package com.evolvedbinary.jnibench.common;

import sun.misc.Unsafe;

import java.util.ArrayList;
import java.util.List;

public class StringProviderStatic {
    private final static Unsafe UNSAFE = Platform.getUnsafe();
    private static StringProvider provider = null;
    private static final ArrayList<Long> preAllocated = new ArrayList<>();

    private static void destroy() {
        for (int i = 0; i < preAllocated.size(); ++i) {
            Platform.freeMemory(preAllocated.get(i));
        }
        preAllocated.clear();
    }

    public static void setup(StringProvider providerGiven) {
        destroy();
        provider = providerGiven;
        int numStrings = provider.getNumStrings();
        preAllocated.ensureCapacity(numStrings);
        if (!preAllocated.isEmpty()) {
            throw new IllegalStateException("Expected nothing allocated");
        }
        for (int i = 0; i < numStrings; ++i) {
            byte[] b = getByteString(i);
            long l = allocateNative(b);
            preAllocated.add(l);
        }
    }

    private static long allocateNative(byte[] arr) {
        long ret = Platform.allocateMemory(arr.length);
        Platform.copyMemory(ret, Platform.BYTE_ARRAY_OFFSET, null, ret, arr.length);
        return ret;
    }

    public static byte[] getByteString(int index) {
        return provider.getByteString(index);
    }

    public static int getByteLength(int index) {
        return getByteString(index).length;
    }

    public static long getByteNativePreallocated(int index) {
        if (preAllocated.size() == 0) {
            throw new IllegalStateException("Unallocated preallocated");
        }
        index = index % preAllocated.size();
        return preAllocated.get(index);
    }

    public static void getByteNativePreallocatedWithSize(int index, long structAddr) {
        if (structAddr <= 0) {
            throw new IllegalArgumentException(Long.toString(structAddr));
        }
        long addr = getByteNativePreallocated(index);
        long size = getByteLength(index);
        UNSAFE.putLong(structAddr, addr);
        UNSAFE.putLong(structAddr + 8L, size);
    }

    public static int getByteNativeCallerAllocated(int index, long addr, int capacity) {
        if (addr <= 0) {
            throw new IllegalArgumentException(Long.toString(addr));
        }
        byte[] actual = getByteString(index);
        int len = actual.length;
        if (len > capacity) {
            return -len;
        }
        Platform.copyMemory(actual, Platform.BYTE_ARRAY_OFFSET, null, addr, len);
        return len;
    }

    public static long getByteNativeAllocate(int index) {
        return allocateNative(getByteString(index));
    }

    public static void freeByteNative(long addr) {
        Platform.freeMemory(addr);
    }

    public static long getStaticNativeLongByReturn(int index) {
        return index * index + 1;
    }

    public static void getStaticNativeLongByArg(int index, long addr) {
        UNSAFE.putLong(addr, getStaticNativeLongByReturn(index));
    }
}
