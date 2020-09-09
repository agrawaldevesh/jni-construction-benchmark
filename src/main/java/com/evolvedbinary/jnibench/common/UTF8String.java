/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.evolvedbinary.jnibench.common;

import javax.annotation.Nonnull;
import java.io.*;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Map;

import static com.evolvedbinary.jnibench.common.Platform.*;

public final class UTF8String implements Comparable<UTF8String>, Externalizable, Cloneable {

  // These are only updated by readExternal() or read()
  @Nonnull
  private Object base;
  private long offset;
  private int numBytes;

  public Object getBaseObject() { return base; }
  public long getBaseOffset() { return offset; }

  /**
   * A helper class that represents a subarray of a byte[].
   * This can be used when UTF8String internal operations need read-only access of the underlying
   * byte array (or convert the underlying buffer to byte array), and the operation only holds on
   * to the byte array for a short amount of time.
   * This reduces copying in the case when this string only refers to a subarray of the underlying
   * byte array.
   */
  private static class ByteSlice {
    final byte[] bytes;
    final int offset;
    final int length;

    ByteSlice(byte[] bytes) {
      this(bytes, 0, bytes.length);
    }

    ByteSlice(byte[] bytes, int offset, int length) {
      this.bytes = bytes;
      this.offset = offset;
      this.length = length;
    }
  }

  /**
   * A char in UTF-8 encoding can take 1-4 bytes depending on the first byte which
   * indicates the size of the char. See Unicode standard in page 126, Table 3-6:
   * http://www.unicode.org/versions/Unicode10.0.0/UnicodeStandard-10.0.pdf
   *
   * Binary    Hex          Comments
   * 0xxxxxxx  0x00..0x7F   Only byte of a 1-byte character encoding
   * 10xxxxxx  0x80..0xBF   Continuation bytes (1-3 continuation bytes)
   * 110xxxxx  0xC0..0xDF   First byte of a 2-byte character encoding
   * 1110xxxx  0xE0..0xEF   First byte of a 3-byte character encoding
   * 11110xxx  0xF0..0xF7   First byte of a 4-byte character encoding
   *
   * As a consequence of the well-formedness conditions specified in
   * Table 3-7 (page 126), the following byte values are disallowed in UTF-8:
   *   C0–C1, F5–FF.
   */
  private static byte[] bytesOfCodePointInUTF8 = {
    1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, // 0x00..0x0F
    1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, // 0x10..0x1F
    1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, // 0x20..0x2F
    1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, // 0x30..0x3F
    1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, // 0x40..0x4F
    1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, // 0x50..0x5F
    1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, // 0x60..0x6F
    1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, // 0x70..0x7F
    // Continuation bytes cannot appear as the first byte
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, // 0x80..0x8F
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, // 0x90..0x9F
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, // 0xA0..0xAF
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, // 0xB0..0xBF
    0, 0, // 0xC0..0xC1 - disallowed in UTF-8
    2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, // 0xC2..0xCF
    2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, // 0xD0..0xDF
    3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, // 0xE0..0xEF
    4, 4, 4, 4, 4, // 0xF0..0xF4
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 // 0xF5..0xFF - disallowed in UTF-8
  };

  private static final boolean IS_LITTLE_ENDIAN =
      ByteOrder.nativeOrder() == ByteOrder.LITTLE_ENDIAN;

  private static final UTF8String COMMA_UTF8 = UTF8String.fromString(",");
  public static final UTF8String EMPTY_UTF8 = UTF8String.fromString("");

  /**
   * Creates an UTF8String from byte array, which should be encoded in UTF-8.
   *
   * Note: `bytes` will be hold by returned UTF8String.
   */
  public static UTF8String fromBytes(byte[] bytes) {
    if (bytes != null) {
      return new UTF8String(bytes, BYTE_ARRAY_OFFSET, bytes.length);
    } else {
      return null;
    }
  }

  /**
   * Creates an UTF8String from byte array, which should be encoded in UTF-8.
   *
   * Note: `bytes` will be hold by returned UTF8String.
   */
  public static UTF8String fromBytes(byte[] bytes, int offset, int numBytes) {
    if (bytes != null) {
      return new UTF8String(bytes, BYTE_ARRAY_OFFSET + offset, numBytes);
    } else {
      return null;
    }
  }

  /**
   * Creates an UTF8String from given address (base and offset) and length.
   */
  public static UTF8String fromAddress(Object base, long offset, int numBytes) {
    return new UTF8String(base, offset, numBytes);
  }

  /**
   * Creates an UTF8String from String.
   */
  public static UTF8String fromString(String str) {
    return str == null ? null : fromBytes(str.getBytes(StandardCharsets.UTF_8));
  }

  /**
   * Creates an UTF8String that contains `length` spaces.
   */
  public static UTF8String blankString(int length) {
    byte[] spaces = new byte[length];
    Arrays.fill(spaces, (byte) ' ');
    return fromBytes(spaces);
  }

  protected UTF8String(Object base, long offset, int numBytes) {
    this.base = base;
    this.offset = offset;
    this.numBytes = numBytes;
  }

  // for serialization
  public UTF8String() {
    this(null, 0, 0);
  }

  /**
   * Returns a ByteSlice wrapping the base object if it is a byte array (regardless of whether this
   * string uses the full array or just a subarray), or a copy of the data into byte array if the
   * underlying buffer is not a byte array.
   */
  @Nonnull
  private ByteSlice getByteSlice() {
    if (base instanceof byte[] && offset >= BYTE_ARRAY_OFFSET) {
      final byte[] bytes = (byte[]) base;

      // the offset includes an object header... this is only needed for unsafe copies
      final long arrayOffset = offset - BYTE_ARRAY_OFFSET;

      // verify that the offset and length points somewhere inside the byte array
      // and that the offset can safely be truncated to a 32-bit integer
      if ((long) bytes.length < arrayOffset + numBytes) {
        throw new ArrayIndexOutOfBoundsException();
      }

      return new ByteSlice(bytes, (int) arrayOffset, numBytes);
    } else {
      return new ByteSlice(getBytes());
    }
  }

  /**
   * Returns a {@link ByteBuffer} wrapping the base object if it is a byte array
   * or a copy of the data if the base object is not a byte array.
   *
   * Unlike getBytes this will not create a copy the array if this is a slice.
   */
  @Nonnull
  public ByteBuffer getByteBuffer() {
    ByteSlice slice = getByteSlice();
    return ByteBuffer.wrap(slice.bytes, slice.offset, slice.length);
  }

  /**
   * Returns the number of bytes for a code point with the first byte as `b`
   * @param b The first byte of a code point
   */
  private static int numBytesForFirstByte(final byte b) {
    final int offset = b & 0xFF;
    byte numBytes = bytesOfCodePointInUTF8[offset];
    return (numBytes == 0) ? 1: numBytes; // Skip the first byte disallowed in UTF-8
  }

  /**
   * Returns the number of bytes
   */
  public int numBytes() {
    return numBytes;
  }

  /**
   * Returns the number of code points in it.
   */
  public int numChars() {
    int len = 0;
    for (int i = 0; i < numBytes; i += numBytesForFirstByte(getByte(i))) {
      len += 1;
    }
    return len;
  }

  /**
   * Returns a 64-bit integer that can be used as the prefix used in sorting.
   */
  public long getPrefix() {
    // Since JVMs are either 4-byte aligned or 8-byte aligned, we check the size of the string.
    // If size is 0, just return 0.
    // If size is between 0 and 4 (inclusive), assume data is 4-byte aligned under the hood and
    // use a getInt to fetch the prefix.
    // If size is greater than 4, assume we have at least 8 bytes of data to fetch.
    // After getting the data, we use a mask to mask out data that is not part of the string.
    long p;
    long mask = 0;
    if (IS_LITTLE_ENDIAN) {
      if (numBytes >= 8) {
        p = Platform.getLong(base, offset);
      } else if (numBytes > 4) {
        p = Platform.getLong(base, offset);
        mask = (1L << (8 - numBytes) * 8) - 1;
      } else if (numBytes > 0) {
        p = (long) Platform.getInt(base, offset);
        mask = (1L << (8 - numBytes) * 8) - 1;
      } else {
        p = 0;
      }
      p = java.lang.Long.reverseBytes(p);
    } else {
      // byteOrder == ByteOrder.BIG_ENDIAN
      if (numBytes >= 8) {
        p = Platform.getLong(base, offset);
      } else if (numBytes > 4) {
        p = Platform.getLong(base, offset);
        mask = (1L << (8 - numBytes) * 8) - 1;
      } else if (numBytes > 0) {
        p = ((long) Platform.getInt(base, offset)) << 32;
        mask = (1L << (8 - numBytes) * 8) - 1;
      } else {
        p = 0;
      }
    }
    p &= ~mask;
    return p;
  }

  /**
   * Returns the underline bytes, will be a copy of it if it's part of another array.
   */
  public byte[] getBytes() {
    // avoid copy if `base` is `byte[]`
    if (offset == BYTE_ARRAY_OFFSET && base instanceof byte[]
      && ((byte[]) base).length == numBytes) {
      return (byte[]) base;
    } else {
      byte[] bytes = new byte[numBytes];
      copyMemory(base, offset, bytes, BYTE_ARRAY_OFFSET, numBytes);
      return bytes;
    }
  }

  /**
   * Returns a substring of this.
   * @param start the position of first code point
   * @param until the position after last code point, exclusive.
   * @param isEphemeral whether to copy the contents or not:
   *                    If true then expect the substring to be short-lived so don't make a copy,
   *                    otherwise make a copy.
   */
  public UTF8String substring(final int start, final int until, final boolean isEphemeral) {
    if (until <= start || start >= numBytes) {
      return EMPTY_UTF8;
    }

    int i = 0;
    int c = 0;
    while (i < numBytes && c < start) {
      i += numBytesForFirstByte(getByte(i));
      c += 1;
    }

    int j = i;
    while (i < numBytes && c < until) {
      i += numBytesForFirstByte(getByte(i));
      c += 1;
    }

    if (i > j) {
      long newOffset = offset + j;
      int newSize = i - j;
      if (isEphemeral) {
        return fromAddress(base, newOffset, newSize);
      } else {
        byte[] bytes = new byte[newSize];
        copyMemory(base, newOffset, bytes, BYTE_ARRAY_OFFSET, newSize);
        return fromBytes(bytes);
      }
    } else {
      return EMPTY_UTF8;
    }
  }

  /**
   * Returns a substring of this.
   * @param start the position of first code point
   * @param until the position after last code point, exclusive.
   */
  public UTF8String substring(final int start, final int until) {
    return substring(start, until, /* isEphemeral */ false);
  }

  public UTF8String substringSQL(int pos, int length, boolean isEphemeral) {
    // Information regarding the pos calculation:
    // Hive and SQL use one-based indexing for SUBSTR arguments but also accept zero and
    // negative indices for start positions. If a start index i is greater than 0, it
    // refers to element i-1 in the sequence. If a start index i is less than 0, it refers
    // to the -ith element before the end of the sequence. If a start index i is 0, it
    // refers to the first element.
    int len = numChars();
    // `len + pos` does not overflow as `len >= 0`.
    int start = (pos > 0) ? pos -1 : ((pos < 0) ? len + pos : 0);

    int end;
    if ((long) start + length > Integer.MAX_VALUE) {
      end = Integer.MAX_VALUE;
    } else if ((long) start + length < Integer.MIN_VALUE) {
      end = Integer.MIN_VALUE;
    } else {
      end = start + length;
    }
    return substring(start, end, isEphemeral);
  }

  public UTF8String substringSQL(int pos, int length) {
    return substringSQL(pos, length, /* isEphemeral */ false);
  }

  public UTF8String ephemeralSubstringSQL(int pos, int length) {
    return substringSQL(pos, length, /* isEphemeral */ true);
  }

  /**
   * Returns the byte at position `i`.
   */
  private byte getByte(int i) {
    return Platform.getByte(base, offset + i);
  }

  /**
   * Returns the upper case of this string
   */
  public UTF8String toUpperCase() {
    if (numBytes == 0) {
      return EMPTY_UTF8;
    }

    byte[] bytes = new byte[numBytes];
    bytes[0] = (byte) Character.toTitleCase(getByte(0));
    for (int i = 0; i < numBytes; i++) {
      byte b = getByte(i);
      if (numBytesForFirstByte(b) != 1) {
        // fallback
        return toUpperCaseSlow();
      }
      int upper = Character.toUpperCase((int) b);
      if (upper > 127) {
        // fallback
        return toUpperCaseSlow();
      }
      bytes[i] = (byte) upper;
    }
    return fromBytes(bytes);
  }

  private UTF8String toUpperCaseSlow() {
    return fromString(toString().toUpperCase());
  }

  /**
   * Returns the lower case of this string
   */
  public UTF8String toLowerCase() {
    if (numBytes == 0) {
      return EMPTY_UTF8;
    }

    byte[] bytes = new byte[numBytes];
    bytes[0] = (byte) Character.toTitleCase(getByte(0));
    for (int i = 0; i < numBytes; i++) {
      byte b = getByte(i);
      if (numBytesForFirstByte(b) != 1) {
        // fallback
        return toLowerCaseSlow();
      }
      int lower = Character.toLowerCase((int) b);
      if (lower > 127) {
        // fallback
        return toLowerCaseSlow();
      }
      bytes[i] = (byte) lower;
    }
    return fromBytes(bytes);
  }

  private UTF8String toLowerCaseSlow() {
    return fromString(toString().toLowerCase());
  }

  /**
   * Returns the title case of this string, that could be used as title.
   */
  public UTF8String toTitleCase() {
    if (numBytes == 0) {
      return EMPTY_UTF8;
    }

    byte[] bytes = new byte[numBytes];
    for (int i = 0; i < numBytes; i++) {
      byte b = getByte(i);
      if (i == 0 || getByte(i - 1) == ' ') {
        if (numBytesForFirstByte(b) != 1) {
          // fallback
          return toTitleCaseSlow();
        }
        int upper = Character.toTitleCase(b);
        if (upper > 127) {
          // fallback
          return toTitleCaseSlow();
        }
        bytes[i] = (byte) upper;
      } else {
        bytes[i] = b;
      }
    }
    return fromBytes(bytes);
  }

  private UTF8String toTitleCaseSlow() {
    String s = toString();
    StringBuilder sb = new StringBuilder(s.length()); // use exact capacity
    sb.append(s);
    sb.setCharAt(0, Character.toTitleCase(sb.charAt(0)));
    for (int i = 1; i < s.length(); i++) {
      if (sb.charAt(i - 1) == ' ') {
        sb.setCharAt(i, Character.toTitleCase(sb.charAt(i)));
      }
    }
    return fromString(sb.toString());
  }

  /**
   * Copy the bytes from the current UTF8String, and make a new UTF8String.
   * @param start the start position of the current UTF8String in bytes.
   * @param end the end position of the current UTF8String in bytes.
   * @return a new UTF8String in the position of [start, end] of current UTF8String bytes.
   */
  private UTF8String copyUTF8String(int start, int end) {
    int len = end - start + 1;
    byte[] newBytes = new byte[len];
    copyMemory(base, offset + start, newBytes, BYTE_ARRAY_OFFSET, len);
    return UTF8String.fromBytes(newBytes);
  }

  /**
   * Trims space characters (ASCII 32) from both ends of this string.
   *
   * @return this string with no spaces at the start or end
   */
  public UTF8String trim() {
    int s = 0;
    // skip all of the space (0x20) in the left side
    while (s < this.numBytes && getByte(s) == ' ') s++;
    if (s == this.numBytes) {
      // Everything trimmed
      return EMPTY_UTF8;
    }
    // skip all of the space (0x20) in the right side
    int e = this.numBytes - 1;
    while (e > s && getByte(e) == ' ') e--;
    if (s == 0 && e == numBytes - 1) {
      // Nothing trimmed
      return this;
    }
    return copyUTF8String(s, e);
  }

  /**
   * Trims whitespaces (<= ASCII 32) from both ends of this string.
   *
   * Note that, this method is the same as java's {@link String#trim}, and different from
   * {@link UTF8String#trim()} which remove only spaces(= ASCII 32) from both ends.
   *
   * @return A UTF8String whose value is this UTF8String, with any leading and trailing white
   * space removed, or this UTF8String if it has no leading or trailing whitespace.
   *
   */
  public UTF8String trimAll() {
    int s = 0;
    // skip all of the whitespaces (<=0x20) in the left side
    while (s < this.numBytes && Character.isWhitespace(getByte(s))) s++;
    if (s == this.numBytes) {
      // Everything trimmed
      return EMPTY_UTF8;
    }
    // skip all of the whitespaces (<=0x20) in the right side
    int e = this.numBytes - 1;
    while (e > s && Character.isWhitespace(getByte(e))) e--;
    if (s == 0 && e == numBytes - 1) {
      // Nothing trimmed
      return this;
    }
    return copyUTF8String(s, e);
  }

  public UTF8String reverse() {
    byte[] result = new byte[this.numBytes];

    int i = 0; // position in byte
    while (i < numBytes) {
      int len = numBytesForFirstByte(getByte(i));
      copyMemory(this.base, this.offset + i, result,
        BYTE_ARRAY_OFFSET + result.length - i - len, len);

      i += len;
    }

    return UTF8String.fromBytes(result);
  }

  public UTF8String repeat(int times) {
    if (times <= 0) {
      return EMPTY_UTF8;
    }

    byte[] newBytes = new byte[numBytes * times];
    copyMemory(this.base, this.offset, newBytes, BYTE_ARRAY_OFFSET, numBytes);

    int copied = 1;
    while (copied < times) {
      int toCopy = Math.min(copied, times - copied);
      System.arraycopy(newBytes, 0, newBytes, copied * numBytes, numBytes * toCopy);
      copied += toCopy;
    }

    return UTF8String.fromBytes(newBytes);
  }

  /**
   * Wrapper over `long` to allow result of parsing long from string to be accessed via reference.
   * This is done solely for better performance and is not expected to be used by end users.
   */
  public static class LongWrapper implements Serializable {
    public transient long value = 0;
  }

  /**
   * Wrapper over `int` to allow result of parsing integer from string to be accessed via reference.
   * This is done solely for better performance and is not expected to be used by end users.
   *
   * {@link LongWrapper} could have been used here but using `int` directly save the extra cost of
   * conversion from `long` to `int`
   */
  public static class IntWrapper implements Serializable {
    public transient int value = 0;
  }

  /**
   * Parses this UTF8String(trimmed if needed) to long.
   *
   * Note that, in this method we accumulate the result in negative format, and convert it to
   * positive format at the end, if this string is not started with '-'. This is because min value
   * is bigger than max value in digits, e.g. Long.MAX_VALUE is '9223372036854775807' and
   * Long.MIN_VALUE is '-9223372036854775808'.
   *
   * This code is mostly copied from LazyLong.parseLong in Hive.
   *
   * @param toLongResult If a valid `long` was parsed from this UTF8String, then its value would
   *                     be set in `toLongResult`
   * @return true if the parsing was successful else false
   */
  public boolean toLong(LongWrapper toLongResult) {
    return toLong(toLongResult, true);
  }

  private boolean toLong(LongWrapper toLongResult, boolean allowDecimal) {
    int offset = 0;
    while (offset < this.numBytes && Character.isWhitespace(getByte(offset))) offset++;
    if (offset == this.numBytes) return false;

    int end = this.numBytes - 1;
    while (end > offset && Character.isWhitespace(getByte(end))) end--;

    byte b = getByte(offset);
    final boolean negative = b == '-';
    if (negative || b == '+') {
      if (end - offset == 0) {
        return false;
      }
      offset++;
    }

    final byte separator = '.';
    final int radix = 10;
    final long stopValue = Long.MIN_VALUE / radix;
    long result = 0;

    while (offset <= end) {
      b = getByte(offset);
      offset++;
      if (b == separator && allowDecimal) {
        // We allow decimals and will return a truncated integral in that case.
        // Therefore we won't throw an exception here (checking the fractional
        // part happens below.)
        break;
      }

      int digit;
      if (b >= '0' && b <= '9') {
        digit = b - '0';
      } else {
        return false;
      }

      // We are going to process the new digit and accumulate the result. However, before doing
      // this, if the result is already smaller than the stopValue(Long.MIN_VALUE / radix), then
      // result * 10 will definitely be smaller than minValue, and we can stop.
      if (result < stopValue) {
        return false;
      }

      result = result * radix - digit;
      // Since the previous result is less than or equal to stopValue(Long.MIN_VALUE / radix), we
      // can just use `result > 0` to check overflow. If result overflows, we should stop.
      if (result > 0) {
        return false;
      }
    }

    // This is the case when we've encountered a decimal separator. The fractional
    // part will not change the number, but we will verify that the fractional part
    // is well formed.
    while (offset <= end) {
      byte currentByte = getByte(offset);
      if (currentByte < '0' || currentByte > '9') {
        return false;
      }
      offset++;
    }

    if (!negative) {
      result = -result;
      if (result < 0) {
        return false;
      }
    }

    toLongResult.value = result;
    return true;
  }

  /**
   * Parses this UTF8String(trimmed if needed) to int.
   *
   * Note that, in this method we accumulate the result in negative format, and convert it to
   * positive format at the end, if this string is not started with '-'. This is because min value
   * is bigger than max value in digits, e.g. Integer.MAX_VALUE is '2147483647' and
   * Integer.MIN_VALUE is '-2147483648'.
   *
   * This code is mostly copied from LazyInt.parseInt in Hive.
   *
   * Note that, this method is almost same as `toLong`, but we leave it duplicated for performance
   * reasons, like Hive does.
   *
   * @param intWrapper If a valid `int` was parsed from this UTF8String, then its value would
   *                    be set in `intWrapper`
   * @return true if the parsing was successful else false
   */
  public boolean toInt(IntWrapper intWrapper) {
    return toInt(intWrapper, true);
  }

  private boolean toInt(IntWrapper intWrapper, boolean allowDecimal) {
    int offset = 0;
    while (offset < this.numBytes && Character.isWhitespace(getByte(offset))) offset++;
    if (offset == this.numBytes) return false;

    int end = this.numBytes - 1;
    while (end > offset && Character.isWhitespace(getByte(end))) end--;

    byte b = getByte(offset);
    final boolean negative = b == '-';
    if (negative || b == '+') {
      if (end - offset == 0) {
        return false;
      }
      offset++;
    }

    final byte separator = '.';
    final int radix = 10;
    final int stopValue = Integer.MIN_VALUE / radix;
    int result = 0;

    while (offset <= end) {
      b = getByte(offset);
      offset++;
      if (b == separator && allowDecimal) {
        // We allow decimals and will return a truncated integral in that case.
        // Therefore we won't throw an exception here (checking the fractional
        // part happens below.)
        break;
      }

      int digit;
      if (b >= '0' && b <= '9') {
        digit = b - '0';
      } else {
        return false;
      }

      // We are going to process the new digit and accumulate the result. However, before doing
      // this, if the result is already smaller than the stopValue(Integer.MIN_VALUE / radix), then
      // result * 10 will definitely be smaller than minValue, and we can stop
      if (result < stopValue) {
        return false;
      }

      result = result * radix - digit;
      // Since the previous result is less than or equal to stopValue(Integer.MIN_VALUE / radix),
      // we can just use `result > 0` to check overflow. If result overflows, we should stop
      if (result > 0) {
        return false;
      }
    }

    // This is the case when we've encountered a decimal separator. The fractional
    // part will not change the number, but we will verify that the fractional part
    // is well formed.
    while (offset <= end) {
      byte currentByte = getByte(offset);
      if (currentByte < '0' || currentByte > '9') {
        return false;
      }
      offset++;
    }

    if (!negative) {
      result = -result;
      if (result < 0) {
        return false;
      }
    }
    intWrapper.value = result;
    return true;
  }

  public boolean toShort(IntWrapper intWrapper) {
    if (toInt(intWrapper)) {
      int intValue = intWrapper.value;
      short result = (short) intValue;
      return result == intValue;
    }
    return false;
  }

  public boolean toByte(IntWrapper intWrapper) {
    if (toInt(intWrapper)) {
      int intValue = intWrapper.value;
      byte result = (byte) intValue;
      return result == intValue;
    }
    return false;
  }

  /**
   * Parses UTF8String(trimmed if needed) to long. This method is used when ANSI is enabled.
   *
   * @return If string contains valid numeric value then it returns the long value otherwise a
   * NumberFormatException  is thrown.
   */
  public long toLongExact() {
    LongWrapper result = new LongWrapper();
    if (toLong(result, false)) {
      return result.value;
    }
    throw new NumberFormatException("invalid input syntax for type numeric: " + this);
  }

  /**
   * Parses UTF8String(trimmed if needed) to int. This method is used when ANSI is enabled.
   *
   * @return If string contains valid numeric value then it returns the int value otherwise a
   * NumberFormatException  is thrown.
   */
  public int toIntExact() {
    IntWrapper result = new IntWrapper();
    if (toInt(result, false)) {
      return result.value;
    }
    throw new NumberFormatException("invalid input syntax for type numeric: " + this);
  }

  public short toShortExact() {
    int value = this.toIntExact();
    short result = (short) value;
    if (result == value) {
      return result;
    }
    throw new NumberFormatException("invalid input syntax for type numeric: " + this);
  }

  public byte toByteExact() {
    int value = this.toIntExact();
    byte result = (byte) value;
    if (result == value) {
      return result;
    }
    throw new NumberFormatException("invalid input syntax for type numeric: " + this);
  }

  @Override
  public String toString() {
    final ByteSlice slice = getByteSlice();
    return new String(slice.bytes, slice.offset, slice.length, StandardCharsets.UTF_8);
  }

  @Override
  public UTF8String clone() {
    return fromBytes(getBytes());
  }

  public UTF8String copy() {
    byte[] bytes = new byte[numBytes];
    copyMemory(base, offset, bytes, BYTE_ARRAY_OFFSET, numBytes);
    return fromBytes(bytes);
  }

  @Override
  public int compareTo(@Nonnull final UTF8String other) {
    int len = Math.min(numBytes, other.numBytes);
    int wordMax = len & 0xFFFFFFF8; // (len / 8) * 8 for non-negative len
    long roffset = other.offset;
    Object rbase = other.base;
    for (int i = 0; i < wordMax; i += 8) {
      long left = getLong(base, offset + i);
      long right = getLong(rbase, roffset + i);
      if (left != right) {
        if (IS_LITTLE_ENDIAN) {
          return Long.compareUnsigned(Long.reverseBytes(left), Long.reverseBytes(right));
        } else {
          return Long.compareUnsigned(left, right);
        }
      }
    }
    for (int i = wordMax; i < len; i++) {
      // In UTF-8, the byte should be unsigned, so we should compare them as unsigned int.
      int res = (getByte(i) & 0xFF) - (Platform.getByte(rbase, roffset + i) & 0xFF);
      if (res != 0) {
        return res;
      }
    }
    return numBytes - other.numBytes;
  }

  public int compare(final UTF8String other) {
    return compareTo(other);
  }

  @Override
  public boolean equals(final Object other) {
    if (other instanceof UTF8String) {
      UTF8String o = (UTF8String) other;
      if (numBytes != o.numBytes) {
        return false;
      }
      return ByteArrayMethods.arrayEquals(base, offset, o.base, o.offset, numBytes);
    } else {
      return false;
    }
  }

  /**
   * Levenshtein distance is a metric for measuring the distance of two strings. The distance is
   * defined by the minimum number of single-character edits (i.e. insertions, deletions or
   * substitutions) that are required to change one of the strings into the other.
   */
  public int levenshteinDistance(UTF8String other) {
    // Implementation adopted from org.apache.common.lang3.StringUtils.getLevenshteinDistance

    int n = numChars();
    int m = other.numChars();

    if (n == 0) {
      return m;
    } else if (m == 0) {
      return n;
    }

    UTF8String s, t;

    if (n <= m) {
      s = this;
      t = other;
    } else {
      s = other;
      t = this;
      int swap;
      swap = n;
      n = m;
      m = swap;
    }

    int[] p = new int[n + 1];
    int[] d = new int[n + 1];
    int[] swap;

    int i, i_bytes, j, j_bytes, num_bytes_j, cost;

    for (i = 0; i <= n; i++) {
      p[i] = i;
    }

    for (j = 0, j_bytes = 0; j < m; j_bytes += num_bytes_j, j++) {
      num_bytes_j = numBytesForFirstByte(t.getByte(j_bytes));
      d[0] = j + 1;

      for (i = 0, i_bytes = 0; i < n; i_bytes += numBytesForFirstByte(s.getByte(i_bytes)), i++) {
        if (s.getByte(i_bytes) != t.getByte(j_bytes) ||
              num_bytes_j != numBytesForFirstByte(s.getByte(i_bytes))) {
          cost = 1;
        } else {
          cost = (ByteArrayMethods.arrayEquals(t.base, t.offset + j_bytes, s.base,
              s.offset + i_bytes, num_bytes_j)) ? 0 : 1;
        }
        d[i + 1] = Math.min(Math.min(d[i] + 1, p[i + 1] + 1), p[i] + cost);
      }

      swap = p;
      p = d;
      d = swap;
    }

    return p[n];
  }

  @Override
  public int hashCode() {
    return Murmur3_x86_32.hashUnsafeBytes(base, offset, numBytes, 42);
  }

  // Mimic existing logic generated by
  //   org.apache.spark.sql.execution.aggregate.HashMapGenerator.genComputeHash()
  public int computeHashForCodegen() {
    int result = 0;
    final int numBytes = numBytes();
    for (int i = 0; i < numBytes; i++) {
      int current = getByte(i);
      result = (result ^ 0x9e3779b9) + current + (result << 6) + (result >>> 2);
    }
    return result;
  }

  /**
   * Soundex mapping table
   */
  private static final byte[] US_ENGLISH_MAPPING = {'0', '1', '2', '3', '0', '1', '2', '7',
    '0', '2', '2', '4', '5', '5', '0', '1', '2', '6', '2', '3', '0', '1', '7', '2', '0', '2'};

  /**
   * Encodes a string into a Soundex value. Soundex is an encoding used to relate similar names,
   * but can also be used as a general purpose scheme to find word with similar phonemes.
   * https://en.wikipedia.org/wiki/Soundex
   */
  public UTF8String soundex() {
    if (numBytes == 0) {
      return EMPTY_UTF8;
    }

    byte b = getByte(0);
    if ('a' <= b && b <= 'z') {
      b -= 32;
    } else if (b < 'A' || 'Z' < b) {
      // first character must be a letter
      return this;
    }
    byte[] sx = {'0', '0', '0', '0'};
    sx[0] = b;
    int sxi = 1;
    int idx = b - 'A';
    byte lastCode = US_ENGLISH_MAPPING[idx];

    for (int i = 1; i < numBytes; i++) {
      b = getByte(i);
      if ('a' <= b && b <= 'z') {
        b -= 32;
      } else if (b < 'A' || 'Z' < b) {
        // not a letter, skip it
        lastCode = '0';
        continue;
      }
      idx = b - 'A';
      byte code = US_ENGLISH_MAPPING[idx];
      if (code == '7') {
        // ignore it
      } else {
        if (code != '0' && code != lastCode) {
          sx[sxi++] = code;
          if (sxi > 3) break;
        }
        lastCode = code;
      }
    }
    return UTF8String.fromBytes(sx);
  }

  public void writeExternal(ObjectOutput out) throws IOException {
    final ByteSlice slice = getByteSlice();
    out.writeInt(slice.length);
    out.write(slice.bytes, slice.offset, slice.length);
  }

  public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
    offset = BYTE_ARRAY_OFFSET;
    numBytes = in.readInt();
    base = new byte[numBytes];
    in.readFully((byte[]) base);
  }

  /**
   * Find the `str` from left to right.
   */
  private int find(UTF8String str, int start) {
    assert (str.numBytes > 0);
    while (start <= numBytes - str.numBytes) {
      if (ByteArrayMethods.arrayEquals(base, offset + start, str.base, str.offset, str.numBytes)) {
        return start;
      }
      start += 1;
    }
    return -1;
  }

  /**
   * Writes the content of this string into a memory address, identified by an object and an offset.
   * The target memory address must already been allocated, and have enough space to hold all the
   * bytes in this string.
   */
  public void writeToMemory(Object target, long targetOffset) {
    Platform.copyMemory(base, offset, target, targetOffset, numBytes);
  }


  public UTF8String replace(UTF8String search, UTF8String replace) {
    // This implementation is loosely based on commons-lang3's StringUtils.replace().
    if (numBytes == 0 || search.numBytes == 0) {
      return this;
    }
    // Find the first occurrence of the search string.
    int start = 0;
    int end = this.find(search, start);
    if (end == -1) {
      // Search string was not found, so string is unchanged.
      return this;
    }
    // At least one match was found. Estimate space needed for result.
    // The 16x multiplier here is chosen to match commons-lang3's implementation.
    int increase = Math.max(0, replace.numBytes - search.numBytes) * 16;
    final UTF8StringBuilder buf = new UTF8StringBuilder(numBytes + increase);
    while (end != -1) {
      buf.appendBytes(this.base, this.offset + start, end - start);
      buf.append(replace);
      start = end + search.numBytes;
      end = this.find(search, start);
    }
    buf.appendBytes(this.base, this.offset + start, numBytes - start);
    return buf.build();
  }
}
