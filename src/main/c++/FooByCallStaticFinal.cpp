/**
 * Copyright © 2016, Evolved Binary Ltd
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *     * Redistributions of source code must retain the above copyright
 *       notice, this list of conditions and the following disclaimer.
 *     * Redistributions in binary form must reproduce the above copyright
 *       notice, this list of conditions and the following disclaimer in the
 *       documentation and/or other materials provided with the distribution.
 *     * Neither the name of the <organization> nor the
 *       names of its contributors may be used to endorse or promote products
 *       derived from this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED. IN NO EVENT SHALL <COPYRIGHT HOLDER> BE LIABLE FOR ANY
 * DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
 * (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */
#include <cstring>
#include <cstdint>
#include <limits>
#include <jni.h>
#include <vector>
#include "com_evolvedbinary_jnibench_common_FooByCallStaticFinal.h"
#include "Foo.h"

constexpr long ERROR_CODE = 0xf00dBeefDeadBeef;

#define ERROR_IF_EXCEPTION_PENDING(env)          \
  do {                                           \
    jthrowable exc = (env)->ExceptionOccurred(); \
    if (exc != nullptr) {                        \
      return ERROR_CODE;                                 \
    }                                            \
  } while (false)

jlong Java_com_evolvedbinary_jnibench_common_FooByCallStaticFinal_newFoo(JNIEnv* env, jclass jcls) {
  jnibench::Foo* foo = new jnibench::Foo();
  return reinterpret_cast<jlong>(foo);
}

void Java_com_evolvedbinary_jnibench_common_FooByCallStaticFinal_disposeInternal(JNIEnv* env, jclass jcls, jlong handle) {
    delete reinterpret_cast<jnibench::Foo*>(handle);
}

jlong Java_com_evolvedbinary_jnibench_common_FooByCallStaticFinal_getStringFromJava(JNIEnv* env, jclass jcls, jobject obj, jint numtimes) {
    int numTimes = static_cast<int>(numtimes);
    long accum = 1;
    jclass objClass = env->FindClass("com/evolvedbinary/jnibench/common/StringProvider");
    ERROR_IF_EXCEPTION_PENDING(env);
    jmethodID method_handle = env->GetMethodID(objClass, "getString", "(I)Ljava/lang/String;");
    ERROR_IF_EXCEPTION_PENDING(env);
    for (int i = 0; i < numTimes; ++i) {
      jstring jfilename = static_cast<jstring>(env->CallObjectMethod(obj, method_handle, i));
      ERROR_IF_EXCEPTION_PENDING(env);
      if (jfilename == nullptr) {
        return ERROR_CODE;
      }
      const char* filename = env->GetStringUTFChars(jfilename, nullptr);
      if (filename == nullptr) {
        return ERROR_CODE;
      }
      accum += strlen(filename);
      env->ReleaseStringUTFChars(jfilename, filename);
    }
    return static_cast<jlong>(accum);
}

jlong Java_com_evolvedbinary_jnibench_common_FooByCallStaticFinal_getUTF8StringFromJava(JNIEnv* env, jclass jcls, jobject obj, jint numtimes) {
    int numTimes = static_cast<int>(numtimes);
    long accum = 1;
    long scans = 1;
    jclass objClass = env->FindClass("com/evolvedbinary/jnibench/common/StringProvider");
    ERROR_IF_EXCEPTION_PENDING(env);
    jmethodID method_handle = env->GetMethodID(objClass, "getUTF8String", "(I)Lcom/evolvedbinary/jnibench/common/UTF8String;");
    ERROR_IF_EXCEPTION_PENDING(env);
    jclass utf8ObjClass = env->FindClass("com/evolvedbinary/jnibench/common/UTF8String");
    ERROR_IF_EXCEPTION_PENDING(env);
    jmethodID bytes_method_handle = env->GetMethodID(utf8ObjClass, "getBytes", "()[B");
    ERROR_IF_EXCEPTION_PENDING(env);
    for (int i = 0; i < numTimes; ++i) {
      jobject utf8String = env->CallObjectMethod(obj, method_handle, i);
      ERROR_IF_EXCEPTION_PENDING(env);
      if (utf8String == nullptr) {
        return ERROR_CODE;
      }
      jbyteArray bytesObj = static_cast<jbyteArray>(env->CallObjectMethod(utf8String, bytes_method_handle));
      jbyte* chars = env->GetByteArrayElements(bytesObj, nullptr);
      int size = static_cast<int>(env->GetArrayLength(bytesObj));
      jbyte scan = static_cast<jbyte>(0);
      for (int j = 0; j < size; ++j) {
        scan ^= chars[j];
      }
      accum += size;
      scans += scan;
      env->ReleaseByteArrayElements(bytesObj, chars, JNI_ABORT);
    }
    return static_cast<jlong>(accum & scans);
}

jlong Java_com_evolvedbinary_jnibench_common_FooByCallStaticFinal_getBytesFromJava(JNIEnv* env, jclass jcls, jobject obj, jint numtimes) {
    int numTimes = static_cast<int>(numtimes);
    long accum = 1;
    long scans = 1;
    jclass objClass = env->FindClass("com/evolvedbinary/jnibench/common/StringProvider");
    ERROR_IF_EXCEPTION_PENDING(env);
    jmethodID method_handle = env->GetMethodID(objClass, "getByteString", "(I)[B");
    ERROR_IF_EXCEPTION_PENDING(env);
    for (int i = 0; i < numTimes; ++i) {
      jbyteArray bytesObj = static_cast<jbyteArray>(env->CallObjectMethod(obj, method_handle, i));
      ERROR_IF_EXCEPTION_PENDING(env);
      if (bytesObj == nullptr) {
        return ERROR_CODE;
      }
      jbyte* chars = env->GetByteArrayElements(bytesObj, nullptr);
      int size = static_cast<int>(env->GetArrayLength(bytesObj));
      jbyte scan = static_cast<jbyte>(0);
      for (int j = 0; j < size; ++j) {
        scan ^= chars[j];
      }
      accum += size;
      scans += scan;
      env->ReleaseByteArrayElements(bytesObj, chars, JNI_ABORT);
    }
    return static_cast<jlong>(accum & scans);
}


jlong Java_com_evolvedbinary_jnibench_common_FooByCallStaticFinal_getStaticBytesFromJava(JNIEnv* env, jclass jcls, jint numtimes) {
    int numTimes = static_cast<int>(numtimes);
    long accum = 1;
    long scans = 1;
    jclass objClass = env->FindClass("com/evolvedbinary/jnibench/common/StringProviderStatic");
    ERROR_IF_EXCEPTION_PENDING(env);
    jmethodID method_handle = env->GetStaticMethodID(objClass, "getByteString", "(I)[B");
    ERROR_IF_EXCEPTION_PENDING(env);
    for (int i = 0; i < numTimes; ++i) {
      jbyteArray bytesObj = static_cast<jbyteArray>(env->CallStaticObjectMethod(objClass, method_handle, i));
      ERROR_IF_EXCEPTION_PENDING(env);
      if (bytesObj == nullptr) {
        return ERROR_CODE;
      }
      jbyte* chars = env->GetByteArrayElements(bytesObj, nullptr);
      int size = static_cast<int>(env->GetArrayLength(bytesObj));
      jbyte scan = static_cast<jbyte>(0);
      for (int j = 0; j < size; ++j) {
        scan ^= chars[j];
      }
      accum += size;
      scans += scan;
      env->ReleaseByteArrayElements(bytesObj, chars, JNI_ABORT);
    }
    return static_cast<jlong>(accum & scans);
}

jlong Java_com_evolvedbinary_jnibench_common_FooByCallStaticFinal_getStaticNativePreallocatedFromJava(JNIEnv* env, jclass jcls, jint numtimes) {
    int numTimes = static_cast<int>(numtimes);
    long accum = 1;
    long scans = 1;
    jclass objClass = env->FindClass("com/evolvedbinary/jnibench/common/StringProviderStatic");
    ERROR_IF_EXCEPTION_PENDING(env);
    jmethodID method_handle = env->GetStaticMethodID(objClass, "getByteNativePreallocated", "(I)J");
    ERROR_IF_EXCEPTION_PENDING(env);
    jmethodID length_method_handle = env->GetStaticMethodID(objClass, "getByteLength", "(I)I");
    ERROR_IF_EXCEPTION_PENDING(env);
    for (int i = 0; i < numTimes; ++i) {
      jlong nativeBytes = env->CallStaticLongMethod(objClass, method_handle, i);
      ERROR_IF_EXCEPTION_PENDING(env);
      if (nativeBytes <= 0) {
        return ERROR_CODE;
      }
      jint nativeBytesLength = env->CallStaticIntMethod(objClass, length_method_handle, i);
      ERROR_IF_EXCEPTION_PENDING(env);
      if (nativeBytesLength <= 0) {
        return ERROR_CODE;
      }
      jbyte* chars = reinterpret_cast<jbyte*>(nativeBytes);
      int size = nativeBytesLength;
      jbyte scan = static_cast<jbyte>(0);
      for (int j = 0; j < size; ++j) {
        scan ^= chars[j];
      }
      accum += size;
      scans += scan;
    }
    return static_cast<jlong>(accum & scans);
}

jlong Java_com_evolvedbinary_jnibench_common_FooByCallStaticFinal_getStaticNativePreallocatedWithSizeFromJava(JNIEnv* env, jclass jcls, jint numtimes) {
    int numTimes = static_cast<int>(numtimes);
    long accum = 1;
    long scans = 1;
    jclass objClass = env->FindClass("com/evolvedbinary/jnibench/common/StringProviderStatic");
    ERROR_IF_EXCEPTION_PENDING(env);
    jmethodID method_handle = env->GetStaticMethodID(objClass, "getByteNativePreallocatedWithSize", "(IJ)V");
    ERROR_IF_EXCEPTION_PENDING(env);
    long withLength[2] = {0L, 0L};
    jlong addressOfWithLength = reinterpret_cast<jlong>(withLength);
    long max_int = std::numeric_limits<int>::max();
    for (int i = 0; i < numTimes; ++i) {
      env->CallStaticVoidMethod(objClass, method_handle, i, addressOfWithLength);
      ERROR_IF_EXCEPTION_PENDING(env);
      long nativeBytes = withLength[0];
      long nativeBytesLength = withLength[1];
      if (nativeBytes <= 0 || nativeBytesLength <= 0 || nativeBytesLength >= max_int) {
        return ERROR_CODE;
      }
      jbyte* chars = reinterpret_cast<jbyte*>(nativeBytes);
      int size = static_cast<int>(nativeBytesLength);
      jbyte scan = static_cast<jbyte>(0);
      for (int j = 0; j < size; ++j) {
        scan ^= chars[j];
      }
      accum += size;
      scans += scan;
    }
    return static_cast<jlong>(accum & scans);
}

jlong Java_com_evolvedbinary_jnibench_common_FooByCallStaticFinal_getStaticByteNativeCallerAllocated(JNIEnv* env, jclass jcls, jint numtimes) {
    int numTimes = static_cast<int>(numtimes);
    long accum = 1;
    long scans = 1;
    jclass objClass = env->FindClass("com/evolvedbinary/jnibench/common/StringProviderStatic");
    ERROR_IF_EXCEPTION_PENDING(env);
    jmethodID method_handle = env->GetStaticMethodID(objClass, "getByteNativeCallerAllocated", "(IJI)I");
    ERROR_IF_EXCEPTION_PENDING(env);
    std::vector<jbyte> storage;
    storage.reserve(100);
    for (int i = 0; i < numTimes; ++i) {
      jlong addr = reinterpret_cast<jlong>(storage.data());
      int capacity = static_cast<int>(storage.capacity());
      jint copied = env->CallStaticIntMethod(objClass, method_handle, i, addr, capacity);
      ERROR_IF_EXCEPTION_PENDING(env);
      if (copied < 0) {
        size_t newSize = 2 * (-copied);
        if (newSize <= 0) {
            return ERROR_CODE;
        }
        storage.reserve(newSize);
        --i;
        continue;
      }
      if (copied == 0) {
        return ERROR_CODE;
      }
      const jbyte* chars = storage.data();
      int size = static_cast<int>(copied);
      jbyte scan = static_cast<jbyte>(0);
      for (int j = 0; j < size; ++j) {
        scan ^= chars[j];
      }
      accum += size;
      scans += scan;
    }
    return static_cast<jlong>(accum & scans);
}

jlong Java_com_evolvedbinary_jnibench_common_FooByCallStaticFinal_getStaticNativeLongByReturn(JNIEnv* env, jclass jcls, jint numtimes) {
    int numTimes = static_cast<int>(numtimes);
    long accum = 1;
    jclass objClass = env->FindClass("com/evolvedbinary/jnibench/common/StringProviderStatic");
    ERROR_IF_EXCEPTION_PENDING(env);
    jmethodID method_handle = env->GetStaticMethodID(objClass, "getStaticNativeLongByReturn", "(I)J");
    ERROR_IF_EXCEPTION_PENDING(env);
    for (int i = 0; i < numTimes; ++i) {
      jlong ret = env->CallStaticLongMethod(objClass, method_handle, i);
      ERROR_IF_EXCEPTION_PENDING(env);
      accum ^= (ret << 1);
    }
    return static_cast<jlong>(accum);
}

jlong Java_com_evolvedbinary_jnibench_common_FooByCallStaticFinal_getStaticNativeLongByArg(JNIEnv* env, jclass jcls, jint numtimes) {
    int numTimes = static_cast<int>(numtimes);
    long accum = 1;
    jclass objClass = env->FindClass("com/evolvedbinary/jnibench/common/StringProviderStatic");
    ERROR_IF_EXCEPTION_PENDING(env);
    jmethodID method_handle = env->GetStaticMethodID(objClass, "getStaticNativeLongByArg", "(IJ)V");
    ERROR_IF_EXCEPTION_PENDING(env);
    long ret = 0;
    jlong retAddr = reinterpret_cast<jlong>(&ret);
    for (int i = 0; i < numTimes; ++i) {
      env->CallStaticVoidMethod(objClass, method_handle, i, retAddr);
      ERROR_IF_EXCEPTION_PENDING(env);
      accum ^= (ret << 1);
    }
    return static_cast<jlong>(accum);
}

jlong Java_com_evolvedbinary_jnibench_common_FooByCallStaticFinal_getStaticNativeFromJava(JNIEnv* env, jclass jcls, jint numtimes) {
    int numTimes = static_cast<int>(numtimes);
    long accum = 1;
    long scans = 1;
    jclass objClass = env->FindClass("com/evolvedbinary/jnibench/common/StringProviderStatic");
    ERROR_IF_EXCEPTION_PENDING(env);
    jmethodID method_handle = env->GetStaticMethodID(objClass, "getByteNativeAllocate", "(I)J");
    ERROR_IF_EXCEPTION_PENDING(env);
    jmethodID free_method_handle = env->GetStaticMethodID(objClass, "freeByteNative", "(J)V");
    ERROR_IF_EXCEPTION_PENDING(env);
    jmethodID length_method_handle = env->GetStaticMethodID(objClass, "getByteLength", "(I)I");
    ERROR_IF_EXCEPTION_PENDING(env);
    for (int i = 0; i < numTimes; ++i) {
      jlong nativeBytes = env->CallStaticLongMethod(objClass, method_handle, i);
      ERROR_IF_EXCEPTION_PENDING(env);
      if (nativeBytes <= 0) {
        return ERROR_CODE;
      }
      jint nativeBytesLength = env->CallStaticIntMethod(objClass, length_method_handle, i);
      ERROR_IF_EXCEPTION_PENDING(env);
      if (nativeBytesLength <= 0) {
        return ERROR_CODE;
      }
      jbyte* chars = reinterpret_cast<jbyte*>(nativeBytes);
      int size = nativeBytesLength;
      jbyte scan = static_cast<jbyte>(0);
      for (int j = 0; j < size; ++j) {
        scan ^= chars[j];
      }
      env->CallStaticVoidMethod(objClass, free_method_handle, nativeBytes);
      ERROR_IF_EXCEPTION_PENDING(env);
      accum += size;
      scans += scan;
    }
    return static_cast<jlong>(accum & scans);
}

jlong Java_com_evolvedbinary_jnibench_common_FooByCallStaticFinal_getStringFromJavaAsNativeUTF16(JNIEnv* env, jclass jcls, jobject obj, jint numtimes) {
    int numTimes = static_cast<int>(numtimes);
    long accum = 1;
    long scans = 1;
    jclass objClass = env->FindClass("com/evolvedbinary/jnibench/common/StringProvider");
    ERROR_IF_EXCEPTION_PENDING(env);
    jmethodID method_handle = env->GetMethodID(objClass, "getString", "(I)Ljava/lang/String;");
    ERROR_IF_EXCEPTION_PENDING(env);
    for (int i = 0; i < numTimes; ++i) {
      jstring jfilename = static_cast<jstring>(env->CallObjectMethod(obj, method_handle, i));
      ERROR_IF_EXCEPTION_PENDING(env);
      if (jfilename == nullptr) {
        return ERROR_CODE;
      }
      int size = static_cast<int>(env->GetStringLength(jfilename));
      const jchar* chars = env->GetStringChars(jfilename, nullptr);
      jchar scan = static_cast<jchar>(0);
      for (int j = 0; j < size; ++j) {
        scan ^= chars[j];
      }
      accum += size;
      scans += scan;
      env->ReleaseStringChars(jfilename, chars);
    }
    return static_cast<jlong>(accum & scans);
}
