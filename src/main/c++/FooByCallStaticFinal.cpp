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
#include <jni.h>
#include "com_evolvedbinary_jnibench_common_FooByCallStaticFinal.h"
#include "Foo.h"

#define ERROR_IF_EXCEPTION_PENDING(env)          \
  do {                                           \
    jthrowable exc = (env)->ExceptionOccurred(); \
    if (exc != nullptr) {                        \
      return 0L;                                 \
    }                                            \
  } while (false)

/*
 * Class:     com_evolvedbinary_jnibench_common_FooByCallStaticFinal
 * Method:    newFoo
 * Signature: ()J
 */
jlong Java_com_evolvedbinary_jnibench_common_FooByCallStaticFinal_newFoo(JNIEnv* env, jclass jcls) {
  jnibench::Foo* foo = new jnibench::Foo();
  return reinterpret_cast<jlong>(foo);
}

/*
 * Class:     com_evolvedbinary_jnibench_common_FooByCallStaticFinal
 * Method:    disposeInternal
 * Signature: (J)V
 */
void Java_com_evolvedbinary_jnibench_common_FooByCallStaticFinal_disposeInternal(JNIEnv* env, jclass jcls, jlong handle) {
    delete reinterpret_cast<jnibench::Foo*>(handle);
}

/*
 * Class:     com_evolvedbinary_jnibench_common_FooByCallStaticFinal
 * Method:    getStringFromJava
 */
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
        return 0;
      }
      const char* filename = env->GetStringUTFChars(jfilename, nullptr);
      if (filename == nullptr) {
        return 0;
      }
      accum += strlen(filename);
      env->ReleaseStringUTFChars(jfilename, filename);
    }
    return static_cast<jlong>(accum);
}

/*
 * Class:     com_evolvedbinary_jnibench_common_FooByCallStaticFinal
 * Method:    getStringFromJava
 */
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
        return 0;
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

/*
 * Class:     com_evolvedbinary_jnibench_common_FooByCallStaticFinal
 * Method:    getStringFromJava
 */
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
        return 0;
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

/*
 * Class:     com_evolvedbinary_jnibench_common_FooByCallStaticFinal
 * Method:    getStringFromJava
 */
jlong Java_com_evolvedbinary_jnibench_common_FooByCallStaticFinal_getStringFromJavaNoWork(JNIEnv* env, jclass jcls, jobject obj, jint numtimes) {
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
        return 0;
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
