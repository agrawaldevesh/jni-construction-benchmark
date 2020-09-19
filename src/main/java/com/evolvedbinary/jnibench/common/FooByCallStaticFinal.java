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
package com.evolvedbinary.jnibench.common;

/**
 * Similar to {@link FooByCallStatic} but this class is marked final.
 *
 * @author <a href="mailto:adam@evolvedbinary.com">Adam Retter</a>
 */
public final class FooByCallStaticFinal extends NativeBackedObject {
    public FooByCallStaticFinal() {
        super();
        this._nativeHandle = newFoo();
    }

    public FooByCallStaticFinal(boolean ignored) {
        super();
        this._nativeHandle = 0;
    }

    @Override
    protected void disposeInternal() {
        disposeInternal(_nativeHandle);
    }

    private static native long newFoo();
    private static native void disposeInternal(final long handle);
    public static native long getStringFromJava(StringProvider provider, int numTimes);
    public static native long getUTF8StringFromJava(StringProvider provider, int numTimes);
    public static native long getBytesFromJava(StringProvider provider, int numTimes);
    public static native long getStaticBytesFromJava(int numTimes);
    public static native long getStaticNativePreallocatedFromJava(int numTimes);
    public static native long getStaticNativePreallocatedWithSizeFromJava(int numTimes);
    public static native long getStaticByteNativeCallerAllocated(int numTimes);
    public static native long getStaticNativeFromJava(int numTimes);
    public static native long getStaticNativeLongByArg(int numTimes);
    public static native long getStaticNativeLongByReturn(int numTimes);
    public static native long getStringFromJavaAsNativeUTF16(StringProvider provider, int numTimes);
}
