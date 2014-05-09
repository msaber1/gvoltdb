package io.netty_voltpatches;
/*
 * Copyright 2014 The Netty Project
 *
 * The Netty Project licenses this file to you under the Apache License,
 * version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at:
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

/**
 * <strong>It is important this will not be reused. After submitted it is not allowed to get submitted again!</strong>
 */
public abstract class MPSCLQNode {

    private static final long nextOffset;
    private static final sun.misc.Unsafe unsafe = getUnsafe();

    private static sun.misc.Unsafe getUnsafe() {
        try {
            return sun.misc.Unsafe.getUnsafe();
        } catch (SecurityException se) {
            try {
                return java.security.AccessController.doPrivileged
                        (new java.security
                                .PrivilegedExceptionAction<sun.misc.Unsafe>() {
                            @Override
                            public sun.misc.Unsafe run() throws Exception {
                                java.lang.reflect.Field f = sun.misc
                                        .Unsafe.class.getDeclaredField("theUnsafe");
                                f.setAccessible(true);
                                return (sun.misc.Unsafe) f.get(null);
                            }});
            } catch (java.security.PrivilegedActionException e) {
                throw new RuntimeException("Could not initialize intrinsics",
                        e.getCause());
            }
        }
    }

    static {
        try {
            nextOffset = unsafe.objectFieldOffset(
                    MPSCLQNode.class.getDeclaredField("tail"));
        } catch (Throwable t) {
            throw new ExceptionInInitializerError(t);
        }
    }

    @SuppressWarnings("unused")
    private volatile MPSCLQNode tail;

    @SuppressWarnings("unchecked")
    final MPSCLQNode next() {
        return (MPSCLQNode) unsafe.getObjectVolatile(this, nextOffset);
    }

    final void setNext(final MPSCLQNode newNext) {
        unsafe.putObjectVolatile(this, nextOffset, newNext);
    }

    final void setNextLazy(final MPSCLQNode newNext) {
        unsafe.putOrderedObject(this, nextOffset, newNext);
    }
}
