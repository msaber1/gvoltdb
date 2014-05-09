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
 * Copyright (C) 2009-2013 Typesafe Inc. <http://www.typesafe.com>
 */
import java.util.Collection;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Queue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedTransferQueue;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;

/**
 * A lock-free concurrent {@link java.util.Queue} implementations for single-consumer multiple-producer pattern.
 * <strong>It's important is is only used for this as otherwise it is not thread-safe.</strong>
 *
 * This implementation is based on:
 * <ul>
 *   <li><a href="https://github.com/akka/akka/blob/wip-2.2.3-for-scala-2.11/akka-actor/src/main/java/akka/dispatch/
 *   AbstractNodeQueue.java">AbstractNodeQueue</a></li>
 *   <li><a href="http://www.1024cores.net/home/lock-free-algorithms/
 *   queues/non-intrusive-mpsc-node-based-queue">Non intrusive MPSC node based queue</a></li>
 * </ul>
 *
 */

@SuppressWarnings("serial")
abstract class MpscLinkedQueuePad1 extends AtomicReference<MPSCLQNode> {
    public long p0, p1, p2, p3, p4, p5, p6;
    public long p7, p8, p9, p10, p11, p12, p13, p14;
}

@SuppressWarnings("serial")
abstract class MpscLinkedQueuedWaiterStorage extends MpscLinkedQueuePad1 {
    protected volatile Thread waiter;
}

@SuppressWarnings("serial")
abstract class MpscLinkedQueuePad2 extends MpscLinkedQueuedWaiterStorage {
    public long p15, p16, p17, p18, p19, p20, p21;
    public long p22, p23, p24, p25, p26, p27, p28, p29;
}

@SuppressWarnings("serial")
abstract class MpscLinkedQueueImpl extends MpscLinkedQueuePad2 implements BlockingQueue<MPSCLQNode>, Queue<MPSCLQNode> {
    //    private static final long tailOffset;
    //    private static final Unsafe unsafe = Bits.unsafe;
    //
    //    static {
    //        try {
    //            tailOffset = unsafe.objectFieldOffset(
    //                    MpscLinkedQueueImpl.class.getDeclaredField("tail"));
    //        } catch (Throwable t) {
    //            throw new ExceptionInInitializerError(t);
    //        }
    //    }
    //
    //    // Extends AtomicReference for the "head" slot (which is the one that is appended to)
    //    // since Unsafe does not expose XCHG operation intrinsically
    //    @SuppressWarnings({ "unused", "FieldMayBeFinal" })
    //    private volatile MPSCLQNode tail;
    private MPSCLQNode tail;

    MpscLinkedQueueImpl() {
        super();
        final MPSCLQNode task = new MPSCLQNode() {};
        tail = task;
        set(task);
    }

    @Override
    public boolean add(MPSCLQNode runnable) {
        return add(runnable, true);
    }

    public boolean add(MPSCLQNode runnable, final boolean wakeBlocker) {
        MPSCLQNode node = (MPSCLQNode) runnable;
        node.setNextLazy(null);
        if (wakeBlocker) {
            getAndSet(node).setNext(node);

            final Thread waiterLocal = waiter;
            if (waiterLocal != null) {
                waiter = null;
                LockSupport.unpark(waiterLocal);
            }
        } else {
            getAndSet(node).setNextLazy(node);
        }

        return true;
    }

    @Override
    public boolean offer(MPSCLQNode runnable) {
        return add(runnable);
    }

    @Override
    public MPSCLQNode remove() {
        MPSCLQNode task = poll();
        if (task == null) {
            throw new NoSuchElementException();
        }
        return task;
    }

    @Override
    public MPSCLQNode poll() {
        final MPSCLQNode next = peekTask();
        if (next == null) {
            return null;
        }
        final MPSCLQNode ret = next;
        tail.setNextLazy(null);
        tail = next;
        return ret;
    }

    @Override
    public MPSCLQNode element() {
        final MPSCLQNode next = peekTask();
        if (next == null) {
            throw new NoSuchElementException();
        }
        return next;
    }

    @Override
    public MPSCLQNode peek() {
        final MPSCLQNode next = peekTask();
        if (next == null) {
            return null;
        }
        return next;
    }

    @Override
    public int size() {
        int count = 0;
        MPSCLQNode n = peekTask();
        for (;;) {
            if (n == null) {
                break;
            }
            count++;
            n = n.next();
        }
        return count;
    }

    //    @SuppressWarnings("unchecked")
    private MPSCLQNode peekTask() {
        return tail.next();
        //        for (;;) {
        //            //final MPSCLQNode tail = (MPSCLQNode) unsafe.getObjectVolatile(this, tailOffset);
        //            final MPSCLQNode next = tail.next();
        //            if (next != null || get() == tail) {
        //                return next;
        //            }
        //        }
    }

    @Override
    public boolean isEmpty() {
        return peek() == null;
    }

    @Override
    public boolean contains(Object o) {
        MPSCLQNode n = peekTask();
        for (;;) {
            if (n == null) {
                break;
            }
            if (n == o) {
                return true;
            }
            n = n.next();
        }
        return false;
    }

    @Override
    public Iterator<MPSCLQNode> iterator() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Object[] toArray() {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> T[] toArray(T[] a) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean remove(Object o) {
        return false;
    }

    @Override
    public boolean containsAll(Collection<?> c) {
        for (Object o: c) {
            if (!contains(o)) {
                return false;
            }
        }
        return true;
    }

    @Override
    public boolean addAll(Collection<? extends MPSCLQNode> c) {
        for (MPSCLQNode r : c) {
            add(r);
        }
        return false;
    }

    @Override
    public boolean removeAll(Collection<?> c) {
        return false;
    }

    @Override
    public boolean retainAll(Collection<?> c) {
        return false;
    }

    @Override
    public void clear() {
        for (;;) {
            if (poll() == null) {
                break;
            }
        }
    }

    @Override
    public void put(MPSCLQNode e) throws InterruptedException {
        offer(e);
    }

    @Override
    public boolean offer(MPSCLQNode e, long timeout, TimeUnit unit)
            throws InterruptedException {
        offer(e);
        return true;
    }

    @Override
    public MPSCLQNode take() throws InterruptedException {
        return awaitNotEmpty(false, 0);
    }

    @Override
    public MPSCLQNode poll(long time, TimeUnit unit) throws InterruptedException {
        return awaitNotEmpty(true, unit.toNanos(time));
    }

    private MPSCLQNode awaitNotEmpty(boolean timed, long nanos)
            throws InterruptedException {
        long lastTime = timed ? System.nanoTime() : 0L;
        Thread w = Thread.currentThread();

        for (;;) {
            MPSCLQNode retval = poll();
            if (retval != null) return retval;

            if (w.isInterrupted()) {
                throw new InterruptedException();
            }

            if (timed && nanos <= 0) {
                return null;
            }

            if (waiter == null) {
                waiter = w;                 // request unpark then recheck
            }
            else if (timed) {
                long now = System.nanoTime();
                if ((nanos -= now - lastTime) > 0)
                    LockSupport.parkNanos(this, nanos);
                lastTime = now;
            }
            else {
                LockSupport.park(this);
            }
        }
    }

    @Override
    public int remainingCapacity() {
        return Integer.MAX_VALUE;
    }

    @Override
    public int drainTo(Collection<? super MPSCLQNode> c) {
        return drainTo(c, Integer.MAX_VALUE);
    }

    @Override
    public int drainTo(Collection<? super MPSCLQNode> c, int maxElements) {
        if (c == null) throw new NullPointerException();
        if (c == this) throw new IllegalArgumentException();
        int transferred = 0;
        MPSCLQNode val = null;
        while (transferred <= maxElements && (val = poll()) != null) {
            c.add(val);
            transferred++;
        }
        return transferred;
    }
}

@SuppressWarnings("serial")
public final class MpscLinkedQueue extends MpscLinkedQueueImpl {
    public long p30, p31, p32, p33, p34, p35, p36;
    public long p37, p38, p39, p40, p41, p42, p43, p44;

    public MpscLinkedQueue() {
        super();
    }

    public static void main(String args[]) throws Exception {
//                final MpscLinkedQueue a = new MpscLinkedQueue();
//                final MpscLinkedQueue b = new MpscLinkedQueue();
        final LinkedTransferQueue<MPSCLQNode> a = new LinkedTransferQueue<MPSCLQNode>();
        final LinkedTransferQueue<MPSCLQNode> b = new LinkedTransferQueue<MPSCLQNode>();

        final int burstSize = 5;
        while (true) {
            final long end = System.currentTimeMillis() + 20000;

            final Runnable Ar = new Runnable() {
                @Override
                public void run() {
                    try {
                        long count = 0;
                        //                  b.offer(new OneTimeTask() {
                        //                      @Override
                        //                      public void run(){}
                        //                  });
                        for (int ii = 0; ii < burstSize; ii++) {
                            b.offer(new MPSCLQNode(){});
                        }
                        final long start = System.nanoTime();
                        while (System.currentTimeMillis() < end) {
                            if (a.poll() != null) {
                                count++;
                                //                          b.offer(new OneTimeTask() {
                                //                              @Override
                                //                              public void run() {}
                                //                          });
                                b.offer(new MPSCLQNode(){});
                            }
                        }
                        System.out.println("Count " + count + " Took " + ((System.nanoTime() - start) / (double)count));
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            };

            final Runnable Br = new Runnable() {
                @Override
                public void run() {
                    try {
                        while (System.currentTimeMillis() < end) {
                            if (b.poll() != null) {
                                a.offer(new MPSCLQNode(){});
                                //                            a.offer(new OneTimeTask() {
                                //                                @Override
                                //                                public void run() {}
                                //                            });
                            }
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            };

            final Thread A = new Thread(Ar);
            final Thread B = new Thread(Br);

            A.start();
            B.start();

            A.join();
            B.join();
            b.clear();
            a.clear();
        }

    }
}
