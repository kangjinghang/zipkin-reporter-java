/*
 * Copyright 2016-2019 The OpenZipkin Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package zipkin2.reporter;

import java.util.Arrays;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Multi-producer, multi-consumer queue that is bounded by both count and size.
 *
 * <p>This is similar to {@link java.util.concurrent.ArrayBlockingQueue} in implementation.
 */
final class ByteBoundedQueue<S> implements SpanWithSizeConsumer<S> { // 类似于 BlockingQueue，是一个既有数量限制，又有字节数限制的阻塞队列

  final ReentrantLock lock = new ReentrantLock(false);
  final Condition available = lock.newCondition();

  final int maxSize;
  final int maxBytes;

  final S[] elements;
  final int[] sizesInBytes; // 每个元素的字节数？
  int count;
  int sizeInBytes;
  int writePos; // 记录写的位置
  int readPos; // 记录读的位置
  // maxSize 是 queue 接受的最大数量，maxBytes 是 queue 接受的最大字节数
  @SuppressWarnings("unchecked") ByteBoundedQueue(int maxSize, int maxBytes) {
    this.elements = (S[]) new Object[maxSize];
    this.sizesInBytes = new int[maxSize];
    this.maxSize = maxSize;
    this.maxBytes = maxBytes;
  }

  /**
   * Returns true if the element could be added or false if it could not due to its size.
   */
  @Override public boolean offer(S next, int nextSizeInBytes) { // 添加 message 到queue 中
    lock.lock();
    try {
      if (count == maxSize) return false;// queue 是满的，则不能继续添加
      if (sizeInBytes + nextSizeInBytes > maxBytes) return false; // 消息加进队列会超出队列字节大小限制，也不能添加新message

      elements[writePos] = next ;// 将 message 放于 writePos 处
      sizesInBytes[writePos++] = nextSizeInBytes; // 将 writePos+1，sizesInBytes数组的 writePos+1 处设置 message 的大小
      // 当 writePos 到达数组尾部，则将 writePos 置为0，让下一次添加从数组头部开始
      if (writePos == maxSize) writePos = 0; // circle back to the front of the array

      count++; // 将 count 计数器加1
      sizeInBytes += nextSizeInBytes; // 更新字节总数
      // 通知其他在 lock 上等待的线程（在drainTo方法中阻塞的线程）继续竞争线程资源
      available.signal(); // alert any drainers
      return true;
    } finally {
      lock.unlock();
    }
  }
  // 提取 message 到 Consumer 中消费
  /** Blocks for up to nanosTimeout for spans to appear. Then, consume as many as possible. */
  int drainTo(SpanWithSizeConsumer<S> consumer, long nanosTimeout) {
    try {
      // This may be called by multiple threads. If one is holding a lock, another is waiting. We
      // use lockInterruptibly to ensure the one waiting can be interrupted.
      lock.lockInterruptibly();
      try {
        long nanosLeft = nanosTimeout;
        while (count == 0) {
          if (nanosLeft <= 0) return 0;
          nanosLeft = available.awaitNanos(nanosLeft); // 如果当时 queue 里没有消息，则每次等待 nanosTimeout，直到 queue 里存入消息为止
        }
        return doDrain(consumer); // 当 while 循环退出，表明 queue 中已经有新的 message 添加进来，可以消费
      } finally {
        lock.unlock();
      }
    } catch (InterruptedException e) {
      return 0;
    }
  }

  /** Clears the queue unconditionally and returns count of spans cleared. */
  int clear() { // 将所有东西清零，该方法在 Reporter 的 close 方法中会被使用。
    lock.lock();
    try {
      int result = count;
      count = sizeInBytes = readPos = writePos = 0;
      Arrays.fill(elements, null);
      return result;
    } finally {
      lock.unlock();
    }
  }

  int doDrain(SpanWithSizeConsumer<S> consumer) {
    int drainedCount = 0;
    int drainedSizeInBytes = 0;
    while (drainedCount < count) { // 提取的 message 数量总数小于 queue 里消息总数时
      S next = elements[readPos];
      int nextSizeInBytes = sizesInBytes[readPos];

      if (next == null) break;
      if (consumer.offer(next, nextSizeInBytes)) { // 尝试调用consumer.offer 方法
        drainedCount++; // 如果 offer 方法返回 true，则将 drainedCount 加1
        drainedSizeInBytes += nextSizeInBytes; // drainedSizeInBytes 加上当前消息的字节数

        elements[readPos] = null;
        if (++readPos == elements.length) readPos = 0; // circle back to the front of the array
      } else {  // 如果 offer 方法返回 false，则跳出循环
        break;
      }
    }
    count -= drainedCount; // 将 queue 的 count 减掉提取的总消息数 drainedCount
    sizeInBytes -= drainedSizeInBytes; // sizeInBytes 减去提取的总字节数 drainedSizeInBytes
    return drainedCount;
  }
}

interface SpanWithSizeConsumer<S> {
  /** Returns true if the element could be added or false if it could not due to its size. */
  boolean offer(S next, int nextSizeInBytes);
}
