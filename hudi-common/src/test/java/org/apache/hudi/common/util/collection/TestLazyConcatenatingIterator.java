/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.common.util.collection;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

public class TestLazyConcatenatingIterator {

  int initTimes;
  int closeTimes;

  private class MockClosableIterator implements ClosableIterator {

    Iterator<Integer> iterator;

    public MockClosableIterator(Iterator<Integer> iterator) {
      initTimes++;
      this.iterator = iterator;
    }

    @Override
    public void close() {
      closeTimes++;
    }

    @Override
    public boolean hasNext() {
      return iterator.hasNext();
    }

    @Override
    public Object next() {
      return iterator.next();
    }
  }

  // Simple test for iterator concatenation
  @Test
  public void testConcatBasic() {
    Supplier<ClosableIterator<Integer>> i1 = () -> new MockClosableIterator(Arrays.asList(5, 3, 2, 1).iterator());
    Supplier<ClosableIterator<Integer>> i2 = () -> new MockClosableIterator(Collections.emptyIterator()); // empty iterator
    Supplier<ClosableIterator<Integer>> i3 = () -> new MockClosableIterator(Collections.singletonList(3).iterator());

    LazyConcatenatingIterator<Integer> ci = new LazyConcatenatingIterator<>(Arrays.asList(i1, i2, i3));

    assertEquals(0, initTimes);

    List<Integer> allElements = new ArrayList<>();
    int count = 0;
    while (ci.hasNext()) {
      count++;
      if (count == 1) {
        assertEquals(1, initTimes);
        assertEquals(0, closeTimes);
      }
      if (count == 5) {
        assertEquals(3, initTimes);
        assertEquals(2, closeTimes);
      }
      allElements.add(ci.next());
    }

    assertEquals(3, initTimes);
    assertEquals(3, closeTimes);

    assertEquals(5, allElements.size());
    assertEquals(Arrays.asList(5, 3, 2, 1, 3), allElements);
  }

  @Test
  public void testConcatError() {
    Supplier<ClosableIterator<Integer>> i1 = () -> new MockClosableIterator(Collections.emptyIterator()); // empty iterator

    LazyConcatenatingIterator<Integer> ci = new LazyConcatenatingIterator<>(Collections.singletonList(i1));
    assertFalse(ci.hasNext());
    try {
      ci.next();
      fail("expected error for empty iterator");
    } catch (IllegalStateException e) {
      //
    }
  }

  @Test
  public void testCloseAfterPartialIterationDoesNotOpenRemainingIterators() {
    Supplier<ClosableIterator<Integer>> first =
        () -> new MockClosableIterator(Arrays.asList(1, 2).iterator());
    Supplier<ClosableIterator<Integer>> second =
        () -> new MockClosableIterator(Collections.singletonList(3).iterator());
    Supplier<ClosableIterator<Integer>> third =
        () -> new MockClosableIterator(Collections.singletonList(4).iterator());

    LazyConcatenatingIterator<Integer> iterator =
        new LazyConcatenatingIterator<>(Arrays.asList(first, second, third));
    assertEquals(0, initTimes, "constructing the chain must not open a child iterator");

    assertEquals(1, iterator.next());
    assertEquals(1, initTimes, "partial iteration must open only the current child iterator");
    assertEquals(0, closeTimes);

    iterator.close();
    assertEquals(1, initTimes, "closing early must not initialize the remaining child iterators");
    assertEquals(1, closeTimes, "closing early must close the one child iterator that was opened");
  }

  @Test
  public void testThousandsOfEmptyChildrenKeepAtMostOneReaderOpen() {
    List<Supplier<ClosableIterator<Integer>>> suppliers = new ArrayList<>();
    for (int i = 0; i < 10000; i++) {
      suppliers.add(() -> {
        assertEquals(initTimes, closeTimes, "previous reader must close before the next one opens");
        return new MockClosableIterator(Collections.emptyIterator());
      });
    }
    suppliers.add(() -> {
      assertEquals(initTimes, closeTimes);
      return new MockClosableIterator(Collections.singletonList(42).iterator());
    });
    try (LazyConcatenatingIterator<Integer> iterator = new LazyConcatenatingIterator<>(suppliers)) {
      assertEquals(0, initTimes);
      assertTrue(iterator.hasNext(), "long empty runs must not recurse or stop before the nonempty reader");
      assertEquals(10001, initTimes);
      assertEquals(10000, closeTimes);
      assertEquals(42, iterator.next());
      assertFalse(iterator.hasNext());
      assertEquals(initTimes, closeTimes);
    }
    assertEquals(10001, closeTimes, "closing an exhausted chain must not double-close its last reader");
  }

  @Test
  public void testCloseBeforeReadingIsIdempotentAndDoesNotOpenChildren() {
    LazyConcatenatingIterator<Integer> iterator = new LazyConcatenatingIterator<>(Collections.singletonList(
        () -> new MockClosableIterator(Collections.singletonList(1).iterator())));
    iterator.close();
    iterator.close();
    assertFalse(iterator.hasNext());
    assertThrows(IllegalStateException.class, iterator::next);
    assertEquals(0, initTimes);
    assertEquals(0, closeTimes);
  }

  @Test
  public void testFailingSupplierDoesNotDoubleClosePreviousChild() {
    Supplier<ClosableIterator<Integer>> first = () -> new MockClosableIterator(Collections.singletonList(1).iterator());
    Supplier<ClosableIterator<Integer>> failing = () -> {
      throw new IllegalStateException("reader open failed");
    };
    LazyConcatenatingIterator<Integer> iterator = new LazyConcatenatingIterator<>(Arrays.asList(first, failing));
    assertEquals(1, iterator.next());
    assertThrows(IllegalStateException.class, iterator::hasNext);
    assertEquals(1, closeTimes);
    iterator.close();
    assertEquals(1, closeTimes, "failure to open the next reader must not close the exhausted reader twice");
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  public void testReadFailurePreservesCauseAndClosesOnlyOpenedChild(boolean failHasNext) {
    IllegalStateException failure = new IllegalStateException("injected reader failure");
    Supplier<ClosableIterator<Integer>> broken = () -> new MockClosableIterator(Collections.singletonList(1).iterator()) {
      @Override
      public boolean hasNext() {
        if (failHasNext) {
          throw failure;
        }
        return true;
      }

      @Override
      public Object next() {
        throw failure;
      }
    };
    Supplier<ClosableIterator<Integer>> unopened = () -> new MockClosableIterator(Collections.singletonList(2).iterator());
    LazyConcatenatingIterator<Integer> iterator = new LazyConcatenatingIterator<>(Arrays.asList(broken, unopened));
    assertSame(failure, assertThrows(IllegalStateException.class, iterator::next));
    iterator.close();
    iterator.close();
    assertEquals(1, initTimes);
    assertEquals(1, closeTimes);
    assertFalse(iterator.hasNext());
  }

  @Test
  public void testCloseFailureStillDisposesChainWithoutOpeningRemainingReaders() {
    IllegalStateException failure = new IllegalStateException("injected close failure");
    Supplier<ClosableIterator<Integer>> broken = () -> new MockClosableIterator(Collections.singletonList(1).iterator()) {
      @Override
      public void close() {
        super.close();
        throw failure;
      }
    };
    Supplier<ClosableIterator<Integer>> unopened = () -> new MockClosableIterator(Collections.singletonList(2).iterator());
    LazyConcatenatingIterator<Integer> iterator = new LazyConcatenatingIterator<>(Arrays.asList(broken, unopened));
    assertEquals(1, iterator.next());
    assertSame(failure, assertThrows(IllegalStateException.class, iterator::close));
    iterator.close();
    assertFalse(iterator.hasNext());
    assertEquals(1, initTimes, "failed close must release unopened suppliers");
    assertEquals(1, closeTimes, "cleanup must not retry a child close that already failed");
  }

}
