/*
 * Copyright DataStax, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datastax.demo.common.dto;

import java.nio.ByteBuffer;
import java.util.Optional;
import java.util.stream.Stream;
import org.springframework.lang.NonNull;
import org.springframework.lang.Nullable;

/**
 * A simple DTO carrying a page of results and an optional paging state pointing to the next page of
 * results.
 *
 * @param <T> The type of results in this page.
 */
public class PagedResults<T> {

  @NonNull private final Stream<T> results;

  @Nullable private final ByteBuffer nextPage;

  public PagedResults(@NonNull Stream<T> results, @Nullable ByteBuffer nextPage) {
    this.results = results;
    this.nextPage = nextPage;
  }

  /** @return The current page of results, as a {@link Stream}. */
  @NonNull
  public Stream<T> getResults() {
    return results;
  }

  /**
   * @return An optional paging state pointing to the next page of results, or empty if this page is
   *     the last one.
   */
  @NonNull
  public Optional<ByteBuffer> getNextPage() {
    return Optional.ofNullable(nextPage);
  }
}
