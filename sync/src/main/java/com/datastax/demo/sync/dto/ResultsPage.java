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
package com.datastax.demo.sync.dto;

import java.net.URI;
import java.util.Optional;
import java.util.stream.Stream;
import org.springframework.lang.NonNull;
import org.springframework.lang.Nullable;

@SuppressWarnings("unused")
public class ResultsPage<T> {

  private final Stream<T> results;

  @Nullable private final URI nextPage;

  public ResultsPage(Stream<T> results, @Nullable URI nextPage) {
    this.results = results;
    this.nextPage = nextPage;
  }

  @NonNull
  public Stream<T> getResults() {
    return results;
  }

  @NonNull
  public Optional<URI> getNextPage() {
    return Optional.ofNullable(nextPage);
  }
}
