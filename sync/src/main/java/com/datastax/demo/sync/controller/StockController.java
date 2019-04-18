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
package com.datastax.demo.sync.controller;

import static org.springframework.http.MediaType.APPLICATION_JSON_VALUE;

import com.datastax.demo.sync.dto.ResultsPage;
import com.datastax.demo.sync.dto.StockDto;
import com.datastax.demo.sync.model.Stock;
import com.datastax.demo.sync.repository.StockRepository;
import java.net.URI;
import java.nio.ByteBuffer;
import java.time.Instant;
import org.springframework.http.ResponseEntity;
import org.springframework.lang.NonNull;
import org.springframework.lang.Nullable;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/v1/stocks")
public class StockController {

  private static final int PAGE_SIZE = 10;

  private final StockRepository stockRepository;

  private final StockUriHelper uriHelper;

  public StockController(StockRepository stockRepository, StockUriHelper uriHelper) {
    this.stockRepository = stockRepository;
    this.uriHelper = uriHelper;
  }

  @GetMapping("/{symbol}")
  public ResultsPage<StockDto> listStocks(
      @PathVariable(name = "symbol") @NonNull String symbol,
      @RequestParam(name = "start", required = false) @Nullable Instant start,
      @RequestParam(name = "end", required = false) @Nullable Instant end,
      @RequestParam(name = "page", required = false) @Nullable ByteBuffer pagingState) {
    var results = stockRepository.findAllBySymbol(symbol, start, end, PAGE_SIZE, pagingState);
    var stocks =
        results.getResults().map(stock -> new StockDto(stock, uriHelper.buildDetailsUri(stock)));
    URI nextPageUri = uriHelper.buildNextPageUri(results);
    return new ResultsPage<>(stocks, nextPageUri);
  }

  @GetMapping("/{symbol}/{date}")
  public ResponseEntity<?> findStock(
      @PathVariable("symbol") String symbol, @PathVariable("date") Instant date) {
    return stockRepository
        .findById(symbol, date)
        .map(ResponseEntity::ok)
        .orElse(ResponseEntity.notFound().build());
  }

  @PostMapping(value = "", consumes = APPLICATION_JSON_VALUE, produces = APPLICATION_JSON_VALUE)
  public ResponseEntity<?> createStock(@RequestBody Stock stock) {
    stockRepository.save(stock);
    URI location = uriHelper.buildDetailsUri(stock);
    return ResponseEntity.created(location).body(stock);
  }

  @PutMapping("/{symbol}/{date}")
  public ResponseEntity<?> updateStock(
      @PathVariable("symbol") String symbol,
      @PathVariable("date") Instant date,
      @RequestBody Stock stock) {
    return stockRepository
        .findById(symbol, date)
        .map(current -> new Stock(current.getSymbol(), current.getDate(), stock.getValue()))
        .map(
            toUpdate -> {
              stockRepository.save(toUpdate);
              return toUpdate;
            })
        .map(ResponseEntity::ok)
        .orElse(ResponseEntity.notFound().build());
  }

  @DeleteMapping("/{symbol}/{date}")
  public void deleteStock(
      @PathVariable("symbol") String symbol, @PathVariable("date") Instant date) {
    stockRepository.deleteById(symbol, date);
  }
}
