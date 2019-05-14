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

import com.datastax.demo.common.controller.StockUriHelper;
import com.datastax.demo.common.model.Stock;
import com.datastax.demo.sync.repository.SyncStockRepository;
import com.datastax.oss.driver.api.core.DriverException;
import java.net.URI;
import java.time.Instant;
import java.util.stream.Stream;
import javax.servlet.http.HttpServletRequest;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.lang.NonNull;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

/** A REST controller that performs CRUD actions on {@link Stock} instances. */
@RestController
@RequestMapping("/api/v1/stocks")
public class SyncStockController {

  private final SyncStockRepository stockRepository;

  private final StockUriHelper uriHelper;

  private final HttpServletRequest request;

  public SyncStockController(
      SyncStockRepository stockRepository, StockUriHelper uriHelper, HttpServletRequest request) {
    this.stockRepository = stockRepository;
    this.uriHelper = uriHelper;
    this.request = request;
  }

  /**
   * Creates a new stock value (POST method).
   *
   * @param stock The stock value to create.
   * @return The created stocked value.
   */
  @PostMapping("")
  public ResponseEntity<Stock> createStock(@RequestBody Stock stock) {
    stock = stockRepository.save(stock);
    URI location = uriHelper.buildDetailsUri(request, stock);
    return ResponseEntity.created(location).body(stock);
  }

  /**
   * Updates the stock value at the given path (PUT method).
   *
   * @param symbol The stock symbol to update.
   * @param date The stock date to update.
   * @param stock The new stock value.
   * @return The updated stock value.
   */
  @PutMapping("/{symbol}/{date}")
  public ResponseEntity<Stock> updateStock(
      @PathVariable("symbol") String symbol,
      @PathVariable("date") Instant date,
      @RequestBody Stock stock) {
    return stockRepository
        .findById(symbol, date)
        .map(found -> new Stock(found.getSymbol(), found.getDate(), stock.getValue()))
        .map(stockRepository::save)
        .map(ResponseEntity::ok)
        .orElse(ResponseEntity.notFound().build());
  }

  /**
   * Deletes a stock value (DELETE method).
   *
   * @param symbol The stock symbol to delete.
   * @param date The stock date to delete.
   */
  @DeleteMapping("/{symbol}/{date}")
  public void deleteStock(
      @PathVariable("symbol") String symbol, @PathVariable("date") Instant date) {
    stockRepository.deleteById(symbol, date);
  }

  /**
   * Retrieves the stock value for the given symbol and date (GET method).
   *
   * @param symbol The stock symbol to find.
   * @param date The stock date to find.
   * @return The found stock value, or empty if no stock value was found.
   */
  @GetMapping("/{symbol}/{date}")
  public ResponseEntity<Stock> findStock(
      @PathVariable("symbol") String symbol, @PathVariable("date") Instant date) {
    return stockRepository
        .findById(symbol, date)
        .map(ResponseEntity::ok)
        .orElse(ResponseEntity.notFound().build());
  }

  /**
   * Lists the available stocks for the given symbol and date range (GET method).
   *
   * @param symbol The symbol to list stocks for.
   * @param start The start of the date range (inclusive).
   * @param end The end of the date range (exclusive).
   * @param offset The zero-based index of the first result to return.
   * @param limit The maximum number of results to return.
   * @return The available stocks for the given symbol and date range.
   */
  @GetMapping("/{symbol}")
  public Stream<Stock> listStocks(
      @PathVariable(name = "symbol") @NonNull String symbol,
      @RequestParam(name = "start") @NonNull Instant start,
      @RequestParam(name = "end") @NonNull Instant end,
      @RequestParam(name = "offset") int offset,
      @RequestParam(name = "limit") int limit) {
    return stockRepository.findAllBySymbol(symbol, start, end, offset, limit);
  }

  /**
   * Converts {@link DriverException}s into HTTP 500 error codes and outputs the error message as
   * the response body.
   *
   * @param e The {@link DriverException}.
   * @return The error message to be used as response body.
   */
  @ExceptionHandler(Exception.class)
  @ResponseStatus(HttpStatus.INTERNAL_SERVER_ERROR)
  public String errorHandler(DriverException e) {
    return e.getMessage();
  }
}
