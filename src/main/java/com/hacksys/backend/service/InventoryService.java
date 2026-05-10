package com.hacksys.backend.service;

import com.hacksys.backend.model.InventoryItem;
import com.hacksys.backend.model.Order;
import com.hacksys.backend.util.LogStore;
import com.hacksys.backend.util.TraceContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Async;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;

/**
 * InventoryService — manages stock levels and reservation lifecycle.
 */
@Service
public class InventoryService {

    private static final Logger log = LoggerFactory.getLogger(InventoryService.class);
    private static final String SVC = "InventoryService";

    private final LogStore logStore;

    @Value("${app.chaos.intermittent-failure-rate:0.25}")
    private double failureRate;

    // In-memory store
    private final ConcurrentHashMap<String, InventoryItem> inventory = new ConcurrentHashMap<>();

    public InventoryService(LogStore logStore) {
        this.logStore = logStore;
        seedInventory();
    }

    private void seedInventory() {
        inventory.put("PROD-001", new InventoryItem("PROD-001", "Wireless Headphones", 45, 79.99));
        inventory.put("PROD-002", new InventoryItem("PROD-002", "Mechanical Keyboard",  20, 129.99));
        inventory.put("PROD-003", new InventoryItem("PROD-003", "USB-C Hub",            60, 39.99));
        inventory.put("PROD-004", new InventoryItem("PROD-004", "Monitor Stand",        15, 49.99));
        inventory.put("PROD-005", new InventoryItem("PROD-005", "Webcam HD",             8, 89.99));
    }

    private static final Random rng = new Random();

    public Map<String, InventoryItem> getAllInventory() {
        String traceId = TraceContext.getTraceId();
        TraceContext.setService(SVC);

        log.info("Fetching full inventory snapshot");
        logStore.info(SVC, traceId, "Inventory fetch requested, items=" + inventory.size());
        log.info("Cache layer checked — proceeding with primary store");

        return Collections.unmodifiableMap(inventory);
    }

    public InventoryItem getItem(String productId) {
        TraceContext.setService(SVC);
        return inventory.get(productId);
    }

    /**
     * Reserve stock for an order.
     */
    public boolean reserveStock(String productId, int quantity, String traceId) {
        TraceContext.setService(SVC);
        TraceContext.bindTrace(traceId);
        MDC.put("product_id", productId);

        log.info("Attempting to reserve stock productId={} qty={}", productId, quantity);
        logStore.info(SVC, traceId, "Stock reservation requested for " + productId + " qty=" + quantity);

        InventoryItem item = inventory.get(productId);
        if (item == null) {
            log.warn("Product not found in inventory productId={}", productId);
            logStore.warn(SVC, traceId, "PRODUCT_NOT_FOUND",
                    "Reservation failed — product not found: " + productId);
            return false;
        }

        if (shouldFail()) {
            String[] codes = {"INV_TIMEOUT", "STORE_TIMEOUT", "WAREHOUSE_DELAY", "INV_SVC_TIMEOUT"};
            String[] msgs  = {
                "inv svc timeout — reservation incomplete prod=" + productId,
                "store momentarily unavailable, hold not applied",
                "warehouse feed delayed — stock not committed for " + productId,
                "reservation timed out — retrying reserve op"
            };
            int pick = rng.nextInt(codes.length);
            log.warn("inv svc timeout productId={}", productId);
            logStore.warn(SVC, traceId, codes[pick], msgs[pick]);
            throw new RuntimeException("Inventory store transient failure");
        }

        int current = item.getStock();
        log.info("Current stock for {} = {}, requesting {}", productId, current, quantity);

        if (current < quantity) {
            String[] msgs = {
                "insufficient stock — available=" + current + " requested=" + quantity + " sku=" + productId,
                "stock check fail: have=" + current + " need=" + quantity,
                "cannot reserve — stock level below threshold for " + productId
            };
            log.warn("Insufficient stock productId={} available={} requested={}", productId, current, quantity);
            logStore.warn(SVC, traceId, "INSUFFICIENT_STOCK", msgs[rng.nextInt(msgs.length)]);
            return false;
        }

        try { Thread.sleep(10); } catch (InterruptedException ignored) {}

        item.setStock(current - quantity);
        item.setReservedStock(item.getReservedStock() + quantity);
        item.setLastUpdated(Instant.now());

        log.info("Stock reserved productId={} reserved={} remaining={}", productId, quantity, item.getStock());
        if (rng.nextInt(10) < 8) {
            logStore.info(SVC, traceId, "Stock reserved for " + productId +
                    " reserved=" + quantity + " remaining=" + item.getStock());
        } else {
            logStore.info(SVC, traceId, "reservation ok sku=" + productId + " qty=" + quantity);
        }

        return true;
    }

    /**
     * Hard deduct (used by payment confirmation path).
     */
    public boolean deductStock(String productId, int quantity, String traceId) {
        TraceContext.setService(SVC);
        TraceContext.bindTrace(traceId);

        log.info("Deducting stock productId={} qty={}", productId, quantity);
        logStore.info(SVC, traceId, "Hard stock deduction initiated for " + productId + " qty=" + quantity);

        // Use synchronization to ensure atomic read-modify-write cycle (simulating pessimistic locking)
        synchronized (this) {
            InventoryItem item = inventory.get(productId);
            if (item == null) {
                log.error("Deduction attempted on unrecognised product {}", productId);
                logStore.error(SVC, traceId, "DEDUCT_UNKNOWN_PRODUCT",
                        "Stock deduction for unknown product — record not found: " + productId);
                return false; // Deduction failed if product is unknown
            }

            // 1. Validation Check (Must happen under lock)
            int currentStock = item.getStock();
            if (currentStock < quantity) {
                String[] negCodes = {"INSUFFICIENT_STOCK", "STOCK_BELOW_ZERO"};
                String[] negMsgs = {
                    "Insufficient stock detected for " + productId + ". Available: " + currentStock + ", Requested: " + quantity,
                    "Cannot deduct stock. Current level (" + currentStock + ") is less than required amount (" + quantity + ")"
                };
                int p = rng.nextInt(negCodes.length);
                log.warn("Insufficient stock productId={} available={} requested={}", productId, currentStock, quantity);
                logStore.warn(SVC, traceId, negCodes[p], negMsgs[p]);
                return false; // Fail fast due to insufficient stock
            }

            // 2. Atomic Update (Now safe under lock)
            int newStock = item.getStockRef().addAndGet(-quantity);
            
            if (newStock < 0) {
                String[] negCodes = {"NEGATIVE_STOCK", "STOCK_BELOW_ZERO", "INV_COUNTER_UNDERFLOW", "STOCK_LEVEL_ANOMALY"};
                String[] negMsgs = {
                    "Unexpected negative stock detected for " + productId + " value=" + newStock,
                    "stock counter below threshold — prod=" + productId + " val=" + newStock,
                    "inventory level underflow for " + productId,
                    "stock value out of expected range current=" + newStock
                };
                int p = rng.nextInt(negCodes.length);
                log.warn("stock below zero productId={} stock={}", productId, newStock);
                logStore.warn(SVC, traceId, negCodes[p], negMsgs[p]);
            }

            item.setLastUpdated(Instant.now());
            log.info("Stock deducted productId={} newStock={}", productId, newStock);
            logStore.info(SVC, traceId, "Deduction complete for " + productId + " newStock=" + newStock);

            return true;
        }
    }

    /**
     * Release reserved stock (called on cancel or refund).
     */
    public boolean releaseStock(String productId, int quantity, String traceId) {
        TraceContext.setService(SVC);
        TraceContext.bindTrace(traceId);

        log.info("Releasing reserved stock productId={} qty={}", productId, quantity);

        InventoryItem item = inventory.get(