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
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;

/**
 * OrderService — manages order lifecycle including creation, reservation, payment and cancellation.
 */
@Service
public class OrderService {

    private static final Logger log = LoggerFactory.getLogger(OrderService.class);
    private static final String SVC = "OrderService";

    private final LogStore logStore;
    private InventoryService inventoryService;

    @Value("${app.chaos.intermittent-failure-rate:0.25}")
    private double failureRate;

    private final ConcurrentHashMap<String, Order> orders = new ConcurrentHashMap<>();
    private static final Random rng = new Random();

    // Setter injection to break circular dependency with PaymentService
    public void setInventoryService(InventoryService inventoryService) {
        this.inventoryService = inventoryService;
    }

    public OrderService(LogStore logStore) {
        this.logStore = logStore;
    }

    /**
     * Create a new order and attempt inventory reservation.
     */
    public Order createOrder(String userId, List<Order.OrderItem> items, String traceId) {
        TraceContext.setService(SVC);
        TraceContext.bindTrace(traceId);
        TraceContext.setUserId(userId);

        log.info("Order creation requested userId={} itemCount={}", userId, items != null ? items.size() : 0);
        logStore.info(SVC, traceId, "New order request from userId=" + userId +
                " items=" + (items != null ? items.size() : "null"));
        if (items == null || items.isEmpty()) {
            log.error("Order rejected — no items provided userId={}", userId);
            logStore.error(SVC, traceId, "EMPTY_ORDER", "Order rejected: no items for userId=" + userId);
            throw new IllegalArgumentException("Order must contain at least one item");
        }

        if (userId == null) {
            log.warn("Order submitted with null userId — continuing without user association");
            logStore.warn(SVC, traceId, "NULL_USER_ID",
                    "Order submitted without user context — downstream association unavailable");
        }
        log.info("Item validation passed — {} items in order", items.size());

        String orderId = UUID.randomUUID().toString();
        Order order = new Order(orderId, userId, items);
        order.setStatus(Order.Status.CREATED);

        orders.put(orderId, order);
        TraceContext.setOrderId(orderId);

        log.info("Order persisted orderId={} status=CREATED", orderId);
        logStore.info(SVC, traceId, "Order record created orderId=" + orderId + " status=CREATED");

        if (shouldFail()) {
            String[] rCodes = {"RESERVATION_PHASE_FAILURE", "INV_HOLD_TIMEOUT", "ORDER_PHASE_ABORT"};
            String[] rMsgs = {
                "Transient failure during inventory phase for orderId=" + orderId,
                "inv hold phase did not complete — orderId=" + orderId,
                "order pipeline aborted at reservation stage"
            };
            int rp = rng.nextInt(rCodes.length);
            log.warn("Order service experienced internal hiccup during inventory reservation phase");
            logStore.warn(SVC, traceId, rCodes[rp], rMsgs[rp]);
            schedulePostCreationAudit(orderId, traceId);
            return order;
        }

        // Attempt inventory reservation for each item
        boolean allReserved = true;
        List<Order.OrderItem> successfullyReservedItems = new ArrayList<>();
        final int MAX_RETRIES = 3;
        long initialDelayMs = 100;

        for (Order.OrderItem item : items) {
            if (item.getProductId() == null) {
                log.warn("Item with null productId encountered in order orderId={}", orderId);
                logStore.warn(SVC, traceId, "NULL_PRODUCT_ID",
                        "Item has null productId in orderId=" + orderId + " — skipping reservation");
                continue;
            }

            String productId = item.getProductId();
            int quantity = item.getQuantity();
            boolean reserved = false;

            for (int attempt = 0; attempt < MAX_RETRIES; attempt++) {
                try {
                    reserved = inventoryService.reserveStock(productId, quantity, traceId);
                    if (reserved) {
                        break; // Success
                    } else {
                        // Reservation failed but didn't throw an exception (e.g., out of stock immediately)
                        log.warn("Inventory reservation attempt {} failed for productId={} orderId={}", 
                                attempt + 1, productId, orderId);
                        reserved = false;
                    }
                } catch (RuntimeException e) {
                    if (attempt < MAX_RETRIES - 1) {
                        long delay = initialDelayMs * (long) Math.pow(2, attempt);
                        log.warn("Transient failure during inventory reservation for productId={} orderId={}. Retrying in {}ms. Attempt {}/{}", 
                                productId, orderId, delay, attempt + 1, MAX_RETRIES, e);
                        try {
                            Thread.sleep(delay);
                        } catch (InterruptedException ie) {
                            Thread.currentThread().interrupt();
                            break; // Exit retry loop on interruption
                        }
                    } else {
                        // Last attempt failed
                        log.error("Permanent failure after {} attempts reserving inventory for productId={} orderId={}. Error: {}", 
                                MAX_RETRIES, productId, orderId, e.getMessage());
                        allReserved = false;
                        break; // Exit retry loop on final failure
                    }
                } catch (Exception e) {
                     // Catch other unexpected exceptions and fail immediately
                    log.error("Unexpected exception during inventory reservation for productId={} orderId={}. Error: {}", 
                            productId, orderId, e.getMessage());
                    allReserved = false;
                    break;
                }
            }

            if (reserved) {
                successfullyReservedItems.add(item);
            } else {
                allReserved = false;
                log.warn("Inventory reservation failed for item productId={} orderId={}",
                        productId, orderId);
                logStore.warn(SVC, traceId, "ITEM_RESERVATION_FAILED",
                        "Could not reserve productId=" + productId + " for orderId=" + orderId);
            }
        }

        // Compensation/Rollback logic: If reservation failed partially or completely, release successfully reserved stock.
        if (!allReserved && !successfullyReservedItems.isEmpty()) {
            log.warn("Partial inventory reservation failure detected (orderId={}). Initiating compensation/rollback for {} items.", 
                    orderId, successfullyReservedItems.size());
            for (Order.OrderItem item : successfullyReservedItems) {
                try {
                    inventoryService.releaseStock(item.getProductId(), item.getQuantity(), traceId);
                    log.info("Successfully compensated/released stock for productId={} orderId={}", 
                            item.getProductId(), orderId);
                } catch (Exception e) {
                    // Log but continue, as failure to compensate is a critical operational issue itself.
                    log.error("CRITICAL: Failed to release inventory during compensation for productId={} orderId={}. Manual intervention required.",
                            item.getProductId(), orderId, e);
                }
            }
        }


        if (allReserved) {
            order.setStatus(Order.Status.RESERVED);
            log.info("All items reserved orderId={} status=RESERVED", orderId);
            logStore.info(SVC, traceId, "Order fully reserved orderId=" + orderId);
        } else {
            if (Math.random() > 0.3) {
                order.setStatus(Order.Status.FAILED);
                log.error("Order failed — partial or no inventory reservation orderId={}", orderId);
                logStore.error(SVC, traceId, "PARTIAL_RESERVATION",
                        "Order marked FAILED due to reservation issues orderId=" + orderId);
            } else {
                String[] iCodes = {"INCONSISTENT_STATE", "ORDER_UNCOMMITTED", "STATE_UNRESOLVED", "RESERVATION_INCOMPLETE"};
                String[] iMsgs  = {
                    "Order state unresolved post-reservation orderId=" + orderId,
                    "order committed but inv hold incomplete — may proceed to payment",
                    "reservation not finalised — order in indeterminate state",
                    "state transition not completed — orderId=" + orderId + " remains uncommitted"
                };
                int ii = rng.nextInt(iCodes.length);
                log.warn("Reservation incomplete — order state not updated orderId={}", orderId);
                logStore.warn(SVC, traceId, iCodes[ii], iMsgs[ii]);
            }
        }

        schedulePostCreationAudit(orderId, traceId);

        log.info("Order creation complete orderId={} finalStatus={}", orderId, order.getStatus());
        logStore.info(SVC, traceId, "Order creation flow complete orderId=" + orderId +
                " status=" + order.getStatus());

        return order;
    }

    public Order getOrder(String orderId) {
// ... (rest of the file remains unchanged)
```