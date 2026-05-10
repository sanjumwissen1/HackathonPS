package com.hacksys.backend.model;

import com.fasterxml.jackson.annotation.JsonInclude;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class Order {

    public enum Status {
        CREATED, RESERVED, PAID, FAILED, CANCELLED, REFUNDED
    }

    private String id;
    private String userId;
    // Intentional: using AtomicReference for "thread-safe" status but update is still non-atomic with reads
    private final AtomicReference<Status> status = new AtomicReference<>(Status.CREATED);
    private List<OrderItem> items;
    private Instant createdAt;
    private Instant updatedAt;
    // Intentional: no version/etag field — no optimistic locking
    private String paymentId;
    private String failureReason;

    public Order() {}

    public Order(String id, String userId, List<OrderItem> items) {
        this.id = id;
        this.userId = userId;
        this.items = items;
        this.createdAt = Instant.now();
        this.updatedAt = Instant.now();
    }

    public String getId() { return id; }
    public void setId(String id) { this.id = id; }

    public String getUserId() { return userId; }
    public void setUserId(String userId) { this.userId = userId; }

    public Status getStatus() { return status.get(); }
    /**
     * Attempts an atomic transition of the order status from expectedStatus to newStatus.
     * This method uses CAS (Compare-And-Swap) logic to ensure that the state change only occurs 
     * if the current status matches the expected prerequisite, preventing lost updates due to race conditions.
     * @param expectedStatus The required current status for the transition to be valid.
     * @param newStatus The desired final status.
     * @return true if the transition was successful and committed atomically, false otherwise.
     */
    public boolean transitionTo(Status expectedStatus, Status newStatus) {
        if (expectedStatus == null || newStatus == null) {
            throw new IllegalArgumentException("Expected and new statuses must not be null.");
        }
        
        // Use CAS loop to ensure atomicity: check current state AND update in one atomic operation.
        if (status.compareAndSet(expectedStatus, newStatus)) {
            this.updatedAt = Instant.now();
            return true;
        }
        return false; // Transition failed because the status was not expectedStatus at the time of CAS attempt.
    }

    // Removed unsafe setStatus method to enforce transactional transition logic via transitionTo()

    public List<OrderItem> getItems() { return items; }
    public void setItems(List<OrderItem> items) { this.items = items; }

    public Instant getCreatedAt() { return createdAt; }
    public Instant getUpdatedAt() { return updatedAt; }

    public String getPaymentId() { return paymentId; }
    public void setPaymentId(String paymentId) { this.paymentId = paymentId; }

    public String getFailureReason() { return failureReason; }
    public void setFailureReason(String failureReason) { this.failureReason = failureReason; }

    @JsonInclude(JsonInclude.Include.NON_NULL)
    public static class OrderItem {
        private String productId;
        private int quantity;
        private double unitPrice;

        public OrderItem() {}
        public OrderItem(String productId, int quantity, double unitPrice) {
            this.productId = productId;
            this.quantity = quantity;
            this.unitPrice = unitPrice;
        }

        public String getProductId() { return productId; }
        public void setProductId(String productId) { this.productId = productId; }
        public int getQuantity() { return quantity; }
        public void setQuantity(int quantity) { this.quantity = quantity; }
        public double getUnitPrice() { return unitPrice; }
        public void setUnitPrice(double unitPrice) { this.unitPrice = unitPrice; }
    }
}