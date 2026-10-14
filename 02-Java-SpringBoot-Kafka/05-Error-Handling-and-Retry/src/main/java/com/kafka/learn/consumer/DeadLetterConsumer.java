package com.kafka.learn.consumer;

import com.kafka.learn.model.Order;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Service;

/**
 * Dead Letter Topic Consumer — processes messages that failed 3 times.
 *
 * Messages land here when:
 * - Original consumer failed after all retries
 * - A "poison pill" message (always causes exception)
 *
 * What to do in DLT:
 * - Log for manual investigation
 * - Alert on-call team
 * - Store in database for later replay
 * - Send notification to order owner
 */
@Slf4j
@Service
public class DeadLetterConsumer {

    @KafkaListener(
            topics = "${app.kafka.topics.dead-letter}",   // orders-topic.DLT
            groupId = "dlt-handler-group"
    )
    public void handleDeadLetter(Order order, Acknowledgment acknowledgment) {
        log.error("💀 [DLT] Processing dead letter message | orderId={} | customerId={}",
                order.getOrderId(), order.getCustomerId());

        // 1. Save to database for investigation
        saveToFailedOrdersDb(order);

        // 2. Alert team
        alertOnCallTeam(order);

        // 3. Optionally: notify customer
        // notifyCustomer(order);

        acknowledgment.acknowledge();
        log.info("✅ [DLT] Dead letter processed and committed for orderId={}", order.getOrderId());
    }

    private void saveToFailedOrdersDb(Order order) {
        log.info("  💾 Saving failed order {} to DB for manual review", order.getOrderId());
        // failedOrderRepository.save(order);
    }

    private void alertOnCallTeam(Order order) {
        log.info("  🚨 ALERT: Order {} failed processing! Manual review needed.", order.getOrderId());
        // slackNotifier.send("ORDER FAILED: " + order.getOrderId());
    }
}
