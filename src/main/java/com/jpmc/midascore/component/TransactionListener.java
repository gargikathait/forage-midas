package com.jpmc.midascore.component;

import com.jpmc.midascore.foundation.Transaction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;

@Component
public class TransactionListener {
    private static final Logger logger = LoggerFactory.getLogger(TransactionListener.class);

    @KafkaListener(topics = "${general.kafka-topic}")
    public void handleTransaction(Transaction transaction, 
                                  @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
                                  @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
                                  @Header(KafkaHeaders.OFFSET) long offset,
                                  Acknowledgment acknowledgment) {
        try {
            logger.info("Processing transaction from topic: {}, partition: {}, offset: {}", topic, partition, offset);
            logger.info("Received transaction: {}", transaction);
            
            // Process the transaction
            processTransaction(transaction);
            
            // Acknowledge the message after successful processing
            acknowledgment.acknowledge();
            logger.info("Successfully processed transaction with amount: {}", transaction.getAmount());
            
        } catch (Exception e) {
            logger.error("Error processing transaction: {}", transaction, e);
            // In production, you might want to implement dead letter queue or retry logic
            // For now, we'll acknowledge to avoid infinite retries
            acknowledgment.acknowledge();
        }
    }
    
    private void processTransaction(Transaction transaction) {
        // Add your transaction processing logic here
        // This could include validation, database operations, etc.
        logger.info("Processing transaction from sender: {} to recipient: {} with amount: {}", 
                    transaction.getSenderId(), transaction.getRecipientId(), transaction.getAmount());
    }
}