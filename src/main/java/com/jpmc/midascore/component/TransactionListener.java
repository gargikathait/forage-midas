package com.jpmc.midascore.component;

import com.jpmc.midascore.foundation.Transaction;
import com.jpmc.midascore.entity.UserRecord;
import com.jpmc.midascore.entity.TransactionRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

@Component
public class TransactionListener {
    private static final Logger logger = LoggerFactory.getLogger(TransactionListener.class);
    
    private final DatabaseConduit databaseConduit;

    public TransactionListener(DatabaseConduit databaseConduit) {
        this.databaseConduit = databaseConduit;
    }

    @KafkaListener(topics = "${general.kafka-topic}")
    @Transactional
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
            // Acknowledge to avoid infinite retries
            acknowledgment.acknowledge();
        }
    }
    
    private void processTransaction(Transaction transaction) {
        logger.info("Validating transaction from sender: {} to recipient: {} with amount: {}", 
                    transaction.getSenderId(), transaction.getRecipientId(), transaction.getAmount());
        
        // Validate sender exists
        UserRecord sender = databaseConduit.findUserById(transaction.getSenderId());
        if (sender == null) {
            logger.warn("Transaction discarded: Invalid senderId {}", transaction.getSenderId());
            return;
        }
        
        // Validate recipient exists
        UserRecord recipient = databaseConduit.findUserById(transaction.getRecipientId());
        if (recipient == null) {
            logger.warn("Transaction discarded: Invalid recipientId {}", transaction.getRecipientId());
            return;
        }
        
        // Validate sender has sufficient balance
        if (sender.getBalance() < transaction.getAmount()) {
            logger.warn("Transaction discarded: Insufficient balance. Sender {} has balance {}, but transaction amount is {}", 
                       sender.getName(), sender.getBalance(), transaction.getAmount());
            return;
        }
        
        // All validations passed, process the transaction
        logger.info("Transaction validation passed. Processing transaction...");
        
        // Create and save transaction record
        TransactionRecord transactionRecord = new TransactionRecord(sender, recipient, transaction.getAmount());
        databaseConduit.save(transactionRecord);
        
        // Update balances
        sender.setBalance(sender.getBalance() - transaction.getAmount());
        recipient.setBalance(recipient.getBalance() + transaction.getAmount());
        
        // Save updated user records
        databaseConduit.save(sender);
        databaseConduit.save(recipient);
        
        logger.info("Transaction processed successfully. Sender {} new balance: {}, Recipient {} new balance: {}", 
                   sender.getName(), sender.getBalance(), recipient.getName(), recipient.getBalance());
    }
}