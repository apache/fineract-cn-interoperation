/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.fineract.cn.interoperation.service.internal.repository;

import org.apache.fineract.cn.interoperation.api.v1.domain.InteropState;
import org.apache.fineract.cn.interoperation.api.v1.domain.TransactionType;
import org.apache.fineract.cn.mariadb.util.LocalDateTimeConverter;

import javax.persistence.*;
import javax.validation.constraints.NotNull;
import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;

@Entity
@Table(name = "hathor_transactions", uniqueConstraints = {@UniqueConstraint(name = "uk_hathor_transactions_id", columnNames = {"identifier"})})
public class InteropTransactionEntity {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "id")
    private Long id;

    @Column(name = "identifier", nullable = false, length = 36)
    private String identifier;

    @Column(name = "a_name", length = 256)
    private String name;

    @Column(name = "description", length = 1024)
    private String description;

    @Column(name = "transaction_type", nullable = false, length = 32)
    @Enumerated(EnumType.STRING)
    private TransactionType transactionType;

    @Column(name = "amount", nullable = false)
    private BigDecimal amount;

    @Column(name = "state", nullable = false, length = 32)
    @Enumerated(EnumType.STRING)
    private InteropState state;

    @Column(name = "customer_account_identifier", nullable = false, length = 32)
    private String customerAccountIdentifier;

    @Column(name = "payable_account_identifier", length = 32)
    private String payableAccountIdentifier;

    @Column(name = "nostro_account_identifier", nullable = false, length = 32)
    private String nostroAccountIdentifier;

    @Column(name = "transaction_date")
    @Convert(converter = LocalDateTimeConverter.class)
    private LocalDateTime transactionDate;

    @Column(name = "expiration_date")
    @Convert(converter = LocalDateTimeConverter.class)
    private LocalDateTime expirationDate;

    @Column(name = "created_by", nullable = false, length = 32)
    private String createdBy;

    @Column(name = "created_on", nullable = false)
    @Convert(converter = LocalDateTimeConverter.class)
    private LocalDateTime createdOn;

    @Column(name = "last_modified_by", length = 32)
    private String lastModifiedBy;

    @Column(name = "last_modified_on")
    @Convert(converter = LocalDateTimeConverter.class)
    private LocalDateTime lastModifiedOn;

    @OneToMany(cascade = CascadeType.ALL, mappedBy = "transaction", orphanRemoval = true, fetch=FetchType.LAZY)
    @OrderBy("seqNo")
    private List<InteropActionEntity> actions = new ArrayList<>();


    protected InteropTransactionEntity() {
    }

    public InteropTransactionEntity(@NotNull String identifier, @NotNull String customerAccountIdentifier, @NotNull String createdBy,
                                    @NotNull LocalDateTime createdOn) {
        this.identifier = identifier;
        this.customerAccountIdentifier = customerAccountIdentifier;
        this.createdBy = createdBy;
        this.createdOn = createdOn;
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getIdentifier() {
        return identifier;
    }

    public void setIdentifier(String identifier) {
        this.identifier = identifier;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public TransactionType getTransactionType() {
        return transactionType;
    }

    public void setTransactionType(TransactionType transactionType) {
        this.transactionType = transactionType;
    }

    public BigDecimal getAmount() {
        return amount;
    }

    public void setAmount(BigDecimal amount) {
        this.amount = amount;
    }

    public InteropState getState() {
        return state;
    }

    public void setState(InteropState state) {
        this.state = state;
    }

    public String getCustomerAccountIdentifier() {
        return customerAccountIdentifier;
    }

    private void setCustomerAccountIdentifier(String customerAccountIdentifier) {
        this.customerAccountIdentifier = customerAccountIdentifier;
    }

    public String getPayableAccountIdentifier() {
        return payableAccountIdentifier;
    }

    public void setPayableAccountIdentifier(String payableAccountIdentifier) {
        this.payableAccountIdentifier = payableAccountIdentifier;
    }

    public String getNostroAccountIdentifier() {
        return nostroAccountIdentifier;
    }

    public void setNostroAccountIdentifier(String nostroAccountIdentifier) {
        this.nostroAccountIdentifier = nostroAccountIdentifier;
    }

    public LocalDateTime getTransactionDate() {
        return transactionDate;
    }

    public void setTransactionDate(LocalDateTime transactionDate) {
        this.transactionDate = transactionDate;
    }

    public LocalDateTime getExpirationDate() {
        return expirationDate;
    }

    public void setExpirationDate(LocalDateTime expirationDate) {
        this.expirationDate = expirationDate;
    }

    public String getCreatedBy() {
        return createdBy;
    }

    private void setCreatedBy(String createdBy) {
        this.createdBy = createdBy;
    }

    public LocalDateTime getCreatedOn() {
        return createdOn;
    }

    private void setCreatedOn(LocalDateTime createdOn) {
        this.createdOn = createdOn;
    }

    public String getLastModifiedBy() {
        return lastModifiedBy;
    }

    public void setLastModifiedBy(String lastModifiedBy) {
        this.lastModifiedBy = lastModifiedBy;
    }

    public LocalDateTime getLastModifiedOn() {
        return lastModifiedOn;
    }

    public void setLastModifiedOn(LocalDateTime lastModifiedOn) {
        this.lastModifiedOn = lastModifiedOn;
    }

    public List<InteropActionEntity> getActions() {
        return actions;
    }

    public void setActions(List<InteropActionEntity> actions) {
        this.actions = actions;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        InteropTransactionEntity that = (InteropTransactionEntity) o;

        return identifier.equals(that.identifier);
    }

    @Override
    public int hashCode() {
        return identifier.hashCode();
    }
}
