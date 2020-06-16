/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.fineract.cn.interoperation.service.internal.service;

import org.apache.fineract.cn.accounting.api.v1.domain.Account;
import org.apache.fineract.cn.accounting.api.v1.domain.AccountType;
import org.apache.fineract.cn.accounting.api.v1.domain.Creditor;
import org.apache.fineract.cn.accounting.api.v1.domain.Debtor;
import org.apache.fineract.cn.accounting.api.v1.domain.JournalEntry;
import org.apache.fineract.cn.api.util.UserContextHolder;
import org.apache.fineract.cn.deposit.api.v1.client.DepositAccountManager;
import org.apache.fineract.cn.deposit.api.v1.definition.domain.Charge;
import org.apache.fineract.cn.deposit.api.v1.definition.domain.Currency;
import org.apache.fineract.cn.deposit.api.v1.definition.domain.ProductDefinition;
import org.apache.fineract.cn.deposit.api.v1.instance.domain.ProductInstance;
import org.apache.fineract.cn.interoperation.api.v1.domain.InteropActionState;
import org.apache.fineract.cn.interoperation.api.v1.domain.InteropActionType;
import org.apache.fineract.cn.interoperation.api.v1.domain.InteropIdentifierType;
import org.apache.fineract.cn.interoperation.api.v1.domain.InteropState;
import org.apache.fineract.cn.interoperation.api.v1.domain.InteropStateMachine;
import org.apache.fineract.cn.interoperation.api.v1.domain.TransactionType;
import org.apache.fineract.cn.interoperation.api.v1.domain.data.InteropIdentifierCommand;
import org.apache.fineract.cn.interoperation.api.v1.domain.data.InteropIdentifierData;
import org.apache.fineract.cn.interoperation.api.v1.domain.data.InteropIdentifierDeleteCommand;
import org.apache.fineract.cn.interoperation.api.v1.domain.data.InteropQuoteRequestData;
import org.apache.fineract.cn.interoperation.api.v1.domain.data.InteropQuoteResponseData;
import org.apache.fineract.cn.interoperation.api.v1.domain.data.InteropRequestData;
import org.apache.fineract.cn.interoperation.api.v1.domain.data.InteropTransactionRequestData;
import org.apache.fineract.cn.interoperation.api.v1.domain.data.InteropTransactionRequestResponseData;
import org.apache.fineract.cn.interoperation.api.v1.domain.data.InteropTransferCommand;
import org.apache.fineract.cn.interoperation.api.v1.domain.data.InteropTransferRequestData;
import org.apache.fineract.cn.interoperation.api.v1.domain.data.InteropTransferResponseData;
import org.apache.fineract.cn.interoperation.api.v1.domain.data.MoneyData;
import org.apache.fineract.cn.interoperation.api.v1.util.MathUtil;
import org.apache.fineract.cn.interoperation.service.ServiceConstants;
import org.apache.fineract.cn.interoperation.service.internal.repository.InteropActionEntity;
import org.apache.fineract.cn.interoperation.service.internal.repository.InteropActionRepository;
import org.apache.fineract.cn.interoperation.service.internal.repository.InteropIdentifierEntity;
import org.apache.fineract.cn.interoperation.service.internal.repository.InteropIdentifierRepository;
import org.apache.fineract.cn.interoperation.service.internal.repository.InteropTransactionEntity;
import org.apache.fineract.cn.interoperation.service.internal.repository.InteropTransactionRepository;
import org.apache.fineract.cn.interoperation.service.internal.service.helper.InteropAccountingService;
import org.apache.fineract.cn.interoperation.service.internal.service.helper.InteropDepositService;
import org.apache.fineract.cn.lang.DateConverter;
import org.slf4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.data.jpa.domain.Specification;
import org.springframework.data.jpa.domain.Specifications;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import javax.persistence.criteria.CriteriaBuilder;
import javax.persistence.criteria.CriteriaQuery;
import javax.persistence.criteria.Path;
import javax.persistence.criteria.Root;
import javax.validation.constraints.NotNull;
import java.math.BigDecimal;
import java.math.MathContext;
import java.time.Clock;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

import static java.math.BigDecimal.ZERO;
import static java.util.Comparator.comparing;
import static org.apache.fineract.cn.deposit.api.v1.EventConstants.PRODUCT_INSTANCE_TRANSACTION;
import static org.apache.fineract.cn.interoperation.api.v1.domain.InteropActionType.COMMIT;
import static org.apache.fineract.cn.interoperation.api.v1.domain.InteropActionType.PREPARE;
import static org.apache.fineract.cn.interoperation.api.v1.domain.InteropActionType.QUOTE;
import static org.apache.fineract.cn.interoperation.api.v1.domain.InteropActionType.RELEASE;
import static org.apache.fineract.cn.interoperation.api.v1.domain.TransactionType.CURRENCY_DEPOSIT;
import static org.apache.fineract.cn.interoperation.api.v1.domain.TransactionType.CURRENCY_WITHDRAWAL;
import static org.apache.fineract.cn.interoperation.api.v1.domain.TransactionType.REIMBURSEMENT;
import static org.apache.fineract.cn.interoperation.api.v1.domain.data.InteropRequestData.IDENTIFIER_SEPARATOR;


@Service
public class InteropService {

    public static final String ACCOUNT_NAME_NOSTRO = "Interoperation NOSTRO";

    private final Logger logger;

    private final InteropIdentifierRepository identifierRepository;
    private final InteropTransactionRepository transactionRepository;
    private final InteropActionRepository actionRepository;
    private final InteropDepositService depositService;
    private final InteropAccountingService accountingService;
    private final DepositAccountManager depositAccountManager;


    @Autowired
    public InteropService(@Qualifier(ServiceConstants.LOGGER_NAME) final Logger logger,
                          InteropIdentifierRepository interopIdentifierRepository,
                          InteropTransactionRepository interopTransactionRepository,
                          InteropActionRepository interopActionRepository,
                          InteropDepositService interopDepositService,
                          InteropAccountingService interopAccountingService,
                          DepositAccountManager depositAccountManager) {
        this.logger = logger;
        this.identifierRepository = interopIdentifierRepository;
        this.transactionRepository = interopTransactionRepository;
        this.actionRepository = interopActionRepository;
        this.depositService = interopDepositService;
        this.accountingService = interopAccountingService;
        this.depositAccountManager = depositAccountManager;
    }

    @NotNull
    public InteropIdentifierData getAccountByIdentifier(@NotNull InteropIdentifierType idType, @NotNull String idValue, String subIdOrType) {
        InteropIdentifierEntity identifier = findIdentifier(idType, idValue, subIdOrType);
        if (identifier == null)
            throw new UnsupportedOperationException("Account not found for identifier " + idType + "/" + idValue + (subIdOrType == null ? "" : ("/" + subIdOrType)));

        return new InteropIdentifierData(identifier.getCustomerAccountIdentifier());
    }

    @NotNull
    @Transactional(propagation = Propagation.MANDATORY)
    public InteropIdentifierData registerAccountIdentifier(@NotNull InteropIdentifierCommand request) {
        String accountId = request.getAccountId();
        Account account = accountingService.findAccount(accountId);
        if (account == null)
            throw new UnsupportedOperationException("Account not found");
        if (!account.getState().equals(Account.State.OPEN.name()))
            throw new UnsupportedOperationException("Account is in state " + account.getState());

        String createdBy = getLoginUser();
        LocalDateTime createdOn = getNow();

        InteropIdentifierEntity identifier = new InteropIdentifierEntity(accountId, request.getIdType(), request.getIdValue(),
                request.getSubIdOrType(), createdBy, createdOn);

        identifierRepository.save(identifier);

        return new InteropIdentifierData(accountId);
    }

    @NotNull
    @Transactional(propagation = Propagation.MANDATORY)
    public InteropIdentifierData deleteAccountIdentifier(@NotNull InteropIdentifierDeleteCommand request) {
        InteropIdentifierType idType = request.getIdType();
        String idValue = request.getIdValue();
        String subIdOrType = request.getSubIdOrType();

        InteropIdentifierEntity identifier = findIdentifier(idType, idValue, subIdOrType);
        if (identifier == null)
            throw new UnsupportedOperationException("Account not found for identifier " + idType + "/" + idValue + (subIdOrType == null ? "" : ("/" + subIdOrType)));

        String customerAccountIdentifier = identifier.getCustomerAccountIdentifier();

        identifierRepository.delete(identifier);

        return new InteropIdentifierData(customerAccountIdentifier);
    }

    public InteropTransactionRequestResponseData getTransactionRequest(@NotNull String transactionCode, @NotNull String requestCode) {
        InteropActionEntity action = validateAndGetAction(transactionCode, calcActionIdentifier(requestCode, InteropActionType.REQUEST),
                InteropActionType.REQUEST);
        return InteropTransactionRequestResponseData.build(transactionCode, action.getState(), action.getExpirationDate(), requestCode);
    }

    @Transactional(propagation = Propagation.MANDATORY)
    public InteropTransactionRequestResponseData createTransactionRequest(@NotNull InteropTransactionRequestData request) {
        // only when Payee request transaction from Payer, so here role must be always Payer
        //TODO: error handling
        AccountWrapper accountWrapper = validateAccount(request, accountingService.findAccount(request.getAccountId()));
        //TODO: transaction expiration separated from action expiration
        InteropTransactionEntity transaction = validateAndGetTransaction(request, accountWrapper, getNow());
        InteropActionEntity action = addAction(transaction, request);

        transactionRepository.save(transaction);

        return InteropTransactionRequestResponseData.build(request.getTransactionCode(), action.getState(), action.getExpirationDate(),
                request.getExtensionList(), request.getRequestCode());
    }

    public InteropQuoteResponseData getQuote(@NotNull String transactionCode, @NotNull String quoteCode) {
        InteropActionEntity action = validateAndGetAction(transactionCode, calcActionIdentifier(quoteCode, InteropActionType.QUOTE),
                InteropActionType.QUOTE);

        Currency currency = getCurrency(action);

        return InteropQuoteResponseData.build(transactionCode, action.getState(), action.getExpirationDate(), quoteCode,
                MoneyData.build(action.getFee(), currency), MoneyData.build(action.getCommission(), currency));
    }

    @Transactional(propagation = Propagation.MANDATORY)
    public InteropQuoteResponseData createQuote(@NotNull InteropQuoteRequestData request) {
        AccountWrapper accountWrapper = validateAccount(request, accountingService.findAccount(request.getAccountId()));
        BigDecimal calculatedFee = ZERO;

        if(request.getTransactionRole().isWithdraw()) {
            BigDecimal transferAmount = request.getAmount().getAmount();
            TransactionType transactionType = request.getTransactionRole().getTransactionType();
            List<Charge> charges = depositService.getCharges(accountWrapper.account.getIdentifier(), transactionType);
            calculatedFee = MathUtil.normalize(calcTotalCharges(charges, transferAmount), MathUtil.DEFAULT_MATH_CONTEXT);
            double calculatedTotal = transferAmount.add(calculatedFee).doubleValue();

            Double withdrawableBalance = accountWrapper.withdrawableBalance;
            if(withdrawableBalance < calculatedTotal) {
                throw new UnsupportedOperationException("Not enough balance amount: " + withdrawableBalance + " < " + calculatedTotal);
            }
        }

        LocalDateTime now = getNow();
        InteropTransactionEntity transaction = validateAndGetTransaction(request, accountWrapper, now);
        if(transaction.getCreatedOn().equals(now)) {
            InteropActionEntity action = addAction(transaction, request);
            action.setFee(calculatedFee);
            transactionRepository.save(transaction);
            logger.info("New quote request {} saved", request.getQuoteCode());
        }

        InteropActionEntity lastAction = getLastAction(transaction);
        return InteropQuoteResponseData.build(request.getTransactionCode(), lastAction.getState(),
                lastAction.getExpirationDate(), request.getExtensionList(), request.getQuoteCode(),
                MoneyData.build(calculatedFee, accountWrapper.productDefinition.getCurrency()), null);
    }

    public InteropTransferResponseData getTransfer(@NotNull String transactionCode, @NotNull String transferCode) {
        InteropActionEntity action = validateAndGetAction(transactionCode, calcActionIdentifier(transferCode, PREPARE),
                PREPARE, false);
        if (action == null)
            action = validateAndGetAction(transactionCode, calcActionIdentifier(transferCode, COMMIT),
                    COMMIT);

        return InteropTransferResponseData.build(transactionCode, action.getState(), action.getExpirationDate(), transferCode, action.getCreatedOn());
    }

    @Transactional(propagation = Propagation.MANDATORY)
    public InteropTransferResponseData prepareTransfer(@NotNull InteropTransferCommand request) {
        AccountWrapper accountWrapper = validateAccount(request, accountingService.findAccount(request.getAccountId()));
        TransactionType transactionType = request.getTransactionRole().getTransactionType();
        BigDecimal transferAmount = request.getAmount().getAmount();
        List<Charge> charges = depositService.getCharges(accountWrapper.account.getIdentifier(), transactionType);
        BigDecimal calculatedFee = MathUtil.normalize(calcTotalCharges(charges, transferAmount), MathUtil.DEFAULT_MATH_CONTEXT);
        BigDecimal calculatedTotal = transferAmount.add(calculatedFee);
        BigDecimal transferTotal = validateTransferAmount(request, accountWrapper);

        if (transferTotal.compareTo(calculatedTotal) != 0) {
            throw new UnsupportedOperationException("Transfer amount+fee: "+transferTotal+" not matching with actual calculation: " + calculatedTotal);
        }

        LocalDateTime transactionDate = getNow();
        InteropTransactionEntity transaction = validateAndGetTransaction(request, accountWrapper, getNow());
        InteropActionEntity action = addAction(transaction, request, transactionDate);
        action.setFee(calculatedFee);

        if (request.getTransactionRole().isWithdraw()) {
            Double withdrawableBalance = accountWrapper.withdrawableBalance;
            if(withdrawableBalance < calculatedTotal.doubleValue()) {
                throw new UnsupportedOperationException("Not enough balance amount: " + withdrawableBalance + " < " + calculatedTotal.doubleValue());
            }
            prepareTransfer(request, accountWrapper, action, calculatedFee, transactionDate);
        }

        transactionRepository.save(transaction);

        return InteropTransferResponseData.build(request.getTransactionCode(), action.getState(), action.getExpirationDate(),
                request.getExtensionList(), request.getTransferCode(), transactionDate);
    }

    private void prepareTransfer(InteropTransferCommand request, AccountWrapper accountWrapper, InteropActionEntity action,
                                 BigDecimal calculatedFee, LocalDateTime transactionDate) {
        String accountId = accountWrapper.account.getIdentifier();
        Account payableAccount = null;
        try {
            payableAccount = validateAccount(request, accountingService.findAccount(action.getTransaction().getPayableAccountIdentifier())).account;
        } catch (Exception ex) {
            String msg = "Can not prepare transfer, payable account was not found for " + accountId + " to hold amount!";
            logger.error(msg);
            throw new UnsupportedOperationException(msg);
        }

        final JournalEntry journalEntry = createJournalEntry(action.getIdentifier(), CURRENCY_WITHDRAWAL.getCode(),
                DateConverter.toIsoString(transactionDate), "withdraw-prepare", getLoginUser());

        HashSet<Debtor> debtors = new HashSet<>(1);
        HashSet<Creditor> creditors = new HashSet<>(1);

        BigDecimal amount = request.getAmount().getAmount();
        addDebtor(accountId, amount.doubleValue(), debtors);
        addCreditor(payableAccount.getIdentifier(), amount.doubleValue(), creditors);

        if (calculatedFee.compareTo(ZERO) > 0) {
            addDebtor(accountId, calculatedFee.doubleValue(), debtors);
            addCreditor(payableAccount.getIdentifier(), calculatedFee.doubleValue(), creditors);
        }

        journalEntry.setDebtors(debtors);
        journalEntry.setCreditors(creditors);
        accountingService.createJournalEntry(journalEntry);
    }

    @Transactional(propagation = Propagation.MANDATORY)
    public InteropTransferResponseData commitTransfer(@NotNull InteropTransferCommand request) {
        AccountWrapper accountWrapper = validateAccount(request, accountingService.findAccount(request.getAccountId()));
        InteropTransactionEntity transaction = validateAndGetTransaction(request, accountWrapper, getNow());

        boolean isWithdraw = request.getTransactionRole().isWithdraw();
        BigDecimal calculatedFee = ZERO;
        List<Charge> charges = new ArrayList<>();
        double preparedAmount = 0;
        if(isWithdraw) {
            TransactionType transactionType = request.getTransactionRole().getTransactionType();
            BigDecimal transferAmount = request.getAmount().getAmount();
            charges.addAll(depositService.getCharges(accountWrapper.account.getIdentifier(), transactionType));
            calculatedFee = MathUtil.normalize(calcTotalCharges(charges, transferAmount), MathUtil.DEFAULT_MATH_CONTEXT);

            InteropActionEntity lastAction = getLastAction(transaction);
            if (lastAction == null || !PREPARE.equals(lastAction.getActionType())) {
                throw new UnsupportedOperationException("Can not commit amount, previous state invalid!");
            }

            JournalEntry prepareJournal = accountingService.findJournalEntry(lastAction.getIdentifier());
            preparedAmount = prepareJournal.getCreditors().stream()
                    .map(Creditor::getAmount)
                    .mapToDouble(Double::parseDouble)
                    .sum();

            BigDecimal transferTotal = validateTransferAmount(request, accountWrapper);
            if (transferTotal.compareTo(new BigDecimal(preparedAmount)) != 0) {
                throw new UnsupportedOperationException("Transfer amount+fee: " + transferTotal + " not matching with prepared amount: " + preparedAmount);
            }
        }

        LocalDateTime transactionDate = getNow();
        InteropActionEntity action = addAction(transaction, request, transactionDate);
        action.setFee(calculatedFee);

        bookTransfer(request, accountWrapper, action, charges, transactionDate, preparedAmount);

        transactionRepository.save(transaction);
        depositAccountManager.postProductInstanceCommand(accountWrapper.account.getIdentifier(), PRODUCT_INSTANCE_TRANSACTION);

        return InteropTransferResponseData.build(request.getTransactionCode(), action.getState(), action.getExpirationDate(),
                request.getExtensionList(), request.getTransferCode(), transactionDate);
    }

    @Transactional(propagation = Propagation.MANDATORY)
    public InteropTransferResponseData releaseTransfer(@NotNull InteropTransferCommand request) {
        AccountWrapper accountWrapper = validateAccount(request, accountingService.findAccount(request.getAccountId()));
        InteropTransactionEntity transaction = validateAndGetTransaction(request, accountWrapper, getNow());

        InteropActionEntity lastAction = getLastAction(transaction);
        if(lastAction == null || !(PREPARE.equals(lastAction.getActionType()) || RELEASE.equals(lastAction.getActionType()))) {
            throw new UnsupportedOperationException("Can not release amount, previous state invalid!");
        }
        if(RELEASE.equals(lastAction.getActionType())) {
            throw new UnsupportedOperationException("Amount already released!");
        }

        LocalDateTime transactionDate = getNow();
        InteropActionEntity newAction = addAction(transaction, request, transactionDate);
        newAction.setFee(ZERO);

        String transactionId = transaction.getIdentifier();
        JournalEntry prepareJournal = accountingService.findJournalEntry(lastAction.getIdentifier());

        double preparedAmount = prepareJournal.getCreditors().stream()
                .map(Creditor::getAmount)
                .mapToDouble(Double::parseDouble)
                .sum();
        BigDecimal transferTotal = validateTransferAmount(request, accountWrapper);
        if (transferTotal.compareTo(new BigDecimal(preparedAmount)) != 0) {
            throw new UnsupportedOperationException("Transfer amount+fee: "+transferTotal+" not matching with prepared amount: " + preparedAmount);
        }

        String loginUser = getLoginUser();
        final JournalEntry releaseJournal = createJournalEntry(RELEASE.name() + IDENTIFIER_SEPARATOR + transactionId,
                REIMBURSEMENT.getCode(), DateConverter.toIsoString(transactionDate), "release", loginUser);
        HashSet<Debtor> debtors = new HashSet<>(1);
        HashSet<Creditor> creditors = new HashSet<>(1);

        addDebtor(transaction.getPayableAccountIdentifier(), preparedAmount, debtors);
        addCreditor(accountWrapper.account.getIdentifier(), preparedAmount, creditors);

        releaseJournal.setDebtors(debtors);
        releaseJournal.setCreditors(creditors);
        accountingService.createJournalEntry(releaseJournal);
        transactionRepository.save(transaction);

        return InteropTransferResponseData.build(request.getTransactionCode(),
                newAction.getState(),
                newAction.getExpirationDate(),
                request.getExtensionList(),
                request.getTransferCode(),
                transactionDate);
    }

    private Double getWithdrawableBalance(Account account, ProductDefinition productDefinition) {
        // on-hold amount, if any, is subtracted to payable account
        return MathUtil.subtractToZero(account.getBalance(), productDefinition.getMinimumBalance());
    }

    private void bookTransfer(InteropTransferCommand request, AccountWrapper accountWrapper, InteropActionEntity action,
                              List<Charge> charges, LocalDateTime transactionDate, double preparedAmount) {
        String transactionId = request.getIdentifier();
        double transferAmount = request.getAmount().getAmount().doubleValue();
        String loginUser = getLoginUser();
        String transactionDateString = DateConverter.toIsoString(transactionDate);
        InteropTransactionEntity transaction = action.getTransaction();

        if (request.getTransactionRole().isWithdraw()) {
            HashSet<Debtor> debtors = new HashSet<>(1);
            HashSet<Creditor> creditors = new HashSet<>(1);

            Account payableAccount = validateAccount(request, accountingService.findAccount(transaction.getPayableAccountIdentifier())).account;
            double totalCharge = 0;
            for (Charge charge : charges) {
                BigDecimal chargeAmount = calcChargeAmount(action.getAmount(),
                        charge, accountWrapper.productDefinition.getCurrency(),
                        true);
                if(chargeAmount.compareTo(ZERO) > 0) {
                    totalCharge += chargeAmount.doubleValue();
                    addDebtor(payableAccount.getIdentifier(), chargeAmount.doubleValue(), debtors);
                    addCreditor(charge.getIncomeAccountIdentifier(), chargeAmount.doubleValue(), creditors);
                }
            }

            final JournalEntry withdrawJournal = createJournalEntry(action.getIdentifier() + IDENTIFIER_SEPARATOR + transactionId,
                    CURRENCY_WITHDRAWAL.getCode(), transactionDateString, "withdraw-commit", loginUser);

            // move from onhold to nostro
            double finalAmount = preparedAmount - totalCharge;
            addDebtor(payableAccount.getIdentifier(), finalAmount, debtors);
            addCreditor(transaction.getNostroAccountIdentifier(), finalAmount, creditors);

            withdrawJournal.setDebtors(debtors);
            withdrawJournal.setCreditors(creditors);
            accountingService.createJournalEntry(withdrawJournal);
        } else { // deposit
            final JournalEntry depositJournal = createJournalEntry(action.getIdentifier() + IDENTIFIER_SEPARATOR + transactionId,
            CURRENCY_DEPOSIT.getCode(), transactionDateString, "deposit", loginUser);

            HashSet<Debtor> debtors = new HashSet<>(1);
            HashSet<Creditor> creditors = new HashSet<>(1);

            // move from nostro to client account
            addDebtor(transaction.getNostroAccountIdentifier(), transferAmount, debtors);
            addCreditor(accountWrapper.account.getIdentifier(), transferAmount, creditors);

            depositJournal.setDebtors(debtors);
            depositJournal.setCreditors(creditors);
            accountingService.createJournalEntry(depositJournal);
        }
    }

    private JournalEntry createJournalEntry(String actionIdentifier, String transactionType, String transactionDate, String message, String loginUser) {
        final JournalEntry fromPrepareToNostroEntry = new JournalEntry();
        fromPrepareToNostroEntry.setTransactionIdentifier(actionIdentifier);
        fromPrepareToNostroEntry.setTransactionType(transactionType);
        fromPrepareToNostroEntry.setTransactionDate(transactionDate);
        fromPrepareToNostroEntry.setMessage(message);
        fromPrepareToNostroEntry.setClerk(loginUser);
        return fromPrepareToNostroEntry;
    }

    private void addCreditor(String accountNumber, double amount, HashSet<Creditor> creditors) {
        Creditor creditor = new Creditor();
        creditor.setAccountNumber(accountNumber);
        creditor.setAmount(Double.toString(amount));
        creditors.add(creditor);
    }

    private void addDebtor(String accountNumber, double amount, HashSet<Debtor> debtors) {
        Debtor debtor = new Debtor();
        debtor.setAccountNumber(accountNumber);
        debtor.setAmount(Double.toString(amount));
        debtors.add(debtor);
    }

    private BigDecimal calcChargeAmount(BigDecimal amount, Charge charge) {
        return calcChargeAmount(amount, charge, null, false);
    }

    private BigDecimal calcChargeAmount(@NotNull BigDecimal amount, @NotNull Charge charge, Currency currency, boolean norm) {
        Double value = charge.getAmount();
        if (value == null)
            return ZERO;

        BigDecimal portion = BigDecimal.valueOf(100.00d);
        MathContext mc = MathUtil.CALCULATION_MATH_CONTEXT;
        BigDecimal feeAmount = BigDecimal.valueOf(MathUtil.nullToZero(charge.getAmount()));
        BigDecimal result = charge.getProportional()
                ? amount.multiply(feeAmount.divide(portion, mc), mc)
                : feeAmount;
        return norm ? MathUtil.normalize(result, currency) : result;
    }

    @NotNull
    private BigDecimal calcTotalCharges(@NotNull List<Charge> charges, BigDecimal amount) {
        return charges.stream().map(charge -> calcChargeAmount(amount, charge)).reduce(MathUtil::add).orElse(ZERO);
    }

    private AccountWrapper validateAccount(InteropRequestData request, Account account) {
        if (!account.getState().equals(Account.State.OPEN.name())) {
            throw new UnsupportedOperationException("Account is in state " + account.getState());
        }

        ProductDefinition productDefinition = null;
        if (account.getHolders() != null) { // customer account
            ProductInstance product = depositService.findProductInstance(account.getIdentifier());
            productDefinition = depositService.findProductDefinition(product.getProductIdentifier());
            if (!Boolean.TRUE.equals(productDefinition.getActive()))
                throw new UnsupportedOperationException("NOSTRO Product Definition is inactive");

            if (!productDefinition.getCurrency().getCode().equals(request.getAmount().getCurrency())) {
                throw new UnsupportedOperationException("Product definition and request has different currencies!");
            }

            request.normalizeAmounts(productDefinition.getCurrency());
        }

        return new AccountWrapper(account, productDefinition, productDefinition == null ? 0 : getWithdrawableBalance(account, productDefinition));
    }

    @NotNull
    private Account getNostroAccount() {
        List<Account> nostros = fetchAccounts(false, ACCOUNT_NAME_NOSTRO, AccountType.ASSET.name(), false, null, null, null, null);
        int size = nostros.size();
        if (size != 1) {
            throw new UnsupportedOperationException("NOSTRO Account " + (size == 0 ? "not found" : "is ambigous"));
        }
        return nostros.get(0);
    }

    private BigDecimal validateTransferAmount(InteropTransferRequestData request, AccountWrapper accountWrapper) {
        String accountCurrency = accountWrapper.productDefinition.getCurrency().getCode();

        MoneyData transferFee = request.getFspFee();
        if (transferFee != null && !accountCurrency.equals(transferFee.getCurrency())) {
            throw new UnsupportedOperationException("Transfer fee currency is invalid!");
        }
        MoneyData transferCommission = request.getFspCommission();
        if (transferCommission != null && !accountCurrency.equals(transferCommission.getCurrency())) {
            throw new UnsupportedOperationException("Transfer commission currency is invalid!");
        }

        return request.getAmount().getAmount()
                .add(transferFee == null ? ZERO : transferFee.getAmount())
                .add(transferCommission == null ? ZERO : transferCommission.getAmount());
    }

    public List<Account> fetchAccounts(boolean includeClosed, String term, String type, boolean includeCustomerAccounts,
                                       Integer pageIndex, Integer size, String sortColumn, String sortDirection) {
        return accountingService.fetchAccounts(includeClosed, term, type, includeCustomerAccounts, pageIndex, size, sortColumn, sortDirection);
    }

    public InteropIdentifierEntity findIdentifier(@NotNull InteropIdentifierType idType, @NotNull String idValue, String subIdOrType) {
        return identifierRepository.findOne(Specifications.where(idTypeEqual(idType)).and(idValueEqual(idValue)).and(subIdOrTypeEqual(subIdOrType)));
    }

    public static Specification<InteropIdentifierEntity> idTypeEqual(@NotNull InteropIdentifierType idType) {
        return (Root<InteropIdentifierEntity> root, CriteriaQuery<?> query, CriteriaBuilder cb) -> cb.and(cb.equal(root.get("type"), idType));
    }

    public static Specification<InteropIdentifierEntity> idValueEqual(@NotNull String idValue) {
        return (Root<InteropIdentifierEntity> root, CriteriaQuery<?> query, CriteriaBuilder cb) -> cb.and(cb.equal(root.get("value"), idValue));
    }

    public static Specification<InteropIdentifierEntity> subIdOrTypeEqual(String subIdOrType) {
        return (Root<InteropIdentifierEntity> root, CriteriaQuery<?> query, CriteriaBuilder cb) -> {
            Path<Object> path = root.get("subValueOrType");
            return cb.and(subIdOrType == null ? cb.isNull(path) : cb.equal(path, subIdOrType));
        };
    }

    @NotNull
    private String calcActionIdentifier(@NotNull String actionCode, @NotNull InteropActionType actionType) {
        return (actionType == QUOTE || actionType == PREPARE || actionType == COMMIT || actionType == RELEASE) ? actionType + IDENTIFIER_SEPARATOR + actionCode : actionCode;
    }

    private InteropActionEntity validateAndGetAction(@NotNull String transactionCode, @NotNull String actionIdentifier, @NotNull InteropActionType actionType) {
        return validateAndGetAction(transactionCode, actionIdentifier, actionType, true);
    }

    private InteropActionEntity validateAndGetAction(@NotNull String transactionCode, @NotNull String actionIdentifier, @NotNull InteropActionType actionType, boolean one) {
        //TODO: error handling
        InteropActionEntity action = actionRepository.findByIdentifier(actionIdentifier);
        if (action == null) {
            if (one)
                throw new UnsupportedOperationException("Interperation action " + actionType + '/' + actionIdentifier + " was not found for this transaction " + transactionCode);
            return null;
        }
        if (!action.getTransaction().getIdentifier().equals(transactionCode))
            throw new UnsupportedOperationException("Interperation action " + actionType + '/' + actionIdentifier + " does not exist in this transaction " + transactionCode);
        if (action.getActionType() != actionType)
            throw new UnsupportedOperationException("Interperation action " + actionIdentifier + " is not this type " + actionType);

        return action;
    }

    private InteropTransactionEntity validateAndGetTransaction(InteropRequestData request, AccountWrapper accountWrapper, LocalDateTime createAt) {
        InteropTransactionEntity transaction = transactionRepository.findOneByIdentifier(request.getTransactionCode());
        if (transaction == null) {
            transaction = new InteropTransactionEntity(request.getTransactionCode(),
                    accountWrapper.account.getIdentifier(),
                    getLoginUser(),
                    createAt);
            InteropState state = InteropStateMachine.handleTransition(null, request.getActionType());
            transaction.setState(state);
            transaction.setAmount(request.getAmount().getAmount());
            transaction.setName(request.getNote());
            transaction.setTransactionType(request.getTransactionRole().getTransactionType());
            transaction.setPayableAccountIdentifier(validateAccount(request, accountingService.findAccount(accountWrapper.account.getReferenceAccount())).account.getIdentifier());
            transaction.setNostroAccountIdentifier(validateAccount(request, getNostroAccount()).account.getIdentifier());
            transaction.setExpirationDate(request.getExpiration());
        }
        LocalDateTime expirationDate = transaction.getExpirationDate();
        if (expirationDate != null && expirationDate.isBefore(createAt))
            throw new UnsupportedOperationException("Interperation transaction expired on " + expirationDate);
        return transaction;
    }

    private Currency getCurrency(InteropActionEntity action) {
        ProductInstance product = depositService.findProductInstance(action.getTransaction().getCustomerAccountIdentifier());
        ProductDefinition productDefinition = depositService.findProductDefinition(product.getProductIdentifier());
        return productDefinition.getCurrency();
    }

    private InteropActionEntity addAction(@NotNull InteropTransactionEntity transaction, @NotNull InteropRequestData request) {
        return addAction(transaction, request, getNow());
    }

    private InteropActionEntity addAction(@NotNull InteropTransactionEntity transaction, @NotNull InteropRequestData request,
                                          @NotNull LocalDateTime createdOn) {
        InteropActionEntity lastAction = getLastAction(transaction);

        InteropActionType actionType = request.getActionType();
        String actionIdentifier = calcActionIdentifier(request.getIdentifier(), request.getActionType());
        InteropActionEntity action = new InteropActionEntity(actionIdentifier, transaction, request.getActionType(),
                (lastAction == null ? 0 : lastAction.getSeqNo() + 1), getLoginUser(),
                (lastAction == null ? transaction.getCreatedOn() : createdOn));
        action.setState(InteropActionState.ACCEPTED);
        action.setAmount(request.getAmount().getAmount());
        action.setExpirationDate(request.getExpiration());
        transaction.getActions().add(action);

        InteropState currentState = transaction.getState();
        if (transaction.getId() != null || InteropStateMachine.isValidAction(currentState, actionType)) // newly created was already set
            transaction.setState(InteropStateMachine.handleTransition(currentState, actionType));
        return action;
    }

    private InteropActionEntity getLastAction(InteropTransactionEntity transaction) {
        return transaction.getActions().stream()
                .max(comparing(InteropActionEntity::getSeqNo))
                .orElse(null);
    }

    private InteropActionEntity findAction(InteropTransactionEntity transaction, InteropActionType actionType) {
        List<InteropActionEntity> actions = transaction.getActions();
        for (InteropActionEntity action : actions) {
            if (action.getActionType() == actionType)
                return action;
        }
        return null;
    }

    private LocalDateTime getNow() {
        return LocalDateTime.now(Clock.systemUTC());
    }

    private String getLoginUser() {
        return UserContextHolder.checkedGetUser();
    }

    public static class AccountWrapper {

        @NotNull
        private final Account account;

        @NotNull
        private final ProductDefinition productDefinition;

        @NotNull
        private final Double withdrawableBalance;

        public AccountWrapper(Account account, ProductDefinition productDefinition, Double withdrawableBalance) {
            this.account = account;
            this.productDefinition = productDefinition;
            this.withdrawableBalance = withdrawableBalance;
        }
    }
}
