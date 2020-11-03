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
package org.apache.fineract.cn.interoperation.service.internal.command.handler;

import org.apache.fineract.cn.command.annotation.Aggregate;
import org.apache.fineract.cn.command.annotation.CommandHandler;
import org.apache.fineract.cn.command.annotation.CommandLogLevel;
import org.apache.fineract.cn.interoperation.api.v1.domain.data.InteropIdentifierCommand;
import org.apache.fineract.cn.interoperation.api.v1.domain.data.InteropIdentifierData;
import org.apache.fineract.cn.interoperation.api.v1.domain.data.InteropIdentifierDeleteCommand;
import org.apache.fineract.cn.interoperation.api.v1.domain.data.InteropQuoteRequestData;
import org.apache.fineract.cn.interoperation.api.v1.domain.data.InteropQuoteResponseData;
import org.apache.fineract.cn.interoperation.api.v1.domain.data.InteropTransactionRequestData;
import org.apache.fineract.cn.interoperation.api.v1.domain.data.InteropTransactionRequestResponseData;
import org.apache.fineract.cn.interoperation.api.v1.domain.data.InteropTransferCommand;
import org.apache.fineract.cn.interoperation.api.v1.domain.data.InteropTransferResponseData;
import org.apache.fineract.cn.interoperation.service.ServiceConstants;
import org.apache.fineract.cn.interoperation.service.internal.service.InteropService;
import org.slf4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.transaction.annotation.Transactional;

import javax.validation.constraints.NotNull;

@SuppressWarnings("unused")
@Aggregate
public class InteropHandler {

    private final Logger logger;
    private final InteropService interopService;

    @Autowired
    public InteropHandler(@Qualifier(ServiceConstants.LOGGER_NAME) final Logger logger,
                          InteropService interopService) {
        this.logger = logger;
        this.interopService = interopService;
    }

    @NotNull
    @Transactional
    @CommandHandler(logStart = CommandLogLevel.INFO, logFinish = CommandLogLevel.INFO)
    public InteropIdentifierData registerAccountIdentifier(@NotNull InteropIdentifierCommand command) {
        return interopService.registerAccountIdentifier(command);
    }

    @NotNull
    @Transactional
    @CommandHandler(logStart = CommandLogLevel.INFO, logFinish = CommandLogLevel.INFO)
    public InteropIdentifierData deleteAccountIdentifier(@NotNull InteropIdentifierDeleteCommand command) {
        return interopService.deleteAccountIdentifier(command);
    }

    @NotNull
    @Transactional
    @CommandHandler(logStart = CommandLogLevel.INFO, logFinish = CommandLogLevel.INFO)
    public InteropTransactionRequestResponseData createTransactionRequest(@NotNull InteropTransactionRequestData command) {
        return interopService.createTransactionRequest(command);
    }

    @NotNull
    @Transactional
    @CommandHandler(logStart = CommandLogLevel.INFO, logFinish = CommandLogLevel.INFO)
    public InteropQuoteResponseData createQuote(@NotNull InteropQuoteRequestData command) {
        return interopService.createQuote(command);
    }

    @NotNull
    @Transactional
    @CommandHandler(logStart = CommandLogLevel.INFO, logFinish = CommandLogLevel.INFO)
    public InteropTransferResponseData performTransfer(@NotNull InteropTransferCommand command) {
        switch (command.getAction()) {
            case PREPARE:
                return interopService.prepareTransfer(command);
            case CREATE:
                return interopService.commitTransfer(command);
            case RELEASE:
                return interopService.releaseTransfer(command);
            default:
                return null;
        }
    }
}
