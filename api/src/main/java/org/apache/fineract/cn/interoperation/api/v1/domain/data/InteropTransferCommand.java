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
package org.apache.fineract.cn.interoperation.api.v1.domain.data;

import org.apache.fineract.cn.interoperation.api.v1.domain.InteropActionType;
import org.apache.fineract.cn.interoperation.api.v1.domain.InteropTransferActionType;

import javax.validation.constraints.NotNull;
import java.beans.Transient;

import static org.apache.fineract.cn.interoperation.api.v1.domain.InteropTransferActionType.PREPARE;

public class InteropTransferCommand extends InteropTransferRequestData {

    @NotNull
    private final InteropTransferActionType action;

    public InteropTransferCommand(@NotNull InteropTransferRequestData requestData, @NotNull InteropTransferActionType action) {
        super(requestData.getTransactionCode(), requestData.getAccountId(), requestData.getAmount(), requestData.getTransactionRole(),
                requestData.getNote(), requestData.getExpiration(), requestData.getExtensionList(), requestData.getTransferCode(),
                requestData.getFspFee(), requestData.getFspCommission());
        this.action = action;
    }

    public InteropTransferActionType getAction() {
        return action;
    }

    @NotNull
    @Transient
    @Override
    public InteropActionType getActionType() {
        switch (action) {
            case PREPARE:
                return InteropActionType.PREPARE;
            case CREATE:
                return InteropActionType.COMMIT;
            case RELEASE:
                return InteropActionType.RELEASE;
            default:
                throw new RuntimeException("Unknown InteropTransferActionType: " + action);
        }
    }
}
