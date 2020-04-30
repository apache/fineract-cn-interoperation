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
package org.apache.fineract.cn.interoperation.api.v1.domain;

public enum InteropState {

    REQUESTED,
    QUOTED,
    PREPARED,
    COMMITTED,
    ABORTED,
    RELEASED;

    public static InteropState forAction(InteropActionType action) {
        switch (action) {
            case REQUEST:
                return REQUESTED;
            case QUOTE:
                return QUOTED;
            case PREPARE:
                return PREPARED;
            case RELEASE:
                return RELEASED;
            case COMMIT:
                return COMMITTED;
            case ABORT:
                return ABORTED;
            default:
                return null; // TODO fail
        }
    }
}
