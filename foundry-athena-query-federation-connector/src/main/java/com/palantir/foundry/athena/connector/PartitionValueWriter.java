/*
 * (c) Copyright 2020 Palantir Technologies Inc. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.palantir.foundry.athena.connector;

import com.amazonaws.athena.connector.lambda.data.Block;
import com.palantir.conjure.java.lib.SafeLong;
import com.palantir.foundry.athena.api.DateDay;
import com.palantir.foundry.athena.api.PartitionValue;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.UnsafeArg;
import com.palantir.logsafe.exceptions.SafeIllegalArgumentException;
import java.math.BigDecimal;
import java.time.OffsetDateTime;

final class PartitionValueWriter implements PartitionValue.Visitor<Boolean> {
    private final Block block;
    private final String fieldName;
    private final int rowNum;

    PartitionValueWriter(Block block, String fieldName, int rowNum) {
        this.block = block;
        this.fieldName = fieldName;
        this.rowNum = rowNum;
    }

    @Override
    public Boolean visitBoolean(boolean value) {
        return block.setValue(fieldName, rowNum, value);
    }

    @Override
    public Boolean visitByte(int value) {
        return block.setValue(fieldName, rowNum, value);
    }

    @Override
    public Boolean visitDate(DateDay value) {
        return block.setValue(fieldName, rowNum, value.get().longValue());
    }

    @Override
    public Boolean visitDatetime(OffsetDateTime value) {
        return block.setValue(fieldName, rowNum, value.toZonedDateTime());
    }

    @Override
    public Boolean visitDecimal(BigDecimal value) {
        return block.setValue(fieldName, rowNum, value);
    }

    @Override
    public Boolean visitDouble(double value) {
        return block.setValue(fieldName, rowNum, value);
    }

    @Override
    public Boolean visitFloat(double value) {
        return block.setValue(fieldName, rowNum, Double.valueOf(value).floatValue());
    }

    @Override
    public Boolean visitInteger(int value) {
        return block.setValue(fieldName, rowNum, value);
    }

    @Override
    public Boolean visitLong(SafeLong value) {
        return block.setValue(fieldName, rowNum, value.longValue());
    }

    @Override
    public Boolean visitShort(int value) {
        return block.setValue(fieldName, rowNum, value);
    }

    @Override
    public Boolean visitString(String value) {
        return block.setValue(fieldName, rowNum, value);
    }

    @Override
    public Boolean visitUnknown(String unknownType) {
        throw new SafeIllegalArgumentException(
                "could not handle partition value with unknown type",
                SafeArg.of("unknownType", unknownType),
                SafeArg.of("rowNum", rowNum),
                UnsafeArg.of("fieldName", fieldName));
    }
}
