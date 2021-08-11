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
import com.amazonaws.athena.connector.lambda.data.BlockAllocator;
import com.amazonaws.athena.connector.lambda.data.BlockAllocatorImpl;
import com.amazonaws.athena.connector.lambda.data.BlockSpiller;
import com.amazonaws.athena.connector.lambda.domain.predicate.ConstraintEvaluator;
import com.amazonaws.athena.connector.lambda.domain.spill.SpillLocation;
import com.google.common.collect.ImmutableList;
import com.palantir.logsafe.exceptions.SafeIllegalStateException;
import java.util.List;
import org.apache.arrow.vector.types.pojo.Schema;

/**
 * An implementation of {@link BlockSpiller} that will never spill and uses a single {@link Block}.
 */
final class InMemorySingleBlockSpiller implements BlockSpiller {

    private final Block block;
    private final ConstraintEvaluator constraintEvaluator;

    InMemorySingleBlockSpiller(Schema schema, ConstraintEvaluator constraintEvaluator) {
        this(schema, constraintEvaluator, new BlockAllocatorImpl());
    }

    InMemorySingleBlockSpiller(Schema schema, ConstraintEvaluator constraintEvaluator, BlockAllocator blockAllocator) {
        this.block = blockAllocator.createBlock(schema);
        this.constraintEvaluator = constraintEvaluator;
    }

    @Override
    public Block getBlock() {
        return block;
    }

    @Override
    public boolean spilled() {
        return false;
    }

    @Override
    public List<SpillLocation> getSpillLocations() {
        return ImmutableList.of();
    }

    @Override
    public void close() {
        try {
            block.close();
        } catch (Exception e) {
            throw new SafeIllegalStateException("unable to close block", e);
        }
    }

    @Override
    public void writeRows(RowWriter rowWriter) {
        int rowCount = block.getRowCount();
        block.getRowCount();
        int rowsWritten;
        try {
            rowsWritten = rowWriter.writeRows(block, rowCount);
        } catch (Exception ex) {
            throw (ex instanceof RuntimeException) ? (RuntimeException) ex : new RuntimeException(ex);
        }
        if (rowsWritten > 0) {
            block.setRowCount(rowCount + rowsWritten);
        }
    }

    @Override
    public ConstraintEvaluator getConstraintEvaluator() {
        return constraintEvaluator;
    }
}
