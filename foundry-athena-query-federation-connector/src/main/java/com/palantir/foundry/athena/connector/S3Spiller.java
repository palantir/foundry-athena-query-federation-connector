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
import com.amazonaws.athena.connector.lambda.data.BlockSpiller;
import com.amazonaws.athena.connector.lambda.data.S3BlockSpiller;
import com.amazonaws.athena.connector.lambda.data.SpillConfig;
import com.amazonaws.athena.connector.lambda.domain.predicate.ConstraintEvaluator;
import com.amazonaws.athena.connector.lambda.domain.spill.S3SpillLocation;
import com.amazonaws.athena.connector.lambda.domain.spill.SpillLocation;
import com.amazonaws.athena.connector.lambda.security.AesGcmBlockCrypto;
import com.amazonaws.athena.connector.lambda.security.BlockCrypto;
import com.amazonaws.athena.connector.lambda.security.EncryptionKey;
import com.amazonaws.athena.connector.lambda.security.NoOpBlockCrypto;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.palantir.logsafe.exceptions.SafeIllegalStateException;
import com.palantir.logsafe.exceptions.SafeRuntimeException;
import java.io.ByteArrayInputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Similar to {@link S3BlockSpiller} except we only support explicitly spilling blocks.
 */
@SuppressWarnings("Slf4jLogsafeArgs")
public final class S3Spiller implements BlockSpiller {

    private static final Logger log = LoggerFactory.getLogger(S3Spiller.class);

    private final AmazonS3 amazonS3;
    private final BlockCrypto blockCrypto;
    private final SpillConfig spillConfig;
    private final List<SpillLocation> spillLocations = new ArrayList<>();
    private final AtomicLong spillNumber = new AtomicLong(0);
    private final AtomicReference<RuntimeException> asyncException = new AtomicReference<>(null);
    private final AtomicLong totalBytesSpilled = new AtomicLong();

    S3Spiller(AmazonS3 amazonS3, SpillConfig spillConfig, BlockAllocator allocator) {
        this.amazonS3 = amazonS3;
        this.blockCrypto = spillConfig.getEncryptionKey() != null
                ? new AesGcmBlockCrypto(allocator)
                : new NoOpBlockCrypto(allocator);
        this.spillConfig = spillConfig;
    }

    @Override
    public boolean spilled() {
        if (asyncException.get() != null) {
            throw asyncException.get();
        }

        return !spillLocations.isEmpty();
    }

    @Override
    public Block getBlock() {
        throw new UnsupportedOperationException("S3Spiller does not hold a block");
    }

    @Override
    public List<SpillLocation> getSpillLocations() {
        if (!spilled()) {
            throw new SafeIllegalStateException(
                    "Blocks have not spilled, calls to getSpillLocations not permitted. use getBlock instead.");
        }
        return spillLocations;
    }

    @Override
    public void close() {}

    @Override
    public void writeRows(RowWriter _rowWriter) {
        throw new UnsupportedOperationException("S3Spiller does not hold a block");
    }

    @Override
    public ConstraintEvaluator getConstraintEvaluator() {
        return null;
    }

    public void spillBlock(Block block) {
        SpillLocation spillLocation = write(block);
        spillLocations.add(spillLocation);
        safeClose(block);
    }

    private SpillLocation write(Block block) {
        try {
            S3SpillLocation spillLocation = makeSpillLocation();
            EncryptionKey encryptionKey = spillConfig.getEncryptionKey();

            log.info("write: Started encrypting block for write to {}", spillLocation);
            byte[] bytes = blockCrypto.encrypt(encryptionKey, block);

            totalBytesSpilled.addAndGet(bytes.length);

            log.info("write: Started spilling block of size {} bytes", bytes.length);

            ObjectMetadata objectMetadata = new ObjectMetadata();
            objectMetadata.setContentLength(bytes.length);
            amazonS3.putObject(
                    spillLocation.getBucket(), spillLocation.getKey(), new ByteArrayInputStream(bytes), objectMetadata);
            log.info("write: Completed spilling block of size {} bytes", bytes.length);

            return spillLocation;
        } catch (RuntimeException ex) {
            asyncException.compareAndSet(null, ex);
            log.warn("write: Encountered error while writing block.", ex);
            throw ex;
        }
    }

    private S3SpillLocation makeSpillLocation() {
        S3SpillLocation splitSpillLocation = (S3SpillLocation) spillConfig.getSpillLocation();
        if (!splitSpillLocation.isDirectory()) {
            throw new SafeRuntimeException(
                    "Split's SpillLocation must be a directory because multiple blocks may be spilled.");
        }
        String blockKey = splitSpillLocation.getKey() + "." + spillNumber.getAndIncrement();
        return new S3SpillLocation(splitSpillLocation.getBucket(), blockKey, false);
    }

    private void safeClose(AutoCloseable block) {
        try {
            block.close();
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }
}
