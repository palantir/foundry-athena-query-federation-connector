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

import com.google.common.collect.Streams;
import com.palantir.logsafe.exceptions.SafeIllegalStateException;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.channels.Channels;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BitVectorHelper;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.VectorUnloader;
import org.apache.arrow.vector.ipc.WriteChannel;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.ipc.message.MessageSerializer;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;

public final class ArrowUtils {

    public static VectorSchemaRoot createBatch(Schema schema, List<List<Object>> columnarData) {
        VectorSchemaRoot vectorSchemaRoot = makeVectorSchemaRoot(TestConstants.ALLOCATOR, schema, columnarData);
        Streams.forEachPair(
                schema.getFields().stream(),
                columnarData.stream(),
                (field, columnData) -> writeToFieldVector(vectorSchemaRoot, field, columnData));
        return vectorSchemaRoot;
    }

    public static ArrowRecordBatch readRecordBatch(
            BufferAllocator allocator, Schema schema, List<List<Object>> columnarData) {
        VectorSchemaRoot vectorSchemaRoot = makeVectorSchemaRoot(allocator, schema, columnarData);
        Streams.forEachPair(
                schema.getFields().stream(),
                columnarData.stream(),
                (field, columnData) -> writeToFieldVector(vectorSchemaRoot, field, columnData));
        return new VectorUnloader(vectorSchemaRoot).getRecordBatch();
    }

    public static InputStream writeToStream(Schema schema, List<VectorSchemaRoot> vectors) {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        WriteChannel writeChannel = new WriteChannel(Channels.newChannel(outputStream));
        writeSchema(writeChannel, schema);

        vectors.forEach(vector -> writeRecordBatch(writeChannel, new VectorUnloader(vector)));

        return new ByteArrayInputStream(outputStream.toByteArray());
    }

    private static VectorSchemaRoot makeVectorSchemaRoot(
            BufferAllocator allocator, Schema schema, List<List<Object>> columnarData) {
        return new VectorSchemaRoot(
                schema,
                schema.getFields().stream().map(t -> t.createVector(allocator)).collect(Collectors.toList()),
                columnarData.stream()
                        .map(List::size)
                        .max(Comparator.comparingInt(size -> size))
                        .orElse(0));
    }

    private static void writeToFieldVector(VectorSchemaRoot vectorSchemaRoot, Field field, List<Object> values) {
        FieldVector vector = vectorSchemaRoot.getVector(field.getName());
        vector.allocateNew();
        vector.setInitialCapacity(values.size());
        vector.setValueCount(values.size());
        for (int i = 0; i < values.size(); i++) {
            Object value = values.get(i);
            if (value == null) {
                BitVectorHelper.setValidityBit(vector.getValidityBuffer(), i, 0);
            } else if (vector instanceof IntVector) {
                IntVector intVector = (IntVector) vector;
                intVector.set(i, (int) value);
            } else if (vector instanceof Float8Vector) {
                Float8Vector floatVector = (Float8Vector) vector;
                floatVector.set(i, (double) value);
            }
        }
    }

    private static void writeSchema(WriteChannel writeChannel, Schema schema) {
        try {
            MessageSerializer.serialize(writeChannel, schema);
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to serialize schema", e);
        }
    }

    private static void writeRecordBatch(WriteChannel writeChannel, VectorUnloader unloader) {
        try {
            MessageSerializer.serialize(writeChannel, unloader.getRecordBatch());
        } catch (IOException e) {
            throw new SafeIllegalStateException("Failed to serialize arrow record batch", e);
        }
    }

    private ArrowUtils() {}
}
