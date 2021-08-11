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
import java.util.ArrayList;
import java.util.List;
import org.apache.arrow.vector.complex.reader.FieldReader;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;

public final class TestUtils {

    public static List<List<Object>> readBlockDataAsColumns(Schema schema, Block block) {
        List<List<Object>> data = new ArrayList<>();
        for (Field field : schema.getFields()) {
            FieldReader fieldReader = block.getFieldReader(field.getName());
            List<Object> columnValues = new ArrayList<>();
            for (int idx = 0; idx < block.getRowCount(); idx++) {
                fieldReader.setPosition(idx);
                columnValues.add(fieldReader.readObject());
            }
            data.add(columnValues);
        }
        return data;
    }

    private TestUtils() {}
}
