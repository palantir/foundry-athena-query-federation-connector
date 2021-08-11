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

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.Channels;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.ipc.ReadChannel;
import org.apache.arrow.vector.ipc.message.MessageChannelReader;

public final class PeekableArrowStreamReader extends ArrowStreamReader {

    private static final int BUFFER_SIZE = 8;

    private final BufferedInputStream in;

    public PeekableArrowStreamReader(InputStream in, BufferAllocator allocator) {
        this(allocator, new BufferedInputStream(in, BUFFER_SIZE));
    }

    private PeekableArrowStreamReader(BufferAllocator allocator, BufferedInputStream in) {
        this(in, new ReadChannel(Channels.newChannel(in)), allocator);
    }

    private PeekableArrowStreamReader(BufferedInputStream in, ReadChannel readChannel, BufferAllocator allocator) {
        super(new MessageChannelReader(readChannel, allocator), allocator);
        this.in = in;
    }

    public boolean hasNextBatch() throws IOException {
        // check that there are still bytes in the stream
        in.mark(BUFFER_SIZE);
        int bytesRead = in.readNBytes(BUFFER_SIZE).length;
        in.reset();
        return bytesRead > 0;
    }
}
