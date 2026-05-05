# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""Example of running a Flight server backed by an async callback.

The callback is `async def`, so it runs on a dedicated asyncio event loop
managed by the server. Any awaitables (e.g. asyncpg, httpx) can be used as
normal; the loop persists across requests so connection pools work.

Build with maturin first:

    maturin develop --release

Then run this file and connect with any Flight client.
"""

import asyncio
import pyarrow as pa

from arrow_flight_pyarrow import FlightServer


async def do_get(ticket: bytes) -> pa.RecordBatchReader:
    # Simulate doing async I/O (e.g. an async DB query).
    await asyncio.sleep(0.05)

    schema = pa.schema([("ticket", pa.string()), ("value", pa.int64())])
    batch = pa.record_batch(
        [
            pa.array([ticket.decode("utf-8", errors="replace")] * 3),
            pa.array([10, 20, 30]),
        ],
        schema=schema,
    )
    return pa.RecordBatchReader.from_batches(schema, [batch])


if __name__ == "__main__":
    server = FlightServer(do_get)
    print("Serving Flight on 0.0.0.0:50051 (async callback, Ctrl-C to stop)")
    server.serve("0.0.0.0:50051")
