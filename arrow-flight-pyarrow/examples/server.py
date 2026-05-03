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

"""Minimal example of running a Flight server in Python.

Build with maturin first:

    maturin develop --release

Then run this file. Connect with any Flight client, e.g.

    import pyarrow.flight as flight
    client = flight.connect("grpc://127.0.0.1:50051")
    reader = client.do_get(flight.Ticket(b"hello"))
    print(reader.read_all())
"""

import pyarrow as pa

from arrow_flight_pyarrow import FlightServer


def do_get(ticket: bytes) -> pa.RecordBatchReader:
    schema = pa.schema([("ticket", pa.string()), ("value", pa.int64())])
    batch = pa.record_batch(
        [
            pa.array([ticket.decode("utf-8", errors="replace")] * 3),
            pa.array([1, 2, 3]),
        ],
        schema=schema,
    )
    return pa.RecordBatchReader.from_batches(schema, [batch])


if __name__ == "__main__":
    server = FlightServer(do_get)
    print("Serving Flight on 0.0.0.0:50051 (Ctrl-C to stop)")
    server.serve("0.0.0.0:50051")
