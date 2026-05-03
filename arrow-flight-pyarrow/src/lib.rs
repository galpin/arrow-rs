// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Minimal Apache Arrow Flight server bindings for Python.
//!
//! This crate exposes a single Python class, `FlightServer`, that lets users
//! implement a Flight `do_get` endpoint with a Python callback. The callback
//! receives the ticket bytes and is expected to return any object implementing
//! the Arrow PyCapsule stream interface (e.g. a `pyarrow.RecordBatchReader`
//! or a `pyarrow.Table`).
//!
//! The callback may be a regular function or an `async def` coroutine
//! function. When `serve()` starts, a dedicated asyncio event loop is launched
//! on a Python thread; coroutine return values are scheduled on that loop and
//! awaited before the result is encoded as Flight data.
//!
//! All other Flight RPCs return `UNIMPLEMENTED`.

#![warn(missing_docs)]

use std::ffi::CStr;
use std::net::SocketAddr;
use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_array::RecordBatchReader;
use arrow_array::ffi_stream::ArrowArrayStreamReader;
use arrow_flight::encode::FlightDataEncoderBuilder;
use arrow_flight::error::FlightError;
use arrow_flight::flight_service_server::{FlightService, FlightServiceServer};
use arrow_flight::{
    Action, ActionType, Criteria, Empty, FlightData, FlightDescriptor, FlightInfo,
    HandshakeRequest, HandshakeResponse, PollInfo, PutResult, SchemaResult, Ticket,
};
use arrow_pyarrow::FromPyArrow;
use arrow_schema::SchemaRef;
use futures::stream::{BoxStream, StreamExt};
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict};
use tokio_stream::wrappers::ReceiverStream;
use tonic::transport::Server;
use tonic::{Request, Response, Status, Streaming};

/// Bound on the number of un-encoded record batches buffered between the
/// reader thread and the gRPC encoder. A small value gives backpressure
/// to a fast Python producer when the network is slow.
const BATCH_CHANNEL_CAPACITY: usize = 2;

/// Internal `FlightService` implementation that delegates `do_get` to a
/// Python callable and returns `UNIMPLEMENTED` for every other RPC.
struct PyFlightService {
    do_get: Arc<Py<PyAny>>,
    /// Optional persistent asyncio event loop running on a dedicated Python
    /// thread; coroutine results from `do_get` are awaited on this loop.
    event_loop: Option<Arc<Py<PyAny>>>,
}

/// If `value` is a coroutine, schedule it on `event_loop` (which must be
/// running on a separate Python thread) and block until it resolves; otherwise
/// return `value` unchanged.
fn await_if_coroutine<'py>(
    py: Python<'py>,
    value: Bound<'py, PyAny>,
    event_loop: Option<&Arc<Py<PyAny>>>,
) -> PyResult<Bound<'py, PyAny>> {
    let asyncio = py.import("asyncio")?;
    if !asyncio
        .call_method1("iscoroutine", (&value,))?
        .is_truthy()?
    {
        return Ok(value);
    }
    let loop_ = event_loop.ok_or_else(|| {
        PyRuntimeError::new_err(
            "do_get returned a coroutine but no asyncio event loop is running",
        )
    })?;
    let cf = asyncio.call_method1("run_coroutine_threadsafe", (value, loop_.bind(py)))?;
    cf.call_method0("result")
}

#[tonic::async_trait]
impl FlightService for PyFlightService {
    type HandshakeStream = BoxStream<'static, Result<HandshakeResponse, Status>>;
    type ListFlightsStream = BoxStream<'static, Result<FlightInfo, Status>>;
    type DoGetStream = BoxStream<'static, Result<FlightData, Status>>;
    type DoPutStream = BoxStream<'static, Result<PutResult, Status>>;
    type DoActionStream = BoxStream<'static, Result<arrow_flight::Result, Status>>;
    type ListActionsStream = BoxStream<'static, Result<ActionType, Status>>;
    type DoExchangeStream = BoxStream<'static, Result<FlightData, Status>>;

    async fn handshake(
        &self,
        _request: Request<Streaming<HandshakeRequest>>,
    ) -> Result<Response<Self::HandshakeStream>, Status> {
        Err(Status::unimplemented("handshake"))
    }

    async fn list_flights(
        &self,
        _request: Request<Criteria>,
    ) -> Result<Response<Self::ListFlightsStream>, Status> {
        Err(Status::unimplemented("list_flights"))
    }

    async fn get_flight_info(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        Err(Status::unimplemented("get_flight_info"))
    }

    async fn poll_flight_info(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<PollInfo>, Status> {
        Err(Status::unimplemented("poll_flight_info"))
    }

    async fn get_schema(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<SchemaResult>, Status> {
        Err(Status::unimplemented("get_schema"))
    }

    async fn do_get(
        &self,
        request: Request<Ticket>,
    ) -> Result<Response<Self::DoGetStream>, Status> {
        let ticket = request.into_inner().ticket;
        let do_get = self.do_get.clone();
        let event_loop = self.event_loop.clone();

        // Step 1: invoke the Python callback (awaiting if it returned a
        // coroutine) and extract a `RecordBatchReader` plus its schema. This
        // is a single short GIL-held operation; we do *not* drain the reader
        // here.
        let (reader, schema) = tokio::task::spawn_blocking(move || {
            Python::attach(|py| -> PyResult<(ArrowArrayStreamReader, SchemaRef)> {
                let cb = do_get.bind(py);
                let py_ticket = PyBytes::new(py, &ticket);
                let result = cb.call1((py_ticket,))?;
                let result = await_if_coroutine(py, result, event_loop.as_ref())?;
                let reader = ArrowArrayStreamReader::from_pyarrow_bound(&result)?;
                let schema = reader.schema();
                Ok((reader, schema))
            })
        })
        .await
        .map_err(|e| Status::internal(format!("do_get task panicked: {e}")))?
        .map_err(|e| Status::internal(format!("do_get callback failed: {e}")))?;

        // Step 2: pull batches from the reader on a blocking thread and push
        // them through a bounded channel. Each `.next()` call (and the final
        // Drop) re-acquires the GIL so Python-implemented stream readers
        // and PyArrow's release callbacks work correctly.
        let (tx, rx) = tokio::sync::mpsc::channel::<Result<RecordBatch, FlightError>>(
            BATCH_CHANNEL_CAPACITY,
        );
        tokio::task::spawn_blocking(move || {
            let mut reader = reader;
            loop {
                let next = Python::attach(|_py| reader.next());
                match next {
                    Some(Ok(batch)) => {
                        if tx.blocking_send(Ok(batch)).is_err() {
                            break; // consumer (client) went away
                        }
                    }
                    Some(Err(e)) => {
                        let _ = tx.blocking_send(Err(e.into()));
                        break;
                    }
                    None => break,
                }
            }
            // Drop the FFI reader under the GIL: PyArrow's release callback
            // is implemented in Python and would deadlock otherwise.
            Python::attach(|_py| drop(reader));
        });

        let input = ReceiverStream::new(rx);
        let encoded = FlightDataEncoderBuilder::new()
            .with_schema(schema)
            .build(input)
            .map(|r| r.map_err(|e| Status::internal(e.to_string())));

        Ok(Response::new(encoded.boxed()))
    }

    async fn do_put(
        &self,
        _request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoPutStream>, Status> {
        Err(Status::unimplemented("do_put"))
    }

    async fn do_action(
        &self,
        _request: Request<Action>,
    ) -> Result<Response<Self::DoActionStream>, Status> {
        Err(Status::unimplemented("do_action"))
    }

    async fn list_actions(
        &self,
        _request: Request<Empty>,
    ) -> Result<Response<Self::ListActionsStream>, Status> {
        Err(Status::unimplemented("list_actions"))
    }

    async fn do_exchange(
        &self,
        _request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoExchangeStream>, Status> {
        Err(Status::unimplemented("do_exchange"))
    }
}

/// Python helper that starts a dedicated asyncio event loop on a daemon thread
/// and returns it. The thread keeps running for the lifetime of the loop; the
/// caller is responsible for stopping the loop when done.
const START_LOOP_PY: &CStr = c"
import asyncio
import threading

def _start_loop():
    loop = asyncio.new_event_loop()
    started = threading.Event()
    def runner():
        asyncio.set_event_loop(loop)
        started.set()
        loop.run_forever()
    threading.Thread(target=runner, daemon=True).start()
    started.wait()
    return loop

loop = _start_loop()
";

fn start_event_loop(py: Python<'_>) -> PyResult<Py<PyAny>> {
    let globals = PyDict::new(py);
    py.run(START_LOOP_PY, Some(&globals), None)?;
    Ok(globals
        .get_item("loop")?
        .ok_or_else(|| PyRuntimeError::new_err("failed to start asyncio event loop"))?
        .unbind())
}

fn stop_event_loop(py: Python<'_>, event_loop: &Py<PyAny>) {
    let bound = event_loop.bind(py);
    if let Ok(stop) = bound.getattr("stop")
        && let Err(e) = bound.call_method1("call_soon_threadsafe", (stop,))
    {
        e.write_unraisable(py, Some(bound));
    }
}

/// Minimal Python-facing Flight server.
///
/// Construct with a `do_get` callback, then call `serve(addr)` to start
/// listening. The callback receives the ticket as `bytes` and must return an
/// object implementing the Arrow PyCapsule stream interface (for example a
/// `pyarrow.RecordBatchReader` or `pyarrow.Table`). The callback may also be
/// `async def`; coroutine return values are awaited on a dedicated asyncio
/// event loop that runs for the lifetime of `serve()`.
#[pyclass(module = "arrow_flight_pyarrow")]
struct FlightServer {
    do_get: Py<PyAny>,
}

#[pymethods]
impl FlightServer {
    #[new]
    fn new(do_get: Py<PyAny>) -> Self {
        Self { do_get }
    }

    /// Start serving on `addr` (for example `"0.0.0.0:50051"`).
    ///
    /// Blocks until the process receives `SIGINT` (Ctrl-C). The GIL is
    /// released while the server runs so that callbacks invoked from the
    /// tokio runtime can re-acquire it. A dedicated asyncio event loop is
    /// started on a Python daemon thread to support `async def` callbacks
    /// and is stopped before this method returns.
    fn serve(&self, py: Python<'_>, addr: &str) -> PyResult<()> {
        let addr: SocketAddr = addr
            .parse()
            .map_err(|e: std::net::AddrParseError| PyRuntimeError::new_err(e.to_string()))?;
        let do_get = Arc::new(self.do_get.clone_ref(py));
        let event_loop = Arc::new(start_event_loop(py)?);
        let event_loop_for_svc = event_loop.clone();

        let result = py.detach(move || {
            let rt = tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()
                .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
            rt.block_on(async move {
                let svc = PyFlightService {
                    do_get,
                    event_loop: Some(event_loop_for_svc),
                };
                let shutdown = async {
                    let _ = tokio::signal::ctrl_c().await;
                };
                Server::builder()
                    .add_service(FlightServiceServer::new(svc))
                    .serve_with_shutdown(addr, shutdown)
                    .await
                    .map_err(|e| PyRuntimeError::new_err(e.to_string()))
            })
        });

        stop_event_loop(py, &event_loop);
        result
    }
}

#[pymodule]
fn arrow_flight_pyarrow(_py: Python<'_>, m: &Bound<PyModule>) -> PyResult<()> {
    m.add_class::<FlightServer>()?;
    Ok(())
}
