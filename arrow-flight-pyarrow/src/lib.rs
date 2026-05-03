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
//! All other Flight RPCs return `UNIMPLEMENTED`.

#![warn(missing_docs)]

use std::net::SocketAddr;
use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_array::RecordBatchReader;
use arrow_array::ffi_stream::ArrowArrayStreamReader;
use arrow_flight::encode::FlightDataEncoderBuilder;
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
use pyo3::types::PyBytes;
use tonic::transport::Server;
use tonic::{Request, Response, Status, Streaming};

/// Internal `FlightService` implementation that delegates `do_get` to a
/// Python callable and returns `UNIMPLEMENTED` for every other RPC.
struct PyFlightService {
    do_get: Arc<Py<PyAny>>,
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

        // Eagerly call the Python callback and collect all batches while
        // holding the GIL. This keeps the binding minimal at the cost of
        // buffering the response in memory.
        let result = tokio::task::spawn_blocking(move || {
            Python::attach(|py| -> PyResult<(SchemaRef, Vec<RecordBatch>)> {
                let cb = do_get.bind(py);
                let py_ticket = PyBytes::new(py, &ticket);
                let result = cb.call1((py_ticket,))?;
                let mut reader = ArrowArrayStreamReader::from_pyarrow_bound(&result)?;
                let schema = reader.schema();
                let batches = (&mut reader)
                    .collect::<Result<Vec<_>, _>>()
                    .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
                Ok((schema, batches))
            })
        })
        .await
        .map_err(|e| Status::internal(format!("do_get task panicked: {e}")))?;

        let (schema, batches) =
            result.map_err(|e| Status::internal(format!("do_get callback failed: {e}")))?;

        let input = futures::stream::iter(batches.into_iter().map(Ok));
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

/// Minimal Python-facing Flight server.
///
/// Construct with a `do_get` callback, then call `serve(addr)` to start
/// listening. The callback receives the ticket as `bytes` and must return an
/// object implementing the Arrow PyCapsule stream interface (for example a
/// `pyarrow.RecordBatchReader` or `pyarrow.Table`).
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
    /// tokio runtime can re-acquire it.
    fn serve(&self, py: Python<'_>, addr: &str) -> PyResult<()> {
        let addr: SocketAddr = addr
            .parse()
            .map_err(|e: std::net::AddrParseError| PyRuntimeError::new_err(e.to_string()))?;
        let do_get = Arc::new(self.do_get.clone_ref(py));

        py.detach(move || {
            let rt = tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()
                .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
            rt.block_on(async move {
                let svc = PyFlightService { do_get };
                let shutdown = async {
                    let _ = tokio::signal::ctrl_c().await;
                };
                Server::builder()
                    .add_service(FlightServiceServer::new(svc))
                    .serve_with_shutdown(addr, shutdown)
                    .await
                    .map_err(|e| PyRuntimeError::new_err(e.to_string()))
            })
        })
    }
}

#[pymodule]
fn arrow_flight_pyarrow(_py: Python<'_>, m: &Bound<PyModule>) -> PyResult<()> {
    m.add_class::<FlightServer>()?;
    Ok(())
}
