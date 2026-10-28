// Copyright ⓒ 2024-2026 Peter Morgan <peter.james.morgan@gmail.com>
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#[cfg(not(target_family = "wasm"))]
use std::sync::Arc;

use nisshi_service::FrameRouteService;
#[cfg(not(target_family = "wasm"))]
use nisshi_service::{
    BytesFrameLayer, BytesFrameService, DEFAULT_PRE_AUTHENTICATION_MAXIMUM_FRAME_SIZE,
    TcpBytesLayer, TcpBytesService, TcpContext, TcpContextLayer, TcpContextService,
};
use nisshi_storage::Storage;
#[cfg(not(target_family = "wasm"))]
use rama::Layer;
#[cfg(not(target_family = "wasm"))]
use rsasl::config::SASLConfig;
use tracing::debug;

use crate::{Error, Result, coordinator::group::Coordinator};

pub mod auth;
pub mod coordinator;
pub mod storage;

#[cfg(not(target_family = "wasm"))]
type TcpRouteFrame =
    TcpContextService<TcpBytesService<BytesFrameService<FrameRouteService<Error>>>>;

#[cfg(not(target_family = "wasm"))]
pub fn services<C, S>(
    cluster_id: &str,
    coordinator: C,
    storage: S,
    sasl_config: Option<Arc<SASLConfig>>,
) -> Result<TcpRouteFrame, Error>
where
    S: Storage + Clone,
    C: Coordinator,
{
    frame_services(coordinator, storage).map(|route| {
        let pre_authentication_maximum_frame_size = sasl_config
            .is_some()
            .then_some(DEFAULT_PRE_AUTHENTICATION_MAXIMUM_FRAME_SIZE);

        (
            TcpContextLayer::new(
                TcpContext::default()
                    .cluster_id(Some(cluster_id.into()))
                    .pre_authentication_maximum_frame_size(pre_authentication_maximum_frame_size),
            ),
            TcpBytesLayer,
            BytesFrameLayer::default().with_sasl_config(sasl_config),
        )
            .into_layer(route)
    })
}

/// Builds the broker request routes independently of its transport.
pub fn frame_services<C, S>(coordinator: C, storage: S) -> Result<FrameRouteService<Error>, Error>
where
    S: Storage + Clone,
    C: Coordinator,
{
    storage::services(FrameRouteService::<Error>::builder(), storage)
        .inspect(|builder| debug!(?builder))
        .and_then(|builder| {
            coordinator::services(builder, coordinator).inspect(|builder| debug!(?builder))
        })
        .and_then(auth::services)
        .and_then(|builder| builder.build().map_err(Into::into))
}
