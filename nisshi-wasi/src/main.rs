use std::{env, error::Error, io, net::SocketAddr, sync::Arc, time::Duration};

use bytes::Bytes;
use nisshi_broker::{
    NODE_ID, coordinator::group::administrator::Controller, service::frame_services,
};
use nisshi_sans_io::{BytesInput, CloseAfterResponseExtension, SuppressResponseExtension};
use nisshi_service::BytesFrameLayer;
use nisshi_storage::{BrokerRegistrationRequest, Storage, StorageContainer};
use nisshi_storage_dynostore::MemoryEngineFactory;
use rama::{Layer as _, extensions::Extensions};
use tokio::{
    io::{AsyncReadExt as _, AsyncWriteExt as _},
    net::{TcpListener, TcpStream},
    sync::Semaphore,
    time::timeout,
};
use url::Url;
use uuid::Uuid;

type Result<T> = std::result::Result<T, Box<dyn Error + Send + Sync>>;
const MAXIMUM_FRAME_SIZE: usize = 16 * 1024 * 1024;
const IO_TIMEOUT: Duration = Duration::from_secs(30);
const MAXIMUM_CONNECTIONS: usize = 64;

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    let mut arguments = env::args().skip(1);
    let address: SocketAddr = arguments
        .next()
        .unwrap_or_else(|| "127.0.0.1:19092".into())
        .parse()?;
    if arguments.next().is_some() || !address.ip().is_loopback() || address.port() == 0 {
        return Err("usage: nisshi-wasi [LOOPBACK_ADDRESS:PORT], with a nonzero port".into());
    }
    let cluster = "nisshi-wasi";
    let mut configuration = StorageContainer::builder()
        .cluster_id(cluster)
        .node_id(NODE_ID)
        .advertised_listener(Url::parse(&format!("tcp://{address}"))?)
        .storage(Url::parse("memory://nisshi-wasi")?)
        .silent(true);
    configuration.with_factory(Arc::new(MemoryEngineFactory));
    let storage = configuration.build().await?;
    storage
        .register_broker(BrokerRegistrationRequest {
            broker_id: NODE_ID,
            cluster_id: cluster.into(),
            incarnation_id: Uuid::now_v7(),
            rack: None,
        })
        .await?;
    let coordinator = Controller::with_storage(storage.clone())?;
    let routes = frame_services(coordinator, storage)?;
    let listener = TcpListener::bind(address).await?;
    let connections = Arc::new(Semaphore::new(MAXIMUM_CONNECTIONS));
    println!("nisshi-wasi ready {address} memory://{cluster}");
    loop {
        let permit = connections.clone().acquire_owned().await?;
        let (stream, peer) = listener.accept().await?;
        let service = BytesFrameLayer::default().into_layer(routes.clone());
        _ = tokio::spawn(async move {
            let _permit = permit;
            if let Err(error) = serve_connection(stream, service).await {
                eprintln!("connection {peer} closed: {error}");
            }
        });
    }
}

async fn serve_connection<S>(mut stream: TcpStream, service: S) -> Result<()>
where
    S: rama::Service<BytesInput, Output = Bytes>,
    S::Error: Error + Send + Sync + 'static,
{
    let extensions = Extensions::default();
    _ = extensions.insert(SuppressResponseExtension::default());
    _ = extensions.insert(CloseAfterResponseExtension::default());
    loop {
        let mut prefix = [0; 4];
        match timeout(IO_TIMEOUT, stream.read_exact(&mut prefix)).await? {
            Ok(_) => (),
            Err(error) if error.kind() == io::ErrorKind::UnexpectedEof => return Ok(()),
            Err(error) => return Err(error.into()),
        }
        let length = i32::from_be_bytes(prefix);
        let length = usize::try_from(length)
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "negative frame length"))?;
        if !(4..=MAXIMUM_FRAME_SIZE).contains(&length) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "frame length outside WASI launcher limit",
            )
            .into());
        }
        let mut frame = vec![0; length + 4];
        frame[..4].copy_from_slice(&prefix);
        _ = timeout(IO_TIMEOUT, stream.read_exact(&mut frame[4..])).await??;
        let response = timeout(
            IO_TIMEOUT,
            service.serve(BytesInput {
                bytes: frame.into(),
                extensions: extensions.clone(),
            }),
        )
        .await??;
        if !SuppressResponseExtension::take(&extensions) {
            timeout(IO_TIMEOUT, stream.write_all(&response)).await??;
        }
        if CloseAfterResponseExtension::take(&extensions) {
            return Ok(());
        }
    }
}
