//! Regression coverage for Android UDP sockets returning `ConnectionAborted` after resume.

use anyhow::{Result, anyhow};
use std::collections::VecDeque;
use std::io;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};
use std::time::Duration;

use webrtc::peer_connection::{
    PeerConnection, PeerConnectionBuilder, PeerConnectionEventHandler, RTCPeerConnectionState,
};
use webrtc::runtime::{
    AsyncInterval, AsyncTcpListener, AsyncTcpStream, AsyncUdpSocket, JoinHandle, RecvMeta, Runtime,
    Sender, Transmit, channel,
};

mod common;
use common::{block_on, runtime, sleep, timeout};

#[derive(Debug, Clone, Copy)]
enum RecvAction {
    ConnectionAborted,
    Success,
}

#[derive(Debug, Default)]
struct RecvState {
    actions: VecDeque<RecvAction>,
    waker: Option<Waker>,
}

#[derive(Debug, Default)]
struct RecvControl {
    state: Mutex<RecvState>,
    processed: AtomicUsize,
}

impl RecvControl {
    fn push(&self, action: RecvAction) {
        let waker = {
            let mut state = self.state.lock().unwrap();
            state.actions.push_back(action);
            state.waker.take()
        };
        if let Some(waker) = waker {
            waker.wake();
        }
    }

    async fn wait_until_processed(&self, expected: usize) -> Result<()> {
        timeout(Duration::from_secs(1), async {
            while self.processed.load(Ordering::Acquire) < expected {
                sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .map_err(|_| anyhow!("timed out waiting for recv action {expected}"))
    }
}

#[derive(Debug)]
struct ControlledRecvRuntime {
    inner: Arc<dyn Runtime>,
    control: Arc<RecvControl>,
}

impl Runtime for ControlledRecvRuntime {
    fn spawn(&self, future: Pin<Box<dyn Future<Output = ()> + Send>>) -> Box<dyn JoinHandle> {
        self.inner.spawn(future)
    }

    fn spawn_reactor(
        &self,
        reactor_pool_size: usize,
        future: Pin<Box<dyn Future<Output = ()> + Send>>,
    ) -> Box<dyn JoinHandle> {
        self.inner.spawn_reactor(reactor_pool_size, future)
    }

    fn wrap_udp_socket(&self, socket: std::net::UdpSocket) -> io::Result<Arc<dyn AsyncUdpSocket>> {
        Ok(Arc::new(ControlledRecvSocket {
            inner: self.inner.wrap_udp_socket(socket)?,
            control: self.control.clone(),
        }))
    }

    fn wrap_tcp_listener(
        &self,
        listener: std::net::TcpListener,
    ) -> io::Result<Arc<dyn AsyncTcpListener>> {
        self.inner.wrap_tcp_listener(listener)
    }

    fn connect_tcp<'a>(
        &'a self,
        remote_addr: SocketAddr,
    ) -> Pin<Box<dyn Future<Output = io::Result<Arc<dyn AsyncTcpStream>>> + Send + 'a>> {
        self.inner.connect_tcp(remote_addr)
    }

    fn resolve_host<'a>(
        &'a self,
        host: &'a str,
    ) -> Pin<Box<dyn Future<Output = io::Result<Vec<SocketAddr>>> + Send + 'a>> {
        self.inner.resolve_host(host)
    }

    fn sleep(&self, duration: Duration) -> Pin<Box<dyn Future<Output = ()> + Send + 'static>> {
        self.inner.sleep(duration)
    }

    fn interval(&self, period: Duration) -> Box<dyn AsyncInterval> {
        self.inner.interval(period)
    }

    fn block_on(&self, future: Pin<Box<dyn Future<Output = ()> + '_>>) {
        self.inner.block_on(future)
    }

    fn name(&self) -> &'static str {
        "controlled-recv"
    }
}

#[derive(Debug)]
struct ControlledRecvSocket {
    inner: Arc<dyn AsyncUdpSocket>,
    control: Arc<RecvControl>,
}

impl AsyncUdpSocket for ControlledRecvSocket {
    fn local_addr(&self) -> io::Result<SocketAddr> {
        self.inner.local_addr()
    }

    fn poll_send(&self, cx: &mut Context<'_>, transmit: &Transmit<'_>) -> Poll<io::Result<usize>> {
        self.inner.poll_send(cx, transmit)
    }

    fn poll_recv(
        &self,
        cx: &mut Context<'_>,
        _bufs: &mut [io::IoSliceMut<'_>],
        meta: &mut [RecvMeta],
    ) -> Poll<io::Result<usize>> {
        let action = {
            let mut state = self.control.state.lock().unwrap();
            match state.actions.pop_front() {
                Some(action) => action,
                None => {
                    state.waker = Some(cx.waker().clone());
                    return Poll::Pending;
                }
            }
        };
        self.control.processed.fetch_add(1, Ordering::Release);

        match action {
            RecvAction::ConnectionAborted => {
                Poll::Ready(Err(io::Error::from(io::ErrorKind::ConnectionAborted)))
            }
            RecvAction::Success => {
                meta[0] = RecvMeta::default();
                meta[0].len = 0;
                meta[0].stride = 1;
                meta[0].addr = "127.0.0.1:9".parse().unwrap();
                Poll::Ready(Ok(1))
            }
        }
    }

    fn max_gso_segments(&self) -> usize {
        self.inner.max_gso_segments()
    }

    fn max_gro_segments(&self) -> usize {
        self.inner.max_gro_segments()
    }
}

struct Handler {
    closed_tx: Sender<()>,
}

#[async_trait::async_trait]
impl PeerConnectionEventHandler for Handler {
    async fn on_connection_state_change(&self, state: RTCPeerConnectionState) {
        if state == RTCPeerConnectionState::Closed {
            let _ = self.closed_tx.try_send(());
        }
    }
}

#[test]
fn connection_aborted_retries_are_bounded_and_notify_closed() {
    block_on(run()).unwrap();
}

async fn run() -> Result<()> {
    let control = Arc::new(RecvControl::default());
    let controlled_runtime: Arc<dyn Runtime> = Arc::new(ControlledRecvRuntime {
        inner: runtime(),
        control: control.clone(),
    });
    let (closed_tx, mut closed_rx) = channel::<()>(1);

    let pc = PeerConnectionBuilder::new()
        .with_handler(Arc::new(Handler { closed_tx }))
        .with_runtime(controlled_runtime)
        .with_udp_addrs(vec!["127.0.0.1:0".to_owned()])
        .build()
        .await?;

    control.push(RecvAction::ConnectionAborted);
    control.wait_until_processed(1).await?;
    control.push(RecvAction::ConnectionAborted);
    control.wait_until_processed(2).await?;
    assert!(
        timeout(Duration::from_millis(100), closed_rx.recv())
            .await
            .is_err(),
        "the first two consecutive aborts must retain the socket"
    );

    control.push(RecvAction::Success);
    control.wait_until_processed(3).await?;
    control.push(RecvAction::ConnectionAborted);
    control.wait_until_processed(4).await?;
    control.push(RecvAction::ConnectionAborted);
    control.wait_until_processed(5).await?;
    assert!(
        timeout(Duration::from_millis(100), closed_rx.recv())
            .await
            .is_err(),
        "a successful receive must reset the consecutive-abort counter"
    );

    control.push(RecvAction::ConnectionAborted);
    control.wait_until_processed(6).await?;
    timeout(Duration::from_secs(1), closed_rx.recv())
        .await
        .map_err(|_| anyhow!("peer connection did not publish Closed after retry exhaustion"))?
        .ok_or_else(|| anyhow!("Closed notification channel ended unexpectedly"))?;

    pc.close().await?;
    Ok(())
}
