// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

use super::*;
use crate::{
    event::api,
    stream::{application::Stream, socket::Protocol},
};

#[derive(Clone, Default)]
struct PacketRecorder(Arc<AtomicUsize>);

impl crate::event::Subscriber for PacketRecorder {
    type ConnectionContext = ();

    fn create_connection_context(&self, _meta: &api::ConnectionMeta, _info: &api::ConnectionInfo) {}

    fn on_stream_packet_received(
        &self,
        _context: &(),
        _meta: &api::ConnectionMeta,
        event: &api::StreamPacketReceived,
    ) {
        self.0.fetch_max(event.packet_len, Ordering::Relaxed);
    }
}

async fn transfer(sender: &mut Stream<PacketRecorder>, receiver: &mut Stream<PacketRecorder>) {
    let data = vec![0x5a; 256 * 1024];
    let expected = data.clone();
    let (sent, received) = tokio::join!(
        async {
            let mut data = &data[..];
            sender.write_all_from(&mut data).await
        },
        async {
            let mut received = Vec::<u8>::with_capacity(expected.len());
            while received.len() < expected.len() {
                assert_ne!(receiver.read_into(&mut received).await.unwrap(), 0);
            }
            assert_eq!(received, expected);
        }
    );
    sent.unwrap();
    let () = received;
}

/// Open UDP streams immediately after the handshake and check the fixed MTU
/// in each direction. No discovery or wait for a later MTU update is needed.
async fn stream_mtu(client_override: Option<u16>, server_override: Option<u16>, offload: usize) {
    let tls = TestTlsProvider {};
    let subscriber = NoopSubscriber {};
    let server_mtu = MtuRecorder::default();
    let client_mtu = MtuRecorder::default();
    let new_map = || {
        Map::new(
            Signer::new(b"default"),
            50_000,
            false,
            StdClock::default(),
            subscriber.clone(),
        )
    };
    let mut server = server::Builder::default().with_thread_count(offload);
    if let Some(mtu) = server_override {
        server = server.with_mtu(mtu);
    }
    let server = server
        .with_event_subscriber(server_mtu.clone())
        .start(
            "[::1]:0".parse().unwrap(),
            tls.clone(),
            subscriber.clone(),
            new_map(),
        )
        .await
        .unwrap();
    let handshake_addr = server.local_addr();
    let mut client = client::Builder::default();
    if let Some(mtu) = client_override {
        client = client.with_mtu(mtu);
    }
    let client = client
        .with_event_subscriber(client_mtu.clone())
        .start(
            "[::1]:0".parse().unwrap(),
            new_map(),
            tls,
            subscriber,
            "localhost".into(),
        )
        .unwrap();

    let client_packets = PacketRecorder::default();
    let server_packets = PacketRecorder::default();
    let client =
        crate::stream::client::tokio::Client::<client::Provider, PacketRecorder>::builder()
            .with_default_protocol(Protocol::Udp)
            .build(client, client_packets.clone())
            .unwrap();
    let server =
        crate::stream::server::tokio::Server::<server::Provider, PacketRecorder>::builder()
            .with_address("[::1]:0".parse().unwrap())
            .with_protocol(Protocol::Udp)
            .with_workers(1.try_into().unwrap())
            .build(server, server_packets.clone())
            .unwrap();
    let (client_stream, server_stream) = tokio::join!(
        client.connect(
            handshake_addr,
            server.acceptor_addr().unwrap(),
            "localhost".into()
        ),
        server.accept(),
    );
    let mut client_stream = client_stream.unwrap();
    let (mut server_stream, _) = server_stream.unwrap();

    transfer(&mut client_stream, &mut server_stream).await;
    transfer(&mut server_stream, &mut client_stream).await;

    for (name, mtu, configured, received) in [
        ("client", client_mtu, client_override, server_packets),
        ("server", server_mtu, server_override, client_packets),
    ] {
        // IPv6's IP and UDP headers consume 48 bytes of the configured IP MTU.
        let expected = configured.unwrap_or(DEFAULT_BASE_MTU).min(DEFAULT_MTU) - 48;
        let initial = mtu.initial_mtu.load(Ordering::Relaxed);
        let maximum = mtu.max_mtu.load(Ordering::Relaxed);
        let received = received.0.load(Ordering::Relaxed);
        let probes = mtu.probes_sent.load(Ordering::Relaxed);
        eprintln!("{name}: override={configured:?} offload={offload} initial={initial} max={maximum} received={received} probes={probes}");
        assert_eq!(initial, expected, "{name} initial MTU");
        assert_eq!(maximum, expected, "{name} maximum MTU");
        assert_eq!(
            received, expected as usize,
            "{name} received stream packet size"
        );
        assert_eq!(probes, 0, "{name} must not perform MTU discovery");
    }
}

#[cfg(target_os = "linux")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn default_fixed_mtu() {
    tokio::time::timeout(Duration::from_secs(15), stream_mtu(None, None, 0))
        .await
        .unwrap();
}

#[cfg(target_os = "linux")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn jumbo_fixed_mtu() {
    tokio::time::timeout(
        Duration::from_secs(15),
        stream_mtu(Some(8840), Some(8840), 0),
    )
    .await
    .unwrap();
}

#[cfg(target_os = "linux")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn jumbo_server_override() {
    tokio::time::timeout(Duration::from_secs(15), stream_mtu(None, Some(8840), 0))
        .await
        .unwrap();
}

#[cfg(target_os = "linux")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn jumbo_client_override() {
    tokio::time::timeout(Duration::from_secs(15), stream_mtu(Some(8840), None, 0))
        .await
        .unwrap();
}

#[cfg(target_os = "linux")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn jumbo_fixed_mtu_with_offload() {
    tokio::time::timeout(
        Duration::from_secs(15),
        stream_mtu(Some(8840), Some(8840), 2),
    )
    .await
    .unwrap();
}

#[cfg(target_os = "linux")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fixed_mtu_respects_packet_buffer_limit() {
    tokio::time::timeout(
        Duration::from_secs(15),
        stream_mtu(Some(u16::MAX), Some(u16::MAX), 0),
    )
    .await
    .unwrap();
}

#[cfg(target_os = "linux")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn smaller_fixed_mtu() {
    tokio::time::timeout(
        Duration::from_secs(15),
        stream_mtu(Some(1280), Some(1280), 0),
    )
    .await
    .unwrap();
}
