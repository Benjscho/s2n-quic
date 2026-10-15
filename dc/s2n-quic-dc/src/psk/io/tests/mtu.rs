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

/// Exercise actual UDP streams, opened as soon as the dc handshake is ready.
/// Waiting before opening would hide the client-side MTU snapshot bug.
async fn stream_mtu(mtu: u16, probing: bool, offload: usize, drop_large_probes: bool) {
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
    let server = server::Builder::default()
        .with_mtu(mtu)
        .with_mtu_probing(probing)
        .with_thread_count(offload)
        .with_event_subscriber(server_mtu.clone())
        .start(
            "[::1]:0".parse().unwrap(),
            tls.clone(),
            subscriber.clone(),
            new_map(),
        )
        .await
        .unwrap();
    let mut handshake_addr = server.local_addr();
    let dropped = Arc::new(AtomicUsize::new(0));
    let _relay = if drop_large_probes {
        let socket = tokio::net::UdpSocket::bind("[::1]:0").await.unwrap();
        let server_addr = handshake_addr;
        handshake_addr = socket.local_addr().unwrap();
        let dropped = dropped.clone();
        Some(Relay(tokio::spawn(async move {
            let mut client_addr = None;
            let mut packet = vec![0; u16::MAX as usize];
            loop {
                let (len, from) = socket.recv_from(&mut packet).await.unwrap();
                let to = if from == server_addr {
                    client_addr.unwrap()
                } else {
                    client_addr = Some(from);
                    server_addr
                };
                // Simulate a path that supports the base MTU but drops every
                // larger probe in either direction.
                if len > 1450 - 48 {
                    dropped.fetch_add(1, Ordering::Relaxed);
                    continue;
                }
                socket.send_to(&packet[..len], to).await.unwrap();
            }
        })))
    } else {
        None
    };
    let client = client::Builder::default()
        .with_mtu(mtu)
        .with_mtu_probing(probing)
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

    let expected = if probing && !drop_large_probes {
        mtu.min(DEFAULT_MTU) - 48
    } else {
        mtu.min(DEFAULT_BASE_MTU) - 48
    };
    // Wait for discovery, not for an arbitrary sleep. This is deliberately
    // after stream creation.
    while !client_mtu.search_complete.load(Ordering::Relaxed)
        || !server_mtu.search_complete.load(Ordering::Relaxed)
    {
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    transfer(&mut client_stream, &mut server_stream).await;
    transfer(&mut server_stream, &mut client_stream).await;

    eprintln!(
        "probing={probing} offload={offload} dropped={}: handshake=({}, {}), stream=({}, {})",
        dropped.load(Ordering::Relaxed),
        client_mtu.max_mtu.load(Ordering::Relaxed),
        server_mtu.max_mtu.load(Ordering::Relaxed),
        client_stream.max_datagram_size(),
        server_stream.max_datagram_size(),
    );
    assert_eq!(client_stream.max_datagram_size(), expected, "client sender");
    assert_eq!(server_stream.max_datagram_size(), expected, "server sender");
    // Verify actual received stream packet sizes, not only handshake probes
    // or the configured sender limit.
    assert_eq!(client_packets.0.load(Ordering::Relaxed), expected as usize);
    assert_eq!(server_packets.0.load(Ordering::Relaxed), expected as usize);
    if drop_large_probes {
        assert!(dropped.load(Ordering::Relaxed) > 0);
    }
}

struct Relay(tokio::task::JoinHandle<()>);

impl Drop for Relay {
    fn drop(&mut self) {
        self.0.abort();
    }
}

#[test]
fn sender_tracks_confirmed_mtu_changes_after_creation() {
    use crate::{packet::stream::Id, path::secret::map::Entry, stream::send::state::State};
    use s2n_quic_core::varint::VarInt;

    let path = Entry::fake("[::1]:443".parse().unwrap(), None);
    path.update_max_datagram_size(1402);
    let mut sender = State::new(Id::normal(VarInt::ZERO).unwrap(), &path.parameters());
    sender.path = Some(path.clone());
    assert_eq!(sender.max_datagram_size, 1402);

    path.update_max_datagram_size(8792);
    sender.update_path_mtu();
    assert_eq!(sender.max_datagram_size, 8792);

    // New transmissions also respect a later reduction in the confirmed size.
    path.update_max_datagram_size(1402);
    sender.update_path_mtu();
    assert_eq!(sender.max_datagram_size, 1402);

    path.update_max_datagram_size(u16::MAX);
    assert_eq!(
        path.max_datagram_size() as usize,
        crate::stream::MAX_DATAGRAM_SIZE
    );
}

#[cfg(target_os = "linux")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn with_mtu_alone_keeps_default_cap() {
    tokio::time::timeout(Duration::from_secs(15), stream_mtu(8840, false, 0, false))
        .await
        .unwrap();
}

#[cfg(target_os = "linux")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn probing_updates_open_streams() {
    tokio::time::timeout(Duration::from_secs(15), stream_mtu(8840, true, 0, false))
        .await
        .unwrap();
}

#[cfg(target_os = "linux")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn probing_updates_open_streams_with_offload() {
    tokio::time::timeout(Duration::from_secs(15), stream_mtu(8840, true, 2, false))
        .await
        .unwrap();
}

#[cfg(target_os = "linux")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn lost_probes_keep_streams_at_base_mtu() {
    tokio::time::timeout(Duration::from_secs(15), stream_mtu(8840, true, 0, true))
        .await
        .unwrap();
}

#[cfg(target_os = "linux")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn probing_respects_packet_buffer_limit() {
    tokio::time::timeout(
        Duration::from_secs(15),
        stream_mtu(u16::MAX, true, 0, false),
    )
    .await
    .unwrap();
}

#[cfg(target_os = "linux")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn probing_respects_smaller_mtu() {
    tokio::time::timeout(Duration::from_secs(15), stream_mtu(1280, true, 0, false))
        .await
        .unwrap();
}
