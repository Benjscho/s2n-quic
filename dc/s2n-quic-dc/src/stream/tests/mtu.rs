// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

use crate::{
    packet::{stream::decoder::Packet, Tag},
    stream::testing::{Client, Server},
    testing::{ext::*, sim},
};
use bach::net::monitor::{self, Command};
use s2n_codec::{DecoderBuffer, DecoderBufferMut};
use std::sync::{Arc, Mutex};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

const BASE: u16 = 1450 - 28;
const JUMBO: u16 = 8840 - 28;

#[derive(Default)]
struct Direction {
    dropped_offset: Option<u64>,
    retransmitted: bool,
    jumbo: bool,
}

/// The PSK tests exercise real discovery. Here, inject its MTU callback exactly when a
/// data packet is lost, so the stream's retransmission and sizing transition is deterministic.
#[test]
fn mtu_increase_with_data_in_flight() {
    sim(|| {
        async move {
            let server = Server::udp().mtu(BASE).build();
            let client = Client::builder().mtu(BASE).build();
            let peer = client.handshake_with(&server).unwrap();
            let client_entry = peer.map().get_by_id_untracked(peer.id()).unwrap();
            let server_entry = server.map().get_by_id_untracked(peer.id()).unwrap();
            client_entry.start_mtu_discovery();
            server_entry.start_mtu_discovery();

            let (client_stream, server_stream) =
                tokio::join!(client.connect_to(&server), server.accept());
            let client_stream = client_stream.unwrap();
            let server_stream = server_stream.unwrap().0;
            assert_eq!(client_stream.max_datagram_size(), BASE);
            assert_eq!(server_stream.max_datagram_size(), BASE);
            let client_addr = client_stream.local_addr().unwrap();
            let server_addr = server_stream.local_addr().unwrap();
            assert_ne!(client_addr, server_addr);

            let directions = Arc::new(Mutex::new([Direction::default(), Direction::default()]));
            let observed = directions.clone();
            monitor::on_packet_sent(move |datagram| {
                let bytes = datagram.transport.payload();
                let Ok((Tag::Stream(_), _)) = DecoderBuffer::new(bytes).decode::<Tag>() else {
                    return Command::Pass;
                };
                let mut bytes = bytes.to_vec();
                let (packet, _) =
                    Packet::decode(DecoderBufferMut::new(&mut bytes), (), 16).unwrap();
                if packet.payload().is_empty() {
                    return Command::Pass;
                }
                let (index, entry) = if datagram.source() == client_addr {
                    (0, &client_entry)
                } else {
                    assert_eq!(datagram.source(), server_addr);
                    (1, &server_entry)
                };
                let mut directions = observed.lock().unwrap();
                let direction = &mut directions[index];
                if direction.dropped_offset.is_none() {
                    assert!(packet.total_len() <= BASE as usize);
                    direction.dropped_offset = Some(packet.stream_offset().as_u64());
                    // The dropped packet remains outstanding while the worker adopts the
                    // larger MTU. Subsequent writes must use jumbo packets, while recovery
                    // must still deliver the original small packet.
                    entry.update_max_datagram_size(JUMBO);
                    entry.finish_mtu_discovery();
                    return Command::Drop;
                }
                if packet.is_retransmission()
                    && Some(packet.stream_offset().as_u64()) == direction.dropped_offset
                {
                    direction.retransmitted = true;
                }
                direction.jumbo |=
                    !packet.is_retransmission() && packet.total_len() == JUMBO as usize;
                assert!(packet.total_len() <= JUMBO as usize);
                Command::Pass
            });

            let (mut client_read, mut client_write) = client_stream.into_split();
            let (mut server_read, mut server_write) = server_stream.into_split();
            let request = vec![0x5a; 1024 * 1024];
            let response = vec![0xa5; request.len()];
            let mut received_request = vec![0; request.len()];
            let mut received_response = vec![0; response.len()];
            let (request_sent, request_read, response_sent, response_read) = tokio::join!(
                client_write.write_all(&request),
                server_read.read_exact(&mut received_request),
                server_write.write_all(&response),
                client_read.read_exact(&mut received_response),
            );
            request_sent.unwrap();
            request_read.unwrap();
            response_sent.unwrap();
            response_read.unwrap();
            assert_eq!(received_request, request);
            assert_eq!(received_response, response);
            for direction in directions.lock().unwrap().iter() {
                assert!(direction.dropped_offset.is_some());
                assert!(
                    direction.retransmitted,
                    "lost small packet must be retransmitted"
                );
                assert!(direction.jumbo, "data must actually use larger packets");
            }
        }
        .primary()
        .spawn();
    });
}
