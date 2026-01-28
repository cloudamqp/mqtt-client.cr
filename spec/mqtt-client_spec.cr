require "./spec_helper"

alias MP = MQTT::Protocol

describe MQTT::Client do
  it "can publish" do
    with_server_socket do |server|
      done = Channel(Nil).new(1)

      # This "mocks" the server
      server.accept_client do |client_io|
        MP::Packet.from_io(client_io)
        MP::Connack.new(false, MP::Connack::ReturnCode::Accepted).to_io(client_io)

        sub = MP::Packet.from_io(client_io).as(MP::Subscribe)
        MP::SubAck.new([MP::SubAck::ReturnCode::QoS1], sub.packet_id).to_io(client_io)

        subscribed = true
        loop do
          packet = MP::Packet.from_io(client_io)
          case packet
          when MP::Publish
            MP::PubAck.new(packet.packet_id.not_nil!("PacketID missing")).to_io(client_io)
            packet.to_io(client_io) if subscribed
          when MP::Unsubscribe
            subscribed = false
            MP::UnsubAck.new(packet.packet_id).to_io(client_io)
          when MP::Disconnect
            break
          end
        end
        done.send(nil)
      end
      mqtt = MQTT::Client.new(server.address.address,
        port: server.address.port,
        client_id: "can publish")

      recieved_message_count = 0
      mqtt.on_message do |msg|
        msg.topic.should eq "foo"
        msg.body.should eq "bar".to_slice
        msg.ack
        recieved_message_count += 1
      end
      mqtt.subscribe("foo", 1)
      mqtt.publish("foo", "bar", 1)
      mqtt.publish("foo", "bar", 1)
      mqtt.unsubscribe("foo")
      mqtt.publish("foo", "bar", 1)
      mqtt.disconnect
      done.receive
      mqtt.close
      recieved_message_count.should eq 2
    end
  end

  it "can read messages of different sizes" do
    with_server_socket do |server|
      done = Channel(Nil).new(1)
      sizes = [
        1, 10, 127, 128, 138, 16_383, 16_384, 16_394,
        2_097_151, 2_097_152, 2_097_162, 268_435_455,
      ]

      expected_body = uninitialized Bytes
      ch_send_next = Channel(Nil).new
      server.accept_client do |client_io|
        MP::Packet.from_io(client_io)
        MP::Connack.new(false, MP::Connack::ReturnCode::Accepted).to_io(client_io)

        sub = MP::Packet.from_io(client_io).as(MP::Subscribe)
        MP::SubAck.new([MP::SubAck::ReturnCode::QoS0], sub.packet_id).to_io(client_io)

        subscribed = true
        random = Random.new
        sizes.each do |size|
          # -5 is for "foo" bytesize + string length which is also part
          # of "remaining length"
          size = {0, size - 5}.max
          expected_body = random.random_bytes(size)
          pub = MP::Publish.new("foo", expected_body, nil, false, 0u8, false)
          pub.to_io(client_io)
          client_io.flush
          ch_send_next.receive
        end
        done.send(nil)
      end

      mqtt = MQTT::Client.new(
        server.address.address,
        port: server.address.port,
        client_id: "can consume"
      )

      mqtt.on_message do |msg|
        msg.body.size.should eq expected_body.size
        msg.body.should eq expected_body
        ch_send_next.send nil
      end
      mqtt.subscribe("foo", 0)
      done.receive
      mqtt.close
    ensure
      ch_send_next.try &.close
    end
  end

  it "can ping" do
    with_server_socket do |server|
      done = Channel(Nil).new(1)

      server.accept_client do |client_io|
        MP::Packet.from_io(client_io)
        MP::Connack.new(false, MP::Connack::ReturnCode::Accepted).to_io(client_io)

        MP::Packet.from_io(client_io).as(MP::PingReq)
        MP::PingResp.new.to_io(client_io)

        done.send(nil)
      end

      mqtt = MQTT::Client.new(server.address.address, port: server.address.port, client_id: "can ping")
      mqtt.ping
      done.receive
      mqtt.close
      mqtt.@connection.not_nil!("Connection missing").@reader.@last_packet_received.should be_close Time.monotonic, 1.second
    end
  end

  it "can keepalive" do
    with_server_socket do |server|
      done = Channel(Nil).new(1)
      ping_recieved = false

      server.accept_client do |client_io|
        connect = MP::Packet.from_io(client_io).as(MP::Connect)
        client_io.@io.as(Socket).read_timeout = (connect.keepalive * 1.5).seconds
        MP::Connack.new(false, MP::Connack::ReturnCode::Accepted).to_io(client_io)
        MP::Packet.from_io(client_io).as(MP::PingReq)
        ping_recieved = true
      rescue IO::Error
        # nop
      ensure
        done.send(nil)
      end
      mqtt = MQTT::Client.new(server.address.address, port: server.address.port, keepalive: 1u16, client_id: "can keepalive")
      select
      when done.receive
        ping_recieved.should be_true
      when timeout 10.seconds
        fail "Timeout waiting for ping"
      end
      mqtt.close
    end
  end
end
