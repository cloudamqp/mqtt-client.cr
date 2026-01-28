require "./packet"

module MQTT
  class Client
    class Writer
      {% if compare_versions(Crystal::VERSION, "1.19.0") < 0 %}
        getter last_packet_sent = ::Time.monotonic
      {% else %}
        getter last_packet_sent = ::Time.instant
      {% end %}

      @packet_id = 0u16
      @requests = Channel(Packet).new(1)

      def initialize(@socket : IO, @acks : Channel(UInt16))
      end

      def send(packet)
        @requests.send(packet)
      end

      def close
        @requests.close
      end

      def run(socket = @socket)
        while packet = @requests.receive?
          case packet
          in Subscribe
            id = send_subscribe(socket, packet.topics)
            wait_for_id(id)
          in Disconnect
            send_disconnect(socket)
            Log.debug { "disconnected" }
            break
          in Unsubscribe
            id = send_unsubscribe(socket, packet.topics)
            wait_for_id(id)
          in Publish
            id = send_publish(socket, packet.topic, packet.body, packet.qos, packet.retain, packet.dup)
            wait_for_id(id) if id
          in PingReq
            send_pingreq(socket)
          in PubAck
            send_puback(socket, packet.packet_id)
          in PubRec
            send_pubrec(socket, packet.packet_id)
          in PubRel
            send_pubrel(socket, packet.packet_id)
          in PubComp
            send_pubcomp(socket, packet.packet_id)
          in Packet
            raise "too abstract"
          end
        end
      ensure
        @requests.close
        @socket.close rescue nil
      end

      private def wait_for_id(id : UInt16)
        acks = @acks
        loop do
          ack_id = acks.receive
          break if ack_id == id
          acks.send ack_id # if unexpected id, put it back on the channel
        end
      end

      private def send_unsubscribe(socket, topics)
        socket.write_byte 0b10100010u8

        length = 2 + topics.sum { |topic| 2 + topic.bytesize }
        encode_length(socket, length)

        id = send_next_packet_id(socket)
        topics.each do |topic|
          send_string(socket, topic)
        end
        socket.flush
        update_last_packet_sent
        id
      end

      private def send_subscribe(socket, topics : Enumerable(Tuple(String, UInt8)))
        socket.write_byte 0b10000010u8

        length = 2 + topics.sum { |topic, _| 2 + topic.bytesize + 1 }
        encode_length(socket, length)

        id = send_next_packet_id(socket)
        topics.each do |topic, qos|
          send_string(socket, topic)
          socket.write_byte qos.to_u8
        end
        socket.flush
        update_last_packet_sent
        id
      end

      private def send_disconnect(socket) : Nil
        socket.write_byte 0b11100000u8
        socket.write_byte 0u8
        socket.flush
        update_last_packet_sent
      end

      private def send_publish(socket, topic : String, body : Slice, qos : UInt8, retain : Bool, dup : Bool) : UInt16?
        raise ArgumentError.new("Invalid QoS") unless 0 <= qos <= 2

        header = 0b00110000u8
        header |= (1u8 << 3) if dup
        header |= (qos << 1)
        header |= (1u8 << 0) if retain
        socket.write_byte header # type + flags

        length = 2 + topic.bytesize + body.bytesize
        length += 2 if qos > 0
        encode_length(socket, length)

        send_string(socket, topic)
        id = send_next_packet_id(socket) if qos > 0
        socket.write body
        socket.flush
        update_last_packet_sent
        id
      end

      private def send_next_packet_id(socket) : UInt16
        id = next_packet_id
        socket.write_bytes id, IO::ByteFormat::NetworkEndian
        id
      end

      private def send_puback(socket, packet_id)
        socket.write_byte 0b01000000 # type + flags
        socket.write_byte 2u8        # length
        socket.write_bytes packet_id, IO::ByteFormat::NetworkEndian
        socket.flush
        update_last_packet_sent
      end

      private def send_pubrec(socket, packet_id)
        socket.write_byte 0b01100010 # type + flags
        socket.write_byte 2u8        # length
        socket.write_bytes packet_id, IO::ByteFormat::NetworkEndian
        socket.flush
        update_last_packet_sent
      end

      private def send_pubrel(socket, packet_id)
        socket.write_byte 0b01110010 # type + flags
        socket.write_byte 2u8        # length
        socket.write_bytes packet_id, IO::ByteFormat::NetworkEndian
        socket.flush
        update_last_packet_sent
      end

      private def send_pubcomp(socket, packet_id)
        socket.write_byte 0b01110000 # type + flags
        socket.write_byte 2u8        # length
        socket.write_bytes packet_id, IO::ByteFormat::NetworkEndian
        socket.flush
        update_last_packet_sent
      end

      private def send_string(socket : IO, str : String)
        socket.write_bytes str.bytesize.to_u16, IO::ByteFormat::NetworkEndian
        socket.write str.to_slice
      end

      private def update_last_packet_sent
        {% if compare_versions(Crystal::VERSION, "1.19.0") < 0 %}
          @last_packet_sent = Time.monotonic
        {% else %}
          @last_packet_sent = Time.instant
        {% end %}
      end

      private def next_packet_id : UInt16
        id = @packet_id &+ 1u16 # let it wrap around on overflow
        id = 1u16 if id.zero?
        @packet_id = id
      end

      private def send_pingreq(socket)
        socket.write_byte 0b11000000u8
        socket.write_byte 0u8
        socket.flush
        update_last_packet_sent
      end

      def connect(client_id, clean_session, user, password, will, keepalive)
        send_connect(@socket, client_id, clean_session, user, password, will, keepalive)
      end

      private def send_connect(socket, client_id, clean_session, user, password, will, keepalive) : Nil
        Log.debug { "sending connect" }
        socket.write_byte 0b00010000u8 # type + flags

        encode_length(socket, connect_length(client_id, user, password, will))

        send_string(socket, "MQTT")
        socket.write_byte 0x04 # protocol version 3.1.1

        flags = 0u8
        flags |= (1u8 << 1) if clean_session
        if w = will
          flags |= (1u8 << 2)
          flags |= (w.qos << 3)
          flags |= (1u8 << 5) if w.retain
        end
        flags |= (1u8 << 6) if password
        flags |= (1u8 << 7) if user
        socket.write_byte flags

        socket.write_bytes (keepalive || 0).to_u16, IO::ByteFormat::NetworkEndian

        send_string(socket, client_id)
        if w = will
          send_string(socket, w.topic)
          socket.write_bytes w.body.bytesize.to_u16, IO::ByteFormat::NetworkEndian
          socket.write w.body
        end
        if u = user
          send_string(socket, u)
        end
        if p = password
          send_string(socket, p)
        end

        Log.debug { "sent connect" }
        socket.flush
        update_last_packet_sent
      end

      private def connect_length(client_id, user, password, will) : Int32
        length = 10
        length += 2 + client_id.bytesize
        if u = user
          length += 2 + u.bytesize
        end
        if p = password
          length += 2 + p.bytesize
        end
        if w = will
          length += 2 + w.topic.bytesize + 2 + w.body.bytesize
        end
        length
      end

      private def encode_length(socket, length)
        loop do
          b = (length % 128).to_u8
          length = length // 128
          b = b | 128 if length > 0
          socket.write_byte b
          break if length <= 0
        end
      end
    end
  end
end
