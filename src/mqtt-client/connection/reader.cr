require "./packet"

module MQTT
  class Client
    class Reader
      record Message, packet_id : UInt16, topic : String, body : Bytes, qos : UInt8, retain : Bool, dup : Bool
      getter messages = Channel(Message).new(16)
      @last_packet_received = Time.monotonic
      @connected = true

      def initialize(@socket : IO, @acks : Channel(UInt16), @writer : Writer, @keepalive : UInt16)
      end

      # http://docs.oasis-open.org/mqtt/mqtt/v3.1.1/os/mqtt-v3.1.1-os.html#_Toc398718021
      def run(socket = @socket) # ameba:disable Metrics/CyclomaticComplexity
        loop do
          b = socket.read_byte || break
          type = b >> 4          # upper 4 bits
          flags = b & 0b00001111 # lower 4 bits
          pktlen = decode_length(socket)

          Log.trace { "got type #{type}" }
          case type
          when 2     then connack(flags, pktlen)
          when 3     then publish(flags, pktlen)
          when 4     then puback(flags, pktlen)
          when 5     then pubrec(flags, pktlen)
          when 6     then pubrel(flags, pktlen)
          when 7     then pubcomp(flags, pktlen)
          when 9     then suback(flags, pktlen)
          when 11    then unsuback(flags, pktlen)
          when 13    then pingresp(flags, pktlen)
          when 0, 15 then raise "forbidden packet type, reserved"
          else            raise "invalid packet type for server to send"
          end

          maybe_send_ping
        rescue ex : IO::TimeoutError
          try_send_ping(ex)
        end
      rescue ex : IO::Error
        Log.debug(exception: ex) { "io error in read_loop" } if @connected
      rescue ex
        raise ex if @connected
      ensure
        close
      end

      def close
        @acks.close
        @messages.close
        @socket.close rescue nil
      end

      private def maybe_send_ping
        return unless @keepalive.positive?

        now = Time.monotonic
        @last_packet_received = now
        if (now - @writer.last_packet_sent).total_seconds > @keepalive * 0.9
          @writer.send PingReq.new
        end
      end

      private def try_send_ping(ex)
        raise ex unless @keepalive.positive?

        now = Time.monotonic
        ping_diff = now - @last_packet_received
        if ping_diff.total_seconds > @keepalive * 1.5
          raise TimeoutError.new("No ping response from server in #{ping_diff}", cause: ex)
        else
          @writer.send PingReq.new
        end
      end

      private def connack(flags, pktlen)
        socket = @socket
        session_present = (socket.read_byte || raise IO::EOFError.new) == 1u8
        return_code = socket.read_byte || raise IO::EOFError.new
        case return_code
        when 0u8
          Log.debug { "connected, session_present #{session_present}" }
          session_present
        when 1u8 then raise InvalidProtocolVersion.new
        when 2u8 then raise IdentifierReject.new
        when 3u8 then raise ServerUnavailable.new
        when 4u8 then raise BadCredentials.new
        when 5u8 then raise NotAuthorized.new
        else          raise InvalidResponse.new(return_code)
        end
      end

      def expect_connack(socket = @socket)
        Log.debug { "waiting for connack" }
        b = socket.read_byte || raise IO::EOFError.new
        type = b >> 4          # upper 4 bits
        flags = b & 0b00001111 # lower 4 bits
        pktlen = decode_length(socket)

        case type
        when 2 then connack(flags, pktlen)
        else        raise UnexpectedPacket.new
        end
        Log.debug { "received connack" }
      rescue ex : IO::TimeoutError
        raise TimeoutError.new("Connect timeout", cause: ex)
      end

      private def pingresp(flags, pktlen)
        flags.zero? || raise "invalid pingresp flags"
        pktlen.zero? || raise "invalid pingresp length"
      end

      private def publish(flags, pktlen)
        socket = @socket
        dup = flags.bit(3) == 1
        qos = (flags & 0b00000110) >> 1
        retain = flags.bit(0) == 1
        topic = read_string(socket)
        header_len = 2 + topic.bytesize
        packet_id = 0u16
        if qos > 0
          packet_id = read_int(socket)
          header_len += 2
        end
        body = Bytes.new(pktlen - header_len)
        socket.read_fully(body)

        @messages.send(Message.new(packet_id, topic, body, qos, retain, dup))
      end

      private def puback(flags, pktlen)
        flags.zero? || raise "invalid puback flags"
        pktlen == 2 || raise "invalid puback length"

        packet_id = read_int(@socket)
        @acks.send packet_id
      end

      private def pubrec(flags, pktlen)
        flags.zero? || raise "invalid pubrec flags"
        pktlen == 2 || raise "invalid pubrec length"

        packet_id = read_int(@socket)
        @writer.send PubRel.new(packet_id)
      end

      private def pubrel(flags, pktlen)
        flags.zero? || raise "invalid pubrel flags"
        pktlen == 2 || raise "invalid pubrel length"

        packet_id = read_int(@socket)
        @acks.send packet_id
      end

      private def pubcomp(flags, pktlen)
        flags.zero? || raise "invalid pubcomp flags"
        pktlen == 2 || raise "invalid pubcomp length"

        packet_id = read_int(@socket)
        @acks.send packet_id
      end

      private def suback(flags, pktlen)
        flags.zero? || raise "invalid suback flags"
        socket = @socket
        packet_id = read_int(socket)

        qos_len = pktlen - 2
        Array(UInt8).new(qos_len) do
          socket.read_byte || raise IO::EOFError.new
        end
        @acks.send packet_id
      end

      private def unsuback(flags, pktlen)
        flags.zero? || raise "invalid puback flags"
        pktlen == 2 || raise "invalid puback length"

        packet_id = read_int(@socket)
        @acks.send packet_id
      end

      private def read_string(socket)
        len = read_int(socket)
        socket.read_string(len)
      end

      private def read_int(socket)
        socket.read_bytes UInt16, IO::ByteFormat::NetworkEndian
      end

      private def decode_length(socket)
        multiplier = 1
        value = 0
        loop do
          b = socket.read_byte || raise IO::EOFError.new
          value = (b & 127) * multiplier
          multiplier *= 128
          raise "invalid packet length" if multiplier > 128*128*128
          break if b & 128 == 0
        end
        value
      end
    end
  end
end
