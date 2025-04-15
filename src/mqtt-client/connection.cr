require "./message"
require "./errors"
require "./connection/writer"
require "./connection/reader"

module MQTT
  class Client
    class Connection
      Log = ::Log.for(self)

      @acks = Channel(UInt16).new
      @on_message : Proc(ReceivedMessage, Nil)?
      @keepalive = 60u16
      getter? connected = true

      def self.new(host : String, port = 1883, tls = false, client_id = "", clean_session = true,
                   user : String? = nil, password : String? = nil, will : Message? = nil,
                   keepalive : Int = 60u16, autoack = true, sock_opts = SocketOptions.new,
                   on_message : Proc(ReceivedMessage, Nil)? = nil)
        Log.debug { "creating connection to #{host}:#{port}" }
        if tls
          socket = connect_tls(connect_tcp(host, port, keepalive, sock_opts), OpenSSL::SSL::VerifyMode::PEER, host)
          Connection.new(socket, client_id, clean_session, user, password, will, keepalive.to_u16, autoack, on_message)
        else
          socket = connect_tcp(host, port, keepalive, sock_opts)
          Connection.new(socket, client_id, clean_session, user, password, will, keepalive.to_u16, autoack, on_message)
        end
      end

      def initialize(@socket : IO, @client_id = "", @clean_session = true,
                     @user : String? = nil, @password : String? = nil,
                     @will : Message? = nil, @keepalive : UInt16 = 60u16,
                     @autoack = false, @on_message : Proc(ReceivedMessage, Nil)? = nil)
        @writer = Writer.new(@socket, @acks)
        @reader = Reader.new(@socket, @acks, @writer, @keepalive)
        @writer.connect(client_id, clean_session, user, password, will, keepalive)
        @reader.expect_connack
        spawn(name: "mqtt-client write_loop", same_thread: true) { @writer.run }
        spawn(name: "mqtt-client reader_loop", same_thread: true) { @reader.run }
        spawn message_loop, name: "mqtt-client message_loop"
      end

      def disconnect
        @writer.send Disconnect.new
      end

      def close
        @connected = false
        @writer.close
        @reader.close
        @acks.close
        @socket.close rescue nil
      end

      private def message_loop
        messages = @reader.messages
        loop do
          m = messages.receive? || break
          message = ReceivedMessage.new(self, m.packet_id, m.topic, m.body, m.qos, m.retain, m.dup)
          if on_message = @on_message
            on_message.call(message)
          end
          message.ack if @autoack
        end
      end

      def ping
        @writer.send PingReq.new
      end

      def puback(packet_id : UInt16)
        @writer.send PubAck.new(packet_id)
      end

      def pubrec(packet_id : UInt16)
        @writer.send PubRec.new(packet_id)
      end

      def on_message=(blk : Proc(ReceivedMessage, Nil)?)
        @on_message = blk
      end

      def on_message(&blk : Proc(ReceivedMessage, Nil))
        @on_message = blk
      end

      def subscribe(topic : String, qos : UInt8 = 0u8)
        subscribe({topic, qos})
      end

      def subscribe(*topics : Tuple(String, UInt8))
        subscribe(topics.to_a)
      end

      def subscribe(topics : Enumerable(Tuple(String, UInt8)))
        @writer.send Subscribe.new(topics)
      end

      def unsubscribe(*topics : String)
        @writer.send Unsubscribe.new(topics)
      end

      def publish(msg : Message)
        publish(msg.topic, msg.body, msg.qos, msg.retain)
      end

      def publish(topic : String, body, qos : Int = 0u8, retain = false, dup = false)
        @writer.send Publish.new(topic, body, qos, retain, dup)
      end

      private def self.connect_tcp(host, port, keepalive, sock_opts : SocketOptions)
        socket = TCPSocket.new(host, port, connect_timeout: 30)
        socket.keepalive = true
        socket.tcp_nodelay = false
        socket.tcp_keepalive_idle = 60
        socket.tcp_keepalive_count = 3
        socket.tcp_keepalive_interval = 10
        socket.sync = false
        socket.read_buffering = true
        socket.buffer_size = sock_opts.buffer_size if sock_opts.buffer_size.positive?
        socket.recv_buffer_size = sock_opts.recv_buffer_size if sock_opts.recv_buffer_size.positive?
        socket.send_buffer_size = sock_opts.send_buffer_size if sock_opts.send_buffer_size.positive?
        socket.read_timeout = keepalive.seconds
        socket
      end

      private def self.connect_tls(socket, verify_mode : LibSSL::VerifyMode, host : String)
        ctx = OpenSSL::SSL::Context::Client.new
        ctx.verify_mode = verify_mode
        connect_tls(socket, ctx, host)
      end

      private def self.connect_tls(socket, ctx, host)
        socket.sync = true
        socket.read_buffering = false
        tls_socket = OpenSSL::SSL::Socket::Client.new(socket, ctx, sync_close: true, hostname: host)
        tls_socket.sync = false
        tls_socket.read_buffering = true
        tls_socket.buffer_size = 16384
        tls_socket
      end
    end

    struct SocketOptions
      property buffer_size, recv_buffer_size, send_buffer_size

      def initialize(@buffer_size : Int32 = 1024,
                     @recv_buffer_size : Int32 = 512,
                     @send_buffer_size : Int32 = 256)
      end

      def self.throughput_optimized
        self.new(-1, -1, -1)
      end

      def self.memory_optimized
        self.new
      end
    end
  end
end
