module MQTT
  class Client
    abstract struct Packet
    end

    struct Subscribe < Packet
      getter topics

      def initialize(@topics : Enumerable(Tuple(String, UInt8)))
      end
    end

    struct PingReq < Packet
    end

    struct PubAck < Packet
      getter packet_id

      def initialize(@packet_id : UInt16)
      end
    end

    struct PubRec < Packet
      getter packet_id

      def initialize(@packet_id : UInt16)
      end
    end

    struct PubRel < Packet
      getter packet_id

      def initialize(@packet_id : UInt16)
      end
    end

    struct PubComp < Packet
      getter packet_id

      def initialize(@packet_id : UInt16)
      end
    end

    struct Disconnect < Packet
    end

    struct Unsubscribe < Packet
      getter topics

      def initialize(@topics : Enumerable(String))
      end
    end

    struct Publish < Packet
      getter topic, body, qos, retain, dup

      def initialize(@topic : String, @body : Bytes, @qos : UInt8, @retain : Bool, @dup : Bool)
      end
    end
  end
end
