module MQTT
  class Client
    abstract struct Packet; end

    record Subscribe < Packet, topics : Enumerable(Tuple(String, UInt8))
    record PingReq < Packet
    record PubAck < Packet, packet_id : UInt16
    record PubRec < Packet, packet_id : UInt16
    record PubRel < Packet, packet_id : UInt16
    record PubComp < Packet, packet_id : UInt16
    record Disconnect < Packet
    record Unsubscribe < Packet, topics : Enumerable(String)
    record Publish < Packet, topic : String, body : Bytes, qos : UInt8, retain : Bool, dup : Bool
  end
end
