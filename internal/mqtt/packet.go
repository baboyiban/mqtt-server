package mqtt

import (
	"errors"
	"fmt"
	"io"
)

const (
	PacketTypeCONNECT    = 1
	PacketTypeCONNACK    = 2
	PacketTypePUBLISH    = 3
	PacketTypePUBACK     = 4
	PacketTypePUBREC     = 5 // Publish Received
	PacketTypePUBREL     = 6 // Publish Release
	PacketTypePUBCOMP    = 7 // Publish Complete
	PacketTypeSUBSCRIBE  = 8
	PacketTypeSUBACK     = 9
	PacketTypePINGREQ    = 12
	PacketTypePINGRESP   = 13
	PacketTypeDISCONNECT = 14
)

// Add a function to write PINGRESP
func WritePingrespPacket(w io.Writer) error {
	packet := []byte{0xD0, 0x00} // PINGRESP packet is type 13 (D0) with no payload (length 0)
	_, err := w.Write(packet)
	return err
}

type PacketHeader struct {
	Type   byte
	Flags  byte
	RemLen int
}

// PacketWithID는 PUBACK, PUBREC, PUBREL, PUBCOMP 같이 Packet ID만 포함하는 패킷을 나타냅니다.
type PacketWithID struct {
	PacketID uint16
}

// ParsePacketWithID는 Packet ID만 포함하는 패킷을 파싱합니다.
func ParsePacketWithID(r io.Reader, remLen int) (*PacketWithID, error) {
	if remLen != 2 {
		return nil, fmt.Errorf("패킷 ID를 포함한 패킷의 길이가 유효하지 않음: %d", remLen)
	}
	buf := make([]byte, 2)
	if _, err := io.ReadFull(r, buf); err != nil {
		return nil, err
	}
	packetID := uint16(buf[0])<<8 | uint16(buf[1])
	return &PacketWithID{PacketID: packetID}, nil
}

func ReadPacketHeader(r io.Reader) (*PacketHeader, error) {
	var b [2]byte
	if _, err := io.ReadFull(r, b[:1]); err != nil {
		return nil, err
	}
	packetType := b[0] >> 4
	flags := b[0] & 0x0F
	remLen, err := readRemainingLength(r)
	if err != nil {
		return nil, err
	}
	return &PacketHeader{
		Type:   packetType,
		Flags:  flags,
		RemLen: remLen,
	}, nil
}

func readRemainingLength(r io.Reader) (int, error) {
	multiplier := 1
	value := 0
	for {
		var encoded [1]byte
		if _, err := io.ReadFull(r, encoded[:]); err != nil {
			return 0, err
		}
		value += int(encoded[0]&127) * multiplier
		if encoded[0]&128 == 0 {
			break
		}
		multiplier *= 128
		if multiplier > 128*128*128 {
			return 0, errors.New("malformed remaining length")
		}
	}
	return value, nil
}

// ConnectPacket은 MQTT CONNECT 패킷의 주요 필드를 나타냅니다.
type ConnectPacket struct {
	ClientID string
}

// ParseConnectPacket은 CONNECT 패킷을 파싱합니다.
func ParseConnectPacket(r io.Reader, remLen int) (*ConnectPacket, error) {
	buf := make([]byte, remLen)
	if _, err := io.ReadFull(r, buf); err != nil {
		return nil, err
	}
	if len(buf) < 10 {
		return nil, errors.New("CONNECT 패킷이 너무 짧음")
	}
	// 프로토콜 이름 길이
	protoNameLen := int(buf[0])<<8 | int(buf[1])
	if len(buf) < 2+protoNameLen+4 {
		return nil, errors.New("CONNECT 패킷이 너무 짧음(프로토콜 이름)")
	}
	// ClientID 위치 계산
	pos := 2 + protoNameLen + 1 + 1 + 2 // protoName + version + flags + keepalive
	if len(buf) < pos+2 {
		return nil, errors.New("CONNECT 패킷이 너무 짧음(ClientID 길이)")
	}
	clientIDLen := int(buf[pos])<<8 | int(buf[pos+1])
	if len(buf) < pos+2+clientIDLen {
		return nil, errors.New("ClientID 길이 오류")
	}
	clientID := string(buf[pos+2 : pos+2+clientIDLen])
	return &ConnectPacket{ClientID: clientID}, nil
}

// SubscribePacket은 MQTT SUBSCRIBE 패킷의 주요 필드를 나타냅니다.
type SubscribePacket struct {
	PacketID uint16
	Topic    string
}

// ParseSubscribePacket은 SUBSCRIBE 패킷을 파싱합니다.
func ParseSubscribePacket(r io.Reader, remLen int) (*SubscribePacket, error) {
	buf := make([]byte, remLen)
	if _, err := io.ReadFull(r, buf); err != nil {
		return nil, err
	}
	if len(buf) < 5 {
		return nil, errors.New("SUBSCRIBE 패킷이 너무 짧음")
	}
	packetID := uint16(buf[0])<<8 | uint16(buf[1])
	topicLen := int(buf[2])<<8 | int(buf[3])
	if len(buf) < 4+topicLen+1 {
		return nil, errors.New("토픽 길이 오류")
	}
	topic := string(buf[4 : 4+topicLen])
	return &SubscribePacket{
		PacketID: packetID,
		Topic:    topic,
	}, nil
}

// WriteSubackPacket은 SUBACK 패킷을 작성합니다.
func WriteSubackPacket(w io.Writer, packetID uint16) error {
	packet := []byte{0x90, 0x03, byte(packetID >> 8), byte(packetID & 0xFF), 0x00}
	_, err := w.Write(packet)
	return err
}

// PublishPacket은 MQTT PUBLISH 패킷의 주요 필드를 나타냅니다.
type PublishPacket struct {
	Topic    string
	Payload  []byte
	PacketID uint16 // QoS > 0 일 때 사용
	Retain   bool   // RETAIN 플래그
}

// ParsePublishPacket은 PUBLISH 패킷을 파싱합니다.
func ParsePublishPacket(r io.Reader, remLen int, headerFlags byte) (*PublishPacket, error) {
	buf := make([]byte, remLen)
	if _, err := io.ReadFull(r, buf); err != nil {
		return nil, err
	}
	if len(buf) < 2 {
		return nil, errors.New("PUBLISH 패킷이 너무 짧음")
	}

	pos := 0
	topicLen := int(buf[pos])<<8 | int(buf[pos+1])
	pos += 2

	if len(buf) < pos+topicLen {
		return nil, errors.New("토픽 길이 오류")
	}
	topic := string(buf[pos : pos+topicLen])
	pos += topicLen

	pkt := &PublishPacket{
		Topic:  topic,
		Retain: (headerFlags & 0x01) == 1, // Retain 플래그 파싱
	}

	qos := (headerFlags >> 1) & 0x03
	if qos > 0 {
		if len(buf) < pos+2 {
			return nil, errors.New("PUBLISH 패킷이 너무 짧음 (Packet ID 없음)")
		}
		pkt.PacketID = uint16(buf[pos])<<8 | uint16(buf[pos+1])
		pos += 2
	}

	pkt.Payload = buf[pos:]
	return pkt, nil
}

// WritePublishPacket은 PUBLISH 패킷을 작성합니다. (retain 플래그 포함)
func WritePublishPacket(w io.Writer, topic string, payload []byte, retain bool) error {
	topicLen := len(topic)
	remLen := 2 + topicLen + len(payload)

	// 기본 패킷 타입은 0x30 (DUP=0, QoS=0, RETAIN=0)
	packetType := byte(0x30)
	if retain {
		packetType |= 0x01 // RETAIN 플래그 설정
	}

	packet := []byte{packetType}
	packet = append(packet, encodeRemainingLength(remLen)...)
	packet = append(packet, byte(topicLen>>8), byte(topicLen&0xFF))
	packet = append(packet, []byte(topic)...)
	packet = append(packet, payload...)
	_, err := w.Write(packet)
	return err
}

// WriteConnackPacket은 CONNACK 패킷을 작성합니다.
func WriteConnackPacket(w io.Writer) error {
	packet := []byte{0x20, 0x02, 0x00, 0x00}
	_, err := w.Write(packet)
	return err
}

// WritePubackPacket은 PUBACK 패킷을 작성합니다.
func WritePubackPacket(w io.Writer, packetID uint16) error {
	packet := []byte{
		byte(PacketTypePUBACK << 4), // Packet Type: PUBACK (4)
		2,                           // Remaining Length
		byte(packetID >> 8),         // Packet ID MSB
		byte(packetID & 0xFF),       // Packet ID LSB
	}
	_, err := w.Write(packet)
	return err
}

// WritePubrecPacket은 PUBREC 패킷을 작성합니다.
func WritePubrecPacket(w io.Writer, packetID uint16) error {
	packet := []byte{byte(PacketTypePUBREC << 4), 2, byte(packetID >> 8), byte(packetID & 0xFF)}
	_, err := w.Write(packet)
	return err
}

// WritePubcompPacket은 PUBCOMP 패킷을 작성합니다.
func WritePubcompPacket(w io.Writer, packetID uint16) error {
	packet := []byte{byte(PacketTypePUBCOMP << 4), 2, byte(packetID >> 8), byte(packetID & 0xFF)}
	_, err := w.Write(packet)
	return err
}

// MQTT Remaining Length 가변 길이 인코딩
func encodeRemainingLength(remLen int) []byte {
	var encoded []byte
	for {
		enc := remLen % 128
		remLen /= 128
		if remLen > 0 {
			enc |= 128
		}
		encoded = append(encoded, byte(enc))
		if remLen == 0 {
			break
		}
	}
	return encoded
}
