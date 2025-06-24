package mqtt

import (
	"errors"
	"io"
)

const (
	PacketTypeCONNECT    = 1
	PacketTypeCONNACK    = 2
	PacketTypePUBLISH    = 3
	PacketTypeSUBSCRIBE  = 8
	PacketTypeSUBACK     = 9
	PacketTypePINGREQ    = 12 // Add this
	PacketTypePINGRESP   = 13 // Add this
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
	Topic   string
	Payload []byte
}

// ParsePublishPacket은 PUBLISH 패킷을 파싱합니다.
func ParsePublishPacket(r io.Reader, remLen int) (*PublishPacket, error) {
	buf := make([]byte, remLen)
	if _, err := io.ReadFull(r, buf); err != nil {
		return nil, err
	}
	if len(buf) < 2 {
		return nil, errors.New("PUBLISH 패킷이 너무 짧음")
	}
	topicLen := int(buf[0])<<8 | int(buf[1])
	if len(buf) < 2+topicLen {
		return nil, errors.New("토픽 길이 오류")
	}
	topic := string(buf[2 : 2+topicLen])
	payload := buf[2+topicLen:]
	return &PublishPacket{
		Topic:   topic,
		Payload: payload,
	}, nil
}

// WritePublishPacket은 PUBLISH 패킷을 작성합니다.
func WritePublishPacket(w io.Writer, topic string, payload []byte) error {
	topicLen := len(topic)
	remLen := 2 + topicLen + len(payload)
	packet := []byte{0x30}
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
