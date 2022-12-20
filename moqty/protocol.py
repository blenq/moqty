from asyncio import BufferedProtocol, Transport
from codecs import decode
import enum
from struct import Struct
from typing import Optional, Union, Dict

STANDARD_BUF_SIZE = 0x2000


subscribed_protocols = set()


class PacketType(enum.IntEnum):
    RESERVED = 0
    CONNECT = 1
    CONNACK = 2
    PUBLISH = 3
    PUBACK = 4
    PUBREC = 5
    PUBREL = 6
    PUBCOMP = 7
    SUBSCRIBE = 8
    SUBACK = 9
    UNSUBSCRIBE = 10
    UNSUBACK = 11
    PINGREQ = 12
    PINGRESP = 13
    DISCONNECT = 14
    AUTH = 15


def read_string(buf: memoryview, pos):
    """ Reads a MQTT string, prepended by 2 bytes length """
    str_len = buf[pos] * 256 + buf[pos + 1]
    pos += 2
    return decode(buf[pos:pos + str_len]), pos + str_len


def read_bytes(buf: memoryview, pos):
    """ Reads a MQTT binary value, prepended by 2 bytes length """
    str_len = buf[pos] * 256 + buf[pos + 1]
    return bytes(buf[2:2 + str_len]), pos + str_len + 2


class PropertyIdentifier(enum.IntEnum):
    """ Should contain all properties """
    ASSIGNED_CLIENT_IDENTIFIER = 18


# all properties that have a string as value
utf8_props = {PropertyIdentifier.ASSIGNED_CLIENT_IDENTIFIER}


def get_variable_bytes(val: int):
    # creates a variable byte integer
    byte_vals = []
    while True:
        byte_val = val % 128
        more = val // 128
        if more:
            byte_val += 128
        byte_vals.append(byte_val)
        if not more:
            break
    return bytes(byte_vals)


def create_props(props: Dict):
    # writes properties
    prop_fmt = []
    prop_vals = []
    for prop_ident, value in props.items():
        if prop_ident in utf8_props:
            prop_val = value.encode()
            prop_len = len(prop_val)
            prop_fmt.append(f"BH{prop_len}s")
            prop_vals.extend([prop_ident, prop_len, prop_val])
        else:
            raise ValueError("Invalid property")
    prop_size = Struct("!" + "".join(prop_fmt)).size
    prop_size_bytes = get_variable_bytes(prop_size)
    prop_fmt = [f"{len(prop_size_bytes)}s"] + prop_fmt
    prop_vals = [prop_size_bytes] + prop_vals
    return prop_fmt, prop_vals, prop_size + len(prop_size_bytes)


def read_vb_len(buf: memoryview, pos: int):
    # reads a variable byte integer
    vb_int_reader = VariableLByteIntReader()
    length = None
    while length is None:
        length = vb_int_reader.feed(buf[pos])
        pos += 1
    return length, pos


def read_props(buf: memoryview, pos: int):
    # reads properties
    props_len, pos = read_vb_len(buf, pos)
    end = pos + props_len
    props = {}
    while pos < end:
        prop_identifier = PropertyIdentifier(buf[pos])
        pos += 1
        prop_value = "wow"
        props[prop_identifier] = prop_value
    return props, pos


class VariableLByteIntReader:
    # class to read a variable byte integer per byte

    MAX_MULTIPLIER = 128 ** 3

    def __init__(self):
        self._length = 0
        self._multiplier = 1

    def feed(self, value):
        self._length += (value & 0x7F) * self._multiplier
        if value & 0x80:
            if self._multiplier == self.MAX_MULTIPLIER:
                raise ValueError("Invalid variable byte integer.")
            self._multiplier *= 128
            return None
        return self._length


class MQTTProtocol(BufferedProtocol):
    """ Asyncio protocol implementation """

    def __init__(self):
        # reading buffers and counters
        self._bytes_read = 0
        self._buf = self._standard_buf = memoryview(
            bytearray(STANDARD_BUF_SIZE))
        self._msg_part_len = 1
        self._packet_type = PacketType.RESERVED
        self._flags = 0
        self._vb_int = None

    def connection_made(self, transport: Transport) -> None:
        self._transport = transport

    def connection_lost(self, exc: Union[Exception, None]) -> None:
        self._transport = None

        # remove from subscriptions, if present
        subscribed_protocols.discard(self)

    def get_buffer(self, sizehint: int) -> memoryview:
        """ Gets a buffer to receive data into. """
        buf = self._buf
        if self._bytes_read:
            buf = buf[self._bytes_read:]
        return buf

    def buffer_updated(self, nbytes: int) -> None:
        """ Data is received """
        self._bytes_read += nbytes
        msg_start = 0

        while self._bytes_read >= self._msg_part_len:
            # read in three stages, first byte, then remaining length, and then
            # content
            if self._packet_type is PacketType.RESERVED:
                # read first byte
                byte1 = self._standard_buf[msg_start]
                self._packet_type = PacketType((byte1 & 0xF0) >> 4)
                self._flags = byte1 & 0x0F

                # set up for reading remaining length
                self._vb_int = VariableLByteIntReader()
                msg_part_len = 1
            elif self._vb_int is not None:
                # read remaining length
                msg_part_len = self._vb_int.feed(self._standard_buf[msg_start])
                if msg_part_len is None:
                    # need another byte for remaining length
                    msg_part_len = 1
                else:
                    # remaining length established, set up for content
                    self._vb_int = None
                    if msg_part_len > STANDARD_BUF_SIZE:
                        # message longer that standard buf, allocate XL buf
                        self._buf = memoryview(bytearray(msg_part_len))
            else:
                # handle the content
                self.handle_message(
                    self._buf[msg_start:msg_start + self._msg_part_len])

                # if XL buffer was used, it is discarded now
                self._buf = self._standard_buf

                # set up for next message
                msg_part_len = 1
                self._packet_type = PacketType.RESERVED

            # set up for reading the next message part
            self._bytes_read -= self._msg_part_len
            msg_start += self._msg_part_len
            self._msg_part_len = msg_part_len

        if self._bytes_read and msg_start:
            # move incomplete trailing message part to start of buffer
            self._buf[:self._bytes_read] = (
                self._standard_buf[msg_start:msg_start + self._bytes_read])

    def handle_message(self, buf: memoryview) -> None:
        if self._packet_type is PacketType.CONNECT:
            self.handle_connect(buf)
        elif self._packet_type is PacketType.SUBSCRIBE:
            self.handle_subscribe(buf)
        elif self._packet_type is PacketType.PUBLISH:
            self.handle_publish(buf)

    client_counter = [1]

    def connack_msg(self, session_present=0):
        """ Creates a CONNACK message """

        # Fixed header:
        # - byte: PacketType.CONNACK + flags (0)
        # - vbyte: remaining length
        # Variable header:
        # - byte: connack flags
        # - byte: reason code
        # - Properties:
        #     vbyte: props_length
        #     props: props

        prop_fmt_parts, prop_vals, prop_size = create_props({
            PropertyIdentifier.ASSIGNED_CLIENT_IDENTIFIER:
                f"client{self.client_counter[0]}"})
        self.client_counter[0] += 1
        fmt_parts = ["BB"] + prop_fmt_parts
        msg_vals = [0, 0] + prop_vals
        msg_size = prop_size + 2
        msg_size_bytes = get_variable_bytes(msg_size)
        msg_size_bytes_len = len(msg_size_bytes)
        fmt_parts = [f"!B{msg_size_bytes_len}s"] + fmt_parts
        msg_vals = [PacketType.CONNACK * 16, msg_size_bytes] + msg_vals
        return Struct("".join(fmt_parts)).pack(*msg_vals)

    def handle_connect(self, buf: memoryview) -> None:
        """ Handles an incoming CONNECT message """

        if self._flags:
            raise ValueError("Invalid flags")
        if buf[:6] != b'\0\x04MQTT':
            raise ValueError("Invalid protocol")
        if buf[6] != 5:
            raise ValueError("Invalid protocol version")
        connect_flags = buf[7]
        if connect_flags & 1:
            raise ValueError("Invalid connect flags")
        clean_start = bool(connect_flags & 2)
        will_flag = bool(connect_flags & 4)
        will_qos = bool((connect_flags & 24) >> 3)
        will_retain = bool(connect_flags & 32)
        has_password = bool(connect_flags & 64)
        has_username = bool(connect_flags & 128)
        self.keep_alive = buf[8] * 256 + buf[9]

        prop_len, pos = read_vb_len(buf, 10)
        pos += prop_len

        self._client_identifier, pos = read_string(buf, pos)

        if will_flag:
            will_prop_len, pos = read_vb_len(buf, pos)
            pos += will_prop_len
            will_topic, pos = read_string(buf, pos)
            will_payload = read_bytes(buf, pos)

        if has_username:
            username, pos = read_string(buf, pos)
        if has_password:
            password, pos = read_bytes(buf, pos)

        if pos != len(buf):
            raise ValueError("Invalid CONNECT")

        self._transport.write(self.connack_msg(0))

    def suback_msg(self, packet_identifier, subscriptions):
        """ Creates a SUBACK message """

        # Variable header: packet identifier
        fmt_parts = ["2s"]
        msg_vals = [packet_identifier]

        # Variable header: properties
        prop_fmt_parts, prop_msg_vals, prop_size = create_props({})
        fmt_parts.extend(prop_fmt_parts)
        msg_vals.extend(prop_msg_vals)

        # Payload: subscription response codes
        fmt_parts.append("B" * len(subscriptions))
        msg_vals.extend([0] * len(subscriptions))

        # Envelope including static header
        msg_size = prop_size + len(subscriptions) + 2
        msg_size_bytes = get_variable_bytes(msg_size)
        msg_size_bytes_len = len(msg_size_bytes)
        fmt_parts = [f"!B{msg_size_bytes_len}s"] + fmt_parts
        msg_vals = [PacketType.SUBACK * 16, msg_size_bytes] + msg_vals
        return Struct("".join(fmt_parts)).pack(*msg_vals)

    def handle_subscribe(self, buf: memoryview):
        """ Handles an incoming subscribe message """

        if self._flags != 2:
            raise ValueError("Invalid subscribe")
        packet_identifier = bytes(buf[:2])
        pos = 2
        props, pos = read_props(buf, pos)
        subscriptions = []
        while pos < len(buf):
            topic_filter, pos = read_string(buf, pos)
            subs_options = buf[pos]
            subscriptions.append((topic_filter, subs_options))
            pos += 1
        if not subscriptions:
            raise ValueError("No topics in SUBSCRIBE message")
        if pos != len(buf):
            raise ValueError("Invalid SUBSCRIBE message")

        # Topic is not checked at all, subscribed to everything
        subscribed_protocols.add(self)

        self._transport.write(
            self.suback_msg(packet_identifier, subscriptions))

    def send(self, data):
        self._transport.write(data)

    def get_publish_msg(self, topic_name, payload):
        """ Creates a PUBLISH message """

        # Variable header: packet identifier
        btopic = topic_name.encode()
        fmt_parts = [f"H{len(btopic)}s"]
        msg_vals = [len(btopic), btopic]
        msg_size = len(btopic) + 2

        # Variable header: properties
        prop_fmt_parts, prop_msg_vals, prop_size = create_props({})
        fmt_parts.extend(prop_fmt_parts)
        msg_vals.extend(prop_msg_vals)
        msg_size += prop_size

        # Payload
        fmt_parts.append(f"{len(payload)}s")
        msg_vals.append(payload)
        msg_size += len(payload)

        # Envelope including static header
        msg_size_bytes = get_variable_bytes(msg_size)
        msg_size_bytes_len = len(msg_size_bytes)
        fmt_parts = [f"!B{msg_size_bytes_len}s"] + fmt_parts
        msg_vals = [PacketType.PUBLISH * 16, msg_size_bytes] + msg_vals
        return Struct("".join(fmt_parts)).pack(*msg_vals)

    def handle_publish(self, buf: memoryview):
        """ Handles an incoming PUBLISH message """

        topic_name, pos = read_string(buf, 0)
        # packet_identifier = buf[pos:pos + 2]
        # pos += 2
        publish_props, pos = read_props(buf, pos)
        payload = bytes(buf[pos:])
        for prot in subscribed_protocols:
            # send message to all subscribers
            prot.send(self.get_publish_msg(topic_name, payload))
