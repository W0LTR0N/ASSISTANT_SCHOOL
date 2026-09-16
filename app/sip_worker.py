"""
SIP Worker — телефония через Plusofon.
SIP signaling: TCP (один reader loop + dispatcher).
RTP media: UDP.
"""

import asyncio
import hashlib
import json
import logging
import random
import re
import signal
import socket
import struct
import time
import uuid
import audioop
from typing import Optional, Dict, List, Tuple

from config import (
    PLUSOFON_SIP_HOST, PLUSOFON_SIP_USER, PLUSOFON_SIP_PASSWORD,
    PUBLIC_IP, RTP_PORT_MIN, RTP_PORT_MAX, TRUSTED_SIP_IPS, BLACKLIST_SIP_IPS,
    HEARTBEAT_INTERVAL, HEARTBEAT_FILE, SIP_CAN_START,
    MAX_CONCURRENT_CALLS, MAX_CALL_DURATION, MEDIA_IDLE_TIMEOUT,
    MAX_RTP_BUFFER_BYTES, SIP_TIMER_B, SIP_RATE_LIMIT_PER_IP,
)

logger = logging.getLogger("sip_worker")

CLEANUP_STATES = {
    "NOT_STARTED": 0, "STARTED": 1, "RESOURCES_CLOSED": 2,
    "FINISH_CALLED": 3, "COMPLETED": 4,
}


# ── Helpers ────────────────────────────────────────────────────────────────

def get_header(msg: str, name: str) -> Optional[str]:
    m = re.search(rf'^{re.escape(name)}\s*:\s*(.+)$', msg, re.MULTILINE | re.IGNORECASE)
    return m.group(1).strip() if m else None


def get_status(first_line: str) -> Optional[int]:
    m = re.match(r'SIP/2\.0\s+(\d{3})', first_line)
    return int(m.group(1)) if m else None


def parse_contact_uri(contact: str) -> Optional[str]:
    if not contact:
        return None
    m = re.search(r'<([^>]+)>', contact)
    uri = m.group(1).strip() if m else contact.split(';')[0].split()[0].strip()
    return uri if uri.startswith(("sip:", "sips:")) else None


def parse_contact_address(contact: str) -> Optional[Tuple[str, int]]:
    uri = parse_contact_uri(contact)
    if not uri:
        return None
    m = re.search(r'sip:[^@]*@([^:]+)(?::(\d+))?', uri)
    if m:
        return m.group(1), int(m.group(2)) if m.group(2) else 5060
    return None


def parse_digest_params(header: str) -> dict:
    return {
        m.group(1): m.group(2) if m.group(2) is not None else m.group(3)
        for m in re.finditer(r'(\w+)=(?:"([^"]+)"|([^,\s]+))', header)
    }


def rtp_seq_lt(a: int, b: int) -> bool:
    return ((a - b) & 0xFFFF) > 32768


def normalize_phone(phone: str) -> Optional[str]:
    if not phone:
        return None
    digits = re.sub(r'\D+', '', phone)
    if digits.startswith('8') and len(digits) == 11:
        digits = '7' + digits[1:]
    elif len(digits) == 10:
        digits = '7' + digits
    if len(digits) != 11 or not digits.startswith('7'):
        return None
    return digits


# ─── Rate Limiter ─────────────────────────────────────────────────────────

class RateLimiter:
    def __init__(self, max_per_minute: int):
        self.max_per_minute = max_per_minute
        self._requests: Dict[str, List[float]] = {}

    def allow(self, ip: str) -> bool:
        now = time.time()
        self._requests[ip] = [t for t in self._requests.get(ip, []) if now - t < 60]
        if len(self._requests[ip]) >= self.max_per_minute:
            return False
        self._requests[ip].append(now)
        return True


# ─── SIP Transaction ───────────────────────────────────────────────────────

class SIPTransaction:
    """SIP transaction с очередью responses."""
    
    def __init__(self, call_id: str, cseq: int, method: str, branch: Optional[str] = None):
        self.call_id = call_id
        self.cseq = cseq
        self.method = method
        self.branch = branch
        self.response_queue: asyncio.Queue = asyncio.Queue()
    
    def key(self) -> Tuple[str, int, str]:
        return (self.call_id, self.cseq, self.method)


# ─── RTP Protocol (UDP media) ───────────────────────────────────────────────

class RTPProtocol(asyncio.DatagramProtocol):
    def __init__(self, worker, call_id, phone, remote_ip, remote_port):
        self.worker = worker
        self.call_id = call_id
        self.phone = phone
        self.remote_target = (remote_ip, remote_port)
        self.transport = None
        self.active = True
        self.speaking = False
        self.pcm_buffer = bytearray()
        self.ssrc = random.randint(0, 0xFFFFFFFF)
        self.sequence_number = random.randint(0, 0xFFFF)
        self.timestamp = random.randint(0, 0xFFFFFFFF)
        self.last_packet_time = time.monotonic()
        self.negotiated_codec = 8
        self._last_rx_seq = None

    def set_codec(self, payload_type: int):
        if payload_type in (0, 8):
            self.negotiated_codec = payload_type

    def connection_made(self, transport):
        self.transport = transport

    def connection_lost(self, exc):
        self.active = False

    def datagram_received(self, data, addr):
        if not self.active or len(data) < 12:
            return
        self.last_packet_time = time.monotonic()
        if (data[0] >> 6) & 0x3 != 2:
            return
        payload_type = data[1] & 0x7F
        if payload_type not in (0, 8):
            return

        cc = data[0] & 0x0F
        x_bit = (data[0] >> 4) & 0x1
        p_bit = (data[0] >> 5) & 0x1
        seq = struct.unpack("!H", data[2:4])[0]
        offset = 12 + cc * 4
        if x_bit and len(data) >= offset + 4:
            offset += 4 + struct.unpack("!H", data[offset+2:offset+4])[0] * 4
        if offset > len(data):
            return

        payload = data[offset:]
        if p_bit and payload:
            pad_len = payload[-1]
            if 0 < pad_len <= len(payload):
                payload = payload[:-pad_len]

        if self._last_rx_seq is not None and (seq == self._last_rx_seq or rtp_seq_lt(seq, self._last_rx_seq)):
            return
        self._last_rx_seq = seq

        try:
            pcm = audioop.alaw2lin(payload, 2) if payload_type == 8 else audioop.ulaw2lin(payload, 2)
            self.pcm_buffer.extend(pcm)
            if len(self.pcm_buffer) > MAX_RTP_BUFFER_BYTES:
                del self.pcm_buffer[:len(self.pcm_buffer) - MAX_RTP_BUFFER_BYTES]
        except Exception as e:
            logger.error("RTP decode error call_id=%s: %s", self.call_id, e)

    async def send_pcm(self, pcm_data: bytes):
        """Отправляет PCM audio через RTP. Вызывается VoiceEngine."""
        if not self.remote_target or not pcm_data:
            return
        self.speaking = True
        try:
            pt = 0x08 if self.negotiated_codec == 8 else 0x00
            encode = audioop.lin2alaw if self.negotiated_codec == 8 else audioop.lin2ulaw
            frame_bytes = 320
            start = time.monotonic()
            for i in range(0, len(pcm_data), frame_bytes):
                chunk = pcm_data[i:i+frame_bytes]
                if len(chunk) < frame_bytes:
                    chunk = chunk + b'\x00' * (frame_bytes - len(chunk))
                encoded = encode(chunk, 2)
                marker = 0x80 if i == 0 else 0x00
                header = struct.pack("!BBHII", 0x80, marker | pt,
                                     self.sequence_number & 0xFFFF,
                                     self.timestamp & 0xFFFFFFFF, self.ssrc)
                self.sequence_number = (self.sequence_number + 1) & 0xFFFF
                self.timestamp = (self.timestamp + 160) & 0xFFFFFFFF
                try:
                    self.transport.sendto(header + encoded, self.remote_target)
                except Exception:
                    break
                delay = start + (i // frame_bytes + 1) * 0.02 - time.monotonic()
                if delay > 0:
                    await asyncio.sleep(delay)
        finally:
            self.speaking = False

    def send_keepalive(self):
        if not self.transport or not self.remote_target or self.speaking or not self.active:
            return
        pt = 0x08 if self.negotiated_codec == 8 else 0x00
        payload = b'\xd5' * 160 if self.negotiated_codec == 8 else b'\xff' * 160
        header = struct.pack("!BBHII", 0x80, pt,
                             self.sequence_number & 0xFFFF,
                             self.timestamp & 0xFFFFFFFF, self.ssrc)
        self.sequence_number = (self.sequence_number + 1) & 0xFFFF
        self.timestamp = (self.timestamp + 160) & 0xFFFFFFFF
        try:
            self.transport.sendto(header + payload, self.remote_target)
        except Exception:
            pass


# ── SIP Worker ─────────────────────────────────────────────────────────────

class SIPWorker:
    def __init__(self, voice_engine, database, integrations):
        self.host = PLUSOFON_SIP_HOST
        self.port = 5060
        self.user = PLUSOFON_SIP_USER
        self.password = PLUSOFON_SIP_PASSWORD

        self.reader: Optional[asyncio.StreamReader] = None
        self.writer: Optional[asyncio.StreamWriter] = None
        self.tcp_buffer = bytearray()
        self.is_running = False
        self.is_connected = False
        self.registered = False

        self.transactions: Dict[Tuple[str, int, str], SIPTransaction] = {}
        self._reader_task: Optional[asyncio.Task] = None

        self.sessions: Dict[str, dict] = {}
        self.used_ports: set = set()
        self._last_allocated_port = RTP_PORT_MIN
        self._port_lock = asyncio.Lock()

        self.register_cseq = 1
        self.register_call_id = f"{random.randint(100000, 999999)}@{self.host}"
        self.register_from_tag = f"tag{random.randint(100000, 999999)}"
        self.auth_cache: Optional[dict] = None

        self.voice_engine = voice_engine
        self.database = database
        self.integrations = integrations

        self._refresh_task = None
        self._heartbeat_task = None
        self.max_concurrent_calls = MAX_CONCURRENT_CALLS
        self.rate_limiter = RateLimiter(SIP_RATE_LIMIT_PER_IP)

    # ─── TCP Reader & Dispatcher ─────────────────────────────────────────

    async def _tcp_reader_loop(self):
        """Единственный reader для всего TCP stream.
        Работает пока существует writer, чтобы получать ответы на BYE при graceful shutdown."""
        try:
            while self.writer:
                header_end = self.tcp_buffer.find(b"\r\n\r\n")
                if header_end < 0:
                    chunk = await self.reader.read(4096)
                    if not chunk:
                        logger.warning("[SIP] TCP connection closed by server")
                        break
                    self.tcp_buffer.extend(chunk)
                    continue

                header_text = self.tcp_buffer[:header_end].decode("latin-1", errors="replace")
                content_length = 0
                for line in header_text.split("\r\n"):
                    if re.match(r"(?i)^content-length\s*:", line) or re.match(r"(?i)^l\s*:", line):
                        try:
                            content_length = int(line.split(":", 1)[1].strip())
                        except ValueError:
                            content_length = 0
                        break

                total = header_end + 4 + content_length
                while len(self.tcp_buffer) < total:
                    chunk = await self.reader.read(total - len(self.tcp_buffer))
                    if not chunk:
                        logger.warning("[SIP] TCP connection closed during body read")
                        break
                    self.tcp_buffer.extend(chunk)

                if len(self.tcp_buffer) < total:
                    break

                msg_bytes = bytes(self.tcp_buffer[:total])
                del self.tcp_buffer[:total]

                first_line = msg_bytes.split(b"\r\n", 1)[0].decode("latin-1", errors="replace")
                headers = msg_bytes.decode("latin-1", errors="replace")
                body_start = header_end + 4
                body = msg_bytes[body_start:body_start + content_length].decode("utf-8", errors="replace")

                await self._dispatch_message(first_line, headers, body)

        except Exception as e:
            logger.error("[SIP] TCP reader error: %s", e)
        finally:
            self.is_connected = False
            self.registered = False

    async def _dispatch_message(self, first_line: str, headers: str, body: str):
        status = get_status(first_line)
        
        if status is None:
            method = first_line.split(" ", 1)[0] if " " in first_line else "UNKNOWN"
            call_id = get_header(headers, "Call-ID") or ""
            logger.info("[SIP] Incoming %s (Call-ID: %s)", method, call_id)
            
            if method in ("NOTIFY", "OPTIONS", "INFO"):
                await self._handle_incoming_request(first_line, headers, body)
            elif method == "INVITE":
                await self._handle_incoming_invite(headers)
            elif method == "BYE":
                await self._handle_incoming_bye(headers)
            return

        call_id = get_header(headers, "Call-ID") or ""
        cseq_str = get_header(headers, "CSeq") or ""
        cseq_parts = cseq_str.split()
        cseq = int(cseq_parts[0]) if cseq_parts else 0
        method = cseq_parts[1] if len(cseq_parts) > 1 else "UNKNOWN"

        logger.info("[SIP] Response %s for Call-ID=%s CSeq=%d %s", 
                    first_line.strip(), call_id, cseq, method)

        tx_key = (call_id, cseq, method)
        tx = self.transactions.get(tx_key)
        
        if tx:
            await tx.response_queue.put((first_line, headers, body))
        else:
            logger.warning("[SIP] No transaction for Call-ID=%s CSeq=%d %s", call_id, cseq, method)

    async def _send_sip(self, msg: str) -> bool:
        if not self.writer:
            return False
        try:
            self.writer.write(msg.encode('utf-8'))
            await self.writer.drain()
            return True
        except Exception as e:
            logger.error("[SIP] Send error: %s", e)
            return False

    async def _wait_response(self, tx: SIPTransaction, timeout: float = 15.0) -> Optional[Tuple[str, str, str]]:
        try:
            return await asyncio.wait_for(tx.response_queue.get(), timeout=timeout)
        except asyncio.TimeoutError:
            return None

    # ─── Port management ────────────────────────────────────────────────

    async def reserve_port(self) -> Optional[int]:
        async with self._port_lock:
            total = RTP_PORT_MAX - RTP_PORT_MIN + 1
            for _ in range(total):
                port = self._last_allocated_port
                self._last_allocated_port = (
                    self._last_allocated_port + 1
                    if self._last_allocated_port < RTP_PORT_MAX
                    else RTP_PORT_MIN
                )
                if port not in self.used_ports:
                    self.used_ports.add(port)
                    return port
        return None

    def release_port(self, port: int):
        self.used_ports.discard(port)

    # ── SDP ────────────────────────────────────────────────────────────

    def generate_sdp(self, rtp_port: int) -> str:
        return (
            f"v=0\r\n"
            f"o=- {int(time.time())} 1 IN IP4 {PUBLIC_IP}\r\n"
            f"s=-\r\n"
            f"c=IN IP4 {PUBLIC_IP}\r\n"
            f"t=0 0\r\n"
            f"m=audio {rtp_port} RTP/AVP 8 0 101\r\n"
            f"a=rtpmap:8 PCMA/8000\r\n"
            f"a=rtpmap:0 PCMU/8000\r\n"
            f"a=rtpmap:101 telephone-event/8000\r\n"
            f"a=fmtp:101 0-16\r\n"
            f"a=sendrecv\r\n"
        )

    def parse_sdp_remote_media(self, msg: str) -> Tuple[Optional[str], Optional[int], Optional[int]]:
        session_ip = media_ip = None
        port = None
        in_media = False
        codecs = []
        for line in msg.splitlines():
            if line.startswith("c=IN IP4"):
                ip = line.split()[2]
                if in_media:
                    media_ip = ip
                else:
                    session_ip = ip
            elif line.startswith("m=audio"):
                in_media = True
                try:
                    port = int(line.split()[1])
                except Exception:
                    port = None
                for pt_str in line.split()[3:]:
                    try:
                        pt = int(pt_str)
                        if pt in (0, 8, 101):
                            codecs.append(pt)
                    except ValueError:
                        pass
        if port is None or not codecs:
            return None, None, None
        codec = 8 if 8 in codecs else (0 if 0 in codecs else codecs[0])
        return (media_ip or session_ip), port, codec

    # ─── Digest Auth ────────────────────────────────────────────────────

    def compute_digest(self, auth_data: dict, method: str, uri: str, proxy: bool = False) -> str:
        realm = auth_data.get("realm", "")
        nonce = auth_data.get("nonce", "")
        qop_raw = auth_data.get("qop", "")
        algorithm = auth_data.get("algorithm", "MD5").upper()
        if algorithm not in ("MD5", ""):
            raise RuntimeError(f"Unsupported Digest algorithm={algorithm}")

        ha1 = hashlib.md5(f"{self.user}:{realm}:{self.password}".encode()).hexdigest()
        ha2 = hashlib.md5(f"{method}:{uri}".encode()).hexdigest()

        header_name = "Proxy-Authorization" if proxy else "Authorization"

        if qop_raw:
            qop_list = [q.strip() for q in qop_raw.split(",") if q.strip()]
            qop = "auth" if "auth" in qop_list else (qop_list[0] if qop_list else None)
            if qop:
                auth_data["_nc_counter"] = auth_data.get("_nc_counter", 0) + 1
                nc = f"{auth_data['_nc_counter']:08x}"
                cnonce = f"{random.randint(0, 0xFFFFFFFF):08x}"
                response = hashlib.md5(
                    f"{ha1}:{nonce}:{nc}:{cnonce}:{qop}:{ha2}".encode()
                ).hexdigest()
                extra = f', qop={qop}, nc={nc}, cnonce="{cnonce}"'
            else:
                response = hashlib.md5(f"{ha1}:{nonce}:{ha2}".encode()).hexdigest()
                extra = ""
        else:
            response = hashlib.md5(f"{ha1}:{nonce}:{ha2}".encode()).hexdigest()
            extra = ""

        value = (
            f'{header_name}: Digest username="{self.user}", realm="{realm}", '
            f'nonce="{nonce}", uri="{uri}", response="{response}"{extra}'
        )
        if "opaque" in auth_data:
            value += f', opaque="{auth_data["opaque"]}"'
        return value

    def parse_auth_challenge(self, msg: str) -> Tuple[Optional[dict], bool]:
        m = re.search(r'Proxy-Authenticate:\s*Digest\s+(.+)', msg, re.IGNORECASE)
        if m:
            return parse_digest_params(m.group(1)), True
        m = re.search(r'WWW-Authenticate:\s*Digest\s+(.+)', msg, re.IGNORECASE)
        if m:
            return parse_digest_params(m.group(1)), False
        return None, False

    # ─── SIP message builders ──────────────────────────────────────────

    def via_header(self, branch: str) -> str:
        return f"SIP/2.0/TCP {PUBLIC_IP}:5060;branch={branch};rport"

    def contact_header(self) -> str:
        return f"<sip:{self.user}@{PUBLIC_IP}:5060;transport=tcp>"

    def build_register(self, auth_header: Optional[str] = None) -> str:
        uri = f"sip:{self.host}"
        lines = [
            f"REGISTER {uri} SIP/2.0",
            f"Via: {self.via_header(f'z9hG4bK{uuid.uuid4().hex}')}",
            f"From: <sip:{self.user}@{self.host}>;tag={self.register_from_tag}",
            f"To: <sip:{self.user}@{self.host}>",
            f"Call-ID: {self.register_call_id}",
            f"CSeq: {self.register_cseq} REGISTER",
            f"Contact: {self.contact_header()}",
            "Max-Forwards: 70",
            "Expires: 300",
        ]
        if auth_header:
            lines.append(auth_header)
        lines.append("Content-Length: 0")
        return "\r\n".join(lines) + "\r\n\r\n"

    def build_invite(self, session: dict, auth_header: Optional[str] = None) -> str:
        phone = session["phone"]
        ruri = f"sip:{phone}@{self.host}"
        branch = session["via_branch"]
        sdp = self.generate_sdp(session["rtp_port"])
        sdp_bytes = sdp.encode("utf-8")
        lines = [
            f"INVITE {ruri} SIP/2.0",
            f"Via: {self.via_header(branch)}",
            f"From: <sip:{self.user}@{self.host}>;tag={session['from_tag']}",
            f"To: <sip:{phone}@{self.host}>",
            f"Call-ID: {session['call_id']}",
            f"CSeq: {session['invite_cseq']} INVITE",
            f"Contact: {self.contact_header()}",
            "Max-Forwards: 70",
            "Content-Type: application/sdp",
        ]
        if auth_header:
            lines.append(auth_header)
        lines.append(f"Content-Length: {len(sdp_bytes)}")
        return "\r\n".join(lines) + "\r\n\r\n" + sdp

    def build_ack(self, session: dict, remote_contact: Optional[str] = None, 
                  use_invite_branch: bool = False) -> str:
        """ACK: use_invite_branch=True для non-2xx (тот же branch что INVITE)."""
        branch = session["via_branch"] if use_invite_branch else f"z9hG4bK{uuid.uuid4().hex}"
        ruri = parse_contact_uri(remote_contact) if remote_contact else f"sip:{session['phone']}@{self.host}"
        to_hdr = session.get("to_hdr", f"<sip:{session['phone']}@{self.host}>")
        if session.get("to_tag") and ";tag=" not in to_hdr.lower():
            to_hdr = f"{to_hdr};tag={session['to_tag']}"
        return (
            f"ACK {ruri} SIP/2.0\r\n"
            f"Via: {self.via_header(branch)}\r\n"
            f"From: <sip:{self.user}@{self.host}>;tag={session['from_tag']}\r\n"
            f"To: {to_hdr}\r\n"
            f"Call-ID: {session['call_id']}\r\n"
            f"CSeq: {session['invite_cseq']} ACK\r\n"
            f"Max-Forwards: 70\r\n"
            f"Content-Length: 0\r\n\r\n"
        )

    def build_bye(self, session: dict) -> str:
        branch = f"z9hG4bK{uuid.uuid4().hex}"
        uri = parse_contact_uri(session.get("remote_contact")) or f"sip:{session['phone']}@{self.host}"
        from_hdr = session["from_hdr"]
        to_hdr = session.get("to_hdr", f"<sip:{session['phone']}@{self.host}>")
        if session.get("to_tag") and ";tag=" not in to_hdr.lower():
            to_hdr = f"{to_hdr};tag={session['to_tag']}"
        return (
            f"BYE {uri} SIP/2.0\r\n"
            f"Via: {self.via_header(branch)}\r\n"
            f"From: {from_hdr}\r\n"
            f"To: {to_hdr}\r\n"
            f"Call-ID: {session['call_id']}\r\n"
            f"CSeq: {session['bye_cseq']} BYE\r\n"
            f"Max-Forwards: 70\r\n"
            f"Content-Length: 0\r\n\r\n"
        )

    def build_cancel(self, session: dict) -> str:
        ruri = f"sip:{session['phone']}@{self.host}"
        return (
            f"CANCEL {ruri} SIP/2.0\r\n"
            f"Via: {self.via_header(session['via_branch'])}\r\n"
            f"From: {session['from_hdr']}\r\n"
            f"To: {session['to_hdr']}\r\n"
            f"Call-ID: {session['call_id']}\r\n"
            f"CSeq: {session['invite_cseq']} CANCEL\r\n"
            f"Max-Forwards: 70\r\n"
            f"Content-Length: 0\r\n\r\n"
        )

    def build_response_200(self, request_headers: str) -> str:
        via = get_header(request_headers, "Via") or ""
        from_hdr = get_header(request_headers, "From") or ""
        to_hdr = get_header(request_headers, "To") or ""
        call_id = get_header(request_headers, "Call-ID") or ""
        cseq = get_header(request_headers, "CSeq") or ""
        if ";tag=" not in to_hdr.lower():
            to_hdr = f"{to_hdr};tag={random.randint(1000, 9999)}"
        return (
            f"SIP/2.0 200 OK\r\n"
            f"Via: {via}\r\n"
            f"From: {from_hdr}\r\n"
            f"To: {to_hdr}\r\n"
            f"Call-ID: {call_id}\r\n"
            f"CSeq: {cseq}\r\n"
            f"Contact: {self.contact_header()}\r\n"
            f"Allow: INVITE, ACK, BYE, CANCEL, OPTIONS, NOTIFY, INFO\r\n"
            f"Content-Length: 0\r\n\r\n"
        )

    # ─── Registration ──────────────────────────────────────────────────

    async def _do_register(self) -> bool:
        tx = SIPTransaction(self.register_call_id, self.register_cseq, "REGISTER")
        self.transactions[tx.key()] = tx
        
        msg = self.build_register()
        logger.info("[SIP] Sending REGISTER cseq=%d", self.register_cseq)
        if not await self._send_sip(msg):
            del self.transactions[tx.key()]
            return False

        resp = await self._wait_response(tx, timeout=15.0)
        del self.transactions[tx.key()]
        
        if not resp:
            logger.error("[SIP] REGISTER timeout")
            return False
        
        first, headers, body = resp
        status = get_status(first)
        
        if status in (401, 407):
            auth_data, proxy = self.parse_auth_challenge(headers)
            if not auth_data:
                logger.error("[SIP] No auth data in 401/407")
                return False
            
            self.auth_cache = auth_data
            self.auth_cache["_proxy"] = proxy
            auth_header = self.compute_digest(auth_data, "REGISTER", f"sip:{self.host}", proxy)
            
            self.register_cseq += 1
            tx2 = SIPTransaction(self.register_call_id, self.register_cseq, "REGISTER")
            self.transactions[tx2.key()] = tx2
            
            msg2 = self.build_register(auth_header)
            logger.info("[SIP] Sending authenticated REGISTER cseq=%d", self.register_cseq)
            if not await self._send_sip(msg2):
                del self.transactions[tx2.key()]
                return False
            
            resp2 = await self._wait_response(tx2, timeout=15.0)
            del self.transactions[tx2.key()]
            
            if not resp2:
                logger.error("[SIP] Authenticated REGISTER timeout")
                return False
            
            first2, _, _ = resp2
            status2 = get_status(first2)
            if status2 == 200:
                self.registered = True
                logger.info("[SIP] SIP account REGISTERED")
                return True
            else:
                logger.error("[SIP] Authenticated REGISTER failed: %s", first2.strip())
                return False
        
        elif status == 200:
            self.registered = True
            logger.info("[SIP] SIP account REGISTERED")
            return True
        else:
            logger.error("[SIP] REGISTER failed: %s", first.strip())
            return False

    async def register_refresh_loop(self):
        while self.is_running:
            await asyncio.sleep(45)
            if self.is_connected and self.registered:
                logger.info("[SIP] Refreshing REGISTER...")
                success = await self._do_register()
                if not success:
                    logger.warning("[SIP] REGISTER refresh failed")

    # ─── Incoming requests ─────────────────────────────────────────────

    async def _handle_incoming_request(self, first: str, headers: str, body: str):
        response = self.build_response_200(headers)
        await self._send_sip(response)
        method = first.split(" ", 1)[0]
        logger.info("[SIP] Sent 200 OK for %s", method)

    async def _handle_incoming_invite(self, headers: str):
        call_id = get_header(headers, "Call-ID") or ""
        cseq = get_header(headers, "CSeq") or ""
        from_hdr = get_header(headers, "From") or ""
        to_hdr = get_header(headers, "To") or ""
        via_hdr = get_header(headers, "Via") or ""
        
        trying = (
            f"SIP/2.0 100 Trying\r\nVia: {via_hdr}\r\nFrom: {from_hdr}\r\n"
            f"To: {to_hdr}\r\nCall-ID: {call_id}\r\nCSeq: {cseq}\r\nContent-Length: 0\r\n\r\n"
        )
        await self._send_sip(trying)
        
        busy = (
            f"SIP/2.0 486 Busy Here\r\nVia: {via_hdr}\r\nFrom: {from_hdr}\r\n"
            f"To: {to_hdr}\r\nCall-ID: {call_id}\r\nCSeq: {cseq}\r\nContent-Length: 0\r\n\r\n"
        )
        await self._send_sip(busy)

    async def _handle_incoming_bye(self, headers: str):
        call_id = get_header(headers, "Call-ID") or ""
        session = self.sessions.get(call_id)
        response = self.build_response_200(headers)
        await self._send_sip(response)
        
        if session:
            session["hangup_reason"] = "remote_hangup"
            await self._cleanup_call(call_id, send_lead=True)
        else:
            logger.info("[SIP] Incoming BYE for unknown Call-ID: %s", call_id)

    # ── Outbound call ─────────────────────────────────────────────────

    async def originate_call(self, phone: str, scenario: str = "BEFORE_LESSON",
                             metadata: Optional[dict] = None) -> Optional[str]:
        if not self.is_connected:
            logger.error("[SIP] Cannot call: not connected")
            return None
        if not self.registered:
            logger.error("[SIP] Cannot call: not registered")
            return None
        if scenario not in ("BEFORE_LESSON", "AFTER_LESSON"):
            return None
        if len(self.get_active_calls()) >= self.max_concurrent_calls:
            return None

        normalized = normalize_phone(phone)
        if not normalized:
            logger.error("[SIP] Invalid phone: %s", phone)
            return None

        rtp_port = await self.reserve_port()
        if rtp_port is None:
            return None

        sock = await self._bind_rtp_socket(rtp_port)
        if not sock:
            self.release_port(rtp_port)
            for _ in range(5):
                rtp_port = await self.reserve_port()
                if rtp_port is None:
                    break
                sock = await self._bind_rtp_socket(rtp_port)
                if sock:
                    break
            if not sock:
                self.release_port(rtp_port)
                return None

        call_id = f"{int(time.time() * 1000)}{uuid.uuid4().hex[:8]}@{PUBLIC_IP}"
        try:
            await self.database.create_call(
                call_id=call_id, direction="outbound", scenario=scenario,
                phone=normalized, metadata=json.dumps(metadata) if metadata else None
            )
        except Exception as e:
            logger.error("DB error: %s", e)
            sock.close()
            self.release_port(rtp_port)
            return None

        from_tag = f"tag{random.randint(100000, 999999)}"
        session = {
            "call_id": call_id, "direction": "outbound", "scenario": scenario,
            "state": "CREATED", "cleanup_state": CLEANUP_STATES["NOT_STARTED"],
            "phone": normalized, "rtp_port": rtp_port, "rtp_bound_sock": sock,
            "started_at": time.time(), "answered_at": None,
            "from_tag": from_tag,
            "invite_cseq": 1, "bye_cseq": 1,
            "via_branch": f"z9hG4bK{uuid.uuid4().hex}",
            "to_tag": None, "to_hdr": None, "from_hdr": None,
            "remote_ip": None, "remote_port": None, "remote_target": None,
            "remote_contact": None, "dialog_target": None,
            "negotiated_codec": None, "proto": None, "rtp_transport": None,
            "signaling_addr": None, "confirmed": False, "stopped": False,
            "voice_engine_task": None, "timeout_task": None, "keepalive_task": None,
            "bye_timeout_task": None, "media_watchdog_task": None,
            "max_duration_task": None, "cancel_timeout_task": None,
            "invite_auth_retries": 0,
            "metadata": metadata or {}, "auth_cache": None, "last_200": None,
            "ack_branch": None, "ack_message": None,
            "hangup_reason": None, "sip_code": None,
            "finish_called": False,
            "terminate_lock": asyncio.Lock(), "rtp_lock": asyncio.Lock(),
        }
        self.sessions[call_id] = session

        invite = self.build_invite(session)
        session["from_hdr"] = f"<sip:{self.user}@{self.host}>;tag={from_tag}"
        session["to_hdr"] = f"<sip:{normalized}@{self.host}>"

        logger.info("[CALL] %s originate to %s", call_id, normalized)
        
        # Transaction создаётся ДО отправки INVITE
        tx = SIPTransaction(call_id, session["invite_cseq"], "INVITE", session["via_branch"])
        self.transactions[tx.key()] = tx
        
        if not await self._send_sip(invite):
            del self.transactions[tx.key()]
            logger.error("[CALL] %s Failed to send INVITE", call_id)
            await self._cleanup_call(call_id, send_lead=False)
            return None

        session["state"] = "DIALING"
        await self.database.update_call_status(call_id, "DIALING")
        session["timeout_task"] = asyncio.create_task(self._outbound_timeout(call_id))

        asyncio.create_task(self._handle_invite_responses(call_id, session, tx))
        return call_id

    async def _handle_invite_responses(self, call_id: str, session: dict, tx: SIPTransaction):
        authenticated = False
        
        try:
            while True:
                resp = await self._wait_response(tx, timeout=30.0)
                
                if not resp:
                    session["hangup_reason"] = "invite_timeout"
                    if tx.key() in self.transactions:
                        del self.transactions[tx.key()]
                    await self._cleanup_call(call_id, send_lead=False)
                    return
                
                first, headers, body = resp
                status = get_status(first)
                
                if status in (100, 180, 183):
                    session["state"] = "RINGING"
                    logger.info("[CALL] %s %s", call_id, first.strip())
                    continue
                
                if status in (401, 407) and not authenticated:
                    auth_data, proxy = self.parse_auth_challenge(headers)
                    if not auth_data:
                        session["hangup_reason"] = "no_auth_data"
                        if tx.key() in self.transactions:
                            del self.transactions[tx.key()]
                        await self._cleanup_call(call_id, send_lead=False)
                        return
                    
                    session["invite_auth_retries"] += 1
                    if session["invite_auth_retries"] > 2:
                        session["hangup_reason"] = "auth_retry_exceeded"
                        if tx.key() in self.transactions:
                            del self.transactions[tx.key()]
                        await self._cleanup_call(call_id, send_lead=False)
                        return
                    
                    if tx.key() in self.transactions:
                        del self.transactions[tx.key()]
                    
                    session["invite_cseq"] += 1
                    session["via_branch"] = f"z9hG4bK{uuid.uuid4().hex}"
                    auth_header = self.compute_digest(
                        auth_data, "INVITE", f"sip:{session['phone']}@{self.host}", proxy
                    )
                    invite = self.build_invite(session, auth_header)
                    
                    tx = SIPTransaction(call_id, session["invite_cseq"], "INVITE", session["via_branch"])
                    self.transactions[tx.key()] = tx
                    
                    logger.info("[CALL] %s Sending authenticated INVITE cseq=%d",
                                call_id, session["invite_cseq"])
                    await self._send_sip(invite)
                    authenticated = True
                    continue
                
                if status == 200:
                    if tx.key() in self.transactions:
                        del self.transactions[tx.key()]
                    await self._handle_invite_200(session, headers, body)
                    return
                
                # 4xx/5xx/6xx
                if tx.key() in self.transactions:
                    del self.transactions[tx.key()]
                
                session["sip_code"] = str(status)
                session["hangup_reason"] = f"error_{status}"
                to_hdr = get_header(headers, "To")
                if to_hdr:
                    session["to_hdr"] = to_hdr
                    m = re.search(r';tag=([^\s;]+)', to_hdr, re.IGNORECASE)
                    if m:
                        session["to_tag"] = m.group(1)
                # ACK для non-2xx использует тот же branch что INVITE
                ack = self.build_ack(session, use_invite_branch=True)
                await self._send_sip(ack)
                logger.info("[CALL] %s Error %s, ACK sent", call_id, status)
                await self._cleanup_call(call_id, send_lead=False)
                return

        except Exception as e:
            logger.error("[CALL] %s Invite response error: %s", call_id, e)
            session["hangup_reason"] = "invite_error"
            if tx.key() in self.transactions:
                del self.transactions[tx.key()]
            await self._cleanup_call(call_id, send_lead=False)

    async def _handle_invite_200(self, session: dict, headers: str, body: str):
        call_id = session["call_id"]
        async with session["rtp_lock"]:
            if session["state"] in ("STOPPING", "ENDING", "ENDED", "BYE_SENT"):
                ack = self.build_ack(session)
                await self._send_sip(ack)
                await self._cleanup_call(call_id, send_lead=False)
                return

            to_hdr = get_header(headers, "To") or session.get("to_hdr", "")
            m = re.search(r';tag=([^\s;]+)', to_hdr, re.IGNORECASE)
            to_tag = m.group(1) if m else None
            if not to_tag:
                logger.error("[CALL] %s No To-tag in 200 OK", call_id)
                await self._cleanup_call(call_id, send_lead=False)
                return

            session["to_tag"] = to_tag
            session["to_hdr"] = to_hdr
            contact = get_header(headers, "Contact") or ""
            session["remote_contact"] = contact
            contact_addr = parse_contact_address(contact)
            session["dialog_target"] = contact_addr

            remote_ip, remote_port, codec = self.parse_sdp_remote_media(body)
            if not remote_ip or not remote_port:
                logger.error("[CALL] %s Invalid remote SDP", call_id)
                await self._cleanup_call(call_id, send_lead=False)
                return
            if codec not in (0, 8):
                logger.error("[CALL] %s Unsupported codec: %s", call_id, codec)
                await self._cleanup_call(call_id, send_lead=False)
                return

            session["remote_ip"] = remote_ip
            session["remote_port"] = remote_port
            session["negotiated_codec"] = codec
            logger.info("[CALL] %s Remote RTP: %s:%d, codec=%d",
                        call_id, remote_ip, remote_port, codec)

            # ACK для 2xx использует новый branch
            ack = self.build_ack(session, contact)
            await self._send_sip(ack)
            session["state"] = "ANSWERED"
            session["answered_at"] = time.time()
            logger.info("[CALL] %s 200 OK, ACK sent", call_id)

            await self._start_outbound_media(call_id, session)

    # ─── RTP media ──────────────────────────────────────────────────────

    async def _bind_rtp_socket(self, port: int) -> Optional[socket.socket]:
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            sock.bind(('0.0.0.0', port))
            return sock
        except OSError as e:
            sock.close()
            logger.error("RTP bind failed port=%d: %s", port, e)
            return None

    async def _start_outbound_media(self, call_id: str, session: dict):
        bound_sock = session.get("rtp_bound_sock")
        if not bound_sock:
            await self._cleanup_call(call_id, send_lead=False)
            return
        if session.get("negotiated_codec") not in (0, 8):
            await self._cleanup_call(call_id, send_lead=False)
            return

        loop = asyncio.get_running_loop()
        try:
            transport, protocol = await loop.create_datagram_endpoint(
                lambda: RTPProtocol(self, call_id, session["phone"],
                                    session["remote_ip"], session["remote_port"]),
                sock=bound_sock,
            )
        except Exception as e:
            logger.error("[CALL] %s RTP endpoint failed: %s", call_id, e)
            await self._cleanup_call(call_id, send_lead=False)
            return

        protocol.set_codec(session["negotiated_codec"])
        session["proto"] = protocol
        session["rtp_transport"] = transport
        session["rtp_bound_sock"] = None
        session["state"] = "IN_PROGRESS"
        await self.database.set_call_answered(call_id)

        session["keepalive_task"] = asyncio.create_task(self._keepalive_loop(session))
        session["voice_engine_task"] = asyncio.create_task(
            self.voice_engine.start(call_id, session)
        )
        session["media_watchdog_task"] = asyncio.create_task(
            self._media_idle_watchdog(call_id)
        )
        session["max_duration_task"] = asyncio.create_task(
            self._max_call_duration_watchdog(call_id)
        )
        logger.info("[RTP] %s RTP session started, codec=%d",
                    call_id, session["negotiated_codec"])

    async def _keepalive_loop(self, session: dict):
        while session.get("proto") and session["proto"].active and not session.get("stopped"):
            await asyncio.sleep(5)
            proto = session.get("proto")
            if proto and proto.active and not proto.speaking and not session.get("stopped"):
                proto.send_keepalive()

    async def _media_idle_watchdog(self, call_id: str):
        session = self.sessions.get(call_id)
        if not session:
            return
        while session.get("proto") and session["proto"].active and not session.get("stopped"):
            await asyncio.sleep(5)
            session = self.sessions.get(call_id)
            if not session:
                return
            proto = session.get("proto")
            if not proto:
                return
            idle = time.monotonic() - proto.last_packet_time
            if idle > MEDIA_IDLE_TIMEOUT:
                logger.warning("[CALL] %s MEDIA_IDLE_TIMEOUT (%.1fs)", call_id, idle)
                session["hangup_reason"] = "media_idle_timeout"
                await self._cleanup_call(call_id, send_lead=True)
                return

    async def _max_call_duration_watchdog(self, call_id: str):
        session = self.sessions.get(call_id)
        if not session:
            return
        await asyncio.sleep(MAX_CALL_DURATION)
        session = self.sessions.get(call_id)
        if not session:
            return
        if session["state"] in ("IN_PROGRESS", "ANSWERED"):
            logger.warning("[CALL] %s MAX_CALL_DURATION exceeded", call_id)
            session["hangup_reason"] = "max_call_duration"
            await self._cleanup_call(call_id, send_lead=True)

    # ─── Cancel / Terminate ─────────────────────────────────────────────

    async def _outbound_timeout(self, call_id: str):
        await asyncio.sleep(SIP_TIMER_B)
        session = self.sessions.get(call_id)
        if session and session["state"] in ("CREATED", "DIALING", "RINGING"):
            logger.warning("[CALL] %s Outbound timeout", call_id)
            session["hangup_reason"] = "timeout"
            await self.send_cancel(call_id)

    async def send_cancel(self, call_id: str):
        session = self.sessions.get(call_id)
        if not session or session["state"] in ("CANCEL_SENT", "STOPPING", "BYE_SENT", "ENDED"):
            return
        
        tx = SIPTransaction(call_id, session["invite_cseq"], "CANCEL", session["via_branch"])
        self.transactions[tx.key()] = tx
        
        cancel = self.build_cancel(session)
        if not await self._send_sip(cancel):
            del self.transactions[tx.key()]
            return
        
        session["state"] = "CANCEL_SENT"
        logger.info("[CALL] %s CANCEL sent", call_id)
        
        resp = await self._wait_response(tx, timeout=10.0)
        del self.transactions[tx.key()]
        
        if resp:
            first, _, _ = resp
            status = get_status(first)
            if status == 200:
                logger.info("[CALL] %s CANCEL 200 OK received", call_id)
        
        session["cancel_timeout_task"] = asyncio.create_task(self._cancel_timeout(call_id))

    async def _cancel_timeout(self, call_id: str):
        await asyncio.sleep(10)
        session = self.sessions.get(call_id)
        if session and session["state"] == "CANCEL_SENT":
            await self._cleanup_call(call_id, send_lead=False)

    async def terminate_call(self, call_id: str) -> bool:
        session = self.sessions.get(call_id)
        if not session or session.get("stopped"):
            return False
        async with session["terminate_lock"]:
            if session.get("stopped"):
                return False
            if session["state"] in ("CREATED", "DIALING", "RINGING", "CANCEL_SENT"):
                session["hangup_reason"] = "telegram_terminate"
                await self.send_cancel(call_id)
                return True
            elif session["state"] in ("ANSWERED", "IN_PROGRESS"):
                session["hangup_reason"] = "telegram_terminate"
                await self._start_bye(call_id, session)
                return True
        return False

    # ─── BYE ────────────────────────────────────────────────────────────

    async def send_bye(self, session: dict) -> bool:
        call_id = session["call_id"]
        addr = session.get("dialog_target")
        if not addr:
            logger.error("[CALL] %s Cannot send BYE: no target", call_id)
            return False
        
        tx = SIPTransaction(call_id, session["bye_cseq"], "BYE")
        self.transactions[tx.key()] = tx
        
        bye = self.build_bye(session)
        session["bye_cseq"] += 1
        session["state"] = "BYE_SENT"
        
        if not await self._send_sip(bye):
            del self.transactions[tx.key()]
            await self._cleanup_call(call_id, send_lead=True)
            return False
        
        logger.info("[CALL] %s BYE sent", call_id)
        
        resp = await self._wait_response(tx, timeout=10.0)
        del self.transactions[tx.key()]
        
        if resp:
            first, headers, _ = resp
            status = get_status(first)
            if status == 200:
                logger.info("[CALL] %s BYE 200 OK received", call_id)
                await self._cleanup_call(call_id, send_lead=True)
                return True
            elif status in (401, 407):
                auth_data, proxy = self.parse_auth_challenge(headers)
                if auth_data:
                    auth_header = self.compute_digest(
                        auth_data, "BYE", 
                        parse_contact_uri(session.get("remote_contact")) or f"sip:{session['phone']}@{self.host}",
                        proxy
                    )
                    tx2 = SIPTransaction(call_id, session["bye_cseq"], "BYE")
                    self.transactions[tx2.key()] = tx2
                    
                    bye2 = self.build_bye(session)
                    lines = bye2.split("\r\n")
                    insert_idx = next((i for i, line in enumerate(lines) if line.startswith("Content-Length")), 0)
                    lines.insert(insert_idx, auth_header)
                    bye2 = "\r\n".join(lines)
                    
                    if await self._send_sip(bye2):
                        resp2 = await self._wait_response(tx2, timeout=10.0)
                        del self.transactions[tx2.key()]
                        if resp2:
                            first2, _, _ = resp2
                            if get_status(first2) == 200:
                                await self._cleanup_call(call_id, send_lead=True)
                                return True
        
        await self._cleanup_call(call_id, send_lead=True)
        return True

    async def _start_bye(self, call_id: str, session: dict):
        session["state"] = "STOPPING"
        for key in ("voice_engine_task", "keepalive_task", "timeout_task",
                    "bye_timeout_task", "cancel_timeout_task",
                    "media_watchdog_task", "max_duration_task"):
            task = session.get(key)
            if task and not task.done():
                task.cancel()
        await self.send_bye(session)

    # ─── Cleanup ────────────────────────────────────────────────────────

    async def _cleanup_call(self, call_id: str, send_lead: bool = True):
        session = self.sessions.get(call_id)
        if not session:
            return
        if session.get("cleanup_state", 0) >= CLEANUP_STATES["STARTED"]:
            return
        current_task = asyncio.current_task()
        session["cleanup_state"] = CLEANUP_STATES["STARTED"]
        try:
            for key in ("voice_engine_task", "timeout_task", "keepalive_task",
                        "bye_timeout_task", "cancel_timeout_task",
                        "media_watchdog_task", "max_duration_task"):
                task = session.get(key)
                if task is not None and task is not current_task and not task.done():
                    task.cancel()
            if session.get("proto"):
                session["proto"].active = False
            if session.get("rtp_transport"):
                try:
                    session["rtp_transport"].close()
                except Exception:
                    pass
            if session.get("rtp_bound_sock"):
                try:
                    session["rtp_bound_sock"].close()
                except Exception:
                    pass
            self.release_port(session["rtp_port"])
            session["cleanup_state"] = CLEANUP_STATES["RESOURCES_CLOSED"]

            try:
                await self.database.set_call_ended(
                    call_id, session.get("hangup_reason", "unknown")
                )
            except Exception as e:
                logger.error("[CALL] %s DB update error: %s", call_id, e)

            if send_lead and not session.get("_lead_sent"):
                session["_lead_sent"] = True
                try:
                    if self.voice_engine:
                        await self.voice_engine.finish_call(call_id, session)
                except Exception as e:
                    logger.error("[CALL] %s finish_call error: %s", call_id, e)

            session["state"] = "ENDED"
            session["stopped"] = True
            session["cleanup_state"] = CLEANUP_STATES["COMPLETED"]
            self.sessions.pop(call_id, None)
            logger.info("[CLEANUP] %s completed", call_id)
        except asyncio.CancelledError:
            if session.get("rtp_bound_sock"):
                try:
                    session["rtp_bound_sock"].close()
                except Exception:
                    pass
            self.release_port(session["rtp_port"])
            session["cleanup_state"] = CLEANUP_STATES["COMPLETED"]
            self.sessions.pop(call_id, None)
            raise

    def get_active_calls(self) -> Dict[str, dict]:
        return {k: v for k, v in self.sessions.items() if not v.get("stopped")}

    # ─── Lifecycle ──────────────────────────────────────────────────────

    async def start(self):
        logger.info("[SIP] Starting SIP Worker (TCP) to %s:%d", self.host, self.port)
        if not SIP_CAN_START:
            raise SystemExit(2)
        self.is_running = True
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGTERM, signal.SIGINT):
            loop.add_signal_handler(sig, lambda: asyncio.create_task(self.graceful_stop()))

        try:
            self.reader, self.writer = await asyncio.open_connection(self.host, self.port)
            self.is_connected = True
            logger.info("[SIP] TCP connected to %s:%d", self.host, self.port)
        except Exception as e:
            logger.error("[SIP] TCP connection failed: %s", e)
            raise

        self._reader_task = asyncio.create_task(self._tcp_reader_loop())

        success = await self._do_register()
        if not success:
            logger.error("[SIP] Registration failed, exiting")
            raise SystemExit(1)

        self._refresh_task = asyncio.create_task(self.register_refresh_loop())
        self._heartbeat_task = asyncio.create_task(self._heartbeat_loop())
        logger.info("[SIP] Worker started")

    async def _heartbeat_loop(self):
        while self.is_running:
            try:
                with open(HEARTBEAT_FILE, "w") as f:
                    f.write(str(time.time()))
            except Exception:
                pass
            await asyncio.sleep(HEARTBEAT_INTERVAL)

    async def graceful_stop(self):
        if not self.is_running:
            return
        self.is_running = False
        for call_id in list(self.sessions.keys()):
            session = self.sessions.get(call_id)
            if session and session["direction"] == "outbound" and session["state"] in ("ANSWERED", "IN_PROGRESS"):
                await self._start_bye(call_id, session)
            else:
                await self._cleanup_call(call_id, send_lead=False)
        await asyncio.sleep(2)
        for task in (self._refresh_task, self._heartbeat_task, self._reader_task):
            if task:
                task.cancel()
        if self.writer:
            try:
                self.writer.close()
                await self.writer.wait_closed()
            except Exception:
                pass
