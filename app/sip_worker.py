"""
SIP Worker — телефония через Plusofon (SIP UDP + RTP UDP).
Финальная версия: исправлена race condition, добавлен handle_invite, усилено логирование, 
корректная обработка Digest Auth и входящих/исходящих SIP-методов.
"""

import asyncio
import json
import logging
import random
import re
import signal
import socket
import struct
import time
import hashlib
import uuid
import audioop
from dataclasses import dataclass
from typing import Optional, Dict, List

from config import (
    PLUSOFON_SIP_HOST, PLUSOFON_SIP_PORT, PLUSOFON_SIP_USER, PLUSOFON_SIP_PASSWORD,
    PUBLIC_IP, OUTBOUND_RURI_TEMPLATE, OUTBOUND_FROM_TEMPLATE,
    RTP_PORT_MIN, RTP_PORT_MAX, TRUSTED_SIP_IPS, BLACKLIST_SIP_IPS,
    HEARTBEAT_INTERVAL, HEARTBEAT_FILE, SIP_CAN_START,
    MAX_CONCURRENT_CALLS, MAX_CALL_DURATION, MEDIA_IDLE_TIMEOUT,
    MAX_RTP_BUFFER_BYTES, SIP_TIMER_B, SIP_RATE_LIMIT_PER_IP, DEVELOPMENT_MODE,
)

logger = logging.getLogger("sip_worker")

CLEANUP_STATES = {"NOT_STARTED": 0, "STARTED": 1, "RESOURCES_CLOSED": 2, "FINISH_CALLED": 3, "COMPLETED": 4}


def parse_contact_uri(contact: str) -> Optional[str]:
    if not contact: return None
    m = re.search(r'<([^>]+)>', contact)
    uri = m.group(1).strip() if m else contact.split(';')[0].split()[0].strip()
    return uri if uri.startswith("sip:") or uri.startswith("sips:") else None


def parse_digest_challenge(header: str) -> dict:
    data = {}
    for match in re.finditer(r'(\w+)=(?:"([^"]+)"|([^\s,]+))', header):
        data[match.group(1)] = match.group(2) if match.group(2) is not None else match.group(3)
    return data


def rtp_seq_lt(seq1: int, seq2: int) -> bool:
    return ((seq1 - seq2) & 0xFFFF) > 32768


@dataclass
class SIPTransaction:
    branch: str
    method: str
    call_id: str
    cseq: int
    message: bytes
    created_at: float
    completed: bool = False
    response_code: Optional[int] = None
    is_invite: bool = False


class RateLimiter:
    def __init__(self, max_per_minute: int):
        self.max_per_minute = max_per_minute
        self._requests: Dict[str, List[float]] = {}
    
    def allow(self, ip: str) -> bool:
        now = time.time()
        if ip not in self._requests: self._requests[ip] = []
        self._requests[ip] = [t for t in self._requests[ip] if now - t < 60]
        if len(self._requests[ip]) >= self.max_per_minute: return False
        self._requests[ip].append(now)
        return True


class RTPProtocol(asyncio.DatagramProtocol):
    def __init__(self, worker, call_id, phone, remote_ip, remote_port):
        self.worker = worker
        self.call_id = call_id
        self.phone = phone
        self.remote_ip = remote_ip
        self.remote_port = remote_port
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

    def set_codec(self, payload_type):
        if payload_type in (0, 8): self.negotiated_codec = payload_type

    def connection_made(self, transport): self.transport = transport
    def connection_lost(self, exc): self.active = False

    def datagram_received(self, data, addr):
        if not self.active or len(data) < 12: return
        self.last_packet_time = time.monotonic()
        version = (data[0] >> 6) & 0x3
        if version != 2: return
        payload_type = data[1] & 0x7F
        if payload_type not in (0, 8): return
        cc = data[0] & 0x0F
        x_bit = (data[0] >> 4) & 0x1
        p_bit = (data[0] >> 5) & 0x1
        seq = struct.unpack("!H", data[2:4])[0]
        offset = 12 + cc * 4
        if x_bit and len(data) >= offset + 4:
            ext_len_words = struct.unpack("!H", data[offset + 2:offset + 4])[0]
            offset += 4 + ext_len_words * 4
        if offset > len(data): return
        payload = data[offset:]
        if p_bit and len(payload) > 0:
            pad_len = payload[-1]
            if pad_len == 0 or pad_len > len(payload): return
            payload = payload[:-pad_len]
        if self._last_rx_seq is not None:
            if seq == self._last_rx_seq or rtp_seq_lt(seq, self._last_rx_seq): return
        self._last_rx_seq = seq
        try:
            pcm_frame = audioop.alaw2lin(payload, 2) if payload_type == 8 else audioop.ulaw2lin(payload, 2)
            self.pcm_buffer.extend(pcm_frame)
            if len(self.pcm_buffer) > MAX_RTP_BUFFER_BYTES:
                del self.pcm_buffer[:len(self.pcm_buffer) - MAX_RTP_BUFFER_BYTES]
        except Exception as e:
            logger.error("RTP decode error call_id=%s: %s", self.call_id, e)

    async def send_pcm(self, pcm_data):
        if not self.remote_target or not pcm_data: return
        self.speaking = True
        try:
            pt = 0x08 if self.negotiated_codec == 8 else 0x00
            encode_func = audioop.lin2alaw if self.negotiated_codec == 8 else audioop.lin2ulaw
            frame_bytes = 320
            start_time = time.monotonic()
            for i in range(0, len(pcm_data), frame_bytes):
                chunk = pcm_data[i:i + frame_bytes]
                if len(chunk) < frame_bytes: chunk = chunk + b'\x00' * (frame_bytes - len(chunk))
                encoded = encode_func(chunk, 2)
                marker = 0x80 if i == 0 else 0x00
                header = struct.pack("!BBHII", 0x80, marker | pt, self.sequence_number & 0xFFFF,
                                    self.timestamp & 0xFFFFFFFF, self.ssrc)
                self.sequence_number = (self.sequence_number + 1) & 0xFFFF
                self.timestamp = (self.timestamp + 160) & 0xFFFFFFFF
                try:
                    self.transport.sendto(header + encoded, self.remote_target)
                except Exception:
                    break
                delay = start_time + (i // frame_bytes + 1) * 0.02 - time.monotonic()
                if delay > 0: await asyncio.sleep(delay)
        finally:
            self.speaking = False

    def send_keepalive(self):
        if not self.transport or not self.remote_target or self.speaking or not self.active: return
        pt = 0x08 if self.negotiated_codec == 8 else 0x00
        payload = b'\xd5' * 160 if self.negotiated_codec == 8 else b'\xff' * 160
        header = struct.pack("!BBHII", 0x80, pt, self.sequence_number & 0xFFFF,
                            self.timestamp & 0xFFFFFFFF, self.ssrc)
        self.sequence_number = (self.sequence_number + 1) & 0xFFFF
        self.timestamp = (self.timestamp + 160) & 0xFFFFFFFF
        try:
            self.transport.sendto(header + payload, self.remote_target)
        except Exception:
            pass


class SIPProtocol(asyncio.DatagramProtocol):
    def __init__(self, worker):
        self.worker = worker
        self.transport = None

    def connection_made(self, transport):
        self.transport = transport
        # ИСПРАВЛЕНИЕ: Устанавливаем transport в worker ДО вызова _on_udp_connected, чтобы избежать race condition
        self.worker.sip_transport = transport
        logger.info("[SIP] UDP transport bound successfully. Initiating REGISTER...")
        self.worker._on_udp_connected()

    def connection_lost(self, exc):
        if exc: logger.warning("[SIP] UDP connection lost: %s", exc)
        self.worker._on_udp_disconnected()

    def datagram_received(self, data, addr):
        try:
            if not self.worker.rate_limiter.allow(addr[0]): return
            msg = data.decode('utf-8', errors='ignore')
            first_line = msg.split('\r\n', 1)[0]
            if first_line.startswith('SIP/2.0'):
                self.worker.handle_response(first_line, msg, addr)
                return
            method = first_line.split(' ', 1)[0].upper()
            if method == 'INVITE':
                asyncio.create_task(self.worker.handle_invite(msg, addr))
            elif method == 'ACK': self.worker.handle_ack(msg, addr)
            elif method == 'BYE': asyncio.create_task(self.worker.handle_bye(msg, addr))
            elif method == 'CANCEL': asyncio.create_task(self.worker.handle_cancel(msg, addr))
            elif method == 'OPTIONS': self.worker.handle_options(msg, addr)
        except Exception as e:
            logger.exception("SIP message processing error: %s", e)


class SIPWorker:
    def __init__(self, voice_engine, database, integrations):
        self.host = PLUSOFON_SIP_HOST
        self.port = PLUSOFON_SIP_PORT
        self.user = PLUSOFON_SIP_USER
        self.password = PLUSOFON_SIP_PASSWORD
        
        self.sip_transport = None
        self.is_running = False
        self.is_connected = False
        self.registered = False
        
        self.sessions: Dict[str, dict] = {}
        self.used_ports = set()
        self._last_allocated_port = RTP_PORT_MIN
        self._port_lock = asyncio.Lock()
        
        self.register_cseq = 1
        self.auth_cache = None
        self._register_challenge_pending = False
        
        self.voice_engine = voice_engine
        self.database = database
        self.integrations = integrations
        
        self._refresh_task = None
        self._heartbeat_task = None
        self.max_concurrent_calls = MAX_CONCURRENT_CALLS
        self.rate_limiter = RateLimiter(SIP_RATE_LIMIT_PER_IP)

    async def reserve_port(self):
        async with self._port_lock:
            for _ in range(RTP_PORT_MAX - RTP_PORT_MIN + 1):
                port = self._last_allocated_port
                self._last_allocated_port = RTP_PORT_MIN if self._last_allocated_port >= RTP_PORT_MAX else self._last_allocated_port + 1
                if port not in self.used_ports:
                    self.used_ports.add(port)
                    return port
            return None

    def release_port(self, port): self.used_ports.discard(port)

    def generate_sdp(self, rtp_port, codecs=None):
        codecs = codecs or [0, 8]
        lines = ["v=0", f"o=- {int(time.time())} 1 IN IP4 {PUBLIC_IP}", "s=-",
                f"c=IN IP4 {PUBLIC_IP}", "t=0 0",
                f"m=audio {rtp_port} RTP/AVP {' '.join(map(str, codecs))}"]
        for pt in codecs:
            lines.append(f"a=rtpmap:{pt} {'PCMA' if pt == 8 else 'PCMU'}/8000")
        lines.append("a=sendrecv")
        return "\r\n".join(lines) + "\r\n"

    def parse_sdp_remote_media(self, msg):
        session_ip = media_ip = port = None
        in_media = False
        supported_codecs = []
        for line in msg.splitlines():
            line = line.strip()
            if line.startswith("c=IN IP4"):
                parts = line.split()
                if len(parts) >= 3:
                    if in_media: media_ip = parts[2]
                    else: session_ip = parts[2]
            elif line.startswith("m=audio"):
                in_media = True
                parts = line.split()
                if len(parts) >= 3:
                    try:
                        port = int(parts[1])
                        if port == 0: port = None
                    except ValueError: port = None
                    for pt_str in parts[3:]:
                        try:
                            pt = int(pt_str)
                            if pt in (0, 8): supported_codecs.append(pt)
                        except ValueError: pass
            elif line.startswith("m=video") or line.startswith("m=application"):
                in_media = False
        if port is None or not supported_codecs: return None, None, None
        return (media_ip or session_ip), port, supported_codecs[0]

    def _extract_header(self, msg, header_name):
        match = re.search(rf'^{header_name}:\s*(.+)$', msg, re.MULTILINE | re.IGNORECASE)
        return match.group(1).strip() if match else None

    def _compute_digest(self, auth_data, method="REGISTER", uri=None, proxy=False):
        uri = uri or f"sip:{self.host}"
        realm = auth_data.get("realm", "")
        nonce = auth_data.get("nonce", "")
        qop_raw = auth_data.get("qop")
        qop = None
        if qop_raw:
            qop_list = [q.strip() for q in qop_raw.split(",")]
            qop = "auth" if "auth" in qop_list else (qop_list[0] if qop_list else None)

        ha1 = hashlib.md5(f"{self.user}:{realm}:{self.password}".encode()).hexdigest()
        ha2 = hashlib.md5(f"{method}:{uri}".encode()).hexdigest()
        header_name = "Proxy-Authorization" if proxy else "Authorization"

        if qop:
            auth_data["_nc_counter"] = auth_data.get("_nc_counter", 0) + 1
            nc = f"{auth_data['_nc_counter']:08x}"
            cnonce = f"{random.randint(0, 0xFFFFFFFF):08x}"
            response = hashlib.md5(f"{ha1}:{nonce}:{nc}:{cnonce}:{qop}:{ha2}".encode()).hexdigest()
            auth_header = f'{header_name}: Digest username="{self.user}", realm="{realm}", nonce="{nonce}", uri="{uri}", response="{response}", qop={qop}, nc={nc}, cnonce="{cnonce}"'
        else:
            response = hashlib.md5(f"{ha1}:{nonce}:{ha2}".encode()).hexdigest()
            auth_header = f'{header_name}: Digest username="{self.user}", realm="{realm}", nonce="{nonce}", uri="{uri}", response="{response}"'

        if "opaque" in auth_data: auth_header += f', opaque="{auth_data["opaque"]}"'
        return auth_header

    def _parse_auth_challenge(self, msg):
        m = re.search(r'Proxy-Authenticate:\s*Digest\s+(.*)', msg, re.I)
        if m: return parse_digest_challenge(m.group(1).strip()), True
        m = re.search(r'WWW-Authenticate:\s*Digest\s+(.*)', msg, re.I)
        if m: return parse_digest_challenge(m.group(1).strip()), False
        return None, False

    def _build_register_message(self, auth_header=None):
        branch = f"z9hG4bK{random.randint(100000,999999)}"
        headers = (
            f"REGISTER sip:{self.host} SIP/2.0\r\n"
            f"Via: SIP/2.0/UDP {PUBLIC_IP}:{self.port};branch={branch}\r\n"
            f"From: <sip:{self.user}@{self.host}>;tag=reg{random.randint(100000,999999)}\r\n"
            f"To: <sip:{self.user}@{self.host}>\r\n"
            f"Call-ID: {self.user}@{PUBLIC_IP}\r\n"
            f"CSeq: {self.register_cseq} REGISTER\r\n"
            f"Contact: <sip:{self.user}@{PUBLIC_IP}:{self.port}>\r\n"
            f"Max-Forwards: 70\r\nExpires: 300\r\n"
        )
        if auth_header: headers += f"{auth_header}\r\n"
        return headers + "Content-Length: 0\r\n\r\n"

    async def _send_sip_message(self, sip_msg: str, target_addr: tuple = None) -> bool:
        if not self.sip_transport:
            logger.error("[SIP] Cannot send: UDP transport is not available")
            return False
        try:
            addr = target_addr if target_addr else (self.host, self.port)
            self.sip_transport.sendto(sip_msg.encode('utf-8'), addr)
            return True
        except Exception as e:
            logger.exception("[SIP] Send error: %s", e)
            return False

    def _build_outbound_ruri(self, phone):
        return OUTBOUND_RURI_TEMPLATE.format(phone=phone, host=self.host)

    def _build_outbound_from(self):
        return OUTBOUND_FROM_TEMPLATE.format(user=self.user, host=self.host)

    def _build_outbound_invite(self, session):
        ruri = self._build_outbound_ruri(session["phone"])
        from_hdr = f"{self._build_outbound_from()};tag={session['from_tag']}"
        to_hdr = f"<sip:{session['phone']}@{self.host}>"
        branch = session["via_branch"]
        sdp_body = self.generate_sdp(session["rtp_port"], codecs=[0, 8])
        sdp_bytes = sdp_body.encode('utf-8')
        invite = (
            f"INVITE {ruri} SIP/2.0\r\n"
            f"Via: SIP/2.0/UDP {PUBLIC_IP}:{self.port};branch={branch}\r\n"
            f"From: {from_hdr}\r\nTo: {to_hdr}\r\n"
            f"Call-ID: {session['call_id']}\r\n"
            f"CSeq: {session['invite_cseq']} INVITE\r\n"
            f"Contact: <sip:{self.user}@{PUBLIC_IP}:{self.port}>\r\n"
            f"Max-Forwards: 70\r\nContent-Type: application/sdp\r\n"
            f"Content-Length: {len(sdp_bytes)}\r\n\r\n{sdp_body}"
        )
        session["from_hdr"] = from_hdr
        session["to_hdr"] = to_hdr
        return invite

    def _build_ack(self, session, contact=None, is_2xx=True):
        branch = f"z9hG4bK{random.randint(100000,999999)}" if is_2xx else session["via_branch"]
        ruri = parse_contact_uri(contact) if contact and is_2xx else self._build_outbound_ruri(session["phone"])
        if not ruri: ruri = self._build_outbound_ruri(session["phone"])
        to_hdr = session["to_hdr"]
        if session["to_tag"] and ";tag=" not in to_hdr.lower():
            to_hdr = f"{to_hdr};tag={session['to_tag']}"
        return (
            f"ACK {ruri} SIP/2.0\r\n"
            f"Via: SIP/2.0/UDP {PUBLIC_IP}:{self.port};branch={branch}\r\n"
            f"From: {session['from_hdr']}\r\nTo: {to_hdr}\r\n"
            f"Call-ID: {session['call_id']}\r\n"
            f"CSeq: {session['invite_cseq']} ACK\r\n"
            f"Max-Forwards: 70\r\nContent-Length: 0\r\n\r\n"
        ), branch

    async def _bind_rtp_socket(self, port):
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            sock.bind(('0.0.0.0', port))
        except OSError as e:
            sock.close()
            logger.error("RTP bind failed port=%d: %s", port, e)
            return None
        sock.setblocking(False)
        return sock

    # --- УПРАВЛЕНИЕ UDP И REGISTER ---

    def _on_udp_connected(self):
        self.is_connected = True
        self.registered = False
        self._register_challenge_pending = False
        logger.info("[SIP] UDP connected successfully. Triggering REGISTER task...")
        asyncio.create_task(self.send_register())

    def _on_udp_disconnected(self):
        self.is_connected = False
        self.registered = False
        self._register_challenge_pending = False
        logger.warning("[SIP] UDP disconnected.")

    async def send_register(self):
        logger.info("[SIP] send_register() executed. is_connected=%s", self.is_connected)
        if not self.is_connected:
            logger.warning("[SIP] Cannot send REGISTER: not connected")
            return
        self._register_challenge_pending = False
        auth_header = self._compute_digest(self.auth_cache, proxy=self.auth_cache.get("_proxy", False)) if self.auth_cache else ""
        msg = self._build_register_message(auth_header)
        logger.info("[SIP] Sending REGISTER cseq=%d", self.register_cseq)
        if await self._send_sip_message(msg):
            self.register_cseq += 1
            logger.info("[SIP] REGISTER packet dispatched to network.")

    async def _register_refresh_loop(self):
        while self.is_running:
            await asyncio.sleep(240) 
            if self.is_connected and self.registered:
                logger.info("[SIP] Refreshing REGISTER...")
                await self.send_register()

    async def handle_register_challenge(self, msg):
        if self._register_challenge_pending:
            logger.warning("[SIP] Register challenge already pending, skipping duplicate 401")
            return
        self._register_challenge_pending = True
        
        auth_data, proxy = self._parse_auth_challenge(msg)
        if not auth_data:
            logger.error("[SIP] REGISTER failed: No auth data in 401/407")
            self._register_challenge_pending = False
            return
        logger.info("[SIP] Received 401. Sending authenticated REGISTER...")
        self.auth_cache = auth_data
        self.auth_cache["_proxy"] = proxy
        auth_header = self._compute_digest(auth_data, proxy=proxy)
        msg = self._build_register_message(auth_header)
        logger.info("[SIP] Sending authenticated REGISTER cseq=%d", self.register_cseq)
        if await self._send_sip_message(msg):
            self.register_cseq += 1
            self._register_challenge_pending = False

    # --- ОБРАБОТКА ВХОДЯЩИХ ВЫЗОВОВ ---

    async def handle_invite(self, msg, addr):
        """Обработка входящего INVITE от Plusofon (отклоняем, так как работаем только на исходящие)."""
        logger.info("[SIP] Received incoming INVITE from %s", addr)
        call_id = self._extract_header(msg, "Call-ID") or ""
        cseq = self._extract_header(msg, "CSeq") or ""
        from_hdr = self._extract_header(msg, "From") or ""
        to_hdr = self._extract_header(msg, "To") or ""
        via_hdr = self._extract_header(msg, "Via") or ""
        
        trying = f"SIP/2.0 100 Trying\r\nVia: {via_hdr}\r\nFrom: {from_hdr}\r\nTo: {to_hdr}\r\nCall-ID: {call_id}\r\nCSeq: {cseq}\r\nContent-Length: 0\r\n\r\n"
        await self._send_sip_message(trying, addr)
        
        busy = f"SIP/2.0 486 Busy Here\r\nVia: {via_hdr}\r\nFrom: {from_hdr}\r\nTo: {to_hdr}\r\nCall-ID: {call_id}\r\nCSeq: {cseq}\r\nContent-Length: 0\r\n\r\n"
        await self._send_sip_message(busy, addr)
        logger.info("[SIP] Sent 486 Busy Here for incoming INVITE")

    # --- ОБРАБОТКА ОТВЕТОВ ---

    def handle_response(self, first_line, msg, addr):
        parts = first_line.split()
        code = parts[1] if len(parts) > 1 else ""
        cseq = self._extract_header(msg, "CSeq") or ""
        logger.info("[SIP] Received response: %s", first_line.strip())
        
        if code in ("401", "407"):
            logger.info("[SIP] Received %s", code)
            if "REGISTER" in cseq: asyncio.create_task(self.handle_register_challenge(msg))
            elif "INVITE" in cseq:
                call_id = self._extract_header(msg, "Call-ID") or ""
                asyncio.create_task(self.handle_invite_challenge(msg, call_id, addr))
        elif code == "200":
            logger.info("[SIP] Received 200 OK")
            if "REGISTER" in cseq:
                self.registered = True
                self._register_challenge_pending = False
                logger.info("[SIP] REGISTERED")
            elif "INVITE" in cseq:
                call_id = self._extract_header(msg, "Call-ID") or ""
                asyncio.create_task(self.handle_invite_200(msg, call_id, addr))
        elif code in ("100", "180", "183"):
            logger.info("[SIP] Received %s", code)
            call_id = self._extract_header(msg, "Call-ID") or ""
            session = self.sessions.get(call_id)
            if session and session["direction"] == "outbound" and code != "100":
                session["state"] = "RINGING"
        elif code == "487":
            call_id = self._extract_header(msg, "Call-ID") or ""
            asyncio.create_task(self.handle_invite_487(msg, call_id, addr))
        elif code and code[0] in ("4", "5", "6"):
            logger.info("[SIP] Received error %s", code)
            if "INVITE" in cseq:
                call_id = self._extract_header(msg, "Call-ID") or ""
                asyncio.create_task(self.handle_invite_error(msg, call_id, code, addr))

    # --- ИСХОДЯЩИЙ ВЫЗОВ ---

    async def originate_call(self, phone, scenario="BEFORE_LESSON", metadata=None):
        if not self.is_connected:
            logger.error("[SIP] Cannot call: UDP not connected")
            return None
        if not self.registered:
            logger.error("[SIP] Cannot call: SIP account NOT REGISTERED")
            return None

        if scenario not in ("BEFORE_LESSON", "AFTER_LESSON"): return None
        if len(self.get_active_calls()) >= self.max_concurrent_calls: return None
        
        rtp_port = await self.reserve_port()
        if rtp_port is None: return None

        sock = await self._bind_rtp_socket(rtp_port)
        if sock is None:
            self.release_port(rtp_port)
            return None

        call_id = f"{int(time.time() * 1000)}{str(uuid.uuid4())[:8]}@{PUBLIC_IP}"
        
        try:
            await self.database.create_call(call_id=call_id, direction="outbound", scenario=scenario, phone=phone, metadata=json.dumps(metadata) if metadata else None)
        except Exception as e:
            logger.error("DB error: %s", e)
            sock.close()
            self.release_port(rtp_port)
            return None
        
        session = {
            "call_id": call_id, "direction": "outbound", "scenario": scenario,
            "state": "CREATED", "cleanup_state": CLEANUP_STATES["NOT_STARTED"],
            "phone": phone, "rtp_port": rtp_port, "rtp_bound_sock": sock,
            "started_at": time.time(), "answered_at": None,
            "from_tag": f"tag{random.randint(100000, 999999)}",
            "invite_cseq": 1, "bye_cseq": 1, "via_branch": f"z9hG4bK{random.randint(100000,999999)}",
            "to_tag": None, "from_hdr": None, "to_hdr": None, "remote_ip": None,
            "remote_port": None, "remote_target": None, "remote_contact": None,
            "negotiated_codec": 8, "proto": None, "rtp_transport": None,
            "signaling_addr": None, "confirmed": False, "stopped": False,
            "voice_engine_task": None, "timeout_task": None, "keepalive_task": None,
            "bye_timeout_task": None, "media_watchdog_task": None, "max_duration_task": None,
            "metadata": metadata or {}, "auth_cache": None, "last_200": None, 
            "ack_branch": None, "ack_message": None, "hangup_reason": None, "sip_code": None, 
            "finish_called": False, "terminate_lock": asyncio.Lock(), "rtp_lock": asyncio.Lock(),
        }
        self.sessions[call_id] = session

        invite = self._build_outbound_invite(session)
        logger.info("[SIP] Sending INVITE to %s", phone)
        
        if not await self._send_sip_message(invite):
            logger.error("[SIP] Failed to send INVITE for %s", phone)
            await self._cleanup_call(call_id, send_lead=False)
            return None
        
        session["state"] = "DIALING"
        await self.database.update_call_status(call_id, "DIALING")
        session["timeout_task"] = asyncio.create_task(self._outbound_timeout(call_id))
        return call_id

    async def handle_invite_challenge(self, msg, call_id, addr):
        session = self.sessions.get(call_id)
        if not session or session["direction"] != "outbound": return

        auth_data, proxy = self._parse_auth_challenge(msg)
        if not auth_data: return

        session["invite_cseq"] += 1
        session["via_branch"] = f"z9hG4bK{random.randint(100000,999999)}"
        session["auth_cache"] = auth_data
        session["auth_cache"]["_proxy"] = proxy

        auth_header = self._compute_digest(auth_data, method="INVITE", uri=self._build_outbound_ruri(session["phone"]), proxy=proxy)
        invite = self._build_outbound_invite(session)
        lines = invite.split("\r\n")
        insert_idx = next((i for i, l in enumerate(lines) if l.startswith("Content-Length")), 0)
        lines.insert(insert_idx, auth_header)
        
        logger.info("[SIP] Sending INVITE with Authorization cseq=%d", session["invite_cseq"])
        await self._send_sip_message("\r\n".join(lines), addr)

    async def handle_invite_200(self, msg, call_id, addr):
        session = self.sessions.get(call_id)
        if not session or session["direction"] != "outbound": return

        async with session.get("rtp_lock"):
            if session["state"] in ("STOPPING", "ENDING", "ENDED", "BYE_SENT"):
                ack, _ = self._build_ack(session)
                await self._send_sip_message(ack, addr)
                await self._cleanup_call(call_id, send_lead=False)
                return

            to_hdr = self._extract_header(msg, "To") or session["to_hdr"]
            m = re.search(r';tag=([^;\s]+)', to_hdr, re.IGNORECASE)
            to_tag = m.group(1) if m else None
            if not to_tag:
                await self._cleanup_call(call_id, send_lead=False)
                return

            session["to_tag"] = to_tag
            session["to_hdr"] = to_hdr
            contact = self._extract_header(msg, "Contact") or ""
            session["remote_contact"] = contact

            remote_ip, remote_port, codec = self.parse_sdp_remote_media(msg)
            if not remote_ip or not remote_port or codec not in (0, 8):
                await self._cleanup_call(call_id, send_lead=False)
                return

            session["remote_ip"] = remote_ip
            session["remote_port"] = remote_port
            session["negotiated_codec"] = codec

            ack, _ = self._build_ack(session, contact, is_2xx=True)
            logger.info("[SIP] Sending ACK")
            await self._send_sip_message(ack, addr)

            session["state"] = "ANSWERED"
            session["answered_at"] = time.time()
            await self._start_outbound_media(call_id, session)

    async def _start_outbound_media(self, call_id, session):
        bound_sock = session.get("rtp_bound_sock")
        if not bound_sock:
            await self._cleanup_call(call_id, send_lead=False)
            return

        loop = asyncio.get_running_loop()
        try:
            transport, protocol = await loop.create_datagram_endpoint(
                lambda: RTPProtocol(self, call_id, session["phone"], session["remote_ip"], session["remote_port"]),
                sock=bound_sock,
            )
        except Exception as e:
            logger.error("RTP endpoint creation failed: %s", e)
            await self._cleanup_call(call_id, send_lead=False)
            return

        protocol.set_codec(session["negotiated_codec"])
        session["proto"] = protocol
        session["rtp_transport"] = transport
        session["rtp_bound_sock"] = None
        session["state"] = "IN_PROGRESS"
        
        await self.database.set_call_answered(call_id)
        session["keepalive_task"] = asyncio.create_task(self._keepalive_loop(session))
        session["voice_engine_task"] = asyncio.create_task(self.voice_engine.start(call_id, session))
        session["media_watchdog_task"] = asyncio.create_task(self._media_idle_watchdog(call_id))
        session["max_duration_task"] = asyncio.create_task(self._max_call_duration_watchdog(call_id))

    async def _keepalive_loop(self, session):
        while session.get("proto") and session["proto"].active and not session.get("stopped"):
            await asyncio.sleep(5)
            proto = session.get("proto")
            if proto and proto.active and not proto.speaking and not session.get("stopped"):
                proto.send_keepalive()

    async def _media_idle_watchdog(self, call_id: str):
        session = self.sessions.get(call_id)
        if not session: return
        while session.get("proto") and session["proto"].active and not session.get("stopped"):
            await asyncio.sleep(5)
            session = self.sessions.get(call_id)
            if not session: return
            proto = session.get("proto")
            if not proto: return
            idle_time = time.monotonic() - proto.last_packet_time
            if idle_time > MEDIA_IDLE_TIMEOUT:
                logger.warning("MEDIA_IDLE_TIMEOUT exceeded call_id=%s (%.1fs)", call_id, idle_time)
                session["hangup_reason"] = "media_idle_timeout"
                await self._cleanup_call(call_id, send_lead=True)
                return

    async def _max_call_duration_watchdog(self, call_id: str):
        session = self.sessions.get(call_id)
        if not session: return
        await asyncio.sleep(MAX_CALL_DURATION)
        session = self.sessions.get(call_id)
        if not session: return
        if session["state"] in ("IN_PROGRESS", "ANSWERED"):
            logger.warning("MAX_CALL_DURATION exceeded call_id=%s (%ds)", call_id, MAX_CALL_DURATION)
            session["hangup_reason"] = "max_call_duration"
            await self._cleanup_call(call_id, send_lead=True)

    async def _outbound_timeout(self, call_id):
        await asyncio.sleep(SIP_TIMER_B)
        session = self.sessions.get(call_id)
        if session and session["state"] in ("CREATED", "DIALING", "RINGING"):
            session["hangup_reason"] = "timeout"
            await self.send_cancel(call_id)

    async def send_cancel(self, call_id):
        session = self.sessions.get(call_id)
        if not session or session["state"] in ("CANCEL_SENT", "STOPPING", "BYE_SENT", "ENDED"): return

        ruri = self._build_outbound_ruri(session["phone"])
        cancel = (
            f"CANCEL {ruri} SIP/2.0\r\n"
            f"Via: SIP/2.0/UDP {PUBLIC_IP}:{self.port};branch={session['via_branch']}\r\n"
            f"From: {session['from_hdr']}\r\nTo: {session['to_hdr']}\r\n"
            f"Call-ID: {call_id}\r\nCSeq: {session['invite_cseq']} CANCEL\r\n"
            f"Max-Forwards: 70\r\nContent-Length: 0\r\n\r\n"
        )
        await self._send_sip_message(cancel, session.get("signaling_addr"))
        session["state"] = "CANCEL_SENT"
        asyncio.create_task(self._cancel_timeout(call_id))

    async def _cancel_timeout(self, call_id):
        await asyncio.sleep(10)
        session = self.sessions.get(call_id)
        if session and session["state"] == "CANCEL_SENT":
            await self._cleanup_call(call_id, send_lead=False)

    async def handle_invite_487(self, msg, call_id, addr):
        session = self.sessions.get(call_id)
        if not session: return
        ack, _ = self._build_ack(session, is_2xx=False)
        await self._send_sip_message(ack, addr)
        await self._cleanup_call(call_id, send_lead=False)

    async def handle_invite_error(self, msg, call_id, code, addr=None):
        session = self.sessions.get(call_id)
        if not session: return
        session["sip_code"] = code
        session["hangup_reason"] = f"error_{code}"
        ack, _ = self._build_ack(session, is_2xx=False)
        await self._send_sip_message(ack, addr)
        await self._cleanup_call(call_id, send_lead=False)

    def handle_ack(self, msg, addr): pass 

    async def handle_bye(self, msg, addr):
        call_id = self._extract_header(msg, "Call-ID") or ""
        session = self.sessions.get(call_id)
        if not session: return
        resp = f"SIP/2.0 200 OK\r\nVia: {self._extract_header(msg, 'Via') or ''}\r\nFrom: {self._extract_header(msg, 'From') or ''}\r\nTo: {self._extract_header(msg, 'To') or ''}\r\nCall-ID: {call_id}\r\nCSeq: {self._extract_header(msg, 'CSeq') or ''}\r\nContent-Length: 0\r\n\r\n"
        await self._send_sip_message(resp, addr)
        session["hangup_reason"] = "remote_hangup"
        await self._cleanup_call(call_id, send_lead=True)

    async def handle_cancel(self, msg, addr):
        call_id = self._extract_header(msg, "Call-ID") or ""
        session = self.sessions.get(call_id)
        if not session or session["state"] not in ("CREATED", "DIALING", "RINGING"): return
        ok = f"SIP/2.0 200 OK\r\nVia: {self._extract_header(msg, 'Via') or ''}\r\nFrom: {self._extract_header(msg, 'From') or ''}\r\nTo: {self._extract_header(msg, 'To') or ''}\r\nCall-ID: {call_id}\r\nCSeq: {self._extract_header(msg, 'CSeq') or ''}\r\nContent-Length: 0\r\n\r\n"
        await self._send_sip_message(ok, addr)
        req_term = f"SIP/2.0 487 Request Terminated\r\nVia: {self._extract_header(msg, 'Via') or ''}\r\nFrom: {self._extract_header(msg, 'From') or ''}\r\nTo: {session['to_hdr']}\r\nCall-ID: {call_id}\r\nCSeq: {session['invite_cseq']}\r\nContent-Length: 0\r\n\r\n"
        await self._send_sip_message(req_term, addr)
        session["hangup_reason"] = "remote_cancel"
        await self._cleanup_call(call_id, send_lead=False)

    def handle_options(self, msg, addr):
        call_id = self._extract_header(msg, "Call-ID") or "0"
        cseq = self._extract_header(msg, "CSeq") or "1 OPTIONS"
        from_hdr = self._extract_header(msg, "From") or ""
        to_hdr = self._extract_header(msg, "To") or ""
        if ";tag=" not in to_hdr.lower(): to_hdr = f"{to_hdr};tag={random.randint(1000,9999)}"
        response = f"SIP/2.0 200 OK\r\nVia: {self._extract_header(msg, 'Via') or ''}\r\nFrom: {from_hdr}\r\nTo: {to_hdr}\r\nCall-ID: {call_id}\r\nCSeq: {cseq}\r\nContact: <sip:{self.user}@{PUBLIC_IP}:{self.port}>\r\nAllow: INVITE, ACK, BYE, CANCEL, OPTIONS\r\nContent-Length: 0\r\n\r\n"
        asyncio.create_task(self._send_sip_message(response, addr))

    async def send_bye(self, session):
        uri = parse_contact_uri(session.get("remote_contact")) or self._build_outbound_ruri(session["phone"])
        new_branch = f"z9hG4bK{random.randint(100000,999999)}"
        from_hdr = session["from_hdr"] if session["direction"] == "outbound" else session["to_hdr"]
        to_hdr = session["to_hdr"] if session["direction"] == "outbound" else session["from_hdr"]
        if session["direction"] == "outbound" and session["to_tag"] and ";tag=" not in to_hdr.lower():
            to_hdr = f"{to_hdr};tag={session['to_tag']}"

        bye = (
            f"BYE {uri} SIP/2.0\r\nVia: SIP/2.0/UDP {PUBLIC_IP}:{self.port};branch={new_branch}\r\n"
            f"From: {from_hdr}\r\nTo: {to_hdr}\r\nCall-ID: {session['call_id']}\r\n"
            f"CSeq: {session['bye_cseq']} BYE\r\nMax-Forwards: 70\r\nContent-Length: 0\r\n\r\n"
        )
        session["bye_cseq"] += 1
        session["state"] = "BYE_SENT"
        await self._send_sip_message(bye, session.get("signaling_addr"))
        session["bye_timeout_task"] = asyncio.create_task(self._bye_timeout(session["call_id"]))

    async def _bye_timeout(self, call_id):
        await asyncio.sleep(5)
        session = self.sessions.get(call_id)
        if session and session["state"] == "BYE_SENT":
            await self._cleanup_call(call_id, send_lead=True)

    async def terminate_call(self, call_id):
        session = self.sessions.get(call_id)
        if not session or session.get("stopped"): return False
        async with session.get("terminate_lock"):
            if session.get("stopped"): return False
            if session["state"] in ("CREATED", "DIALING", "RINGING", "CANCEL_SENT"):
                session["hangup_reason"] = "telegram_terminate"
                await self.send_cancel(call_id)
                return True
            elif session["state"] in ("ANSWERED", "IN_PROGRESS"):
                session["hangup_reason"] = "telegram_terminate"
                await self._start_bye(call_id, session)
                return True
        return False

    async def _start_bye(self, call_id, session):
        session["state"] = "STOPPING"
        session["stopped"] = True
        for key in ("voice_engine_task", "keepalive_task", "timeout_task", "media_watchdog_task", "max_duration_task"):
            if session.get(key): session[key].cancel()
        await self.send_bye(session)

    async def _cleanup_call(self, call_id, send_lead=True):
        session = self.sessions.get(call_id)
        if not session or session.get("cleanup_state", 0) >= CLEANUP_STATES["STARTED"]: return
        session["cleanup_state"] = CLEANUP_STATES["STARTED"]

        for key in ("voice_engine_task", "timeout_task", "keepalive_task", "bye_timeout_task", "cancel_timeout_task", "media_watchdog_task", "max_duration_task"):
            if session.get(key): session[key].cancel()

        if session.get("proto"): session["proto"].active = False
        if session.get("rtp_transport"): 
            try: session["rtp_transport"].close()
            except: pass
        if session.get("rtp_bound_sock"):
            try: session["rtp_bound_sock"].close()
            except: pass
        
        self.release_port(session["rtp_port"])
        session["cleanup_state"] = CLEANUP_STATES["RESOURCES_CLOSED"]

        try:
            await self.database.set_call_ended(call_id, session.get("hangup_reason", "unknown"))
        except Exception as e:
            logger.error("DB update error: %s", e)

        if send_lead and not session.get("_lead_sent"):
            session["_lead_sent"] = True
            try:
                if self.voice_engine: await self.voice_engine.finish_call(call_id, session)
            except Exception as e:
                logger.error("finish_call error: %s", e)

        session["state"] = "ENDED"
        session["stopped"] = True
        session["cleanup_state"] = CLEANUP_STATES["COMPLETED"]
        self.sessions.pop(call_id, None)

    def get_active_calls(self):
        return {k: v for k, v in self.sessions.items() if not v.get("stopped")}

    async def start(self):
        logger.info("[SIP] Starting SIP Worker (UDP) to %s:%d", self.host, self.port)
        if not SIP_CAN_START:
            raise SystemExit(2)
        
        self.is_running = True
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGTERM, signal.SIGINT):
            loop.add_signal_handler(sig, lambda: asyncio.create_task(self.graceful_stop()))
        
        try:
            logger.info("[SIP] Attempting to bind UDP socket to 0.0.0.0:%d", self.port)
            transport, protocol = await loop.create_datagram_endpoint(
                lambda: SIPProtocol(self),
                local_addr=('0.0.0.0', self.port)
            )
            self.sip_transport = transport
            logger.info("[SIP] UDP socket bound successfully. Waiting for connection_made callback...")
        except Exception as e:
            logger.error("[SIP] CRITICAL: Failed to bind UDP transport: %s", e)
            raise
        
        self._refresh_task = asyncio.create_task(self._register_refresh_loop())
        self._heartbeat_task = asyncio.create_task(self._heartbeat_loop())
        logger.info("[SIP] Worker started.")

    async def _heartbeat_loop(self):
        while self.is_running:
            try:
                with open(HEARTBEAT_FILE, "w") as f: f.write(str(time.time()))
            except Exception: pass
            await asyncio.sleep(HEARTBEAT_INTERVAL)

    async def graceful_stop(self):
        if not self.is_running: return
        self.is_running = False
        for call_id in list(self.sessions.keys()):
            session = self.sessions.get(call_id)
            if session and session["direction"] == "outbound" and session["state"] in ("ANSWERED", "IN_PROGRESS"):
                await self._start_bye(call_id, session)
            else:
                await self._cleanup_call(call_id, send_lead=False)
        await asyncio.sleep(2)
        for task in (self._refresh_task, self._heartbeat_task):
            if task: task.cancel()
        if self.sip_transport: self.sip_transport.close()
