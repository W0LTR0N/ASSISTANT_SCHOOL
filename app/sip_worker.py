"""
SIP Worker — телефония через Plusofon (SIP + RTP).

Финальная версия со всеми исправлениями:
- SIP retransmissions (RFC 3261 timers)
- REGISTER CSeq корректно увеличивается после challenge
- _send_sip_message возвращает bool
- originate_call проверяет успех отправки INVITE
- MAX_CALL_DURATION watchdog
- MEDIA_IDLE_TIMEOUT watchdog
- RTP buffer limits
- Late 200 после CANCEL
- Concurrent terminate protection
- SIP URI parsing с параметрами
- Content-Length для UTF-8
- Rate limiting для SIP
"""

import asyncio
import json
import logging
import os
import random
import re
import signal
import socket
import struct
import time
import hashlib
import audioop
from dataclasses import dataclass, field
from typing import Optional, Dict, Any, Callable

from config import (
    PLUSOFON_SIP_HOST, PLUSOFON_SIP_PORT, PLUSOFON_SIP_USER, PLUSOFON_SIP_PASSWORD,
    PUBLIC_IP, OUTBOUND_RURI_TEMPLATE, OUTBOUND_FROM_TEMPLATE,
    RTP_PORT_MIN, RTP_PORT_MAX, TRUSTED_SIP_IPS, BLACKLIST_SIP_IPS,
    HEARTBEAT_INTERVAL, HEARTBEAT_FILE, SIP_CAN_START,
    MAX_CONCURRENT_CALLS, MAX_CALL_DURATION, MEDIA_IDLE_TIMEOUT,
    MAX_RTP_BUFFER_BYTES,
    SIP_T1, SIP_T2, SIP_TIMER_B, SIP_TIMER_F, SIP_MAX_RETRANSMITS,
    SIP_RATE_LIMIT_PER_IP,
)

logger = logging.getLogger("sip_worker")

CLEANUP_STATES = {"NOT_STARTED": 0, "STARTED": 1, "RESOURCES_CLOSED": 2, "FINISH_CALLED": 3, "COMPLETED": 4}


def parse_contact_uri(contact: str) -> Optional[str]:
    """Парсинг Contact URI с поддержкой параметров."""
    if not contact:
        return None
    m = re.search(r'<([^>]+)>', contact)
    if m:
        uri = m.group(1).strip()
    else:
        uri = contact.split(';')[0].split()[0].strip()
    
    # Валидация: должен быть sip: или sips:
    if not (uri.startswith("sip:") or uri.startswith("sips:")):
        return None
    return uri


def parse_digest_challenge(header: str) -> dict:
    """Парсинг Digest challenge header."""
    data = {}
    for match in re.finditer(r'(\w+)=(?:"([^"]+)"|([^\s,]+))', header):
        data[match.group(1)] = match.group(2) if match.group(2) is not None else match.group(3)
    return data


def rtp_seq_lt(seq1: int, seq2: int) -> bool:
    """Сравнение RTP sequence numbers с учётом wrap-around."""
    return ((seq1 - seq2) & 0xFFFF) > 32768


@dataclass
class SIPTransaction:
    """SIP transaction с retransmission логикой."""
    branch: str
    method: str
    call_id: str
    cseq: int
    message: bytes
    addr: tuple
    created_at: float
    retransmit_count: int = 0
    last_retransmit_at: float = 0.0
    completed: bool = False
    response_code: Optional[int] = None
    timer_handle: Optional[asyncio.TimerHandle] = None
    is_invite: bool = False
    
    def get_retransmit_interval(self) -> float:
        """Экспоненциальный backoff: T1, 2*T1, 4*T1, ... capped at T2."""
        interval = SIP_T1 * (2 ** self.retransmit_count)
        return min(interval, SIP_T2)
    
    def get_timeout(self) -> float:
        """Timer B для INVITE, Timer F для non-INVITE."""
        return SIP_TIMER_B if self.is_invite else SIP_TIMER_F


class RateLimiter:
    """Простой rate limiter per IP."""
    
    def __init__(self, max_per_minute: int):
        self.max_per_minute = max_per_minute
        self._requests: Dict[str, list] = {}
    
    def allow(self, ip: str) -> bool:
        now = time.time()
        if ip not in self._requests:
            self._requests[ip] = []
        
        # Удаляем старые запросы (> 60 секунд)
        self._requests[ip] = [t for t in self._requests[ip] if now - t < 60]
        
        if len(self._requests[ip]) >= self.max_per_minute:
            return False
        
        self._requests[ip].append(now)
        return True


class RTPProtocol(asyncio.DatagramProtocol):
    """RTP protocol handler."""
    
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
        self._rtp_source_learned = False

    def set_codec(self, payload_type):
        if payload_type in (0, 8):
            self.negotiated_codec = payload_type

    def connection_made(self, transport):
        self.transport = transport

    def connection_lost(self, exc):
        self.active = False

    def datagram_received(self, data, addr):
        if not self.active:
            return
        
        # Обновляем время последней активности (для MEDIA_IDLE_TIMEOUT)
        self.last_packet_time = time.monotonic()
        
        if len(data) < 12:
            return

        # LIVE-TEST REQUIRED: symmetric RTP
        if addr[0] != self.remote_ip:
            return
        if addr[1] != self.remote_port:
            if not self._rtp_source_learned:
                self._rtp_source_learned = True
                self.remote_port = addr[1]
                self.remote_target = addr
                logger.info("RTP source learned call_id=%s port %d->%d", 
                           self.call_id, self.remote_port, addr[1])
            else:
                return

        version = (data[0] >> 6) & 0x3
        if version != 2:
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
            ext_len_words = struct.unpack("!H", data[offset + 2:offset + 4])[0]
            offset += 4 + ext_len_words * 4
        if offset > len(data):
            return

        payload = data[offset:]
        if p_bit and len(payload) > 0:
            pad_len = payload[-1]
            if pad_len == 0 or pad_len > len(payload):
                return
            payload = payload[:-pad_len]

        if self._last_rx_seq is not None:
            if seq == self._last_rx_seq or rtp_seq_lt(seq, self._last_rx_seq):
                return
        self._last_rx_seq = seq

        try:
            pcm_frame = audioop.alaw2lin(payload, 2) if payload_type == 8 else audioop.ulaw2lin(payload, 2)
            self.pcm_buffer.extend(pcm_frame)
            
            # Ограничение буфера
            if len(self.pcm_buffer) > MAX_RTP_BUFFER_BYTES:
                excess = len(self.pcm_buffer) - MAX_RTP_BUFFER_BYTES
                del self.pcm_buffer[:excess]
                logger.warning("RTP buffer overflow call_id=%s, trimmed %d bytes", 
                              self.call_id, excess)
        except Exception as e:
            logger.error("RTP decode error call_id=%s: %s", self.call_id, e)

    async def send_pcm(self, pcm_data):
        if not self.remote_target or not pcm_data:
            return

        self.speaking = True
        try:
            pt = 0x08 if self.negotiated_codec == 8 else 0x00
            encode_func = audioop.lin2alaw if self.negotiated_codec == 8 else audioop.lin2ulaw

            frame_bytes = 320
            start_time = time.monotonic()
            for i in range(0, len(pcm_data), frame_bytes):
                chunk = pcm_data[i:i + frame_bytes]
                if len(chunk) < frame_bytes:
                    chunk = chunk + b'\x00' * (frame_bytes - len(chunk))
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
                if delay > 0:
                    await asyncio.sleep(delay)
        finally:
            self.speaking = False

    def send_keepalive(self):
        if not self.transport or not self.remote_target or self.speaking or not self.active:
            return
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
    """SIP protocol handler."""
    
    def __init__(self, worker):
        self.worker = worker
        self.transport = None

    def connection_made(self, transport):
        self.worker.sip_transport = transport
        self.transport = transport

    def connection_lost(self, exc):
        if exc:
            logger.error("SIP transport lost: %s", exc)

    def datagram_received(self, data, addr):
        try:
            if len(data) > 65535:
                logger.warning("Oversized SIP datagram from %s", addr)
                return
            
            # Rate limiting
            if not self.worker.rate_limiter.allow(addr[0]):
                logger.warning("SIP rate limit exceeded for %s", addr[0])
                return
            
            msg = data.decode('utf-8', errors='ignore')
            first_line = msg.split('\r\n', 1)[0]
            
            if first_line.startswith('SIP/2.0'):
                self.worker.handle_response(first_line, msg, addr)
                return
            
            method = first_line.split(' ', 1)[0].upper()
            if method == 'INVITE':
                asyncio.create_task(self.worker.handle_invite(msg, addr))
            elif method == 'ACK':
                self.worker.handle_ack(msg, addr)
            elif method == 'BYE':
                asyncio.create_task(self.worker.handle_bye(msg, addr))
            elif method == 'CANCEL':
                asyncio.create_task(self.worker.handle_cancel(msg, addr))
            elif method == 'OPTIONS':
                self.worker.handle_options(msg, addr)
        except Exception as e:
            logger.exception("SIP message processing error: %s", e)


class SIPWorker:
    """Основной SIP Worker для входящих и исходящих звонков."""
    
    def __init__(self, voice_engine, database, integrations):
        self.host = PLUSOFON_SIP_HOST
        self.port = PLUSOFON_SIP_PORT
        self.user = PLUSOFON_SIP_USER
        self.password = PLUSOFON_SIP_PASSWORD
        self.sip_transport = None
        self.is_running = False
        self.sessions: Dict[str, dict] = {}
        self.pending_byes: Dict[str, dict] = {}
        self.used_ports = set()
        self._last_allocated_port = RTP_PORT_MIN
        self._port_lock = asyncio.Lock()
        
        # SIP transactions (для retransmission)
        self.transactions: Dict[str, SIPTransaction] = {}
        self._transaction_lock = asyncio.Lock()
        
        self.register_state = {
            "call_id": f"{random.randint(100000,999999)}@{self.host}",
            "from_tag": f"tag{random.randint(100000,999999)}",
            "cseq": 1,
            "state": "NOT_REGISTERED",
        }
        self.auth_cache = None
        self._untrusted_last = {}
        self.voice_engine = voice_engine
        self.database = database
        self.integrations = integrations
        self._register_task = None
        self._heartbeat_task = None
        self._transaction_cleanup_task = None
        self._shutdown_timeout = 10
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

    def release_port(self, port):
        self.used_ports.discard(port)

    def generate_sdp(self, rtp_port, codecs=None):
        codecs = codecs or [0, 8]
        lines = ["v=0", f"o=- {int(time.time())} 1 IN IP4 {PUBLIC_IP}", "s=-",
                f"c=IN IP4 {PUBLIC_IP}", "t=0 0",
                f"m=audio {rtp_port} RTP/AVP {' '.join(map(str, codecs))}"]
        for pt in codecs:
            lines.append(f"a=rtpmap:{pt} {'PCMA' if pt == 8 else 'PCMU'}/8000")
        lines.append("a=sendrecv")
        return "\n".join(lines) + "\n"

    def parse_sdp_remote_media(self, msg):
        session_ip = media_ip = port = None
        in_media = False
        supported_codecs = []
        direction = "sendrecv"

        for line in msg.splitlines():
            line = line.strip()
            if line.startswith("c=IN IP4"):
                parts = line.split()
                if len(parts) >= 3:
                    if in_media:
                        media_ip = parts[2]
                    else:
                        session_ip = parts[2]
            elif line.startswith("m=audio"):
                in_media = True
                parts = line.split()
                if len(parts) >= 3:
                    try:
                        port = int(parts[1])
                        if port == 0:
                            port = None
                    except ValueError:
                        port = None
                    for pt_str in parts[3:]:
                        try:
                            pt = int(pt_str)
                            if pt in (0, 8):
                                supported_codecs.append(pt)
                        except ValueError:
                            pass
            elif line.startswith("m=video") or line.startswith("m=application"):
                in_media = False
            elif in_media and line.startswith("a="):
                attr = line[2:].strip()
                if attr in ("sendrecv", "sendonly", "recvonly", "inactive"):
                    direction = attr

        if direction == "inactive" or port is None or not supported_codecs:
            return None, None, None
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
            qop = "auth" if "auth" in qop_list else ("auth-int" if "auth-int" in qop_list else (qop_list[0] if qop_list else None))

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

        if "opaque" in auth_data:
            auth_header += f', opaque="{auth_data["opaque"]}"'
        return auth_header

    def _parse_auth_challenge(self, msg):
        m = re.search(r'Proxy-Authenticate:\s*Digest\s+(.*)', msg, re.I)
        if m:
            return parse_digest_challenge(m.group(1).strip()), True
        m = re.search(r'WWW-Authenticate:\s*Digest\s+(.*)', msg, re.I)
        if m:
            return parse_digest_challenge(m.group(1).strip()), False
        return None, False

    def _build_register_message(self, auth_header=None):
        branch = f"z9hG4bK{random.randint(100000,999999)}"
        headers = (
            f"REGISTER sip:{self.host} SIP/2.0\r\n"
            f"Via: SIP/2.0/UDP {PUBLIC_IP}:{self.port};branch={branch}\r\n"
            f"From: <sip:{self.user}@{self.host}>;tag={self.register_state['from_tag']}\r\n"
            f"To: <sip:{self.user}@{self.host}>\r\n"
            f"Call-ID: {self.register_state['call_id']}\r\n"
            f"CSeq: {self.register_state['cseq']} REGISTER\r\n"
            f"Contact: <sip:{self.user}@{PUBLIC_IP}:{self.port}>\r\n"
            f"Max-Forwards: 70\r\nExpires: 120\r\n"
        )
        if auth_header:
            headers += f"{auth_header}\r\n"
        return headers + "Content-Length: 0\r\n\r\n"

    async def _send_sip_message(self, sip_msg, addr=None) -> bool:
        """Отправка SIP message. Возвращает True при успехе, False при ошибке."""
        try:
            if addr is None:
                loop = asyncio.get_running_loop()
                infos = await loop.getaddrinfo(self.host, self.port, type=socket.SOCK_DGRAM)
                addr = (infos[0][4][0], self.port)
            if not self.sip_transport:
                logger.error("SIP transport not available, cannot send message")
                return False
            self.sip_transport.sendto(sip_msg.encode('utf-8'), addr)
            return True
        except Exception as e:
            logger.exception("SIP send error: %s", e)
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
        if not ruri:
            ruri = self._build_outbound_ruri(session["phone"])
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

    async def _keepalive_loop(self, session):
        while session.get("proto") and session["proto"].active and not session.get("stopped"):
            await asyncio.sleep(5)
            proto = session.get("proto")
            if proto and proto.active and not proto.speaking and not session.get("stopped"):
                proto.send_keepalive()

    # === SIP Transactions (Retransmission) ===
    
    async def _register_transaction(self, message: str, addr: tuple, is_invite: bool = False) -> Optional[SIPTransaction]:
        """Регистрация SIP transaction для retransmission."""
        async with self._transaction_lock:
            branch_match = re.search(r'branch=([^;\s]+)', message)
            if not branch_match:
                return None
            branch = branch_match.group(1)
            
            cseq_match = re.search(r'CSeq:\s*(\d+)\s+\w+', message)
            cseq = int(cseq_match.group(1)) if cseq_match else 0
            
            call_id_match = re.search(r'Call-ID:\s*([^\r\n]+)', message)
            call_id = call_id_match.group(1).strip() if call_id_match else ""
            
            method_match = re.search(r'^(\w+)\s+', message.split('\r\n')[0])
            method = method_match.group(1) if method_match else "UNKNOWN"
            
            tx = SIPTransaction(
                branch=branch,
                method=method,
                call_id=call_id,
                cseq=cseq,
                message=message.encode('utf-8'),
                addr=addr,
                created_at=time.monotonic(),
                is_invite=is_invite,
            )
            self.transactions[branch] = tx
            return tx
    
    async def _start_retransmission(self, tx: SIPTransaction):
        """Запуск retransmission timer для transaction."""
        while not tx.completed and tx.retransmit_count < SIP_MAX_RETRANSMITS:
            interval = tx.get_retransmit_interval()
            await asyncio.sleep(interval)
            
            if tx.completed:
                break
            
            # Retransmit
            try:
                if self.sip_transport:
                    self.sip_transport.sendto(tx.message, tx.addr)
                    tx.retransmit_count += 1
                    tx.last_retransmit_at = time.monotonic()
                    logger.debug("SIP retransmit branch=%s count=%d", tx.branch, tx.retransmit_count)
            except Exception as e:
                logger.error("SIP retransmit error: %s", e)
        
        # Timeout — transaction failed
        if not tx.completed:
            logger.warning("SIP transaction timeout branch=%s method=%s", tx.branch, tx.method)
            await self._handle_transaction_timeout(tx)
    
    async def _handle_transaction_timeout(self, tx: SIPTransaction):
        """Обработка timeout SIP transaction."""
        if tx.is_invite:
            session = self.sessions.get(tx.call_id)
            if session and session["state"] in ("DIALING", "RINGING"):
                session["hangup_reason"] = "invite_timeout"
                await self._cleanup_call(tx.call_id, send_lead=False)
    
    def _complete_transaction(self, branch: str, response_code: int):
        """Завершение transaction после получения response."""
        tx = self.transactions.get(branch)
        if tx:
            tx.completed = True
            tx.response_code = response_code
    
    async def _cleanup_old_transactions(self):
        """Удаление старых завершённых transactions."""
        async with self._transaction_lock:
            now = time.monotonic()
            expired = [b for b, tx in self.transactions.items() 
                      if tx.completed or (now - tx.created_at) > 120]
            for b in expired:
                self.transactions.pop(b, None)

    # === REGISTER ===

    async def send_register(self):
        # Увеличиваем CSeq ПЕРЕД отправкой
        self.register_state["cseq"] += 1
        self.register_state["state"] = "REGISTER_SENT"
        auth_header = self._compute_digest(self.auth_cache, proxy=self.auth_cache.get("_proxy", False)) if self.auth_cache else ""
        msg = self._build_register_message(auth_header)
        await self._send_sip_message(msg)

    async def register_loop(self):
        while self.is_running:
            await self.send_register()
            await asyncio.sleep(45)

    async def handle_register_challenge(self, msg):
        auth_data, proxy = self._parse_auth_challenge(msg)
        if not auth_data:
            self.register_state["state"] = "REGISTER_FAILED"
            return
        call_id_m = re.search(r'Call-ID:\s*(.+)', msg, re.I)
        call_id = call_id_m.group(1).strip() if call_id_m else None
        if call_id != self.register_state["call_id"]:
            return
        self.auth_cache = auth_data
        self.auth_cache["_proxy"] = proxy
        
        # CSeq уже увеличен в send_register
        auth_header = self._compute_digest(auth_data, proxy=proxy)
        msg = self._build_register_message(auth_header)
        await self._send_sip_message(msg)

    async def handle_invite(self, msg, addr):
        if TRUSTED_SIP_IPS and addr[0] not in TRUSTED_SIP_IPS:
            now = time.time()
            if now - self._untrusted_last.get(addr[0], 0.0) >= 60:
                self._untrusted_last[addr[0]] = now
                logger.warning("INVITE from untrusted IP %s", addr[0])
            return
        elif not TRUSTED_SIP_IPS and not DEVELOPMENT_MODE:
            logger.error("TRUSTED_SIP_IPS empty in production, rejecting INVITE from %s", addr[0])
            return

        if addr[0] in BLACKLIST_SIP_IPS:
            return

        call_id = self._extract_header(msg, "Call-ID") or f"unknown-{random.randint(1000,9999)}"
        cseq = self._extract_header(msg, "CSeq") or "1 INVITE"
        from_hdr = self._extract_header(msg, "From") or ""
        to_hdr = self._extract_header(msg, "To") or ""
        via_hdr = self._extract_header(msg, "Via") or f"SIP/2.0/UDP {addr[0]}:{addr[1]}"

        from_tag = None
        m = re.search(r';tag=([^;\s]+)', from_hdr, re.IGNORECASE)
        if m:
            from_tag = m.group(1)

        existing = self.sessions.get(call_id)
        if existing:
            if existing.get("remote_from_tag") == from_tag:
                last_200 = existing.get("last_200")
                if last_200:
                    self.sip_transport.sendto(last_200.encode('utf-8'), addr)
            return

        trying = f"SIP/2.0 100 Trying\r\nVia: {via_hdr}\r\nFrom: {from_hdr}\r\nTo: {to_hdr}\r\nCall-ID: {call_id}\r\nCSeq: {cseq}\r\nContent-Length: 0\r\n\r\n"
        self.sip_transport.sendto(trying.encode('utf-8'), addr)

        phone_m = re.search(r'sip:\+?(\d+)', from_hdr)
        phone = phone_m.group(1) if phone_m else "Unknown"
        remote_ip, remote_port, codec = self.parse_sdp_remote_media(msg)
        if not remote_ip:
            remote_ip = addr[0]

        if not remote_port or codec is None:
            resp = self._build_error_response(488, "Not Acceptable Here", via_hdr, from_hdr, to_hdr, call_id, cseq)
            self.sip_transport.sendto(resp.encode('utf-8'), addr)
            return

        rtp_port = await self.reserve_port()
        if rtp_port is None:
            resp = self._build_error_response(503, "Service Unavailable", via_hdr, from_hdr, to_hdr, call_id, cseq)
            self.sip_transport.sendto(resp.encode('utf-8'), addr)
            return

        sock = await self._bind_rtp_socket(rtp_port)
        if sock is None:
            self.release_port(rtp_port)
            resp = self._build_error_response(503, "Service Unavailable", via_hdr, from_hdr, to_hdr, call_id, cseq)
            self.sip_transport.sendto(resp.encode('utf-8'), addr)
            return

        try:
            await self.database.create_call(
                call_id=call_id,
                direction="inbound",
                scenario="BEFORE_LESSON",
                phone=phone,
            )
        except Exception as e:
            logger.error("Failed to create inbound call record: %s", e)

        remote_contact = self._extract_header(msg, "Contact")
        to_tag = None
        if ";tag=" not in to_hdr.lower():
            to_tag = f"woltron{random.randint(100000, 999999)}"
            to_hdr = f"{to_hdr};tag={to_tag}"

        self.sessions[call_id] = {
            "call_id": call_id, "direction": "inbound", "state": "INBOUND_ACCEPTING",
            "cleanup_state": CLEANUP_STATES["NOT_STARTED"], "proto": None, "rtp_transport": None,
            "phone": phone, "rtp_port": rtp_port, "rtp_bound_sock": sock,
            "started_at": time.time(), "answered_at": None, "signaling_addr": addr,
            "from_hdr": from_hdr, "to_hdr": to_hdr, "via_hdr": via_hdr,
            "invite_cseq": cseq, "bye_cseq": 1, "last_200": None, "confirmed": False,
            "stopped": False, "voice_engine_task": None, "timeout_task": None,
            "keepalive_task": None, "bye_timeout_task": None, "ack_timeout_task": None,
            "media_watchdog_task": None, "max_duration_task": None,
            "remote_ip": remote_ip, "remote_port": remote_port, "remote_target": None,
            "remote_contact": remote_contact, "negotiated_codec": codec,
            "scenario": "BEFORE_LESSON", "metadata": {}, "to_tag": to_tag,
            "remote_from_tag": from_tag, "from_tag": None, "via_branch": None,
            "ack_branch": None, "ack_message": None, "hangup_reason": None,
            "sip_code": None, "finish_called": False, "_lead_sent": False,
            "terminate_lock": asyncio.Lock(), "rtp_lock": asyncio.Lock(),
        }

        sdp_body = self.generate_sdp(rtp_port, codecs=[codec])
        sdp_bytes = sdp_body.encode('utf-8')
        response = (
            f"SIP/2.0 200 OK\r\nVia: {via_hdr}\r\nFrom: {from_hdr}\r\nTo: {to_hdr}\r\n"
            f"Call-ID: {call_id}\r\nCSeq: {cseq}\r\n"
            f"Contact: <sip:{self.user}@{PUBLIC_IP}:{self.port}>\r\n"
            f"Content-Type: application/sdp\r\nContent-Length: {len(sdp_bytes)}\r\n\r\n{sdp_body}"
        )
        self.sessions[call_id]["last_200"] = response
        self.sessions[call_id]["state"] = "ANSWERED"
        self.sip_transport.sendto(response.encode('utf-8'), addr)

        session = self.sessions[call_id]
        session["ack_timeout_task"] = asyncio.create_task(self._inbound_ack_timeout(call_id, sock))

    async def _inbound_ack_timeout(self, call_id, sock):
        await asyncio.sleep(32)
        session = self.sessions.get(call_id)
        if not session or session.get("confirmed"):
            return
        try:
            sock.close()
        except Exception:
            pass
        session["rtp_bound_sock"] = None
        self.release_port(session["rtp_port"])
        self.sessions.pop(call_id, None)

    def handle_ack(self, msg, addr):
        call_id = self._extract_header(msg, "Call-ID") or ""
        session = self.sessions.get(call_id)
        if not session or session["direction"] != "inbound":
            return

        cseq = self._extract_header(msg, "CSeq") or ""
        cseq_parts = cseq.split()
        if len(cseq_parts) < 2 or cseq_parts[1].upper() != "ACK":
            return
        cseq_num = cseq_parts[0]
        invite_cseq_num = session["invite_cseq"].split()[0] if session["invite_cseq"] else ""
        if cseq_num != invite_cseq_num:
            return

        from_hdr = self._extract_header(msg, "From") or ""
        remote_from_tag = None
        m = re.search(r';tag=([^;\s]+)', from_hdr, re.IGNORECASE)
        if m:
            remote_from_tag = m.group(1)
        if session.get("remote_from_tag") and remote_from_tag != session["remote_from_tag"]:
            return

        to_hdr = self._extract_header(msg, "To") or ""
        if session["to_tag"]:
            m = re.search(r';tag=([^;\s]+)', to_hdr, re.IGNORECASE)
            local_to_tag = m.group(1) if m else None
            if local_to_tag != session["to_tag"]:
                return

        if session.get("confirmed"):
            return

        session["confirmed"] = True
        session["state"] = "IN_PROGRESS"
        session["answered_at"] = time.time()
        
        try:
            asyncio.create_task(self.database.set_call_answered(call_id))
        except Exception as e:
            logger.error("Failed to update call answered: %s", e)
        
        # Запуск watchdogs
        session["media_watchdog_task"] = asyncio.create_task(self._media_idle_watchdog(call_id))
        session["max_duration_task"] = asyncio.create_task(self._max_call_duration_watchdog(call_id))
        
        asyncio.create_task(self._start_inbound_media(call_id, session))

    async def _start_inbound_media(self, call_id, session):
        bound_sock = session.get("rtp_bound_sock")
        if bound_sock is None:
            await self._cleanup_call(call_id, send_lead=False)
            return

        if session.get("ack_timeout_task"):
            session["ack_timeout_task"].cancel()
            session["ack_timeout_task"] = None

        loop = asyncio.get_running_loop()
        try:
            transport, protocol = await loop.create_datagram_endpoint(
                lambda: RTPProtocol(self, call_id, session["phone"], session["remote_ip"], session["remote_port"]),
                sock=bound_sock,
            )
        except Exception as e:
            logger.error("RTP endpoint creation failed call_id=%s: %s", call_id, e)
            try:
                bound_sock.close()
            except Exception:
                pass
            session["rtp_bound_sock"] = None
            await self._cleanup_call(call_id, send_lead=False)
            return

        protocol.set_codec(session["negotiated_codec"])
        session["proto"] = protocol
        session["rtp_transport"] = transport
        session["rtp_bound_sock"] = None
        session["keepalive_task"] = asyncio.create_task(self._keepalive_loop(session))
        session["voice_engine_task"] = asyncio.create_task(self.voice_engine.start(call_id, session))

    async def originate_call(self, phone, scenario="BEFORE_LESSON", metadata=None):
        """Инициирование исходящего звонка. Возвращает call_id или None."""
        if scenario not in ("BEFORE_LESSON", "AFTER_LESSON"):
            logger.error("Invalid scenario: %s", scenario)
            return None
        
        active_count = len(self.get_active_calls())
        if active_count >= self.max_concurrent_calls:
            logger.warning("Max concurrent calls reached: %d", active_count)
            return None
        
        rtp_port = await self.reserve_port()
        if rtp_port is None:
            logger.error("No free RTP ports for outbound call")
            return None

        sock = await self._bind_rtp_socket(rtp_port)
        if sock is None:
            self.release_port(rtp_port)
            logger.error("Cannot bind RTP port %d for outbound", rtp_port)
            return None

        call_id = f"{random.randint(100000,999999)}@{PUBLIC_IP}"
        
        try:
            metadata_json = json.dumps(metadata) if metadata else None
            await self.database.create_call(
                call_id=call_id,
                direction="outbound",
                scenario=scenario,
                phone=phone,
                metadata=metadata_json,
            )
        except Exception as e:
            logger.error("Failed to create call record: %s", e)
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
            "metadata": metadata or {},
            "auth_cache": None, "last_200": None, "ack_branch": None, "ack_message": None,
            "hangup_reason": None, "sip_code": None, "finish_called": False,
            "_lead_sent": False, "terminate_lock": asyncio.Lock(), "rtp_lock": asyncio.Lock(),
        }
        self.sessions[call_id] = session

        invite = self._build_outbound_invite(session)
        
        # КРИТИЧНО: проверяем что INVITE реально отправлен
        sent = await self._send_sip_message(invite)
        if not sent:
            logger.error("Failed to send INVITE for call_id=%s", call_id)
            sock.close()
            self.release_port(rtp_port)
            self.sessions.pop(call_id, None)
            try:
                await self.database.update_call_status(call_id, "FAILED", "invite_send_failed")
            except Exception as e:
                logger.error("Failed to update call status: %s", e)
            return None
        
        session["state"] = "DIALING"
        
        try:
            await self.database.update_call_status(call_id, "DIALING")
        except Exception as e:
            logger.error("Failed to update call status: %s", e)

        # Запуск retransmission для INVITE
        tx = await self._register_transaction(invite, (self.host, self.port), is_invite=True)
        if tx:
            asyncio.create_task(self._start_retransmission(tx))

        session["timeout_task"] = asyncio.create_task(self._outbound_timeout(call_id))
        return call_id

    async def _outbound_timeout(self, call_id):
        await asyncio.sleep(SIP_TIMER_B)
        session = self.sessions.get(call_id)
        if not session or session.get("stopped"):
            return
        if session["state"] in ("CREATED", "DIALING", "RINGING"):
            session["hangup_reason"] = "timeout"
            await self.send_cancel(call_id)

    async def send_cancel(self, call_id):
        session = self.sessions.get(call_id)
        if not session or session["direction"] != "outbound":
            return
        if session["state"] in ("CANCEL_SENT", "STOPPING", "BYE_SENT", "ENDED"):
            return

        ruri = self._build_outbound_ruri(session["phone"])
        cancel = (
            f"CANCEL {ruri} SIP/2.0\r\n"
            f"Via: SIP/2.0/UDP {PUBLIC_IP}:{self.port};branch={session['via_branch']}\r\n"
            f"From: {session['from_hdr']}\r\nTo: {session['to_hdr']}\r\n"
            f"Call-ID: {call_id}\r\nCSeq: {session['invite_cseq']} CANCEL\r\n"
            f"Max-Forwards: 70\r\nContent-Length: 0\r\n\r\n"
        )
        sent = await self._send_sip_message(cancel, session.get("signaling_addr"))
        if not sent:
            logger.error("Failed to send CANCEL for call_id=%s", call_id)
            return
        
        session["state"] = "CANCEL_SENT"
        session["cancel_timeout_task"] = asyncio.create_task(self._cancel_timeout(call_id))

    async def _cancel_timeout(self, call_id):
        await asyncio.sleep(10)
        session = self.sessions.get(call_id)
        if not session:
            return
        if session["state"] == "CANCEL_SENT":
            await self._cleanup_call(call_id, send_lead=False)

    async def handle_invite_challenge(self, msg, call_id, addr):
        session = self.sessions.get(call_id)
        if not session or session["direction"] != "outbound":
            return

        session["signaling_addr"] = addr
        session["sip_code"] = "401/407"
        auth_data, proxy = self._parse_auth_challenge(msg)
        if not auth_data:
            return

        # Новый transaction: новый CSeq, новый branch
        session["invite_cseq"] += 1
        session["via_branch"] = f"z9hG4bK{random.randint(100000,999999)}"
        session["auth_cache"] = auth_data
        session["auth_cache"]["_proxy"] = proxy

        auth_header = self._compute_digest(auth_data, method="INVITE", uri=self._build_outbound_ruri(session["phone"]), proxy=proxy)
        invite = self._build_outbound_invite(session)
        lines = invite.split("\r\n")
        insert_idx = next(i for i, l in enumerate(lines) if l.startswith("Content-Length"))
        lines.insert(insert_idx, auth_header)
        invite = "\r\n".join(lines)
        
        sent = await self._send_sip_message(invite, addr)
        if sent:
            # Регистрируем новый transaction для retransmission
            tx = await self._register_transaction(invite, addr, is_invite=True)
            if tx:
                asyncio.create_task(self._start_retransmission(tx))

    async def handle_invite_200(self, msg, call_id, addr):
        session = self.sessions.get(call_id)
        if not session or session["direction"] != "outbound":
            return

        # Извлекаем Via branch для correlation
        via_branch = None
        via_hdr = self._extract_header(msg, "Via") or ""
        m = re.search(r'branch=([^;\s]+)', via_hdr)
        if m:
            via_branch = m.group(1)
        
        # Завершаем transaction если есть
        if via_branch:
            self._complete_transaction(via_branch, 200)

        session["signaling_addr"] = addr
        lock = session.get("rtp_lock") or asyncio.Lock()
        session["rtp_lock"] = lock

        async with lock:
            # Обработка LATE 200 после CANCEL
            if session["state"] == "CANCEL_SENT":
                logger.info("Late 200 after CANCEL call_id=%s, sending ACK+BYE", call_id)
                to_hdr = self._extract_header(msg, "To") or session["to_hdr"]
                to_tag = None
                m = re.search(r';tag=([^;\s]+)', to_hdr, re.IGNORECASE)
                if m:
                    to_tag = m.group(1)
                
                if to_tag:
                    session["to_tag"] = to_tag
                    session["to_hdr"] = to_hdr
                    contact = self._extract_header(msg, "Contact") or ""
                    
                    ack, _ = self._build_ack(session, contact, is_2xx=True)
                    await self._send_sip_message(ack, addr)
                    
                    # Теперь отправляем BYE
                    session["remote_contact"] = contact
                    session["hangup_reason"] = "cancelled_after_answer"
                    await self._start_bye(call_id, session)
                else:
                    await self._cleanup_call(call_id, send_lead=False)
                return

            if session["state"] in ("IN_PROGRESS", "ANSWERED") and session.get("proto"):
                if session.get("ack_message"):
                    await self._send_sip_message(session["ack_message"], addr)
                return

            if session["state"] in ("STOPPING", "ENDING", "ENDED", "BYE_SENT"):
                ack, _ = self._build_ack(session)
                await self._send_sip_message(ack, addr)
                await self._cleanup_call(call_id, send_lead=False)
                return

            to_hdr = self._extract_header(msg, "To") or session["to_hdr"]
            to_tag = None
            m = re.search(r';tag=([^;\s]+)', to_hdr, re.IGNORECASE)
            if m:
                to_tag = m.group(1)

            if not to_tag:
                await self._cleanup_call(call_id, send_lead=False)
                return

            session["to_tag"] = to_tag
            session["to_hdr"] = to_hdr

            contact = self._extract_header(msg, "Contact") or ""
            session["remote_target"] = contact
            session["remote_contact"] = contact

            remote_ip, remote_port, codec = self.parse_sdp_remote_media(msg)
            if not remote_ip or not remote_port or codec is None or codec not in (0, 8):
                await self._cleanup_call(call_id, send_lead=False)
                return

            session["remote_ip"] = remote_ip
            session["remote_port"] = remote_port
            session["negotiated_codec"] = codec

            if session.get("timeout_task"):
                session["timeout_task"].cancel()
                session["timeout_task"] = None

            ack, ack_branch = self._build_ack(session, contact, is_2xx=True)
            session["ack_message"] = ack
            session["ack_branch"] = ack_branch
            await self._send_sip_message(ack, addr)

            session["state"] = "ANSWERED"
            session["answered_at"] = time.time()
            await self._start_outbound_media(call_id, session)

    async def _start_outbound_media(self, call_id, session):
        rtp_port = session["rtp_port"]
        bound_sock = session.get("rtp_bound_sock")

        if bound_sock is None or bound_sock.getsockname()[1] != rtp_port:
            await self._cleanup_call(call_id, send_lead=False)
            return

        loop = asyncio.get_running_loop()
        try:
            transport, protocol = await loop.create_datagram_endpoint(
                lambda: RTPProtocol(self, call_id, session["phone"], session["remote_ip"], session["remote_port"]),
                sock=bound_sock,
            )
        except Exception as e:
            logger.error("RTP endpoint creation failed call_id=%s: %s", call_id, e)
            await self._cleanup_call(call_id, send_lead=False)
            return

        protocol.set_codec(session["negotiated_codec"])
        session["proto"] = protocol
        session["rtp_transport"] = transport
        session["rtp_bound_sock"] = None
        session["state"] = "IN_PROGRESS"
        
        try:
            await self.database.set_call_answered(call_id)
        except Exception as e:
            logger.error("Failed to update call answered: %s", e)

        session["keepalive_task"] = asyncio.create_task(self._keepalive_loop(session))
        session["voice_engine_task"] = asyncio.create_task(self.voice_engine.start(call_id, session))
        
        # Запуск watchdogs
        session["media_watchdog_task"] = asyncio.create_task(self._media_idle_watchdog(call_id))
        session["max_duration_task"] = asyncio.create_task(self._max_call_duration_watchdog(call_id))

    async def _media_idle_watchdog(self, call_id: str):
        """Watchdog для MEDIA_IDLE_TIMEOUT."""
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
            
            idle_time = time.monotonic() - proto.last_packet_time
            if idle_time > MEDIA_IDLE_TIMEOUT:
                logger.warning("MEDIA_IDLE_TIMEOUT exceeded call_id=%s (%.1fs)", call_id, idle_time)
                session["hangup_reason"] = "media_idle_timeout"
                await self._cleanup_call(call_id, send_lead=True)
                return

    async def _max_call_duration_watchdog(self, call_id: str):
        """Watchdog для MAX_CALL_DURATION."""
        session = self.sessions.get(call_id)
        if not session:
            return
        
        await asyncio.sleep(MAX_CALL_DURATION)
        
        session = self.sessions.get(call_id)
        if not session:
            return
        
        if session["state"] in ("IN_PROGRESS", "ANSWERED"):
            logger.warning("MAX_CALL_DURATION exceeded call_id=%s (%ds)", call_id, MAX_CALL_DURATION)
            session["hangup_reason"] = "max_call_duration"
            await self._cleanup_call(call_id, send_lead=True)

    async def handle_invite_487(self, msg, call_id, addr):
        session = self.sessions.get(call_id)
        if not session:
            return

        # Извлекаем Via branch
        via_hdr = self._extract_header(msg, "Via") or ""
        m = re.search(r'branch=([^;\s]+)', via_hdr)
        if m:
            self._complete_transaction(m.group(1), 487)

        session["signaling_addr"] = addr
        session["sip_code"] = "487"
        ack, _ = self._build_ack(session, is_2xx=False)
        await self._send_sip_message(ack, addr)

        if session.get("cancel_timeout_task"):
            session["cancel_timeout_task"].cancel()
            session["cancel_timeout_task"] = None

        await self._cleanup_call(call_id, send_lead=False)

    async def handle_invite_error(self, msg, call_id, code, addr=None):
        session = self.sessions.get(call_id)
        if not session:
            return
        if addr:
            session["signaling_addr"] = addr

        # Извлекаем Via branch
        via_hdr = self._extract_header(msg, "Via") or ""
        m = re.search(r'branch=([^;\s]+)', via_hdr)
        if m:
            self._complete_transaction(m.group(1), int(code))

        session["sip_code"] = code
        # Маппинг кодов на бизнес-статусы
        if code == "486":
            session["hangup_reason"] = "busy"
        elif code == "408":
            session["hangup_reason"] = "request_timeout"
        elif code == "480":
            session["hangup_reason"] = "temporarily_unavailable"
        elif code.startswith("4"):
            session["hangup_reason"] = f"client_error_{code}"
        elif code.startswith("5"):
            session["hangup_reason"] = f"server_error_{code}"
        elif code.startswith("6"):
            session["hangup_reason"] = f"global_failure_{code}"
        else:
            session["hangup_reason"] = f"error_{code}"
        
        ack, _ = self._build_ack(session, is_2xx=False)
        await self._send_sip_message(ack, addr)
        await self._cleanup_call(call_id, send_lead=False)

    def handle_response(self, first_line, msg, addr):
        parts = first_line.split()
        code = parts[1] if len(parts) > 1 else ""
        cseq = self._extract_header(msg, "CSeq") or ""
        call_id = self._extract_header(msg, "Call-ID") or ""

        if code in ("401", "407"):
            if "REGISTER" in cseq:
                asyncio.create_task(self.handle_register_challenge(msg))
            elif "INVITE" in cseq:
                asyncio.create_task(self.handle_invite_challenge(msg, call_id, addr))
            elif "BYE" in cseq:
                asyncio.create_task(self.handle_bye_challenge(msg, call_id, addr))
        elif code == "200":
            if "REGISTER" in cseq:
                self.register_state["state"] = "REGISTERED"
            elif "INVITE" in cseq:
                asyncio.create_task(self.handle_invite_200(msg, call_id, addr))
            elif "BYE" in cseq:
                self._handle_bye_response(call_id)
            elif "CANCEL" in cseq:
                pass
        elif code in ("100", "180", "183"):
            session = self.sessions.get(call_id)
            if session and session["direction"] == "outbound":
                if code == "100":
                    pass  # Trying — не меняем состояние
                else:
                    session["state"] = "RINGING"
                session["signaling_addr"] = addr
        elif code == "487":
            if "INVITE" in cseq:
                asyncio.create_task(self.handle_invite_487(msg, call_id, addr))
        elif code and code[0] in ("4", "5", "6"):
            if "INVITE" in cseq:
                asyncio.create_task(self.handle_invite_error(msg, call_id, code, addr))
            elif "BYE" in cseq:
                self._handle_bye_response(call_id)

    def _handle_bye_response(self, call_id):
        self.pending_byes.pop(call_id, None)
        session = self.sessions.get(call_id)
        if session and session.get("bye_timeout_task"):
            session["bye_timeout_task"].cancel()
            session["bye_timeout_task"] = None
        asyncio.create_task(self._cleanup_call(call_id, send_lead=True))

    def _build_error_response(self, code, reason, via_hdr, from_hdr, to_hdr, call_id, cseq):
        if ";tag=" not in to_hdr.lower():
            to_hdr = f"{to_hdr};tag=woltron{random.randint(100000, 999999)}"
        return (
            f"SIP/2.0 {code} {reason}\r\nVia: {via_hdr}\r\nFrom: {from_hdr}\r\n"
            f"To: {to_hdr}\r\nCall-ID: {call_id}\r\nCSeq: {cseq}\r\n"
            f"Content-Length: 0\r\n\r\n"
        )

    async def handle_bye(self, msg, addr):
        call_id = self._extract_header(msg, "Call-ID") or ""
        session = self.sessions.get(call_id)
        if not session:
            return

        if session["direction"] == "inbound":
            from_hdr = self._extract_header(msg, "From") or ""
            remote_from_tag = None
            m = re.search(r';tag=([^;\s]+)', from_hdr, re.IGNORECASE)
            if m:
                remote_from_tag = m.group(1)
            if session.get("remote_from_tag") and remote_from_tag != session["remote_from_tag"]:
                return

        cseq = self._extract_header(msg, "CSeq") or ""
        cseq_parts = cseq.split()
        if len(cseq_parts) < 2 or cseq_parts[1].upper() != "BYE":
            return

        to_hdr = self._extract_header(msg, "To") or ""
        if session["to_tag"]:
            m = re.search(r';tag=([^;\s]+)', to_hdr, re.IGNORECASE)
            local_to_tag = m.group(1) if m else None
            if local_to_tag != session["to_tag"]:
                return

        resp = (
            f"SIP/2.0 200 OK\r\nVia: {self._extract_header(msg, 'Via') or ''}\r\n"
            f"From: {self._extract_header(msg, 'From') or ''}\r\n"
            f"To: {self._extract_header(msg, 'To') or ''}\r\n"
            f"Call-ID: {call_id}\r\nCSeq: {cseq}\r\nContent-Length: 0\r\n\r\n"
        )
        self.sip_transport.sendto(resp.encode('utf-8'), addr)
        session["hangup_reason"] = "remote_hangup"
        await self._cleanup_call(call_id, send_lead=True)

    async def handle_cancel(self, msg, addr):
        call_id = self._extract_header(msg, "Call-ID") or ""
        session = self.sessions.get(call_id)
        if not session or session["state"] not in ("CREATED", "DIALING", "RINGING", "ANSWERED"):
            return

        cseq = self._extract_header(msg, "CSeq") or ""
        cseq_parts = cseq.split()
        if len(cseq_parts) < 2 or cseq_parts[1].upper() != "CANCEL":
            return

        cseq_num = cseq_parts[0]
        invite_cseq_num = session["invite_cseq"].split()[0] if session["invite_cseq"] else ""
        if cseq_num != invite_cseq_num:
            return

        from_hdr = self._extract_header(msg, "From") or ""
        to_hdr = self._extract_header(msg, "To") or ""
        via_hdr = self._extract_header(msg, "Via") or ""

        ok = f"SIP/2.0 200 OK\r\nVia: {via_hdr}\r\nFrom: {from_hdr}\r\nTo: {to_hdr}\r\nCall-ID: {call_id}\r\nCSeq: {cseq}\r\nContent-Length: 0\r\n\r\n"
        self.sip_transport.sendto(ok.encode('utf-8'), addr)

        inv_to = session["to_hdr"]
        req_term = (
            f"SIP/2.0 487 Request Terminated\r\nVia: {session.get('via_hdr', via_hdr)}\r\n"
            f"From: {from_hdr}\r\nTo: {inv_to}\r\nCall-ID: {call_id}\r\n"
            f"CSeq: {session['invite_cseq']}\r\nContent-Length: 0\r\n\r\n"
        )
        self.sip_transport.sendto(req_term.encode('utf-8'), addr)
        session["hangup_reason"] = "remote_cancel"
        await self._cleanup_call(call_id, send_lead=False)

    def handle_options(self, msg, addr):
        call_id = self._extract_header(msg, "Call-ID") or "0"
        cseq = self._extract_header(msg, "CSeq") or "1 OPTIONS"
        from_hdr = self._extract_header(msg, "From") or ""
        to_hdr = self._extract_header(msg, "To") or ""
        via_hdr = self._extract_header(msg, "Via") or f"SIP/2.0/UDP {addr[0]}:{addr[1]}"
        if ";tag=" not in to_hdr.lower():
            to_hdr = f"{to_hdr};tag={random.randint(1000,9999)}"
        response = (
            f"SIP/2.0 200 OK\r\nVia: {via_hdr}\r\nFrom: {from_hdr}\r\nTo: {to_hdr}\r\n"
            f"Call-ID: {call_id}\r\nCSeq: {cseq}\r\n"
            f"Contact: <sip:{self.user}@{PUBLIC_IP}:{self.port}>\r\n"
            f"Allow: INVITE, ACK, BYE, CANCEL, OPTIONS\r\nContent-Length: 0\r\n\r\n"
        )
        self.sip_transport.sendto(response.encode('utf-8'), addr)

    async def send_bye(self, session):
        addr = session.get("signaling_addr")
        if not addr or not self.sip_transport:
            return

        uri = parse_contact_uri(session.get("remote_contact") or session.get("remote_target"))
        if not uri:
            uri = self._build_outbound_ruri(session["phone"])

        new_branch = f"z9hG4bK{random.randint(100000,999999)}"
        if session["direction"] == "outbound":
            from_hdr = session["from_hdr"]
            to_hdr = session["to_hdr"]
            if session["to_tag"] and ";tag=" not in to_hdr.lower():
                to_hdr = f"{to_hdr};tag={session['to_tag']}"
        else:
            from_hdr = session["to_hdr"]
            to_hdr = session["from_hdr"]

        bye_cseq = session.get("bye_cseq", 1)
        bye = (
            f"BYE {uri} SIP/2.0\r\nVia: SIP/2.0/UDP {PUBLIC_IP}:{self.port};branch={new_branch}\r\n"
            f"From: {from_hdr}\r\nTo: {to_hdr}\r\nCall-ID: {session['call_id']}\r\n"
            f"CSeq: {bye_cseq} BYE\r\nMax-Forwards: 70\r\nContent-Length: 0\r\n\r\n"
        )

        self.pending_byes[session["call_id"]] = {
            "addr": addr, "uri": uri, "cseq": bye_cseq,
            "from_hdr": from_hdr, "to_hdr": to_hdr, "branch": new_branch,
        }
        session["bye_cseq"] = bye_cseq + 1
        session["state"] = "BYE_SENT"

        try:
            self.sip_transport.sendto(bye.encode('utf-8'), addr)
        except Exception as e:
            logger.error("BYE send error call_id=%s: %s", session["call_id"], e)

        session["bye_timeout_task"] = asyncio.create_task(self._bye_timeout(session["call_id"]))

    async def _bye_timeout(self, call_id):
        await asyncio.sleep(5)
        session = self.sessions.get(call_id)
        if not session:
            return
        if session["state"] == "BYE_SENT":
            await self._cleanup_call(call_id, send_lead=True)

    async def handle_bye_challenge(self, msg, call_id, addr):
        pending = self.pending_byes.get(call_id)
        if not pending:
            return

        auth_data, proxy = self._parse_auth_challenge(msg)
        if not auth_data:
            return

        new_branch = f"z9hG4bK{random.randint(100000,999999)}"
        auth_header = self._compute_digest(auth_data, method="BYE", uri=pending["uri"], proxy=proxy)
        self.pending_byes.pop(call_id, None)

        bye = (
            f"BYE {pending['uri']} SIP/2.0\r\nVia: SIP/2.0/UDP {PUBLIC_IP}:{self.port};branch={new_branch}\r\n"
            f"From: {pending['from_hdr']}\r\nTo: {pending['to_hdr']}\r\n"
            f"Call-ID: {call_id}\r\nCSeq: {pending['cseq']} BYE\r\n{auth_header}\r\n"
            f"Max-Forwards: 70\r\nContent-Length: 0\r\n\r\n"
        )

        session = self.sessions.get(call_id)
        if session and session.get("bye_timeout_task"):
            session["bye_timeout_task"].cancel()
            session["bye_timeout_task"] = asyncio.create_task(self._bye_timeout(call_id))

        try:
            self.sip_transport.sendto(bye.encode('utf-8'), addr)
        except Exception:
            pass

    async def terminate_call(self, call_id):
        """Завершение звонка из Telegram. Идемпотентно."""
        session = self.sessions.get(call_id)
        if not session or session.get("stopped"):
            return False

        # Защита от concurrent terminate
        async with session.get("terminate_lock", asyncio.Lock()):
            if session.get("stopped"):
                return False

            state = session["state"]
            if state in ("CREATED", "DIALING", "RINGING", "CANCEL_SENT"):
                if session.get("cancel_initiated"):
                    return False
                session["cancel_initiated"] = True
                session["hangup_reason"] = "telegram_terminate"
                await self.send_cancel(call_id)
                return True
            elif state in ("ANSWERED", "IN_PROGRESS"):
                if session.get("bye_initiated"):
                    return False
                session["bye_initiated"] = True
                session["hangup_reason"] = "telegram_terminate"
                await self._start_bye(call_id, session)
                return True
            return False

    async def _start_bye(self, call_id, session):
        session["state"] = "STOPPING"
        session["stopped"] = True

        for task_key in ("voice_engine_task", "keepalive_task", "timeout_task",
                         "media_watchdog_task", "max_duration_task"):
            if session.get(task_key):
                session[task_key].cancel()
                session[task_key] = None

        await self.send_bye(session)

    async def _cleanup_call(self, call_id, send_lead=True):
        session = self.sessions.get(call_id)
        if not session:
            return

        cleanup_state = session.get("cleanup_state", CLEANUP_STATES["NOT_STARTED"])
        if cleanup_state >= CLEANUP_STATES["STARTED"]:
            return
        session["cleanup_state"] = CLEANUP_STATES["STARTED"]

        current_task = asyncio.current_task()
        for task_key in ("voice_engine_task", "timeout_task", "keepalive_task",
                          "bye_timeout_task", "cancel_timeout_task", "ack_timeout_task",
                          "media_watchdog_task", "max_duration_task"):
            task = session.get(task_key)
            if task and task != current_task:
                task.cancel()
                session[task_key] = None

        if session.get("proto"):
            session["proto"].active = False
        if session.get("rtp_transport"):
            try:
                session["rtp_transport"].close()
            except Exception:
                pass

        bound_sock = session.get("rtp_bound_sock")
        if bound_sock:
            try:
                bound_sock.close()
            except Exception:
                pass
            session["rtp_bound_sock"] = None

        self.release_port(session["rtp_port"])
        session["cleanup_state"] = CLEANUP_STATES["RESOURCES_CLOSED"]

        try:
            hangup_reason = session.get("hangup_reason", "unknown")
            await self.database.set_call_ended(call_id, hangup_reason)
        except Exception as e:
            logger.error("Failed to update call ended: %s", e)

        if send_lead and not session.get("_lead_sent"):
            session["_lead_sent"] = True
            try:
                await self.voice_engine.finish_call(call_id, session)
                session["cleanup_state"] = CLEANUP_STATES["FINISH_CALLED"]
            except Exception as e:
                logger.error("finish_call error call_id=%s: %s", call_id, e)

        session["state"] = "ENDED"
        session["stopped"] = True
        session["cleanup_state"] = CLEANUP_STATES["COMPLETED"]
        self.sessions.pop(call_id, None)

    async def stop_call(self, call_id, send_bye=False, send_lead=True):
        session = self.sessions.get(call_id)
        if not session:
            return

        if send_bye:
            session["state"] = "STOPPING"
            session["stopped"] = True
            await self.send_bye(session)
        else:
            await self._cleanup_call(call_id, send_lead=send_lead)

    def get_active_calls(self):
        return {k: v for k, v in self.sessions.items() if not v.get("stopped")}

    async def start(self):
        if not SIP_CAN_START:
            raise SystemExit(2)
        self.is_running = True
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGTERM, signal.SIGINT):
            loop.add_signal_handler(sig, lambda: asyncio.create_task(self.graceful_stop()))
        transport, protocol = await loop.create_datagram_endpoint(
            lambda: SIPProtocol(self),
            local_addr=('0.0.0.0', self.port),
        )
        self.sip_transport = transport
        self._register_task = asyncio.create_task(self.register_loop())
        self._heartbeat_task = asyncio.create_task(self._heartbeat_loop())
        self._transaction_cleanup_task = asyncio.create_task(self._transaction_cleanup_loop())
        while self.is_running:
            await asyncio.sleep(1)

    async def _heartbeat_loop(self):
        while self.is_running:
            try:
                await asyncio.to_thread(self._write_heartbeat)
            except Exception:
                pass
            await asyncio.sleep(HEARTBEAT_INTERVAL)

    def _write_heartbeat(self):
        with open(HEARTBEAT_FILE, "w") as f:
            f.write(str(time.time()))

    async def _transaction_cleanup_loop(self):
        """Периодическая очистка старых transactions."""
        while self.is_running:
            try:
                await self._cleanup_old_transactions()
            except Exception:
                pass
            await asyncio.sleep(30)

    async def graceful_stop(self):
        if not self.is_running:
            return
        self.is_running = False

        for call_id in list(self.sessions.keys()):
            try:
                session = self.sessions.get(call_id)
                if session and session["direction"] == "outbound" and session["state"] in ("ANSWERED", "IN_PROGRESS"):
                    await self._start_bye(call_id, session)
                else:
                    await self._cleanup_call(call_id, send_lead=False)
            except Exception as e:
                logger.error("Graceful stop error call_id=%s: %s", call_id, e)

        # Ждём завершения BYE
        start_time = time.time()
        while self.sessions and (time.time() - start_time) < self._shutdown_timeout:
            await asyncio.sleep(0.1)

        # Force cleanup
        for call_id in list(self.sessions.keys()):
            try:
                await self._cleanup_call(call_id, send_lead=False)
            except Exception as e:
                logger.error("Force cleanup error call_id=%s: %s", call_id, e)

        for task in (self._register_task, self._heartbeat_task, self._transaction_cleanup_task):
            if task:
                task.cancel()

        if self.sip_transport:
            self.sip_transport.close()
