"""Реальные тесты для WOLTRON Voice AI."""

import pytest
import json

from app.agent import Agent
from app.models import CallRecord, CallResult
from app.bot import clean_phone


class TestPhoneNormalization:
    def test_normalize_plus7(self):
        assert clean_phone("+79991234567") == "79991234567"

    def test_normalize_8(self):
        assert clean_phone("89991234567") == "79991234567"

    def test_normalize_7(self):
        assert clean_phone("79991234567") == "79991234567"

    def test_normalize_with_spaces(self):
        assert clean_phone("+7 999 123 45 67") == "79991234567"

    def test_normalize_with_dashes(self):
        assert clean_phone("+7-999-123-45-67") == "79991234567"

    def test_normalize_10_digits(self):
        assert clean_phone("9991234567") == "79991234567"

    def test_normalize_invalid(self):
        assert clean_phone("abc") == ""
        assert clean_phone("123") == ""
        assert clean_phone("") == ""

    def test_normalize_sip_injection(self):
        assert clean_phone("sip:evil@host") == ""
        assert clean_phone("+7999@evil") == ""
        assert clean_phone(";transport=tcp") == ""


class TestModels:
    def test_call_record_creation(self):
        record = CallRecord(
            id="test-id",
            direction="inbound",
            scenario="BEFORE_LESSON",
            phone="+79001234567",
        )
        assert record.id == "test-id"
        assert record.direction == "inbound"
        assert record.status == "CREATED"

    def test_call_result_serialization(self):
        result = CallResult(
            call_id="test-call-id",
            direction="outbound",
            scenario="BEFORE_LESSON",
            phone="+79001234567",
            name="Иван",
            interest="high",
            lesson_status="scheduled",
            next_step="Перезвонить завтра",
            objections=None,
            summary="Клиент заинтересован",
            transcript="Ассистент: Здравствуйте\nКлиент: Здравствуйте",
        )
        
        json_str = result.to_json()
        assert "test-call-id" in json_str
        assert "Иван" in json_str
        
        restored = CallResult.from_json(json_str)
        assert restored.call_id == result.call_id
        assert restored.name == result.name

    def test_call_result_null_fields(self):
        result = CallResult(
            call_id="test",
            direction="inbound",
            scenario="BEFORE_LESSON",
            phone="+79001234567",
        )
        data = result.to_dict(include_none=False)
        assert "name" not in data
        assert "objections" not in data


class TestSIPWorker:
    def test_parse_contact_uri(self):
        from app.sip_worker import parse_contact_uri
        
        assert parse_contact_uri("<sip:user@host>") == "sip:user@host"
        assert parse_contact_uri("sip:user@host") == "sip:user@host"
        assert parse_contact_uri("<sip:user@host;transport=udp>") == "sip:user@host;transport=udp"
        assert parse_contact_uri("") is None
        assert parse_contact_uri("http://not-sip") is None

    def test_rtp_seq_lt(self):
        from app.sip_worker import rtp_seq_lt
        
        assert rtp_seq_lt(100, 200) is True
        assert rtp_seq_lt(200, 100) is False
        assert rtp_seq_lt(100, 100) is False
        assert rtp_seq_lt(65535, 0) is True
        assert rtp_seq_lt(0, 65535) is False


class TestRateLimiter:
    def test_rate_limiter(self):
        from app.sip_worker import RateLimiter
        
        limiter = RateLimiter(max_per_minute=3)
        
        assert limiter.allow("1.2.3.4") is True
        assert limiter.allow("1.2.3.4") is True
        assert limiter.allow("1.2.3.4") is True
        assert limiter.allow("1.2.3.4") is False
        assert limiter.allow("5.6.7.8") is True


class TestSDP:
    def test_parse_sdp(self):
        from app.sip_worker import SIPWorker
        
        worker = SIPWorker.__new__(SIPWorker)
        
        sdp = """v=0
o=- 123 1 IN IP4 1.2.3.4
s=-
c=IN IP4 1.2.3.4
t=0 0
m=audio 10000 RTP/AVP 0 8
a=rtpmap:0 PCMU/8000
a=rtpmap:8 PCMA/8000
a=sendrecv
"""
        
        ip, port, codec = worker.parse_sdp_remote_media(sdp)
        assert ip == "1.2.3.4"
        assert port == 10000
        assert codec in (0, 8)

    def test_parse_sdp_inactive(self):
        from app.sip_worker import SIPWorker
        
        worker = SIPWorker.__new__(SIPWorker)
        
        sdp = """v=0
o=- 123 1 IN IP4 1.2.3.4
s=-
c=IN IP4 1.2.3.4
t=0 0
m=audio 10000 RTP/AVP 0 8
a=inactive
"""
        
        ip, port, codec = worker.parse_sdp_remote_media(sdp)
        assert ip is None
        assert port is None
        assert codec is None
