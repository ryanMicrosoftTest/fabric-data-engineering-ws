import sys
import pathlib
import struct

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent.parent))

from src.token_provider import pack_token_for_odbc, SQL_COPT_SS_ACCESS_TOKEN, WAREHOUSE_TOKEN_SCOPE


def test_constants():
    """Assert that the token constants have the correct values."""
    assert SQL_COPT_SS_ACCESS_TOKEN == 1256
    assert WAREHOUSE_TOKEN_SCOPE == "https://database.windows.net/.default"


def test_packing_format_known_token():
    """Test packing a known token 'abc' to verify UTF-16-LE encoding and LE-prefixed format."""
    result = pack_token_for_odbc("abc")
    
    # Token "abc" encoded as UTF-16-LE: b"a\x00b\x00c\x00" (6 bytes)
    token_bytes = "abc".encode("utf-16-le")
    assert token_bytes == b"a\x00b\x00c\x00"
    
    # Expected format: 4-byte little-endian uint with length, followed by token bytes
    expected = struct.pack("<I", 6) + b"a\x00b\x00c\x00"
    assert result == expected


def test_packing_empty_string():
    """Test packing an empty string token."""
    result = pack_token_for_odbc("")
    
    # Empty string should pack to just the 4-byte length field (0)
    expected = struct.pack("<I", 0)
    assert result == expected
    assert len(result) == 4


def test_packing_unicode():
    """Test packing a token with unicode characters."""
    result = pack_token_for_odbc("héllo")
    
    # "héllo" encoded as UTF-16-LE
    token_bytes = "héllo".encode("utf-16-le")
    
    # Extract the length prefix from result
    length_prefix = struct.unpack("<I", result[:4])[0]
    
    # Assert that the length prefix matches the encoded token length
    assert length_prefix == len(token_bytes)
    
    # Assert that the full result matches expected format
    expected = struct.pack("<I", len(token_bytes)) + token_bytes
    assert result == expected


def test_packing_realistic_jwt_length():
    """Test packing a realistic JWT-like token (1500 chars)."""
    fake_token = "x" * 1500
    result = pack_token_for_odbc(fake_token)
    
    # UTF-16-LE encoding doubles the byte length
    expected_byte_length = 1500 * 2
    
    # Check total result length
    assert len(result) == 4 + expected_byte_length
    
    # Check that the first 4 bytes decode to the correct length
    length_prefix = struct.unpack("<I", result[:4])[0]
    assert length_prefix == expected_byte_length
