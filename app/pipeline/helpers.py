def deterministic_uuid(value: str):
    import hmac
    from hashlib import sha256
    from uuid import UUID

    secret = b"my-unique-secret"
    h = hmac.new(secret, msg=str(value).encode("utf-8"), digestmod=sha256)
    # digest should be truncated to 128 bits = 16 bytes
    return str(UUID(bytes=h.digest()[:16], version=4))
