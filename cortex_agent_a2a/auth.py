import jwt
import time
import hashlib
import base64
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.backends import default_backend


def load_private_key(private_key_path: str):
    with open(private_key_path, "rb") as key_file:
        private_key = serialization.load_pem_private_key(
            key_file.read(),
            password=None,
            backend=default_backend()
        )
    return private_key


def get_public_key_fingerprint(private_key) -> str:
    public_key = private_key.public_key()
    public_key_bytes = public_key.public_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PublicFormat.SubjectPublicKeyInfo
    )
    sha256_hash = hashlib.sha256(public_key_bytes).digest()
    fingerprint = base64.standard_b64encode(sha256_hash).decode("utf-8")
    return f"SHA256:{fingerprint}"


def generate_jwt_token(account_locator: str, user: str, private_key) -> str:
    fingerprint = get_public_key_fingerprint(private_key)
    qualified_username = f"{account_locator.upper()}.{user.upper()}"
    
    now = int(time.time())
    payload = {
        "iss": f"{qualified_username}.{fingerprint}",
        "sub": qualified_username,
        "iat": now,
        "exp": now + 3600,
    }
    
    token = jwt.encode(payload, private_key, algorithm="RS256")
    return token
