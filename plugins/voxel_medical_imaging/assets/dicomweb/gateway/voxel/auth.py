"""Snowflake session construction for the Voxel DICOMweb gateway.

=============================================================================
THIS MODULE IS THE AUTHORIZATION BOUNDARY
=============================================================================

With ``executeAsCaller: true`` the SPCS ingress proxy injects two headers into
every request:

    Sf-Context-Current-User          the caller's Snowflake username
    Sf-Context-Current-User-Token    a token representing that caller

A session built from ``<container token>.<caller token>`` runs as the CALLER, so
``CURRENT_USER()`` inside every query is the actual radiologist and row access
policies filter in the query engine.

A session built from the container token ALONE runs as the service OWNER, which
sees everything.

Both work. Only one is correct. If the caller token is missing -- dropped by a
misconfigured proxy, stripped by a load balancer, absent because the service was
created without ``executeAsCaller`` -- the naive implementation silently falls
back to owner rights and EVERY ROW ACCESS POLICY QUIETLY PASSES. No error, no
failed request, nothing in a log. A PHI disclosure that looks exactly like
success.

So this module REFUSES to build an owner-rights session on a request path. The
only way to get one is ``owner_session()``, which is used for startup health
checks and never for anything that reads patient data.
"""
from __future__ import annotations

import logging
import os
import threading
from dataclasses import dataclass

import snowflake.connector

log = logging.getLogger("voxel.auth")

SESSION_TOKEN_PATH = "/snowflake/session/token"
HDR_USER = "sf-context-current-user"
HDR_TOKEN = "sf-context-current-user-token"

# The router forwards these. See dicomweb_nginx_conf.j2 -- if that forwarding
# breaks, this is where it surfaces, as a 401 rather than as silent owner access.
_ACCOUNT = os.environ.get("SNOWFLAKE_ACCOUNT", "")
_HOST = os.environ.get("SNOWFLAKE_HOST", "")
_DATABASE = os.environ.get("VOXEL_DB", "")
_SCHEMA = os.environ.get("VOXEL_SCHEMA", "")
_WAREHOUSE = os.environ.get("VOXEL_WAREHOUSE", "")


class IdentityError(RuntimeError):
    """Raised when a request cannot be attributed to a caller.

    Deliberately distinct from a generic error so the route layer can map it to
    401 and never to 500. A 500 invites a retry; a 401 tells the truth.
    """


def _read_container_token() -> str:
    """Read the container's own OAuth token.

    Read fresh on every call rather than cached at import. The token rotates, and
    a cached one produces authentication failures that look like intermittent
    network problems hours after startup.
    """
    try:
        with open(SESSION_TOKEN_PATH) as fh:
            return fh.read().strip()
    except OSError as exc:
        raise IdentityError(
            f"container session token unreadable at {SESSION_TOKEN_PATH}: {exc}. "
            "This process is probably not running inside SPCS."
        ) from exc


@dataclass(frozen=True)
class Caller:
    """The authenticated end user for one request."""

    username: str
    token: str

    @classmethod
    def from_headers(cls, headers) -> "Caller":
        """Extract the caller from proxy-injected headers, or refuse.

        Header lookup is case-insensitive because HTTP header casing is not
        guaranteed through a proxy chain and matching on the exact casing
        Snowflake happens to use today is a latent break.
        """
        lowered = {k.lower(): v for k, v in headers.items()}
        user = lowered.get(HDR_USER)
        token = lowered.get(HDR_TOKEN)

        if not token:
            # The critical branch. Everything else in this file exists to make
            # sure this is the outcome rather than a silent owner-rights session.
            raise IdentityError(
                "no caller identity on this request. Either the service was not "
                "created with capabilities.securityContext.executeAsCaller = true, "
                "or the nginx router is not forwarding the Sf-Context-* headers. "
                "Refusing to fall back to owner rights."
            )
        return cls(username=user or "UNKNOWN", token=token)


def caller_session(caller: Caller):
    """Open a connection that runs AS THE CALLER.

    The concatenation is the whole mechanism:
        <container token> + "." + <caller token>
    Snowflake reads that as "this service, acting for this user".
    """
    login_token = f"{_read_container_token()}.{caller.token}"
    conn = snowflake.connector.connect(
        host=_HOST,
        account=_ACCOUNT,
        token=login_token,
        authenticator="oauth",
        database=_DATABASE,
        schema=_SCHEMA,
        warehouse=_WAREHOUSE,
        client_session_keep_alive=False,
        # Tag every query with the caller and request so QUERY_HISTORY.QUERY_TAG
        # joins back to the DICOMweb request that caused it. Without this,
        # investigating "who viewed this patient" means correlating timestamps.
        session_parameters={
            "QUERY_TAG": f'{{"voxel_caller":"{caller.username}"}}',
        },
    )
    return conn


def owner_session():
    """Open an OWNER-RIGHTS connection. NOT for request paths.

    Used only by the startup readiness check, which needs to confirm the catalog
    is reachable before any user arrives. Named explicitly so that a future
    reader can grep for it and audit every use site -- there should be exactly
    one.
    """
    conn = snowflake.connector.connect(
        host=_HOST,
        account=_ACCOUNT,
        token=_read_container_token(),
        authenticator="oauth",
        database=_DATABASE,
        schema=_SCHEMA,
        warehouse=_WAREHOUSE,
    )
    return conn


class SessionCache:
    """Short-lived per-caller connection reuse.

    A fresh Snowflake connection per HTTP request is untenable at frame-scroll
    rates -- a 500-slice series generates dozens of requests in a few seconds.

    But caching a SESSION is caching an AUTHORIZATION DECISION, which is exactly
    the mistake the incumbent makes by hashing the user's group list into a cache key.
    Two mitigations:

      1. The cache key is the caller TOKEN, not the username. A token change (new
         login, re-auth, expiry) is automatically a new key, so a revoked or
         re-scoped session cannot be reused.
      2. Entries expire on a short TTL regardless, so a privilege revocation
         takes effect within seconds rather than at process restart.

    Row access policies are still evaluated by the query engine on EVERY query,
    so this caches the connection, never the result of an access decision.
    """

    def __init__(self, ttl_seconds: int = 60, max_entries: int = 64):
        self._ttl = ttl_seconds
        self._max = max_entries
        self._lock = threading.Lock()
        self._entries: dict[str, tuple[float, object]] = {}

    def get(self, caller: Caller):
        import time

        now = time.monotonic()
        key = caller.token
        with self._lock:
            hit = self._entries.get(key)
            if hit and (now - hit[0]) < self._ttl:
                return hit[1]
            if hit:
                self._close(hit[1])
                self._entries.pop(key, None)

        conn = caller_session(caller)
        with self._lock:
            # Evict the oldest rather than growing unbounded. A busy service with
            # many concurrent radiologists must not accumulate connections.
            if len(self._entries) >= self._max:
                oldest = min(self._entries.items(), key=lambda kv: kv[1][0])[0]
                self._close(self._entries[oldest][1])
                self._entries.pop(oldest, None)
            self._entries[key] = (now, conn)
        return conn

    @staticmethod
    def _close(conn) -> None:
        try:
            conn.close()
        except Exception:  # noqa: BLE001 - closing must never raise
            pass

    def purge(self) -> None:
        with self._lock:
            for _, conn in self._entries.values():
                self._close(conn)
            self._entries.clear()
