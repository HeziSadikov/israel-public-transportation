"""Compatibility app module; canonical HTTP app lives in backend.mcp_server.transport.http."""

from backend.mcp_server.transport import http as _http

app = _http.app
create_app = _http.create_app

# Preserve historical `import app as app_mod` access patterns used by tests/tools.
for _name, _value in vars(_http).items():
    if _name.startswith("__"):
        continue
    globals().setdefault(_name, _value)
