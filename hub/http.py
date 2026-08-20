from __future__ import annotations

import requests

# Shared HTTP session for Supabase, SmiteSource, Resend, and maintenance tools.
# trust_env=False avoids local proxy/VPN environment settings unexpectedly
# changing production-like behavior during manual imports.
HTTP = requests.Session()
HTTP.trust_env = False
