"""
Multi-Provider AI Fallback Engine
==================================
Auto-selects the best available AI for dashboard Q&A.

Priority chain:
  1. OpenAI GPT-4o-mini         — primary paid API
  2. Ollama + Llama3             — free, local (offline)
  3. HuggingFace Inference API   — free cloud tier
  4. GPT4All (Phi-3 Mini)        — free, fully offline
  5. Anthropic Claude Haiku      — existing integration (final fallback)

Behaviour:
  • Auto-detects installed packages & configured API keys.
  • Falls through chain silently until a provider succeeds.
  • Background thread checks primary (OpenAI) every 5 minutes;
    silently restores it when it recovers.
  • Every response is prefixed with the 'Who is this?' identity banner.
  • All events logged to ai_fallback_log.json.

Install commands (run once in terminal):
  pip install openai                  → Provider 1 (OpenAI)
  pip install ollama                  → Provider 2 client (+ install Ollama app from ollama.com)
  pip install huggingface-hub         → Provider 3
  pip install gpt4all                 → Provider 4
  pip install anthropic               → Provider 5 (already installed)
"""

from __future__ import annotations

import datetime
import json
import os
import threading
import time
from pathlib import Path
from typing import Optional

BASE_DIR  = Path(__file__).parent
LOG_FILE  = BASE_DIR / "ai_fallback_log.json"
CFG_FILE  = BASE_DIR / "ai_fallback_config.json"

MAX_TOKENS          = 1024
MONITOR_INTERVAL    = 300   # 5 minutes in seconds
OLLAMA_ENDPOINT     = "http://localhost:11434"
HF_MODEL            = "HuggingFaceH4/zephyr-7b-beta"
GPT4ALL_MODEL       = "Phi-3-mini-4k-instruct.Q4_0.gguf"
OPENAI_MODEL        = "gpt-4o-mini"
CLAUDE_MODEL        = "claude-haiku-4-5-20251001"

# ── Provider registry ─────────────────────────────────────────────────────────
#   Each entry describes one AI provider in priority order.

PROVIDER_CHAIN: list[dict] = [
    {
        "id":          "openai",
        "name":        "OpenAI GPT-4o-mini",
        "short":       "OpenAI",
        "type":        "Paid — Primary",
        "icon":        "🤖",
        "pkg":         "openai",
        "needs_key":   True,
        "env_key":     "OPENAI_API_KEY",
        "cfg_key":     "openai_api_key",
        "install_cmd": "pip install openai",
        "setup_note":  "Get key at: https://platform.openai.com/api-keys",
    },
    {
        "id":          "ollama",
        "name":        "Ollama Llama3",
        "short":       "Ollama",
        "type":        "Free — Local Offline",
        "icon":        "🦙",
        "pkg":         "ollama",
        "needs_key":   False,
        "env_key":     "",
        "cfg_key":     "",
        "install_cmd": "pip install ollama",
        "setup_note":  "1) Install Ollama app from ollama.com  2) Run: ollama pull llama3",
    },
    {
        "id":          "huggingface",
        "name":        "HuggingFace Zephyr-7B",
        "short":       "HuggingFace",
        "type":        "Free — Cloud API",
        "icon":        "🤗",
        "pkg":         "huggingface_hub",
        "needs_key":   False,   # Works without token (rate-limited)
        "env_key":     "HUGGINGFACE_TOKEN",
        "cfg_key":     "huggingface_token",
        "install_cmd": "pip install huggingface-hub",
        "setup_note":  "Optional token at huggingface.co/settings/tokens (increases rate limits)",
    },
    {
        "id":          "gpt4all",
        "name":        "GPT4All Phi-3 Mini",
        "short":       "GPT4All",
        "type":        "Free — Fully Offline",
        "icon":        "💻",
        "pkg":         "gpt4all",
        "needs_key":   False,
        "env_key":     "",
        "cfg_key":     "",
        "install_cmd": "pip install gpt4all",
        "setup_note":  "First run downloads ~2 GB model automatically to ~/.cache/gpt4all/",
    },
    {
        "id":          "claude",
        "name":        "Anthropic Claude Haiku",
        "short":       "Claude",
        "type":        "Paid — Final Fallback",
        "icon":        "🔮",
        "pkg":         "anthropic",
        "needs_key":   True,
        "env_key":     "ANTHROPIC_API_KEY",
        "cfg_key":     "anthropic_api_key",
        "install_cmd": "pip install anthropic",
        "setup_note":  "Get key at: https://console.anthropic.com",
    },
]

# ── Shared mutable state (thread-safe) ───────────────────────────────────────

_lock = threading.RLock()
_G: dict = {
    "active_idx":      0,      # current position in PROVIDER_CHAIN
    "last_errors":     {},     # {provider_id: last_error_message}
    "monitor_started": False,
}

# ══════════════════════════════════════════════════════════════════════════════
# CONFIG & KEY MANAGEMENT
# ══════════════════════════════════════════════════════════════════════════════

def _load_cfg() -> dict:
    if CFG_FILE.exists():
        try:
            return json.loads(CFG_FILE.read_text(encoding="utf-8"))
        except Exception:
            pass
    return {}

def _save_cfg(data: dict):
    CFG_FILE.write_text(json.dumps(data, indent=2, ensure_ascii=False), encoding="utf-8")

def get_api_key(provider_id: str) -> str:
    p = next((x for x in PROVIDER_CHAIN if x["id"] == provider_id), None)
    if not p:
        return ""
    env_key = p.get("env_key", "")
    if env_key:
        val = os.environ.get(env_key, "").strip()
        if val:
            return val
    cfg_key = p.get("cfg_key", "")
    if cfg_key:
        return _load_cfg().get(cfg_key, "").strip()
    return ""

def save_api_key(provider_id: str, key: str):
    p = next((x for x in PROVIDER_CHAIN if x["id"] == provider_id), None)
    if not p:
        return
    cfg_key = p.get("cfg_key", "")
    if cfg_key:
        data = _load_cfg()
        data[cfg_key] = key.strip()
        _save_cfg(data)

# ══════════════════════════════════════════════════════════════════════════════
# PACKAGE DETECTION
# ══════════════════════════════════════════════════════════════════════════════

def _pkg_ok(pkg: str) -> bool:
    try:
        __import__(pkg)
        return True
    except ImportError:
        return False

def _ollama_running() -> bool:
    """Check if Ollama daemon is live on localhost."""
    try:
        import urllib.request
        urllib.request.urlopen(f"{OLLAMA_ENDPOINT}/api/tags", timeout=2)
        return True
    except Exception:
        return False

def get_provider_status() -> list[dict]:
    """Return status dict for all providers (used by UI)."""
    rows = []
    for p in PROVIDER_CHAIN:
        pkg_ok  = _pkg_ok(p["pkg"])
        has_key = bool(get_api_key(p["id"])) if p["needs_key"] else True
        daemon  = _ollama_running() if p["id"] == "ollama" else True
        ready   = pkg_ok and has_key and daemon
        with _lock:
            last_err  = _G["last_errors"].get(p["id"], "")
            is_active = PROVIDER_CHAIN.index(p) == _G["active_idx"]
        rows.append({
            **p,
            "pkg_installed": pkg_ok,
            "has_key":       has_key,
            "daemon_ok":     daemon,
            "ready":         ready,
            "is_active":     is_active,
            "last_error":    last_err,
        })
    return rows

# ══════════════════════════════════════════════════════════════════════════════
# PROVIDER QUERY FUNCTIONS
# ══════════════════════════════════════════════════════════════════════════════

def _query_openai(question: str, context: str, key: str) -> str:
    """Query OpenAI GPT-4o-mini. Tries SDK first, then falls back to requests.post."""
    messages = [
        {"role": "system", "content": (
            "You are a Bitumen Sales Dashboard AI assistant for PPS Anantams Logistics. "
            "Answer ONLY from the dashboard data provided. Use Indian number formatting "
            "(₹ crore/lakh). Dates: DD-MM-YYYY. Be concise but complete.\n\n"
            f"LIVE DASHBOARD DATA:\n{context[:6000]}"
        )},
        {"role": "user", "content": question},
    ]

    # --- Try OpenAI SDK (if installed) ---
    try:
        import openai as _oai
        client = _oai.OpenAI(api_key=key)
        resp = client.chat.completions.create(
            model=OPENAI_MODEL,
            messages=messages,
            max_tokens=MAX_TOKENS,
            temperature=0.3,
        )
        return resp.choices[0].message.content.strip()
    except ImportError:
        pass  # SDK not installed — fall through to requests

    # --- HTTP requests fallback (no SDK needed) ---
    import requests as _req
    headers = {
        "Authorization": f"Bearer {key}",
        "Content-Type":  "application/json",
    }
    payload = {
        "model":       OPENAI_MODEL,
        "messages":    messages,
        "max_tokens":  MAX_TOKENS,
        "temperature": 0.3,
    }
    r = _req.post(
        "https://api.openai.com/v1/chat/completions",
        headers=headers,
        json=payload,
        timeout=30,
    )
    r.raise_for_status()
    return r.json()["choices"][0]["message"]["content"].strip()


def _query_ollama(question: str, context: str) -> str:
    import ollama
    resp = ollama.chat(
        model="llama3",
        messages=[
            {"role": "system", "content": (
                "You are a Bitumen Sales Dashboard AI. Answer from this data:\n"
                f"{context[:3000]}"
            )},
            {"role": "user", "content": question},
        ],
    )
    # ollama >= 0.2.0 returns a Pydantic object; older returns a dict
    if hasattr(resp, "message"):
        return resp.message.content.strip()
    return resp["message"]["content"].strip()


def _query_huggingface(question: str, context: str, token: str) -> str:
    from huggingface_hub import InferenceClient
    client = InferenceClient(
        model=HF_MODEL,
        token=token if token else None,
    )
    # Mistral/Zephyr instruction format
    prompt = (
        f"<|system|>\nYou are a Bitumen Sales Dashboard AI for PPS Anantams Logistics. "
        f"Answer ONLY from this data:\n{context[:2500]}</s>\n"
        f"<|user|>\n{question}</s>\n"
        f"<|assistant|>\n"
    )
    result = client.text_generation(
        prompt,
        max_new_tokens=512,
        temperature=0.3,
        stop_sequences=["</s>", "<|user|>"],
    )
    return result.strip()


def _query_gpt4all(question: str, context: str) -> str:
    from gpt4all import GPT4All
    # allow_download=True fetches the model on first run (~2 GB)
    model = GPT4All(GPT4ALL_MODEL, allow_download=True)
    with model.chat_session(
        system_prompt=(
            "You are a Bitumen Sales Dashboard AI. "
            f"Answer only from this data:\n{context[:2000]}"
        )
    ):
        return model.generate(question, max_tokens=512)


def _query_claude(question: str, context: str, key: str) -> str:
    import anthropic
    client = anthropic.Anthropic(api_key=key)
    resp = client.messages.create(
        model=CLAUDE_MODEL,
        max_tokens=MAX_TOKENS,
        system=(
            "You are a Bitumen Sales Dashboard AI for PPS Anantams Logistics. "
            "Answer ONLY from the data provided. Indian format: ₹ crore/lakh, DD-MM-YYYY.\n\n"
            f"LIVE DASHBOARD DATA:\n{context[:5000]}"
        ),
        messages=[{"role": "user", "content": question}],
    )
    return resp.content[0].text.strip()


def _run_provider(pid: str, question: str, context: str) -> tuple[str, Optional[str]]:
    """
    Call the appropriate provider. Returns (answer, error_or_None).
    """
    key = get_api_key(pid)
    try:
        if pid == "openai":
            if not key:
                return "", "OpenAI API key not configured"
            return _query_openai(question, context, key), None

        elif pid == "ollama":
            if not _ollama_running():
                return "", "Ollama daemon not running (start the Ollama app first)"
            return _query_ollama(question, context), None

        elif pid == "huggingface":
            return _query_huggingface(question, context, key), None

        elif pid == "gpt4all":
            return _query_gpt4all(question, context), None

        elif pid == "claude":
            if not key:
                return "", "Anthropic API key not configured"
            return _query_claude(question, context, key), None

    except Exception as exc:
        return "", str(exc)

    return "", f"Unknown provider: {pid}"

# ══════════════════════════════════════════════════════════════════════════════
# CORE FALLBACK LOGIC
# ══════════════════════════════════════════════════════════════════════════════

def ask_with_fallback(question: str, context: str = "", start_from_primary: bool = False) -> dict:
    """
    Try providers in priority order starting from the current active provider
    (or from provider 0 / primary if start_from_primary=True).

    Returns:
      {
        answer, provider_id, provider_name, provider_type, provider_icon,
        fallback_reason, tried, error
      }
    """
    with _lock:
        start_idx = 0 if start_from_primary else _G["active_idx"]

    tried: list[dict] = []

    for offset in range(len(PROVIDER_CHAIN)):
        idx = (start_idx + offset) % len(PROVIDER_CHAIN)
        p   = PROVIDER_CHAIN[idx]

        # Skip if package not installed
        if not _pkg_ok(p["pkg"]):
            tried.append({"id": p["id"], "name": p["name"],
                          "reason": f"Package '{p['pkg']}' not installed. Run: {p['install_cmd']}"})
            continue

        # Skip if needs key but key missing
        if p["needs_key"] and not get_api_key(p["id"]):
            tried.append({"id": p["id"], "name": p["name"],
                          "reason": f"API key not configured for {p['name']}"})
            continue

        answer, err = _run_provider(p["id"], question, context)
        if err:
            with _lock:
                _G["last_errors"][p["id"]] = err
            tried.append({"id": p["id"], "name": p["name"], "reason": err})
            _log("fallback", p["id"], f"Failed — {err}")
            continue

        # ── SUCCESS ─────────────────────────────────────────────────────────
        with _lock:
            _G["active_idx"] = idx
            _G["last_errors"].pop(p["id"], None)

        fallback_reason = ""
        if tried:
            fallback_reason = " → ".join(f'{t["name"]} ({t["reason"]})' for t in tried)

        _log("success", p["id"], f"Answered: {question[:60]}")

        return {
            "answer":          answer,
            "provider_id":     p["id"],
            "provider_name":   p["name"],
            "provider_type":   p["type"],
            "provider_icon":   p["icon"],
            "fallback_reason": fallback_reason,
            "tried":           tried,
            "error":           None,
        }

    # ── ALL FAILED ───────────────────────────────────────────────────────────
    _log("all_failed", "none", f"All providers failed for: {question[:60]}")
    return {
        "answer": (
            "⚠️ All AI providers are currently unavailable.\n\n"
            "**To activate a provider:**\n"
            "• OpenAI: enter API key in sidebar\n"
            "• Ollama: install from ollama.com then run `ollama pull llama3`\n"
            "• HuggingFace: `pip install huggingface-hub`\n"
            "• GPT4All: `pip install gpt4all`\n"
            "• Claude: enter Anthropic API key in sidebar"
        ),
        "provider_id":     "none",
        "provider_name":   "None Available",
        "provider_type":   "—",
        "provider_icon":   "❌",
        "fallback_reason": "All providers failed",
        "tried":           tried,
        "error":           "all_failed",
    }


def format_who_response(result: dict) -> str:
    """
    Wrap the raw AI answer in the standard 'Who is this?' identity banner.
    """
    name  = result["provider_name"]
    ptype = result["provider_type"]
    icon  = result["provider_icon"]
    tried = result.get("tried", [])
    ts    = datetime.datetime.now().strftime("%d-%m-%Y %H:%M IST")

    # Build status line
    if result["provider_id"] == "openai" and not tried:
        status_line = "✅ Paid OpenAI API is **active and responding**."
    elif tried:
        # Find primary failure reason
        primary_fail = next((t["reason"] for t in tried if t["id"] == "openai"), "")
        if primary_fail:
            status_line = (
                f"⚠️ Paid OpenAI API is **unavailable** ({primary_fail}). "
                f"Auto-switched to **{name}** (free fallback). "
                "Will restore silently when OpenAI recovers."
            )
        else:
            status_line = f"⚠️ Switched to **{name}** — higher-priority providers unavailable."
    else:
        status_line = f"ℹ️ Serving via **{ptype}**."

    header = (
        f"{icon} **Who is this?** I am **{name} — Free Dashboard AI** "
        f"({ptype}).\n{status_line}"
    )

    if result.get("error") == "all_failed":
        return f"{header}\n\n{result['answer']}"

    return (
        f"{header}\n\n"
        f"---\n\n"
        f"{result['answer']}\n\n"
        f"---\n"
        f"_Active AI: {name} | {ptype} | {ts}_"
    )

# ══════════════════════════════════════════════════════════════════════════════
# BACKGROUND MONITOR — silently restores primary every 5 minutes
# ══════════════════════════════════════════════════════════════════════════════

def _monitor_loop():
    while True:
        time.sleep(MONITOR_INTERVAL)
        with _lock:
            if _G["active_idx"] == 0:
                continue   # Already on primary — nothing to do
        # Test if primary (OpenAI) is back
        primary = PROVIDER_CHAIN[0]
        if not _pkg_ok(primary["pkg"]):
            continue
        key = get_api_key(primary["id"])
        if not key:
            continue
        _, err = _run_provider("openai", "Reply with exactly: OK", "Dashboard health check.")
        if not err:
            with _lock:
                _G["active_idx"] = 0
            _log("restored", "openai",
                 "Primary OpenAI API recovered — silently switched back from fallback")


def start_monitor():
    """Start the background monitor thread (idempotent)."""
    with _lock:
        if _G["monitor_started"]:
            return
        _G["monitor_started"] = True
    t = threading.Thread(target=_monitor_loop, daemon=True, name="AIFallbackMonitor")
    t.start()

# ══════════════════════════════════════════════════════════════════════════════
# LOGGING
# ══════════════════════════════════════════════════════════════════════════════

def _log(event: str, provider: str, message: str):
    record = {
        "ts":       datetime.datetime.now().strftime("%d-%m-%Y %H:%M:%S IST"),
        "event":    event,
        "provider": provider,
        "message":  message,
    }
    try:
        logs: list = []
        if LOG_FILE.exists():
            logs = json.loads(LOG_FILE.read_text(encoding="utf-8"))
        logs.append(record)
        if len(logs) > 1000:
            logs = logs[-1000:]
        LOG_FILE.write_text(json.dumps(logs, indent=2, ensure_ascii=False), encoding="utf-8")
    except Exception:
        pass


def get_logs(n: int = 100) -> list[dict]:
    if not LOG_FILE.exists():
        return []
    try:
        data = json.loads(LOG_FILE.read_text(encoding="utf-8"))
        return list(reversed(data[-n:]))
    except Exception:
        return []


def get_active_provider() -> dict:
    with _lock:
        return PROVIDER_CHAIN[_G["active_idx"]]


def force_provider(provider_id: str) -> bool:
    """Manually override the active provider (for testing/UI)."""
    for i, p in enumerate(PROVIDER_CHAIN):
        if p["id"] == provider_id:
            with _lock:
                _G["active_idx"] = i
            _log("manual_override", provider_id, f"User manually switched to {p['name']}")
            return True
    return False
