#!/usr/bin/env bash
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TARGET="${PWD}"

if [ "$TARGET" = "$SCRIPT_DIR" ]; then
  echo "Run this from your project root, not from inside kumo-coding-agent:"
  echo "  cd your-project && bash kumo-coding-agent/setup.sh"
  exit 1
fi

# ── Claude Code permissions ──────────────────────────────────────────────────
mkdir -p "$TARGET/.claude"
if [ -f "$TARGET/.claude/settings.json" ]; then
  echo "! .claude/settings.json already exists — skipping (merge permissions manually if needed)"
else
  cat > "$TARGET/.claude/settings.json" << 'EOF'
{
  "permissions": {
    "allow": [
      "Bash(.venv/bin/*)",
      "Bash(ls .venv/bin/*)",
      "Bash(echo *)",
      "Bash(python -m venv .venv)",
      "Bash(python3 -m venv .venv)"
    ]
  }
}
EOF
  echo "+ Created .claude/settings.json"
fi

# ── Codex permissions ────────────────────────────────────────────────────────
mkdir -p "$TARGET/.codex"
if [ -f "$TARGET/.codex/config.toml" ]; then
  echo "! .codex/config.toml already exists — skipping"
else
  cat > "$TARGET/.codex/config.toml" << 'EOF'
approval_policy = "on-request"
sandbox_mode    = "workspace-write"
EOF
  echo "+ Created .codex/config.toml"
fi

echo ""
echo "Done. Restart Claude Code or Codex to apply."
