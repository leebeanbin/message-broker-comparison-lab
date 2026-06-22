"""
MCP 서버 독립 실행 (stdio 전송)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Claude Desktop 또는 Claude Code에서 직접 연결할 때 사용.

실행:
  uv run mcp_server.py

Claude Code .claude/settings.json 설정:
  {
    "mcpServers": {
      "broker-lab": {
        "type": "stdio",
        "command": "uv",
        "args": ["run", "mcp_server.py"],
        "cwd": "/path/to/message-broker-comparison-lab"
      }
    }
  }

Claude Desktop ~/Library/Application Support/Claude/claude_desktop_config.json:
  {
    "mcpServers": {
      "broker-lab": {
        "command": "uv",
        "args": ["run", "mcp_server.py"],
        "cwd": "/path/to/message-broker-comparison-lab"
      }
    }
  }

주의: 브로커가 실행 중이어야 합니다.
  docker compose up redis rabbitmq kafka -d
"""

import asyncio
import sys
from pathlib import Path

# 프로젝트 루트를 Python 경로에 추가
sys.path.insert(0, str(Path(__file__).parent))

from app.brokers import kafka_broker, rabbitmq_broker, redis_broker
from app.mcp.server import mcp


async def _connect_brokers() -> None:
    """브로커 연결 (연결 실패 시 해당 브로커 도구는 동작 안 할 수 있음)"""
    for broker in [redis_broker, rabbitmq_broker, kafka_broker]:
        try:
            await broker.connect()
        except Exception as e:
            print(f"[MCP] {broker.name} 연결 실패 (해당 도구 사용 불가): {e}", file=sys.stderr)


async def _disconnect_brokers() -> None:
    for broker in [redis_broker, rabbitmq_broker, kafka_broker]:
        try:
            await broker.disconnect()
        except Exception:
            pass


if __name__ == "__main__":
    async def _main():
        await _connect_brokers()
        try:
            await mcp.run_stdio_async()
        finally:
            await _disconnect_brokers()

    asyncio.run(_main())
