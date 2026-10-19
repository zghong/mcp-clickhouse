## 测试

```txt
# 跑测试时，需要注释掉 CLICKHOUSE_MCP_SERVER_TRANSPORT/CLICKHOUSE_MCP_AUTH_DISABLED
# CLICKHOUSE_MCP_SERVER_TRANSPORT=http
# CLICKHOUSE_MCP_AUTH_DISABLED=true
CLICKHOUSE_HOST=127.0.0.1
CLICKHOUSE_PORT=8123
CLICKHOUSE_SECURE=false
CLICKHOUSE_VERIFY=false
CLICKHOUSE_USER=default
CLICKHOUSE_PASSWORD=
```

## 运行

- `clickhouse-connect` 这个模块运行时不能有 proxy 代理的环境变量，否则无法连接集群。
- 本地调试：
  - 用 MCP Inspector 测试：`npx -y @modelcontextprotocol/inspector python -m mcp_clickhouse.main`
  - API 测试：`curl -i http://127.0.0.1:8000/health`

### stdio

让 agent 自己启动一个本地线程：

```json
{
  "mcpServers": {
    "mcp-clickhouse": {
      "command": "python",
      "args": ["-m", "mcp_clickhouse.main"],
      "env": {
        "PYTHONPATH": "/绝对路径/项目",
        "CLICKHOUSE_HOST": "127.0.0.1",
        "CLICKHOUSE_PORT": "8123",
        "CLICKHOUSE_USER": "default",
        "CLICKHOUSE_PASSWORD": "",
        "CLICKHOUSE_SECURE": "false"
      }
    }
  }
}
```

### http

- `python -m mcp_clickhouse.main`
- 开启环境变量：`CLICKHOUSE_MCP_SERVER_TRANSPORT=http CLICKHOUSE_MCP_AUTH_DISABLED=true`
- 如果开启鉴权（`CLICKHOUSE_MCP_AUTH_DISABLED=false`），则需要在配置中添加`Authorization`

```json
// ./.mcp.json
{
  "mcpServers": {
    "mcp-clickhouse": {
      "type": "http",
      "url": "http://127.0.0.1:8000/mcp"
      // "headers": {
      //   "Authorization": "Bearer <some token>"
      // }
    }
  }
}
```

## 其他

- stdio（默认使用）不需要认证。
- 如果用 HTTP/SSE 传输方式（例如远程部署），必须配置以下三种认证方式之一：
  - 静态 Bearer Token
  - OAuth/OIDC
  - 或显式禁用认证（仅限本地开发）
- 环境变量设置方式：可以放在 env 字段、系统环境变量、或 `.env` 文件中三种方式之一，效果相同。
- command vs url：command 用于 stdio 传输方式，agent 直接本地启动一个子进程与其通过标准输入输出通信；url 用于 HTTP/SSE 传输方式，连接一个已经独立运行、监听某端口的远程/本地 MCP 服务器。
