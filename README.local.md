## 测试

```txt
# 跑测试时，需要注释掉 CLICKHOUSE_MCP_SERVER_TRANSPORT/CLICKHOUSE_MCP_AUTH_DISABLED/CLICKHOUSE_CLUSTER_REGISTRY_ENABLED
# CLICKHOUSE_MCP_SERVER_TRANSPORT=http
# CLICKHOUSE_MCP_AUTH_DISABLED=true
CLICKHOUSE_HOST=xxx
CLICKHOUSE_PORT=8623
CLICKHOUSE_SECURE=false
CLICKHOUSE_VERIFY=false
CLICKHOUSE_USER=default
CLICKHOUSE_PASSWORD=xxx

# jd inner
CLICKHOUSE_CLUSTER_REGISTRY_ENABLED=true
```

## 运行

- `clickhouse-connect` 这个模块运行时不能有 proxy 代理的环境变量，否则无法连接集群。
- 日志等级调整成 WARNING：
  - `main.py`: `mcp.run(transport=transport, log_level="WARNING")`
  - `mcp_clickhouse.py`: `logging.basicConfig(level=logging.WARNING, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")`
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

```json
{
  "mcpServers": {
    "mcp-clickhouse": {
      "command": "/data0/xxx/miniconda3/envs/mcp-ck/bin/python",
      "args": ["-m", "mcp_clickhouse.main"],
      "env": {
        "PYTHONPATH": "/data0/xxx/ClickHouse/mcp-clickhouse",
        "CLICKHOUSE_HOST": "xxx",
        "CLICKHOUSE_PORT": "8623",
        "CLICKHOUSE_USER": "default",
        "CLICKHOUSE_PASSWORD": "xxx",
        "CLICKHOUSE_SECURE": "false",
        "CLICKHOUSE_VERIFY": "false"
      }
    }
  }
}
```

```log
2026-10-20 01:20:42.140 [info] [Solution] 命令 "/data0/xxx/miniconda3/envs/mcp-ck/bin/python" 未找到，请确保已安装并添加到系统 PATH 中
2026-10-20 01:20:42.140 [info] 启动服务器连接失败: Error: spawn /data0/xxx/miniconda3/envs/mcp-ck/bin/python ENOENT
2026-10-20 01:20:46.195 [info] 传输错误: spawn /data0/xxx/miniconda3/envs/mcp-ck/bin/python ENOENT
```

- 证据：日志是`spawn … ENOENT`，这意味着客户端尝试创建子进程时，操作系统报告找不到必要的路径，尚未进入 Python 项目代码。
- 远程实测表明：原解释器路径存在，符号链接目标存在，项目包也能定位。因此，“远程未安装 Python”不成立。最可能的原因是执行环境错位：项目位于远程，不代表 MCP 启动器也运行在远程。若启动器运行在本机，操作系统会在本机查找远程绝对路径，自然找不到。
- 日志中“添加到 PATH”只是通用提示；原命令使用绝对路径，修改 PATH 无法解决该路径在当前机器不存在的问题。但仅凭 ENOENT 不能完全证明启动器在本机：不存在的子进程工作目录、不同容器的文件系统或缺少可执行文件加载器也可能产生该错误。之前未获取启动器实际主机和工作目录，因此“本机执行远程路径”属于高概率判断，不是已证实结论。此时还未涉及数据库连接。
- 间接通过远程启动脚本的方式启动：

```json
{
  "mcpServers": {
    "mcp-clickhouse": {
      "command": "ssh",
      "args": [
        "-T",
        "-o",
        "BatchMode=yes",
        "-o",
        "ConnectTimeout=15",
        "201",
        "/data0/xxx/miniconda3/envs/mcp-ck/bin/python",
        "-u",
        "/data0/xxx/ClickHouse/mcp-clickhouse/.joycode/start_mcp.py"
      ],
      "env": {
        "PYTHONPATH": "/data0/xxx/ClickHouse/mcp-clickhouse",
        "CLICKHOUSE_HOST": "xxx",
        "CLICKHOUSE_PORT": "8623",
        "CLICKHOUSE_USER": "default",
        "CLICKHOUSE_PASSWORD": "xxx",
        "CLICKHOUSE_SECURE": "false",
        "CLICKHOUSE_VERIFY": "false"
      }
    }
  }
}
```

```py
# start_mcp.py
"""Start the stdio MCP server using the adjacent project configuration."""

import json
import os
from pathlib import Path
import runpy
import sys


def main():
    config_path = Path(__file__).resolve().with_name("mcp.json")
    root = config_path.parent.parent
    with config_path.open(encoding="utf-8") as config_file:
        config = json.load(config_file)
    os.environ.update(config["mcpServers"]["mcp-clickhouse"]["env"])
    os.environ["CLICKHOUSE_MCP_SERVER_TRANSPORT"] = "stdio"
    os.chdir(root)
    sys.path.insert(0, str(root))
    runpy.run_module("mcp_clickhouse.main", run_name="__main__")


if __name__ == "__main__":
    main()
```

### http

- `python -m mcp_clickhouse.main`
- 开启环境变量：`CLICKHOUSE_MCP_SERVER_TRANSPORT=http CLICKHOUSE_MCP_AUTH_DISABLED=true`
- 如果开启鉴权（`CLICKHOUSE_MCP_AUTH_DISABLED=false`），则需要在配置中添加`Authorization`

```json
// joycode
// ./.joycode/mcp.json
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

> joycode 插件是运行在笔记本上的，所以在 ssh 上运行的服务要进行端口转发。

## 其他

- stdio（默认使用）不需要认证。
- 如果用 HTTP/SSE 传输方式（例如远程部署），必须配置以下三种认证方式之一：
  - 静态 Bearer Token
  - OAuth/OIDC
  - 或显式禁用认证（仅限本地开发）
- 环境变量设置方式：可以放在 env 字段、系统环境变量、或 `.env` 文件中三种方式之一，效果相同。
- command vs url：command 用于 stdio 传输方式，agent 直接本地启动一个子进程与其通过标准输入输出通信；url 用于 HTTP/SSE 传输方式，连接一个已经独立运行、监听某端口的远程/本地 MCP 服务器。
