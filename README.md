# BTC 事件合约布林带面板

用于在独立的 Ubuntu 服务器上运行 BTCUSDT 行情观察、盘中双触布林带信号、模拟记录和可手动启用的币安事件合约自动交易。真实交易**初始关闭**；客户必须使用自己的币安 Web 会话，并在面板中选择金额、合约周期与方向。仓库不包含任何客户数据、密码、SSH 私钥或交易 cookie。

> 目前下单桥接使用币安网页的非公开接口，接口和登录流程可能变化。`configured` 模式的赔率是管理员填入的请求参数，不是实时报价；接口“已受理”也不是成交或结算证明。币安的[事件合约说明](https://www.binance.com/en/support/faq/detail/172247f1c4b8401ab106fb63900f3825)列有合约期限、最低金额和风险规则。

## 内容

- `app/`：行情采集、观察面板、站点认证、交易控制、私有下单桥接和测试。
- `chart-strategy/`：支持选择 1m、5m、10m、15m、30m 和 1h 多周期共同触及布林带的盘中观察器。
- `scripts/configure.py`：为每台服务器生成私有配置、systemd 用户服务与 Caddy 配置，不覆盖已有配置。
- `scripts/capture_binance_session.py`：可选的本地可见浏览器扫码辅助工具；仅在客户同意并亲自扫码时使用。
- `skills/btc-client-deploy/`：给 Codex 使用的客户部署 skill，包括给腾讯云服务器 AI 的指令模板。

使用 Codex 的部署者可以将 `skills/btc-client-deploy` 复制到自己的 `~/.codex/skills/`，随后用 `$btc-client-deploy` 发起新客户安装。给客户服务器 AI 的可复制消息见 [指令模板](skills/btc-client-deploy/references/customer-messages.md)。

## Ubuntu 24.04 部署

在一台由你控制的服务器上准备 Python 3.12、`python3-venv`、公网 HTTPS 和足够磁盘空间。部署目录应是绝对路径且不含空格。若服务器已经在运行其他网站，先检查端口和现有 Caddy/Nginx 配置；不要直接覆盖它们。面板内部监听 `127.0.0.1:8765`、登录服务 `127.0.0.1:8766`、桥接 `127.0.0.1:8876`，这些端口不可被公网直接访问。

```bash
git clone https://github.com/jonyjanytb-dev/btc-event-bollinger-panel.git
cd btc-event-bollinger-panel
python3 -m venv venv
venv/bin/pip install -r app/requirements.txt -r app/requirements-strategy.txt
python3 scripts/configure.py --base "$PWD" --origin https://YOUR_DOMAIN_OR_IP --username YOUR_PANEL_USER --max-amount 30
```

最后一条命令会交互式询问**面板密码**，不会把密码放进 shell 命令。它生成的 `private/`、`data/`、`deploy/` 已被 `.gitignore` 排除。默认不配置赔率，桥接会把所有交易周期标为不可用，直到管理员核对后为实际需要的周期配置比例。`--max-amount` 是单笔 USDT 上限，不会自动启用交易。

### OpenMarket 只读数据

完整观察功能需要经授权的 OpenMarket `mcp:read` OAuth 凭据。可在**有浏览器的可信电脑**上使用同一仓库执行：

```bash
PYTHONPATH=app venv/bin/python -m shadow.bootstrap --credentials "$HOME/openmarket-readonly.json"
```

授权后通过 SSH 把该文件传到服务器的 `private/openmarket.json`，确保属主为运行服务的用户、权限为 `0600`。也可使用已获许可共享的只读凭据。不要上传源账号的交易凭据。初始配置关闭模型预测，仅运行币安指数和布林带观察；行情与时钟健康度仍需核查。

### 启动服务和 HTTPS

将 `deploy/btc-panel-*.service` 安装到运行用户的 `~/.config/systemd/user/`，执行 `systemctl --user daemon-reload`，再依次启动 `btc-panel-shadow`、`btc-panel-chart`、`btc-panel-auth`、`btc-panel-bridge`、`btc-panel-live-worker`、`btc-panel-web`。若需退出 SSH 后继续运行，管理员可为该用户启用 linger。所有交易服务启动后开关仍为关闭。

`deploy/Caddyfile` 是由 `--origin` 生成的 HTTPS 站点模板。先运行 `caddy validate --config deploy/Caddyfile`，再将站点块接入现有 Caddy；如已有反向代理，应只新增对应站点，保留原有配置。公网放通 443，按证书签发方式决定是否需要 80。无域名时可尝试支持 IP 证书的 Caddy 配置；若证书无法签发，先使用 SSH 隧道验证，不要让密码登录和交易控制暴露在明文 HTTP 上。

验收：未登录访问 `/live` 应跳转 `/login`；本机读取 `/api/live` 应返回 `enabled: false`、空订单账本；行情数据库应更新，图表观察器应产生可读记录。`journalctl --user -u btc-panel-chart -u btc-panel-live-worker` 可检查运行错误。

## 客户币安登录与真实交易

站点账号与币安账号完全独立。客户可在自己的可信浏览器完成币安登录；可选辅助脚本需要 Playwright：

交易页支持固定金额或自定义三档亏损阶梯。阶梯按指数代理结果自动推进；代理结果与币安真实成交、结算可能不同，结果缺失时会等待而不继续推进。该选项不会自动启用真实交易。

```bash
venv/bin/pip install -r scripts/requirements-login.txt
venv/bin/python -m playwright install chromium
venv/bin/python scripts/capture_binance_session.py --output "$HOME/binance-customer-session.json"
```

客户亲自在币安 App 扫码确认后，脚本只在同一 Web 会话取得 `p20t` 与 `csrftoken` 时写出本地 `0600` 文件。通过 SSH 把它传到客户服务器 `private/binance-session.json`，设置权限 `0600`，并从可信电脑清除临时输出。不要通过聊天、网页表单或 Git 传输 cookie。币安 App 的“新设备已授权”通知本身不证明 Web 会话已经可用。

若要开放具体周期，管理员须先核对当前币安页面的赔率参数，再在首次配置时传入 `--ratio-10`、`--ratio-30` 或 `--ratio-60`；已部署实例需谨慎编辑 `private/bridge.json` 并重启桥接。比例变化可能使请求被拒绝。交易开关由客户在面板按自己选择的参数手动启用。

测试真实订单必须单独明确币种、周期、方向、金额；只提交一次。接口回执为 `ACCEPTED_UNVERIFIED` 时，客户仍须在币安 App 订单记录中核对。先前手动测试单待核对会导致 `BRIDGE_RECONCILE_REQUIRED` 并阻止自动交易；确认后才 acknowledge。关闭自动交易不会撤销已提交的合约。

## 测试与许可

```bash
PYTHONPATH=app venv/bin/python -m unittest discover -s app/tests
PYTHONPATH=app venv/bin/python -m unittest discover -s chart-strategy
```

项目代码按 [MIT License](LICENSE) 发布。图表依赖 Lightweight Charts 的许可见 [随附许可](app/shadow/static/LIGHTWEIGHT-CHARTS-LICENSE.txt)。
