---
name: btc-client-deploy
description: Deploy the public BTC Bollinger observation and Binance Event Contract panel to a customer's server, including Tencent Cloud server-AI handoff, customer login, and live-control verification. Use for a new customer installation or repair.
---

# BTC 客户独立部署

从 [公开仓库](https://github.com/jonyjanytb-dev/btc-event-bollinger-panel) 的指定版本把 BTC 观察与布林带程序部署到客户自己的服务器。若用户要求复制某台现有服务器，先核对那台服务器实际运行的版本；不要仅凭仓库名称猜测生产版本。

## 工作边界

- 客户服务器、站点登录、观察数据、币安交易会话分开确认。站点账号只用于打开面板，不能代替币安登录。
- 可以沿用明确获准共享的**只读行情数据源**；客户的真实交易会话必须来自客户自己的币安账号。不要复制源服务器的下单凭据、数据库、登录 cookie 或密钥。
- 部署完成时真实交易保持关闭。客户可在面板设置周期、金额、方向和保护时间；只有他们明确要求或亲自在面板启动时才开启。
- 单次真实测试订单需要明确的品种、合约周期、方向和金额。提交只发一次；超时或结果未知时查账本，不重试。接口“已受理”不等于订单已在币安成交。
- 不覆盖客户现有服务。使用独立目录、服务名、端口和数据目录，并记录回滚点。

## 路线

1. 阅读仓库根目录 `README.md` 和 [部署与验收](references/deployment.md)：确认源版本、取得 SSH 访问、隔离安装、核对服务和公开入口。
2. 客户只有腾讯云助手或服务器 AI 时，使用 [给客户的指令模板](references/customer-messages.md) 获取服务器信息并安装本次生成的 SSH **公钥**。不得要求客户在聊天里发送私钥、币安 cookie 或验证码。
3. 客户需要真实交易时，阅读 [币安会话与交易控制](references/trading.md)：从客户本人完成的扫码登录取得同一 Web 会话的 `p20t` 和 `csrftoken`，只经 SSH 导入客户服务器；先验证只读状态，再开放交易控制。
4. 交付时报告：实际源版本、客户访问地址、登录方式、观察服务状态、币安会话状态、交易开关、订单账本和未验证事项。客户若报“启动没有打开”，先查桥接待核对请求和 `/api/live`，不要代替客户猜选交易参数。

## 适用性

此 skill 记录的是这套项目的部署不变量。路径、云厂商、域名、证书方式、策略版本、赔率和端口均以新客户现场证据为准；`shadow.live_api` 与 `shadow.live_worker` 当前将本机桥接地址固定为 `127.0.0.1:8876`，若要改端口需同步改代码。第三方逆向接口可能变化，使用前检查当前代码与币安实际页面或官方规则。
