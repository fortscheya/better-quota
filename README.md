# better-quota

面向 CLIProxyAPI **v8.0.57、单进程、Linux arm64** 的 Go C ABI 插件。它统一实现 Codex 额度路由、凭证并发、临期 reset credit 兑换和可选定时预热。CPA 继续处理凭证合法性、实际执行及原有重试；插件不修改 CPA 核心，也不写凭证 weight、Priority 或 token 文件。

当前版本 **0.1.9** 包含凭证并发上限、量化权重、sticky/parent 绑定和持久优先目标管理。宿主要求的仓库元数据引用 CPA 上游；本地插件尚无独立公共仓库。

## 行为

- 每 30 秒发现所有 Codex 凭证并发布评分。usage 采集采用有界并发、每凭证去重和 30 秒基准间隔，失败指数退避到 480 秒；采集与评分独立。大账号池或上游失败时不能保证所有额度都在 30 秒内更新，状态接口显示快照时间和错误。
- 路由策略有两种：`capacity_weighted`（界面显示“按额度”）：最长实际窗口的 `C × remainingFraction`；`capacity_reset`（界面显示“按额度和重置时间”）：各实际窗口 `C × remainingFraction / max(300, secondsUntilReset)` 的最小值。`C` 默认 1，可按凭证和窗口手工校准。相同百分比不代表不同套餐的相同绝对容量。
- 普通纯 Codex 请求先复用同模型、同 caller scope 的有效 sticky/parent binding；没有绑定时按只读的 0–10000 量化权重排序取前三，再按 `(active + reserved) / effective_limit` 的最低利用率选择。权重只决定候选排名，不形成严格流量比例；跨模型视为新会话。
- 每个账号可设置一个持久的一次性优先目标和一个持续优先目标。一次性目标优先，并在成功完成 after-auth 准入后清除；优先请求绕过权重、sticky、额度底线和普通并发上限，但继续检查禁用、凭证失效、宿主 cooldown 与可靠额度快照。目标不可用时请求拒绝且不回退其他账号。
- 5h 剩余 **小于** `floor_5h`，或周剩余 **小于** `floor_7d` 时停止新准入，等于阈值仍合格；耗尽始终拒绝。按窗口秒数识别，不依赖 primary/secondary 位置。缺失窗口保持缺失，不虚构 100%；只使用 Codex 池，不借用 Spark/code-review 额度。
- 同一凭证跨模型共享并发上限。选择与预留原子完成；有合格空闲凭证就先承接，全池满才进入有界队列。默认每凭证 1 并发、队列 128、最长等待 2 秒；队列满或超时返回 429，额度不可用返回 503。
- `max_active` 是全部凭证的默认并发上限，`active_overrides` 可按 auth ID 覆盖。管理页支持修改单个、多个或全部凭证的上限；每个凭证显示上限、已用/预留槽位和受失败退避、宿主冷却、禁用或额度不可用影响的槽位。灰格表示可用，绿格表示已用或预留，红格表示暂不可用。正在执行的请求不会因降低上限而被终止，只有占用回落后才接收新的请求。
- 会话粘性在路由模型内按 CPA 的 canonical/execution session 和 caller scope 隔离，支持 parent session 继承。绑定账号满载、失效或低额度时迁移到合格空闲账号。插件不复制 CPA 的全部 LCP/SessionTree 行为；粘性流量也不保证精确的权重比例。
- 非 Codex 路由交还原生 selector。混合 provider 候选池也交还原生选择；若最后选中 Codex，after-auth 仍实施额度与并发准入，但混合池不承诺“先绕过所有满载 Codex”的选路语义。
- 非 Codex 请求不替换请求体、不丢弃流块、不占用 Codex 并发槽。CPA 的 before-auth 不提供 provider，插件仍需建立内部执行关联，并在 after-auth 清除私有 header；普通业务头保持。请求钩子不解码 Body，超大流 RPC 直接返回无修改的成功响应，避免 16 MiB 通用限制使透传失败；其他管理/调度 RPC 仍保留原大小限制。
- 默认底线为 5h 10%、周 5%，120 秒后额度视为 stale。这些是可配置的实施默认值。
- 页面只显示 0–10000 量化分数，不提供 RoutePriority 或分数编辑入口。旧 `/priorities` 管理写入接口返回废弃错误；优先目标通过受管理的 `POST .../priority-controls` 设置 `mode`（`one_shot` 或 `continuous`）、`auth_id` 和 `enabled`。

## 并发与生命周期

CPA scheduler 没有 RequestID 字段。插件在 before-auth 用宿主生成的执行 ID 替换内部 header `X-Better-Quota-Execution`，scheduler 仅接受已登记的执行上下文，在 after-auth 清除该 header 后准入上游。客户端伪造同名 header 不会建立预留。

同 RequestID、同凭证重复准入幂等；宿主顺序换凭证重试时原子交接槽位。完成、失败、拒绝或取消终态只释放一次。SSE/WS 事件刷新活动时间；已准入的请求不会因静默、长流或固定 TTL 被遗忘。10 秒 TTL 仅用于尚未准入的预留，迟到的 after-auth 必须重新竞争名额。

SDK 没有按 RequestID 任意取消普通模型执行的通用接口，因此插件不把“删掉超时计数”当作真实终止。若终态永久丢失，活动槽保守保留，状态显示最早活动时间；需要确认上游结束后排空处理。同实例 reconfigure 保留已有计数，quiesce 停止新 Codex 准入并在仍有活动请求时报告未排空。跨插件 epoch 的热替换不能继承旧内存计数，安装或替换前需排空；本次通过排空后的进程重启安装，未测试跨 epoch 热替换。

## 自动兑换

`auto_reset.enabled` 默认 true。`auto_reset.auth_ids` 为空时覆盖所有启用、未排除的 Codex OAuth 文件凭证；填写 auth ID 后只对选中的账号生效。图形界面可以修改生效账号和巡检频率，设置保存在插件 state。宿主 quota cooldown/Unavailable 不等于人工 Disabled，不因此跳过兑换。按真实账户 ID 的哈希合并重复导入凭证，账户内只运行一个兑换任务。

默认每 300 秒巡检 credit，在到期前 1800 秒进入触发窗口；已发现的未来 credit 单独安排触发时刻。只选择最早到期、仍 available 且尚未过期的 credit；没有到期时间的 credit 不自动消费。消费前重新读取凭证启用状态、账户身份和额度。

POST 仅包含目标 `credit_id` 与本轮固定 UUID `redeem_request_id`。目标、前置额度、credit 数量、次数和幂等 ID 必须在 POST 前通过临时文件、文件 fsync、rename、目录 fsync 持久化；落盘失败会暂停自动化。网络断线/回包丢失后先核验结果，重试沿用原 ID，最多 5 次；确定 4xx（除 429）、`no_credit`、`nothing_to_reset` 转入 manual 状态，管理状态可见。manual 或无法证实的结果不会自动换 credit 补偿兑换。管理状态中的 `reset_accounts` 按账户哈希显示最近巡检时间、可用 credit 数、下次到期时间与错误；没有到期 credit 的正常巡检也可观察。

确认成功需要目标已不可兑换、credit 数量减少、相同实际窗口真实恢复且达到准入条件，不能只凭 POST 200 或 reset 时间经过。随后逐一刷新重复凭证的额度，并清理 CPA cooldown。cooldown 同步独立落盘，恢复时只继续同步，不重复兑换。

状态文件损坏、未知 schema 或不完整意图会拒绝加载；不会自动使用可能早于 POST 的旧备份继续兑换。请为 `state_path` 使用持久卷和仅 CPA 可写的目录。状态不保存 token、代理地址、原始账户 ID 或对话内容；只保存必要兑换意图、最近 100 次成功记录和两天内的预热防重键。

API Key 和 runtime-only 没有可用的 wham OAuth 查询能力时会明确显示 `unsupported`，保守暂停 Codex 新准入；不声称这些类型已完成额度与兑换适配。`exclude_auth_ids` 可显式排除凭证。

## 可选预热

`auto_ping.enabled` 默认 false。使用固定 UTC+8，不依赖容器时区：

- `daily_5h`：每天 00:00、05:00、10:00、15:00、20:00；每日重新锚定，所以 20:00 到次日 00:00 是 4 小时。
- `fixed_times`：每天去重排序后的 `HH:MM` 时刻列表，例如 `["12:00", "17:00"]`。

启动不补跑错过时刻，延迟超过 30 秒或凭证名额不足就跳过，不额外重试或积压。同一时点的账号池由两个 worker 在原 30 秒触发窗口内分批执行；worker 暂时繁忙会在此窗口内等待，不再直接跳过其余账号。预热执行前持久化防重键，并显式占用同一个凭证 gate；通过 `host.model.execute`、`forced_provider=codex` 和 `auth_id` 定向执行。嵌套执行跳过本插件 hooks，所以用宿主内部 source/pinned-auth 上下文复用已准入的内部槽，结束后显式释放。预热会消耗实际额度；启用前设置可用的 `model`。

## 构建与测试

本机有 Docker 与缓存的 `golang:1.26-bookworm` 镜像时：

```sh
./build.sh
```

脚本在一次性 **network=none** 的 Linux arm64 容器里运行 race 测试、覆盖率、go vet、c-shared 构建与 C ABI smoke。依赖 YAML 已 vendor；不连接 CPA，也不下载依赖。产物在 `dist/better-quota-v0.1.9.so`、对应 `.h`、`SHA256SUMS` 和 `coverage.out`。共享库面向 Debian bookworm/glibc 的 CPA 容器，不能在 macOS 直接加载。

真实 CPA loader/RPC adapter 契约测试使用公共 v8.0.57 源码副本与合成 auth 目录：

```sh
CPA_SOURCE=/path/to/public/CLIProxyAPI-v8.0.57 ./tests/cpa_host.sh
```

这会创建另一个一次性 Go 容器，按宿主要求自动选择 Go 工具链，并缓存公共模块与编译结果。测试验证真实 C loader、生产元数据校验、typed RPC、能力注册、header、调度错误状态、流透明性、completion 与管理边界；合成 TLS CONNECT 上游配合真实 host.auth 回调，还验证额度/credits 采集、双凭证满载分流和 429 retry 提示。测试只向本地合成代理发请求；测试 CA 仅安装在一次性容器内，不接触正在运行的 CPA 或真实账户。宿主验收基线为 CPA v8.0.57。

核心测试还使用 mock HTTP 验证定点兑换、POST 前写盘、回包丢失后的恢复、相同幂等 ID 重试、cooldown 恢复与账号去重；合成 executor 测试同时观察真实执行活跃数与插件计数。进程状态恢复测试只重建测试 Runtime 对象，不重启 CPA。

## 安装配置

将 Linux arm64 产物命名为 **`better-quota-v0.1.9.so`** 放到 CPA 的插件目录。架构后缀不要加在版本后面，否则 CPA 会把它当作版本的一部分。先排空并停用冲突的旧 scheduler、smart-router weight 控制器和重复 auto-reset；同一 CPA 只会选择一个 scheduler，多个兑换插件同时运行也不能共享本插件幂等状态。

把 [config.example.yaml](config.example.yaml) 中的字段放到 `plugins.configs.better-quota`，宿主层结构如下；示例不是现有运行配置的替换文件：

```yaml
plugins:
  enabled: true
  dir: plugins
  configs:
    better-quota:
      enabled: true
      priority: 100
      state_path: /CLIProxyAPI/plugins/data/better-quota/state.json
      fallback_proxy_url: direct
      strategy: capacity_weighted
      max_active: 1
      auto_ping:
        enabled: false
```

当凭证缺少 `proxy_url` 时必须显式设置 fallback；`direct` 表示用户选择直连，不会在代理失败时静默回退直连。状态路径必须绝对且持久可写。SDK 传来的宿主保留字段 `enabled`、`priority`、`store` 可识别，其余未知配置字段拒绝，避免拼写错误被忽略。

管理数据注册在 CPA 已鉴权的 `GET /v0/management/plugins/better-quota/status`；`POST .../settings` 保存路由策略、autoreset 账号/频率和并发上限，`POST .../priority-controls` 保存优先目标，`POST .../refresh` 提前触发凭证发现，保留探测退避。资源页面为 `/v0/resource/plugins/better-quota/dashboard`，可从 CPA 的插件资源菜单打开。页面会显示每个账号的 bank reset 数量、各张卡的过期时间和当前按最早过期优先的 autoreset 目标，同时提供并发槽位图。没有公开对话日志。quota provider 对外提供缓存额度，查询管理页不会额外探测上游。

本轮通过真实 OAuth wham usage/credits 读取、合成文本推理、SSE、WebSocket、并发与客户端取消测试。当前账号没有可用 reset credit，因此真实 consume 未触发；兑换、幂等恢复、落盘故障与 cooldown 同步由合成上游验证。未覆盖：跨 epoch 热替换、多实例共享并发/兑换、API Key/runtime-only 的 wham 能力。采样底线控制的是观察到低额度后的新准入，不能保证长请求执行期间余额永远高于底线。
