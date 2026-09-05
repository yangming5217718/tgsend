# tgsend 消息更新接入说明

本文只讲**上游怎么推更新事件**。两条 Redis Stream：

| 流 | 用途 | 消费者 |
|---|---|---|
| `msg:update` | 编辑已发出的普通消息 | `MsgUpdateStreamWorker` |
| `inline:update` | 编辑已分享出去的 inline 消息 | `InlineUpdateStreamWorker` |

两条流都**只处理编辑**。发送走 `cp_botmessage_send_user` 表（插一行、`status=0`，由 `OutboundSender` 扫描发出）。

---

## 一、`msg:update` —— 编辑普通消息

### 事件字段

| 字段 | 必填 | 说明 |
|---|---|---|
| `chatid` | ✅ | 会话 id |
| `msgid` | ✅ | **要编辑哪条消息**的 message_id |
| `content` | | 新文案。省略 = 不改文案 |
| `buttontext` | | 新按钮。三态，见下 |
| `opTime` | | 仅记日志 |

### ⚠️ `msgid` 对应表上的 `sendid`，不是同名的 `msgid` 列

`cp_botmessage_send_user` 上有两个都叫「消息 id」的列，**语义正相反**：

| 列 | 含义 |
|---|---|
| `sendid` | **本条发出后**拿到的 message_id，发送成功时由服务自动写入 |
| `msgid` | **回复目标**的 message_id，发送时传给 `setReplyToMessageId` |

事件里的 `msgid` 指「要编辑哪条」，落到表上是 **`sendid`**。

服务按 `(chatid, sendid)` 回查原始行，用它的 `parsemode` 和 `buttontext`。走索引 `idx_chatid_sendid`。

### 回查不到会怎样

发失败的行没有 `sendid`，消息也可能由别的系统发出，这些情况回查不到。此时：

- `parsemode` → 走默认值 `MarkdownV2`
- `buttontext` 为 KEEP（省略/空）→ **整条事件跳过，不编辑**，日志记 WARN

为什么跳过而不是照编：Telegram 没有「编辑文案但别动键盘」这种调用，重建不出按钮就只能在「文案更新但按钮消失、日志还记成功」和「什么都不做、日志有 WARN」之间选。选后者——看得见的故障比看不见的好。

### `content` 由服务转义，上游不要自己转

服务会对 `content` 做整段 MarkdownV2 转义，与发送路径用的是同一份实现。

**上游按纯文本写就行**，`.` `-` `=` 这些字符直接写，不要加反斜杠——加了会显示成字面反斜杠。

---

## 二、`inline:update` —— 编辑 inline 消息

### 事件字段

| 字段 | 说明 |
|---|---|
| `inlineId` | 母版 id。**更新该母版的全部存活实例** |
| `itemId` | 实例 id。**只更新这一个实例** |
| `content` | 新文案。省略 = 不改文案 |
| `buttontext` | 新按钮。三态，见下 |

`itemId` 优先：两个字段都给时，按实例级处理。

### 母版级和实例级的区别

| | 母版级（只给 `inlineId`） | 实例级（给 `itemId`） |
|---|---|---|
| 影响范围 | 该母版所有 `status=1` 的实例 | 只这一条 |
| 写回母版 `content` | 写 | 写 |
| 写回母版 `buttontext` | **写** | **不写** |

实例级刻意不写回母版：那份 `buttontext` 是为这一个实例单独拼的（`startapp` 尾号是它自己的 `itemId`），写回去的话上游逐实例循环推送时**最后一个实例会赢**，母版从此带着某个实例的 id，不再是母版。

### ⚠️ 要保住每条实例自己的 itemId，必须推实例级事件

母版级更新对所有实例复用**同一份** markup，编辑路径不做 itemId 注入。所以一次母版级按钮更新会把各实例按钮里的 `itemId` 尾号抹平。

图省事推一条母版级事件去改按钮，归因就断了。

### `content` 不转义，上游必须写成合法 MarkdownV2

**这条跟 `msg:update` 相反。** inline 母版文案是人手写的、可能带 `*粗体*`，服务整段转义会把格式全打平，所以这里不转义。

上游要自己负责：

```
保留字符：  _ * [ ] ( ) ~ ` > # + - = | { } . !
写法：      每个都要前置 \，比如  \-99\.99 USDT，占比 \= 88%
```

漏一个字符整条就 400，而且错误只告诉你是哪个字符、**不告诉你在哪个位置**。

最容易漏的是**标题和固定文案里的横杠**——正文里的金额时间大家都记得转，`【xx-yy】` 这种反而看不见。

### 换 parse mode

母版行的 `parsemode` 列说了算，默认 `MarkdownV2`。填 `Markdown`（legacy）就不转义也不严格，但 legacy 已被 Telegram 标为过时，且无法可靠转义任意保留字符。

取值大小写要与 Telegram API 一致：`MarkdownV2` / `Markdown` / `HTML`。

---

## 三、`buttontext` 三态（两条流一致）

| 传什么 | 含义 | 结果 |
|---|---|---|
| **省略 / 空串** | 本次不改按钮 | 按**数据行/母版里记录的按钮**重建后原样发回 |
| **`[]` 或 `[[]]`** | 显式清空 | 键盘真的被删掉 |
| **正常 JSON** | 替换 | 换成这套 |

解析失败也归到第一种——一个格式错误换来按钮全没，代价太大。

### 为什么必须区分「省略」和「清空」

Telegram 的 `editMessageText` / `editMessageCaption` / `editMessageReplyMarkup`，**只要不带 `reply_markup` 就等于「这条消息没有键盘」**，不是「保持原样」，没有第三种。

所以如果不区分，「上游没提按钮」会被执行成「上游要求删掉按钮」。而删键盘对 Telegram 是一次合法编辑，返回 200，日志记成功——**出事时没有任何信号**。

### ⚠️ 「不改按钮」不等于「保持当前按钮」

省略 `buttontext` 时，重建的依据是**数据行/母版里记录的那套**，而不是消息上当前显示的那套。

`msg:update` 不回写 `cp_botmessage_send_user`，所以行里的 `buttontext` 永远停在发送那一刻。于是：

```
1. 发送，按钮 = A          → 行里记 A
2. 推 SET，按钮改成 B      → 消息显示 B，行里还是 A
3. 推「只改文案」（省略 buttontext）
                          → 按行重建 → 按钮退回 A
```

**改过按钮之后再推文案更新，按钮会回滚。** 要避免就每次都带上完整 `buttontext`。

`inline:update` 的母版级路径会写回母版，不存在这个问题；实例级路径同样从母版重建，会重新注入该实例的 `itemId`。

---

## 四、按钮 JSON 格式

二维数组，外层是行、内层是同一行的按钮：

```json
[[{"text":"🧧 领红包","type":"url","value":"https://t.me/xxx_bot/hbpic?startapp=abc"}],
 [{"text":"↩ 再分享","type":"switch_inline","value":"abc"}]]
```

`type` 支持 `url` / `callback` / `switch_inline` / `switch_inline_current` / `webapp`。

**`t.me/<bot>/<app>?startapp=` 这种链接只能给 `url` 类型**，给 `webapp` 会被 Telegram 直接拒掉（`400 BUTTON_URL_INVALID`）。

---

## 五、入站方式

服务用 long polling（`getUpdates`）收消息，**没有开关，总是启用**。

`cp_config.bot_token` 那个 bot **不能设 webhook**，否则 `getUpdates` 一直返回 409、一条 update 都收不到。换 bot 前先用 `getWebhookInfo` 确认 `url` 为空。
