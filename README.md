# Telegram 频道媒体搬运 · 上传下载提速 Cookbook

用 **Go + [gotd](https://github.com/gotd/td)**（MTProto user-account）做 Telegram 频道媒体高速搬运（下载源频道媒体 → 可选加工 → 上传到目标频道）时，一整套**上传/下载提速**的工程复盘与可复用结论。全部为单台 VPS + 真实 user 账号实测。

> 打开 [`index.html`](./index.html) 看图文完整版（自包含，双击即可）。下面是速查。

---

## 核心结论（TL;DR）

1. **"下载慢"的头号真凶常是你自己加的客户端 ratelimit 中间件**，不是 Telegram。文件块（`getFile`）每块是一次 RPC，被 `rate_per_min=20` 掐到 ~1.6 Mbps。去掉后单连接 27、上传 1.4→145。**ratelimit 只能套控制面，别套文件块。**
2. **单账号下载是 per-account（非 per-IP）限速**：冷门/自传文件 ~30–49 Mbps 硬封顶，连接 >4 或单连接 worker >16 直接被 Telegram 关连接。
3. **突破单账号上限两条路**：
   - **多账号并行分块**：把一个文件的块分给 N 个空闲账号（共享队列、块级重试、合并落盘）。origin 冷门源 27 号 = **463–547 Mbps**，热门源 30 号 = **706 Mbps**。
   - **CDN 文件**：热门文件被 Telegram 分发到 CDN，用 gotd **"全 client"下载器**（自动处理 `fileCdnRedirect`+解密+跨 DC），单 Premium **同 DC**、`rate_per_min=0`、线程 ≤8 = **315 Mbps**。
4. **上传**用"当前 DC 连接池"（多连接、绕 ratelimit、保留 floodwait）= **145 Mbps**（单连接的 100 倍）。
5. **能引用秒传就别下载**：`messages.sendMedia` 直接引用源文件 `file_reference` = 0 带宽服务端复制。
6. 最终做一个**下载调度联动器**：跟踪账号忙闲、只用空闲号，在"多账号并行 / 单会员 CDN"间自动选更快的那条。

## 速查表（下载单文件吞吐）

| 方案 | Mbps | 适用 |
|---|---:|---|
| 单连接 + ratelimit(20/min) | 1.6 | ❌ 历史瓶颈 |
| 单连接 无 ratelimit (origin) | 27 | |
| 单账号 连接池×4 (origin) | 49 | origin 单账号硬上限 |
| 单 Premium 同 DC · gotd全client (CDN, thr8) | **315** | ✅ CDN 单会员 |
| 多账号并行分块 ×27 (origin 冷门) | **463–547** | ✅ 私有/冷门源主力 |
| 多账号并行分块 ×30 (热门源) | **706** | ✅ 最快 |
| telethon tdlid 单连接 (origin, 对照) | 32 | 无"魔法"，差异全在文件是否 CDN |

上传：单连接+ratelimit **1.4** → 连接池×8 **145** Mbps。

## 致命坑速记

- **home-DC vs file-DC**：`channels.getMessages`（取 file_reference）必须走账号 home DC 主连接；`getFile` 走文件 DC 连接池。混了 → 跨 DC 账号取不到消息（多账号只有 7/27 出力）。
- **跨 DC 下载很慢**：单账号下载务必同 DC（跨 DC 仅 ~45）。
- **连接池/手写解不了 CDN**：CDN 文件只能用完整 client 下载器。
- **导入无代理账号默认不上线**：`UPDATE accounts SET state='need_relogin'`（有效 session → 秒上线免码）。
- **部署二进制截断**：高并发下载吃满上行 → scp 传一半 → 203 崩溃循环。先 `systemctl stop`，传完 `stat -c%s` 校验再 `mv`。
- **网速单位**：`*_bps` 字段存的是字节/秒，和 Mbps 差 8 倍。

## 目录

```
index.html              图文完整复盘(自包含,双击打开)
code/
  pool.go               连接池封装(Pool/DC/MediaPool + floodwait + Close)
  multi_download.go     多账号并行分块下载核心(共享队列 + 块级重试 + CDN 探测)
  scheduler.go          下载调度联动器(忙闲跟踪 + origin/CDN 择优 + 同DC会员)
```

## 关键 gotd API

| 需求 | API |
|---|---|
| 当前 DC 多连接（无授权转移） | `Client.Pool(max int64)` |
| 指定 DC 多连接（自动 exportAuthorization） | `Client.DC(ctx, dc, max)`（转移到自身 DC 会 `DC_ID_INVALID` → 回退 Pool） |
| 下载（自动 CDN + DC 迁移） | `downloader.NewDownloader().WithPartSize(1MB).Download(api,loc).WithThreads(n).ToPath()` |
| 手写分块（多账号） | `api.UploadGetFile(...)` → `*UploadFile` / `*UploadFileCDNRedirect` |
| 引用秒传（0 带宽） | `MessagesSendMedia` + `InputMediaDocument{InputDocument{...}}` |

分块大小：`getFile` 上限 1MB；`saveFilePart` 上限 512KB。

---

## 推到远程（可选）

本仓库已 `git init`。要推到自己的远程：

```bash
# GitHub CLI
gh repo create tg-transfer-speed-cookbook --public --source=. --push
# 或手动
git remote add origin <your-repo-url>
git push -u origin main
```

> 数据来自单台 VPS + 真实账号实测，会随 Telegram 策略/地域/文件冷热变化，请以你自己的实测为准。文档不含任何账号凭据。
