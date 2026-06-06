// 参考片段（非独立编译单元）—— 下载调度联动器。
// 跟踪账号忙闲、只用空闲号，在「多账号并行(origin/热门最快) / 单会员同DC CDN」间自动选更快的一条。
package reference

import (
	"context"
	"errors"
	"fmt"

	"github.com/gotd/td/telegram/downloader"
	"github.com/gotd/td/tg"
)

const (
	estPerAccountOriginMbps = 17.0  // origin 文件单账号并行分块的人均吞吐
	estSingleOriginMbps     = 49.0  // origin 文件单账号(连接池)封顶
	estCDNSingleMbps        = 300.0 // CDN 文件单会员(同DC、全client)吞吐
)

// ---- 账号忙闲跟踪（多任务并行时不互抢同一批账号）----
// w.busy map[int64]int + mutex；acctAcquire/acctRelease 计数。

// claimIdle 原子地「挑 + 立即占用」最多 max 个空闲在线号（§10.6 Bug1 修复）。
// 「挑」和「占」必须在同一把锁内完成，否则并发任务各自的空闲快照会过时、拿到【重叠】的号。
// buildMdlPool 改用它先占一批，再逐号 resolve；不可用的号即时 acctRelease 归还。
func (w *Worker) claimIdle(userID int64, max int) []int64 {
	online := w.reg.OnlineIDs(userID) // 先取在线快照，再进锁，无锁嵌套
	w.busyMu.Lock()
	defer w.busyMu.Unlock()
	var claimed []int64
	for _, id := range online {
		if max > 0 && len(claimed) >= max { // 单任务限号(maxPoolPerTask=16)，留余量给并发任务
			break
		}
		if w.busy[id] == 0 {
			w.busy[id]++ // 挑到就立即占，锁内原子
			claimed = append(claimed, id)
		}
	}
	return claimed
}

// pickSameDCPremium：为「文件所在 DC」挑一个最佳单账号下载器：
// 优先「同 DC + 会员 + rate_per_min=0 + 在线空闲」。跨 DC 会掉到 ~45，所以务必同 DC。
func (w *Worker) pickSameDCPremium(userID int64, dc int) (*tg.Client, int64, bool) {
	rows, _ := w.pg.Query(context.Background(),
		`SELECT id FROM accounts WHERE user_id=$1 AND dc_id=$2
		   AND COALESCE(is_premium,false)=true AND COALESCE(rate_per_min,0)=0 ORDER BY id`, userID, dc)
	var ids []int64
	for rows.Next() {
		var id int64
		if rows.Scan(&id) == nil {
			ids = append(ids, id)
		}
	}
	rows.Close()
	for _, id := range ids { // 优先空闲
		if !w.acctBusy(id) {
			if api, _, ok := w.reg.ClientFor(userID, id); ok {
				return api, id, true
			}
		}
	}
	return nil, 0, false
}

// smartDownloadMedia：调度决策 + 执行，返回(方案 plan, 字节数, err)。
//   mdl：预建的多账号下载池(只含空闲号)；fallbackApi：执行账号 api(兜底)。
func (w *Worker) smartDownloadMedia(ctx context.Context, userID int64, mdl *mdlPool, fallbackApi *tg.Client, srcUsername string, msg *tg.Message, media *Media, dest string, threads int) (string, int64, error) {
	n := 0
	if mdl != nil {
		n = len(mdl.accts)
	}
	estMulti := float64(n) * estPerAccountOriginMbps

	// 路径A：多账号并行分块（origin/热门最快）。号够多就先试。
	if n >= 2 && estMulti >= estSingleOriginMbps {
		b, e := w.mdlDownload(ctx, mdl, msg.ID, dest, 1024, 4)
		if e == nil {
			return fmt.Sprintf("multi-%dacc(%.0fMbps级)", n, estMulti), b, nil
		}
		if !errors.Is(e, errCDNRedirect) {
			// 真失败(非CDN) → 落到路径B 兜底
		}
		// CDN 文件手写解不了 → 落到路径B
	}

	// 路径B：单会员·同DC·gotd「全 client」下载器（自动解 CDN 重定向 + 跨 DC 迁移）。
	return w.singleClientDownload(ctx, userID, fallbackApi, srcUsername, msg.ID, media, dest, threads)
}

// singleClientDownload：单账号·全 client 下载（CDN/跨DC 均可）。优先同 DC 会员空闲号；线程封顶 8。
// §10.6 Bug2 修复：换会员号下载前，必须用「那个号自己」重取消息拿它自己的 file_reference
// (file_reference 是按账号的；用执行号取的 ref 给会员号下载会 FILE_REFERENCE_EXPIRED)。
func (w *Worker) singleClientDownload(ctx context.Context, userID int64, fallbackApi *tg.Client, srcUsername string, msgID int, media *Media, dest string, threads int) (string, int64, error) {
	if threads > 8 {
		threads = 8 // 单账号 >8 并发撞 TG 限速
	}
	if srcUsername != "" { // 仅公开源可让别的号自己重取(私有源/重取失败 → 退执行号兜底)
		if pApi, pid, ok := w.pickSameDCPremium(userID, media.DC); ok {
			w.acctAcquire(pid)
			md, e := w.refetchMedia(ctx, pApi, srcUsername, msgID) // ← 会员号取「自己视角」的 file_reference
			if e == nil {
				err := dl(ctx, pApi, md, dest, threads)
				w.acctRelease(pid)
				if err == nil {
					return fmt.Sprintf("cdn-single-#%d(同DC会员)", pid), md.Size, nil
				}
			} else {
				w.acctRelease(pid)
			}
		}
	}
	// 兜底：执行账号自己下(它取的 file_reference 对它有效)
	if err := dl(ctx, fallbackApi, media, dest, threads); err != nil {
		return "", 0, err
	}
	return "single-client(执行号)", media.Size, nil
}

// refetchMedia：用给定账号【自己】resolve 公开源 + 取消息，拿到该账号视角的下载位置(file_reference 按账号)。
func (w *Worker) refetchMedia(ctx context.Context, api *tg.Client, srcUsername string, msgID int) (*Media, error) {
	chn, err := resolveChannel(ctx, api, srcUsername)
	if err != nil {
		return nil, err
	}
	msg, err := getChannelMessage(ctx, api, chn, msgID) // 走该账号 home DC 主连接
	if err != nil || msg.Media == nil {
		return nil, fmt.Errorf("取消息失败")
	}
	return extractMedia(msg.Media) // → *Media{Location, Size, DC}
}

// dl：gotd 自带 downloader 自动处理 upload.fileCdnRedirect → 连 CDN DC → getCdnFile → 解密。
func dl(ctx context.Context, api *tg.Client, md *Media, dest string, threads int) error {
	_, err := downloader.NewDownloader().WithPartSize(1024 * 1024).
		Download(api, md.Location).WithThreads(threads).ToPath(ctx, dest)
	return err
}
