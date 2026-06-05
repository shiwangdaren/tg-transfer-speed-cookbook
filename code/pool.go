// 参考片段（非独立编译单元）—— gotd 连接池封装。
// 要点：批量传输走「无 ratelimit 的多连接池」，但保留 floodwait。
package reference

import (
	"context"

	"github.com/gotd/contrib/middleware/floodwait"
	"github.com/gotd/td/telegram"
	"github.com/gotd/td/tg"
)

// Client 是你自己的账号客户端句柄，内含 *telegram.Client。
type Client struct{ tg *telegram.Client }

// poolInvoker 给连接池调用器套上 floodwait（自动等待 FLOOD_WAIT），同时保留 Close。
type poolInvoker struct {
	tg.Invoker
	closer func() error
}

func (p poolInvoker) Close() error { return p.closer() }

// HomePool：当前(home) DC 上 conns 条连接（无授权转移）。适合本账号自有频道的高速上传/下载。
func (c *Client) HomePool(conns int) (telegram.CloseInvoker, error) {
	p, err := c.tg.Pool(int64(conns))
	if err != nil {
		return nil, err
	}
	waiter := floodwait.NewSimpleWaiter()
	return poolInvoker{Invoker: waiter.Handle(p), closer: p.Close}, nil
}

// DCPool：到「文件所在 DC」的 conns 条连接。跨 DC 会自动 exportAuthorization 转移；
// 转移到自身 home DC 会报 DC_ID_INVALID → 回退 HomePool（当前 DC 多连接即可）。
func (c *Client) DCPool(ctx context.Context, dc, conns int) (telegram.CloseInvoker, error) {
	inv, err := c.tg.DC(ctx, dc, int64(conns))
	if err != nil {
		return c.HomePool(conns)
	}
	waiter := floodwait.NewSimpleWaiter()
	return poolInvoker{Invoker: waiter.Handle(inv), closer: inv.Close}, nil
}

// 用法：
//   inv, _ := c.HomePool(8)          // 或 c.DCPool(ctx, fileDC, 1)
//   defer inv.Close()
//   api := tg.NewClient(inv)         // 把池包装成 *tg.Client
//   // 上传：uploader.NewUploader(api)...  下载：手写 api.UploadGetFile(...)
//
// 注意：连接池能高速跑 saveFilePart / 普通 getFile，但【解不了 CDN 重定向】——
// CDN 文件必须用完整 client 的 downloader（见 §8 / scheduler.go）。
