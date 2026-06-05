// 参考片段（非独立编译单元）—— 多账号并行分块下载核心（TDLID 真复刻）。
// 把一个文件的分块分发给 N 个账号，共享工作队列(快账号多抓)+块级重试，合并落盘。
// 实测：27 号 1GB=547Mbps、热门源 30 号=706Mbps。
package reference

import (
	"context"
	"errors"
	"os"
	"sync"
	"time"

	"github.com/gotd/td/tg"
)

// errCDNRedirect：手写 getFile 收到 CDN 重定向（解不了）→ 让上层改用 gotd 全 client 下载器。
var errCDNRedirect = errors.New("CDN重定向")

// dlSource = 某账号「到文件 DC 的无 ratelimit 连接池 api」+ 本文件对该账号有效的下载位置。
//   关键：file_reference 是【按账号】的，所以每个账号要用【自己 home DC 主连接】取消息拿到
//   自己的 location，再喂给【文件 DC 的池】抓块。混了 DC → 跨 DC 账号取不到消息被跳过。
type dlSource struct {
	id  int64
	api *tg.Client // 文件 DC 的连接池(无 ratelimit)
	loc tg.InputFileLocationClass
}

// downloadAcrossSources：共享队列把 [0,num) 块分给所有 source，每 source 起 perAcct 个 worker。
func downloadAcrossSources(ctx context.Context, sources []dlSource, size int64, dest string, chunk, perAcct int) error {
	f, err := os.OpenFile(dest, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return err
	}
	defer f.Close()
	if err := f.Truncate(size); err != nil {
		return err
	}
	num := int((size + int64(chunk) - 1) / int64(chunk))
	idxCh := make(chan int, num)
	for i := 0; i < num; i++ {
		idxCh <- i
	}
	close(idxCh)

	var wg sync.WaitGroup
	var firstErr error
	var mu sync.Mutex
	setErr := func(e error) {
		mu.Lock()
		if firstErr == nil {
			firstErr = e
		}
		mu.Unlock()
	}
	for si := range sources {
		for k := 0; k < perAcct; k++ { // 每账号 perAcct(=4) 个 worker
			wg.Add(1)
			go func(s dlSource) {
				defer wg.Done()
				for idx := range idxCh { // 抢块：快账号自然多抓
					off := int64(idx) * int64(chunk)
					var got []byte
					for attempt := 0; attempt < 6; attempt++ { // 块级重试，扛 FLOOD/断连
						r, e := s.api.UploadGetFile(ctx, &tg.UploadGetFileRequest{
							Location: s.loc, Offset: off, Limit: chunk,
						})
						if e != nil {
							select {
							case <-time.After(time.Duration(150*(attempt+1)) * time.Millisecond):
							case <-ctx.Done():
								setErr(ctx.Err())
								return
							}
							continue
						}
						if fr, ok := r.(*tg.UploadFile); ok {
							got = fr.Bytes
							break
						}
						if _, ok := r.(*tg.UploadFileCDNRedirect); ok {
							setErr(errCDNRedirect) // CDN → 放弃多账号手写，让上层换全 client
							return
						}
					}
					if got == nil {
						setErr(errors.New("块重试仍失败"))
						return
					}
					if _, e := f.WriteAt(got, off); e != nil {
						setErr(e)
						return
					}
				}
			}(sources[si])
		}
	}
	wg.Wait()
	return firstErr
}
