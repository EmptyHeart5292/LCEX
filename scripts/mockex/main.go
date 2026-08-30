// mockex:模拟交易所公共行情 WS,供 price-index 在无外网环境下做确定性验收。
//
// 用法:go run ./scripts/mockex -addr :9999 -stream btcusdt@bookTicker -bid 50000 -ask 50001
// 进阶:给 -file 参数(如 /tmp/px.txt)后,每次推送前读取文件里的 "bid ask",
//      测试中途改价即可驱动指数变动。
package main

import (
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/gorilla/websocket"
)

func main() {
	addr := flag.String("addr", ":9999", "listen addr")
	stream := flag.String("stream", "btcusdt@bookTicker", "stream name")
	bid := flag.String("bid", "50000", "initial bid")
	ask := flag.String("ask", "50001", "initial ask")
	interval := flag.Duration("interval", 300*time.Millisecond, "push interval")
	file := flag.String("file", "", "可选:动态价格文件(内容: bid ask)")
	flag.Parse()

	http.HandleFunc("/ws/", func(w http.ResponseWriter, r *http.Request) {
		ws, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer ws.Close()
		b, a := *bid, *ask
		for {
			if *file != "" {
				if data, err := os.ReadFile(*file); err == nil {
					fields := strings.Fields(string(data))
					if len(fields) == 2 {
						b, a = fields[0], fields[1]
					}
				}
			}
			frame := fmt.Sprintf(`{"stream":"%s","data":{"b":"%s","B":"1","a":"%s","A":"1"}}`, *stream, b, a)
			ws.SetWriteDeadline(time.Now().Add(5 * time.Second))
			if err := ws.WriteMessage(websocket.TextMessage, []byte(frame)); err != nil {
				return
			}
			time.Sleep(*interval)
		}
	})
	log.Printf("mockex listening on %s (stream=%s bid=%s ask=%s)", *addr, *stream, *bid, *ask)
	log.Fatal(http.ListenAndServe(*addr, nil))
}

var upgrader = websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}

var _ = strconv.Itoa
