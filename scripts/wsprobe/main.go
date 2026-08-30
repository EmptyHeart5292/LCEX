// wsprobe:WebSocket 验收探针。
// 用法:go run ./scripts/wsprobe -url ws://localhost:8082/stream -sub 'ticker@BTC-USDT,depth@BTC-USDT@50' -dur 6s
// 订阅后把收到的每条消息打印为一行 JSON,超时退出。
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"strings"
	"time"

	"github.com/gorilla/websocket"
)

func main() {
	url := flag.String("url", "ws://localhost:8082/stream", "ws endpoint")
	subs := flag.String("sub", "", "逗号分隔的订阅频道")
	dur := flag.Duration("dur", 6*time.Second, "接收时长")
	flag.Parse()

	conn, _, err := websocket.DefaultDialer.Dial(*url, nil)
	if err != nil {
		fmt.Println(`{"probe":"dial-error","err":` + quote(err.Error()) + `}`)
		return
	}
	defer conn.Close()

	if *subs != "" {
		payload, _ := json.Marshal(map[string]any{"op": "subscribe", "args": strings.Split(*subs, ",")})
		if err := conn.WriteMessage(websocket.TextMessage, payload); err != nil {
			fmt.Println(`{"probe":"write-error","err":` + quote(err.Error()) + `}`)
			return
		}
	}

	deadline := time.Now().Add(*dur)
	conn.SetReadDeadline(deadline)
	go func() {
		time.Sleep(*dur + 2*time.Second)
		conn.Close()
	}()
	for {
		_, data, err := conn.ReadMessage()
		if err != nil {
			fmt.Println(`{"probe":"done"}`)
			return
		}
		fmt.Println(string(data))
	}
}

func quote(s string) string {
	b, _ := json.Marshal(s)
	return string(b)
}
