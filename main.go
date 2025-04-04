package main

import (
	"fmt"

	"github.com/aethiopicuschan/flashkv/store"
	"github.com/tidwall/evio"
)

func main() {
	s := store.New(16)
	events := &evio.Events{
		Opened: func(c evio.Conn) (out []byte, opts evio.Options, action evio.Action) {
			ctx := store.NewConnContext(1024)
			c.SetContext(ctx)
			return
		},
		Data: func(c evio.Conn, in []byte) (out []byte, action evio.Action) {
			ctx, ok := c.Context().(*store.ConnContext)
			if !ok {
				ctx = store.NewConnContext(1024)
				c.SetContext(ctx)
			}
			buf := ctx.GetBuffer()
			buf = append(buf, in...)
			ctx.SetBuffer(buf)
			out = s.ProcessCommands(ctx)
			return
		},
		Closed: func(c evio.Conn, err error) (action evio.Action) {
			return
		},
	}

	if err := evio.Serve(*events, fmt.Sprintf("tcp://%s", GetConfig().Port)); err != nil {
		panic(err)
	}
}
