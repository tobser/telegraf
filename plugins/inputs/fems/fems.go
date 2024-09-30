package fems

import (
	_ "embed"
	"errors"

	"encoding/json"
	"net/url"
	"time"

	"github.com/google/uuid"
	"github.com/gorilla/websocket"
	"github.com/influxdata/telegraf"
	"github.com/influxdata/telegraf/plugins/inputs"
)

//go:embed sample.conf
var sampleConfig string

type Fems struct {
	URL         string          `toml:"url"`
	Password    string          `toml:"password"`
	Channels    []string        `toml:"channels"`
	Log         telegraf.Logger `toml:"-"`
	cnt         int
	conn        *websocket.Conn
	is_stopping bool
	acc         telegraf.Accumulator
}

type RpcResponse struct {
	Id    string    `json:"id"`
	Error *ErrorMsg `json:"error"`
}

type ErrorMsg struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

type DataUpdateEvent struct {
	Method       string                `json:"method"`
	EdgRpcParams DataUpdateEventParams `json:"params"`
}

type DataUpdateEventParams struct {
	Method  string                 `json:"method"`
	Payload DataUpdateEventPayload `json:"payload"`
}

type DataUpdateEventPayload struct {
	Params map[string]any `json:"params"`
}

type RpcReq struct {
	Jsonrpc string `json:"jsonrpc"`
	Method  string `json:"method"`
	Id      string `json:"id"`
	Params  any    `json:"params"`
}

func init() {
	inputs.Add("fems", func() telegraf.Input { return &Fems{} })
}

func (f *Fems) SampleConfig() string {
	return sampleConfig
}

func (f *Fems) Init() error {
	if f.Password == "" {
		f.Password = "owner"
	}

	if len(f.URL) == 0 {
		return errors.New("FEMS URL missing")
	}

	if len(f.Channels) == 0 {
		return errors.New("No FEMS channels configured")
	}

	return nil
}

func (f *Fems) Start(acc telegraf.Accumulator) error {
	f.Log.Info("Start")
	f.acc = acc
	go f.connect()
	return nil
}

func (f *Fems) Stop() {
	f.Log.Info("Stop")
	f.is_stopping = true
	f.cleanup()
}

func (f *Fems) Gather(acc telegraf.Accumulator) error {
	return nil
}

func getRequest(method string) RpcReq {
	r := RpcReq{
		Jsonrpc: "2.0",
		Id:      uuid.New().String(),
		Method:  method,
	}
	return r
}

func (f *Fems) connect() {
	first := true
	for {
		if first == false {
			f.cleanup()
			if f.is_stopping {
				return
			}
			wait := 10 * time.Second
			f.Log.Warn("Connection failure reconnecting in ", wait)
			time.Sleep(wait)
		}

		first = false

		u := url.URL{Scheme: "ws", Host: f.URL}
		f.Log.Info("Connecting to ", u.String())

		ws, _, err := websocket.DefaultDialer.Dial(u.String(), nil)
		if err != nil {
			f.Log.Error("dial:", err)
			continue
		}
		f.conn = ws

		if f.try_login() == false {
			continue
		}

		if f.try_subscribe_channels() == false {
			continue
		}

		f.try_handel_msgs_forever()
	}

}

func (f *Fems) try_subscribe_channels() bool {
	req := f.get_subscribe_channels_request()
	err := f.conn.WriteJSON(req)
	if err != nil {
		f.Log.Error("failed to send subscribe channels:", err)
		return false
	}

	_, msg, err := f.conn.ReadMessage()
	if err != nil {
		f.Log.Info("read subscribe response:", err)
		return false
	}

	var resp RpcResponse
	err = json.Unmarshal(msg, &resp)
	if err != nil {
		f.Log.Info("could not parse subscribe response:", err)
		f.Log.Info("data was:", string(msg))
		return false
	}

	if resp.Id != req.Id {
		f.Log.Error("unexpected response id. sent: ", req, " received: ", resp)
		return false
	}

	if resp.Error != nil {
		f.Log.Error("channel subscribe failed: ", resp.Error.Code, " - ", resp.Error.Message)
		return false
	}

	return true
}

func (f *Fems) try_login() bool {
	loginReq := getRequest("authenticateWithPassword")
	loginReq.Params = map[string]interface{}{"password": f.Password}

	f.Log.Trace("sending login request")
	err := f.conn.WriteJSON(loginReq)
	if err != nil {
		f.Log.Error("write:", err)
		return false
	}

	_, msg, err := f.conn.ReadMessage()
	if err != nil {
		f.Log.Info("read login response:", err)
		return false
	}

	var loginResp RpcResponse
	err = json.Unmarshal(msg, &loginResp)
	if err != nil {
		f.Log.Info("could not parse login response:", err)
		f.Log.Info("data was:", string(msg))
		return false
	}

	if loginResp.Id != loginReq.Id {
		f.Log.Error("unexpected response id: ", loginResp)
		return false
	}

	f.Log.Trace("got login response")
	if loginResp.Error != nil {
		f.Log.Error("Login failed: ", loginResp.Error.Code, " - ", loginResp.Error.Message)
		return false
	}

	f.Log.Info("login succeeded")

	return true
}

func (f *Fems) try_handel_msgs_forever() {
	for {
		_, msg, err := f.conn.ReadMessage()
		if err != nil {
			if f.is_stopping == false {
				f.Log.Info("read error: ", err)
			}
			return
		}

		var dataEv DataUpdateEvent
		err = json.Unmarshal(msg, &dataEv)
		if err != nil {
			f.Log.Info("could not parse received data:", err)
			f.Log.Info("data was:", string(msg))
			continue
		}
		data := dataEv.EdgRpcParams.Payload.Params
		f.Log.Trace("FEMS RX: ", data)

		for key, value := range data {
			if value == nil {
				f.Log.Warn("no data for channel '", key, "' received. This most likely means the channel does not exist.")
				delete(data, key)
			}
		}

		if len(data) == 0 {
			f.Log.Warn("No measurement data!!! Original message was: ", string(msg))
			continue
		}

		f.acc.AddFields("fems", data, nil)
	}
}

func (f *Fems) cleanup() {
	if f.conn == nil {
		return
	}

	f.conn.Close()
	f.conn = nil
	f.Log.Info("connection closed")
}

func (f *Fems) get_subscribe_channels_request() RpcReq {

	edgeRpc := getRequest("edgeRpc")
	subsRpc := getRequest("subscribeChannels")
	subsRpc.Params = map[string]interface{}{"count": 0, "channels": f.Channels}
	edgeRpc.Params = map[string]interface{}{"edgeId": "0", "payload": subsRpc} // edgeId is a string!

	return edgeRpc
}
