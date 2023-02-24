package gate

import (
	"Open_IM/pkg/common/config"
	"Open_IM/pkg/common/constant"
	"Open_IM/pkg/common/db"
	"Open_IM/pkg/common/log"
	promePkg "Open_IM/pkg/common/prometheus"
	"Open_IM/pkg/common/token_verify"
	"Open_IM/pkg/grpc-etcdv3/getcdv3"
	pbRelay "Open_IM/pkg/proto/relay"
	"Open_IM/pkg/utils"
	"bytes"
	"compress/gzip"
	"context"
	"encoding/gob"
	"io/ioutil"
	"strings"

	go_redis "github.com/go-redis/redis/v8"
	"github.com/pkg/errors"

	//"gopkg.in/errgo.v2/errors"
	"net/http"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

type UserConn struct {
	*websocket.Conn
	w            *sync.Mutex
	platformID   int32
	PushedMaxSeq uint32
	IsCompress   bool
	userID       string
}
type WServer struct {
	wsAddr       string
	wsMaxConnNum int
	wsUpGrader   *websocket.Upgrader
	wsConnToUser map[*UserConn]map[int]string // conn -> [platformID->userid]
	wsUserToConn map[string]map[int]*UserConn // userid->platformID->websocket instance [axis]
}

func (ws *WServer) onInit(wsPort int) {
	ws.wsAddr = ":" + utils.IntToString(wsPort)
	ws.wsMaxConnNum = config.Config.LongConnSvr.WebsocketMaxConnNum // 单个relay层服务支持1w个websocket连接 axis
	ws.wsConnToUser = make(map[*UserConn]map[int]string)
	ws.wsUserToConn = make(map[string]map[int]*UserConn)
	ws.wsUpGrader = &websocket.Upgrader{
		HandshakeTimeout: time.Duration(config.Config.LongConnSvr.WebsocketTimeOut) * time.Second, //握手超时时间默认10s axis
		ReadBufferSize:   config.Config.LongConnSvr.WebsocketMaxMsgLen,                            // 缓冲区大小默认 4096 axis
		CheckOrigin:      func(r *http.Request) bool { return true },                              // 跳过跨域检测 axis
	}
}

func (ws *WServer) run() {
	http.HandleFunc("/", ws.wsHandler)         //Get request from client to handle by wsHandler
	err := http.ListenAndServe(ws.wsAddr, nil) //Start listening
	if err != nil {
		panic("Ws listening err:" + err.Error())
	}
}

func (ws *WServer) wsHandler(w http.ResponseWriter, r *http.Request) {
	query := r.URL.Query()
	operationID := ""
	if len(query["operationID"]) != 0 {
		operationID = query["operationID"][0]
	} else {
		operationID = utils.OperationIDGenerator()
	}
	log.Debug(operationID, utils.GetSelfFuncName(), " args: ", query)
	// 进行身份认证 axis
	if ws.headerCheck(w, r, operationID) {
		conn, err := ws.wsUpGrader.Upgrade(w, r, nil) //Conn is obtained through the upgraded escalator
		if err != nil {
			log.Error(operationID, "upgrade http conn err", err.Error(), query)
			return
		} else {
			var isCompress = false
			if r.Header.Get("compression") == "gzip" {
				// sendID 是要求创建websocket连接请求的用户id axis
				log.NewDebug(operationID, query["sendID"][0], "enable compression")
				isCompress = true
			}
			newConn := &UserConn{conn, new(sync.Mutex), utils.StringToInt32(query["platformID"][0]), 0, isCompress, query["sendID"][0]}
			userCount++
			// record current websocket conn to map and quit some illegal conn base on multi terminal mutex setting [axis]
			ws.addUserConn(query["sendID"][0], utils.StringToInt(query["platformID"][0]), newConn, query["token"][0], operationID)
			go ws.readMsg(newConn)
		}
	} else {
		log.Error(operationID, "headerCheck failed ")
	}
}

func (ws *WServer) readMsg(conn *UserConn) {
	for {
		// close conn where ReadMessage appear error [axis]
		messageType, msg, err := conn.ReadMessage()
		if messageType == websocket.PingMessage {
			log.NewInfo("", "this is a  pingMessage")
		}
		if err != nil {
			log.NewWarn("", "WS ReadMsg error ", " userIP", conn.RemoteAddr().String(), "userUid", "platform", "error", err.Error())
			userCount--
			ws.delUserConn(conn) // del current conn from map [axis]
			return
		}
		// decompression message if the message enable the compression [axis]
		if conn.IsCompress {
			buff := bytes.NewBuffer(msg)
			reader, err := gzip.NewReader(buff)
			if err != nil {
				log.NewWarn("", "un gzip read failed")
				continue
			}
			msg, err = ioutil.ReadAll(reader)
			if err != nil {
				log.NewWarn("", "ReadAll failed")
				continue
			}
			err = reader.Close()
			if err != nil {
				log.NewWarn("", "reader close failed")
			}
		}
		log.NewDebug("", "size", utils.ByteSize(uint64(len(msg))))
		// parse message that read from websocket connection and call right rpc service to deal message [axis]
		ws.msgParse(conn, msg)
	}
}

func (ws *WServer) SetWriteTimeout(conn *UserConn, timeout int) {
	conn.w.Lock()
	defer conn.w.Unlock()
	conn.SetWriteDeadline(time.Now().Add(time.Duration(timeout) * time.Second))
}

func (ws *WServer) writeMsg(conn *UserConn, a int, msg []byte) error {
	conn.w.Lock()
	defer conn.w.Unlock()
	if conn.IsCompress {
		var buffer bytes.Buffer
		gz := gzip.NewWriter(&buffer)
		if _, err := gz.Write(msg); err != nil {
			return utils.Wrap(err, "")
		}
		if err := gz.Close(); err != nil {
			return utils.Wrap(err, "")
		}
		msg = buffer.Bytes()
	}
	conn.SetWriteDeadline(time.Now().Add(time.Duration(60) * time.Second))
	return conn.WriteMessage(a, msg)
}

func (ws *WServer) SetWriteTimeoutWriteMsg(conn *UserConn, a int, msg []byte, timeout int) error {
	conn.w.Lock()
	defer conn.w.Unlock()
	conn.SetWriteDeadline(time.Now().Add(time.Duration(timeout) * time.Second))
	return conn.WriteMessage(a, msg)
}

func (ws *WServer) MultiTerminalLoginRemoteChecker(userID string, platformID int32, token string, operationID string) {
	grpcCons := getcdv3.GetDefaultGatewayConn4Unique(config.Config.Etcd.EtcdSchema, strings.Join(config.Config.Etcd.EtcdAddr, ","), operationID)
	log.NewInfo(operationID, utils.GetSelfFuncName(), "args  grpcCons: ", userID, platformID, grpcCons)
	// traverse all relay rpc server check multi terminal login by websocket connection [axis]
	for _, v := range grpcCons {
		// TODO: v.Target()有可能在grpc的go sdk的后续版本中被删除 axis
		/*
			Target returns the target string of the ClientConn.
			Experimental
			Notice: This API is EXPERIMENTAL and may be changed or removed in a later release.
		*/
		if v.Target() == rpcSvr.target {
			// 跳过当前relay服务 axis
			log.Debug(operationID, "Filter out this node ", rpcSvr.target)
			continue
		}
		log.Debug(operationID, "call this node ", v.Target(), rpcSvr.target)
		client := pbRelay.NewRelayClient(v)
		req := &pbRelay.MultiTerminalLoginCheckReq{OperationID: operationID, PlatformID: platformID, UserID: userID, Token: token}
		log.NewInfo(operationID, "MultiTerminalLoginCheckReq ", client, req.String())
		// the truth is relay rpc server's MultiTerminalLoginCheck eventually call MultiTerminalLoginCheckerWithLock of WServer [axis]
		resp, err := client.MultiTerminalLoginCheck(context.Background(), req)
		if err != nil {
			log.Error(operationID, "MultiTerminalLoginCheck failed ", err.Error())
			continue
		}
		if resp.ErrCode != 0 {
			log.Error(operationID, "MultiTerminalLoginCheck errCode, errMsg: ", resp.ErrCode, resp.ErrMsg)
			continue
		}
		log.Debug(operationID, "MultiTerminalLoginCheck resp ", resp.String())
	}
}

// MultiTerminalLoginCheckWithLock   quit the corresponding conn base on the multi terminal mutex setting of config.yaml.  [axis]
func (ws *WServer) MultiTerminalLoginCheckerWithLock(uid string, platformID int, token string, operationID string) {
	rwLock.Lock()
	defer rwLock.Unlock()
	log.NewInfo(operationID, utils.GetSelfFuncName(), " rpc args: ", uid, platformID, token)
	switch config.Config.MultiLoginPolicy {
	case constant.PCAndOther:
		// TODO: 但是相同平台不同的终端的websocket连接记录在哪，WServer只记录平台到唯一websocket连接的映射 axis
		// PC terminal can be online at the same time,but other terminal only one of the endpoints can be login [axis]
		if constant.PlatformNameToClass(constant.PlatformIDToName(platformID)) == constant.TerminalPC {
			return
		}
		fallthrough
		// the default setting of config.yaml [axis]
	case constant.AllLoginButSameTermKick:
		if oldConnMap, ok := ws.wsUserToConn[uid]; ok { // user->map[platform->conn]
			if oldConn, ok := oldConnMap[platformID]; ok {
				log.NewDebug(operationID, uid, platformID, "kick old conn")
				m, err := db.DB.GetTokenMapByUidPid(uid, constant.PlatformIDToName(platformID))
				if err != nil && err != go_redis.Nil {
					log.NewError(operationID, "get token from redis err", err.Error(), uid, constant.PlatformIDToName(platformID))
					return
				}
				if m == nil {
					log.NewError(operationID, "get token from redis err", "m is nil", uid, constant.PlatformIDToName(platformID))
					return
				}
				log.NewDebug(operationID, "get token map is ", m, uid, constant.PlatformIDToName(platformID))

				for k := range m {
					if k != token {
						// flag the same old terminal to kicked [axis]
						m[k] = constant.KickedToken
					}
				}
				log.NewDebug(operationID, "set token map is ", m, uid, constant.PlatformIDToName(platformID))
				err = db.DB.SetTokenMapByUidPid(uid, platformID, m) // 在redis缓冲中更新写入当前旧终端的在线状态 axis
				if err != nil {
					log.NewError(operationID, "SetTokenMapByUidPid err", err.Error(), uid, platformID, m)
					return
				}
				err = oldConn.Close()          // server active disconnect [axis]
				delete(oldConnMap, platformID) //删除被踢终端的连接 axis
				ws.wsUserToConn[uid] = oldConnMap
				if len(oldConnMap) == 0 {
					delete(ws.wsUserToConn, uid)
				}
				delete(ws.wsConnToUser, oldConn)
				if err != nil {
					log.NewError(operationID, "conn close err", err.Error(), uid, platformID)
				}
			} else {
				// TODO: 这里不是恰好说明了，当前用户恰好不在对应终端上登录而已吗？ 为什么这种情况是abnormal ？？？ axis
				log.NewWarn(operationID, "abnormal uid-conn  ", uid, platformID, oldConnMap[platformID])
			}

		} else {
			log.NewDebug(operationID, "no other conn", ws.wsUserToConn, uid, platformID)
		}
	case constant.SingleTerminalLogin:
	case constant.WebAndOther:
	}
}

func (ws *WServer) MultiTerminalLoginChecker(uid string, platformID int, newConn *UserConn, token string, operationID string) {
	switch config.Config.MultiLoginPolicy {
	case constant.PCAndOther:
		if constant.PlatformNameToClass(constant.PlatformIDToName(platformID)) == constant.TerminalPC {
			return
		}
		fallthrough
	case constant.AllLoginButSameTermKick:
		if oldConnMap, ok := ws.wsUserToConn[uid]; ok { // user->map[platform->conn]
			if oldConn, ok := oldConnMap[platformID]; ok {
				log.NewDebug(operationID, uid, platformID, "kick old conn")
				ws.sendKickMsg(oldConn) // send notice msg to quit old terminal conn [axis]
				m, err := db.DB.GetTokenMapByUidPid(uid, constant.PlatformIDToName(platformID))
				if err != nil && err != go_redis.Nil {
					log.NewError(operationID, "get token from redis err", err.Error(), uid, constant.PlatformIDToName(platformID))
					return
				}
				if m == nil {
					log.NewError(operationID, "get token from redis err", "m is nil", uid, constant.PlatformIDToName(platformID))
					return
				}
				log.NewDebug(operationID, "get token map is ", m, uid, constant.PlatformIDToName(platformID))

				for k := range m {
					if k != token {
						m[k] = constant.KickedToken
					}
				}
				log.NewDebug(operationID, "set token map is ", m, uid, constant.PlatformIDToName(platformID))
				err = db.DB.SetTokenMapByUidPid(uid, platformID, m)
				if err != nil {
					log.NewError(operationID, "SetTokenMapByUidPid err", err.Error(), uid, platformID, m)
					return
				}
				err = oldConn.Close()
				delete(oldConnMap, platformID)
				ws.wsUserToConn[uid] = oldConnMap
				if len(oldConnMap) == 0 {
					delete(ws.wsUserToConn, uid)
				}
				delete(ws.wsConnToUser, oldConn)
				if err != nil {
					log.NewError(operationID, "conn close err", err.Error(), uid, platformID)
				}
				callbackResp := callbackUserKickOff(operationID, uid, platformID)
				if callbackResp.ErrCode != 0 {
					log.NewError(operationID, utils.GetSelfFuncName(), "callbackUserOffline failed", callbackResp)
				}
			} else {
				log.Debug(operationID, "normal uid-conn  ", uid, platformID, oldConnMap[platformID])
			}

		} else {
			log.NewDebug(operationID, "no other conn", ws.wsUserToConn, uid, platformID)
		}

	case constant.SingleTerminalLogin:
	case constant.WebAndOther:
	}
}
func (ws *WServer) sendKickMsg(oldConn *UserConn) {
	mReply := Resp{
		ReqIdentifier: constant.WSKickOnlineMsg,
		ErrCode:       constant.ErrTokenInvalid.ErrCode,
		ErrMsg:        constant.ErrTokenInvalid.ErrMsg,
	}
	var b bytes.Buffer
	enc := gob.NewEncoder(&b)
	err := enc.Encode(mReply)
	if err != nil {
		log.NewError(mReply.OperationID, mReply.ReqIdentifier, mReply.ErrCode, mReply.ErrMsg, "Encode Msg error", oldConn.RemoteAddr().String(), err.Error())
		return
	}
	err = ws.writeMsg(oldConn, websocket.BinaryMessage, b.Bytes())
	if err != nil {
		log.NewError(mReply.OperationID, mReply.ReqIdentifier, mReply.ErrCode, mReply.ErrMsg, "sendKickMsg WS WriteMsg error", oldConn.RemoteAddr().String(), err.Error())
	}
}

func (ws *WServer) addUserConn(uid string, platformID int, conn *UserConn, token string, operationID string) {
	rwLock.Lock()
	defer rwLock.Unlock()
	log.Info(operationID, utils.GetSelfFuncName(), " args: ", uid, platformID, conn, token, "ip: ", conn.RemoteAddr().String())
	callbackResp := callbackUserOnline(operationID, uid, platformID, token)
	if callbackResp.ErrCode != 0 {
		log.NewError(operationID, utils.GetSelfFuncName(), "callbackUserOnline resp:", callbackResp)
	}
	// 多终端登录检测 axis. quit some illegal conn by relay rpc server
	go ws.MultiTerminalLoginRemoteChecker(uid, int32(platformID), token, operationID)
	ws.MultiTerminalLoginChecker(uid, platformID, conn, token, operationID) // quit some illegal conn and send kicked msg to corresponding client [axis]
	if oldConnMap, ok := ws.wsUserToConn[uid]; ok {
		oldConnMap[platformID] = conn
		ws.wsUserToConn[uid] = oldConnMap
		log.Debug(operationID, "user not first come in, add conn ", uid, platformID, conn, oldConnMap)
	} else {
		i := make(map[int]*UserConn)
		i[platformID] = conn
		ws.wsUserToConn[uid] = i
		log.Debug(operationID, "user first come in, new user, conn", uid, platformID, conn, ws.wsUserToConn[uid])
	}
	if oldStringMap, ok := ws.wsConnToUser[conn]; ok {
		oldStringMap[platformID] = uid
		ws.wsConnToUser[conn] = oldStringMap
	} else {
		i := make(map[int]string)
		i[platformID] = uid
		ws.wsConnToUser[conn] = i
	}
	count := 0
	for _, v := range ws.wsUserToConn {
		count = count + len(v)
	}
	promePkg.PromeGaugeInc(promePkg.OnlineUserGauge)
	log.Debug(operationID, "WS Add operation", "", "wsUser added", ws.wsUserToConn, "connection_uid", uid, "connection_platform", constant.PlatformIDToName(platformID), "online_user_num", len(ws.wsUserToConn), "online_conn_num", count)
}

// delUserConn del current user's conn that on the same platform. [axis]
func (ws *WServer) delUserConn(conn *UserConn) {
	rwLock.Lock()
	defer rwLock.Unlock()
	operationID := utils.OperationIDGenerator()
	var uid string
	var platform int
	if oldStringMap, ok := ws.wsConnToUser[conn]; ok {
		for k, v := range oldStringMap {
			platform = k
			uid = v
		}
		if oldConnMap, ok := ws.wsUserToConn[uid]; ok {
			delete(oldConnMap, platform)
			ws.wsUserToConn[uid] = oldConnMap
			if len(oldConnMap) == 0 {
				delete(ws.wsUserToConn, uid)
			}
			count := 0
			for _, v := range ws.wsUserToConn {
				count = count + len(v)
			}
			log.Debug(operationID, "WS delete operation", "", "wsUser deleted", ws.wsUserToConn, "disconnection_uid", uid, "disconnection_platform", platform, "online_user_num", len(ws.wsUserToConn), "online_conn_num", count)
		} else {
			log.Debug(operationID, "WS delete operation", "", "wsUser deleted", ws.wsUserToConn, "disconnection_uid", uid, "disconnection_platform", platform, "online_user_num", len(ws.wsUserToConn))
		}
		delete(ws.wsConnToUser, conn)

	}
	err := conn.Close()
	if err != nil {
		log.Error(operationID, " close err", "", "uid", uid, "platform", platform)
	}
	callbackResp := callbackUserOffline(operationID, uid, platform)
	if callbackResp.ErrCode != 0 {
		log.NewError(operationID, utils.GetSelfFuncName(), "callbackUserOffline failed", callbackResp)
	}
	promePkg.PromeGaugeDec(promePkg.OnlineUserGauge)
}

func (ws *WServer) getUserConn(uid string, platform int) *UserConn {
	rwLock.RLock()
	defer rwLock.RUnlock()
	if connMap, ok := ws.wsUserToConn[uid]; ok {
		if conn, flag := connMap[platform]; flag {
			return conn
		}
	}
	return nil
}
func (ws *WServer) getUserAllCons(uid string) map[int]*UserConn {
	rwLock.RLock()
	defer rwLock.RUnlock()
	if connMap, ok := ws.wsUserToConn[uid]; ok {
		newConnMap := make(map[int]*UserConn)
		for k, v := range connMap {
			newConnMap[k] = v
		}
		return newConnMap
	}
	return nil
}

//	func (ws *WServer) getUserUid(conn *UserConn) (uid string, platform int) {
//		rwLock.RLock()
//		defer rwLock.RUnlock()
//
//		if stringMap, ok := ws.wsConnToUser[conn]; ok {
//			for k, v := range stringMap {
//				platform = k
//				uid = v
//			}
//			return uid, platform
//		}
//		return "", 0
//	}

// headerCheck 用来处理websocket连接创建前的身份认证操作 axis
func (ws *WServer) headerCheck(w http.ResponseWriter, r *http.Request, operationID string) bool {
	status := http.StatusUnauthorized
	query := r.URL.Query()
	if len(query["token"]) != 0 && len(query["sendID"]) != 0 && len(query["platformID"]) != 0 {
		if ok, err, msg := token_verify.WsVerifyToken(query["token"][0], query["sendID"][0], query["platformID"][0], operationID); !ok {
			if errors.Is(err, constant.ErrTokenExpired) {
				status = int(constant.ErrTokenExpired.ErrCode)
			}
			if errors.Is(err, constant.ErrTokenInvalid) {
				status = int(constant.ErrTokenInvalid.ErrCode)
			}
			// token 格式错误 axis
			if errors.Is(err, constant.ErrTokenMalformed) {
				status = int(constant.ErrTokenMalformed.ErrCode)
			}
			if errors.Is(err, constant.ErrTokenNotValidYet) {
				status = int(constant.ErrTokenNotValidYet.ErrCode)
			}
			if errors.Is(err, constant.ErrTokenUnknown) {
				status = int(constant.ErrTokenUnknown.ErrCode)
			}
			// 当前平台的登录已经被踢下线，类似WeChat在App端退出PC端的登录 axiszql@e.gzhu.edu.cn
			if errors.Is(err, constant.ErrTokenKicked) {
				status = int(constant.ErrTokenKicked.ErrCode)
			}
			// 当前token不属于当前platform axis
			if errors.Is(err, constant.ErrTokenDifferentPlatformID) {
				status = int(constant.ErrTokenDifferentPlatformID.ErrCode)
			}
			if errors.Is(err, constant.ErrTokenDifferentUserID) {
				status = int(constant.ErrTokenDifferentUserID.ErrCode)
			}
			//switch errors.Cause(err) {
			//case constant.ErrTokenExpired:
			//	status = int(constant.ErrTokenExpired.ErrCode)
			//case constant.ErrTokenInvalid:
			//	status = int(constant.ErrTokenInvalid.ErrCode)
			//case constant.ErrTokenMalformed:
			//	status = int(constant.ErrTokenMalformed.ErrCode)
			//case constant.ErrTokenNotValidYet:
			//	status = int(constant.ErrTokenNotValidYet.ErrCode)
			//case constant.ErrTokenUnknown:
			//	status = int(constant.ErrTokenUnknown.ErrCode)
			//case constant.ErrTokenKicked:
			//	status = int(constant.ErrTokenKicked.ErrCode)
			//case constant.ErrTokenDifferentPlatformID:
			//	status = int(constant.ErrTokenDifferentPlatformID.ErrCode)
			//case constant.ErrTokenDifferentUserID:
			//	status = int(constant.ErrTokenDifferentUserID.ErrCode)
			//}

			log.Error(operationID, "Token verify failed ", "query ", query, msg, err.Error(), "status: ", status)
			w.Header().Set("Sec-Websocket-Version", "13")
			w.Header().Set("ws_err_msg", err.Error())
			http.Error(w, err.Error(), status)
			return false
		} else {
			log.Info(operationID, "Connection Authentication Success", "", "token ", query["token"][0], "userID ", query["sendID"][0], "platformID ", query["platformID"][0])
			return true
		}
	} else {
		status = int(constant.ErrArgs.ErrCode)
		log.Error(operationID, "Args err ", "query ", query)
		w.Header().Set("Sec-Websocket-Version", "13")
		errMsg := "args err, need token, sendID, platformID"
		w.Header().Set("ws_err_msg", errMsg)
		http.Error(w, errMsg, status)
		return false
	}
}
