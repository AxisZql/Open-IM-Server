package msg

import (
	utils2 "Open_IM/internal/utils"
	"Open_IM/pkg/common/config"
	"Open_IM/pkg/common/constant"
	"Open_IM/pkg/common/db"
	rocksCache "Open_IM/pkg/common/db/rocks_cache"
	"Open_IM/pkg/common/log"
	"Open_IM/pkg/common/token_verify"
	"Open_IM/pkg/grpc-etcdv3/getcdv3"
	cacheRpc "Open_IM/pkg/proto/cache"
	pbConversation "Open_IM/pkg/proto/conversation"
	pbChat "Open_IM/pkg/proto/msg"
	pbPush "Open_IM/pkg/proto/push"
	pbRelay "Open_IM/pkg/proto/relay"
	sdk_ws "Open_IM/pkg/proto/sdk_ws"
	"Open_IM/pkg/utils"
	"context"
	"errors"
	"math/rand"
	"strconv"
	"strings"
	"sync"
	"time"

	promePkg "Open_IM/pkg/common/prometheus"

	go_redis "github.com/go-redis/redis/v8"
	"github.com/golang/protobuf/proto"
)

// When the number of group members is greater than this value，Online users will be sent first，Guaranteed service availability
const GroupMemberNum = 500

var (
	// axis 排除的消息类型：1. 已读消息回执  2. 群组已读消息回执
	ExcludeContentType = []int{constant.HasReadReceipt, constant.GroupHasReadReceipt}
)

type Validator interface {
	validate(pb *pbChat.SendMsgReq) (bool, int32, string)
}

//type MessageValidator struct {
//
//}

// MessageRevoked 撤回类型消息 axis
type MessageRevoked struct {
	RevokerID                   string `json:"revokerID"`
	RevokerRole                 int32  `json:"revokerRole"`
	ClientMsgID                 string `json:"clientMsgID"`
	RevokerNickname             string `json:"revokerNickname"`
	RevokeTime                  int64  `json:"revokeTime"`
	SourceMessageSendTime       int64  `json:"sourceMessageSendTime"`
	SourceMessageSendID         string `json:"sourceMessageSendID"`
	SourceMessageSenderNickname string `json:"sourceMessageSenderNickname"`
	SessionType                 int32  `json:"sessionType"`
	Seq                         uint32 `json:"seq"`
}
type MsgCallBackReq struct {
	SendID       string `json:"sendID"`
	RecvID       string `json:"recvID"`
	Content      string `json:"content"`
	SendTime     int64  `json:"sendTime"`
	MsgFrom      int32  `json:"msgFrom"`
	ContentType  int32  `json:"contentType"`
	SessionType  int32  `json:"sessionType"`
	PlatformID   int32  `json:"senderPlatformID"`
	MsgID        string `json:"msgID"`
	IsOnlineOnly bool   `json:"isOnlineOnly"`
}
type MsgCallBackResp struct {
	ErrCode         int32  `json:"errCode"`
	ErrMsg          string `json:"errMsg"`
	ResponseErrCode int32  `json:"responseErrCode"`
	ResponseResult  struct {
		ModifiedMsg string `json:"modifiedMsg"`
		Ext         string `json:"ext"`
	}
}

// 判断消息已读功能是否启用
func isMessageHasReadEnabled(pb *pbChat.SendMsgReq) (bool, int32, string) {
	switch pb.MsgData.ContentType {
	case constant.HasReadReceipt:
		if config.Config.SingleMessageHasReadReceiptEnable {
			return true, 0, ""
		} else {
			return false, constant.ErrMessageHasReadDisable.ErrCode, constant.ErrMessageHasReadDisable.ErrMsg
		}
	case constant.GroupHasReadReceipt:
		if config.Config.GroupMessageHasReadReceiptEnable {
			return true, 0, ""
		} else {
			return false, constant.ErrMessageHasReadDisable.ErrCode, constant.ErrMessageHasReadDisable.ErrMsg
		}
	}
	return true, 0, ""
}

func userIsMuteInGroup(groupID, userID string) (bool, error) {
	// 从缓存中获取群成员信息，用rackcache确保缓存和db的强一致性
	groupMemberInfo, err := rocksCache.GetGroupMemberInfoFromCache(groupID, userID)
	if err != nil {
		return false, utils.Wrap(err, "")
	}
	if groupMemberInfo.MuteEndTime.Unix() >= time.Now().Unix() {
		return true, nil
	}
	return false, nil
}

func (rpc *RpcChat) messageVerification(data *pbChat.SendMsgReq) (bool, int32, string, []string) {
	switch data.MsgData.SessionType {
	case constant.SingleChatType:
		// 管理员发的的消息免验证  axis
		if utils.IsContain(data.MsgData.SendID, config.Config.Manager.AppManagerUid) {
			return true, 0, "", nil
		}
		// 通知类型的消息免验证 axis
		if data.MsgData.ContentType <= constant.NotificationEnd && data.MsgData.ContentType >= constant.NotificationBegin {
			return true, 0, "", nil
		}
		log.NewDebug(data.OperationID, *config.Config.MessageVerify.FriendVerify)
		// 获取消息接收方的黑名单列表信息 axis
		reqGetBlackIDListFromCache := &cacheRpc.GetBlackIDListFromCacheReq{UserID: data.MsgData.RecvID, OperationID: data.OperationID}
		etcdConn := getcdv3.GetDefaultConn(config.Config.Etcd.EtcdSchema, strings.Join(config.Config.Etcd.EtcdAddr, ","), config.Config.RpcRegisterName.OpenImCacheName, data.OperationID)
		if etcdConn == nil {
			errMsg := data.OperationID + "getcdv3.GetDefaultConn == nil"
			log.NewError(data.OperationID, errMsg)
			return true, 0, "", nil
		}

		cacheClient := cacheRpc.NewCacheClient(etcdConn)
		cacheResp, err := cacheClient.GetBlackIDListFromCache(context.Background(), reqGetBlackIDListFromCache)
		if err != nil {
			log.NewError(data.OperationID, "GetBlackIDListFromCache rpc call failed ", err.Error())
		} else {
			if cacheResp.CommonResp.ErrCode != 0 {
				log.NewError(data.OperationID, "GetBlackIDListFromCache rpc logic call failed ", cacheResp.String())
			} else {
				if utils.IsContain(data.MsgData.SendID, cacheResp.UserIDList) {
					return false, 600, "in black list", nil
				}
			}
		}
		// 消息好友验证 axis
		log.NewDebug(data.OperationID, *config.Config.MessageVerify.FriendVerify)
		if *config.Config.MessageVerify.FriendVerify {
			reqGetFriendIDListFromCache := &cacheRpc.GetFriendIDListFromCacheReq{UserID: data.MsgData.RecvID, OperationID: data.OperationID}
			etcdConn := getcdv3.GetDefaultConn(config.Config.Etcd.EtcdSchema, strings.Join(config.Config.Etcd.EtcdAddr, ","), config.Config.RpcRegisterName.OpenImCacheName, data.OperationID)
			if etcdConn == nil {
				errMsg := data.OperationID + "getcdv3.GetDefaultConn == nil"
				log.NewError(data.OperationID, errMsg)
				return true, 0, "", nil
			}
			cacheClient := cacheRpc.NewCacheClient(etcdConn)
			cacheResp, err := cacheClient.GetFriendIDListFromCache(context.Background(), reqGetFriendIDListFromCache)
			if err != nil {
				log.NewError(data.OperationID, "GetFriendIDListFromCache rpc call failed ", err.Error())
			} else {
				if cacheResp.CommonResp.ErrCode != 0 {
					log.NewError(data.OperationID, "GetFriendIDListFromCache rpc logic call failed ", cacheResp.String())
				} else {
					if !utils.IsContain(data.MsgData.SendID, cacheResp.UserIDList) {
						return false, 601, "not friend", nil
					}
				}
			}
			return true, 0, "", nil
		} else {
			return true, 0, "", nil
		}
	case constant.GroupChatType:
		userIDList, err := utils2.GetGroupMemberUserIDList(data.MsgData.GroupID, data.OperationID)
		if err != nil {
			errMsg := data.OperationID + err.Error()
			log.NewError(data.OperationID, errMsg)
			return false, 201, errMsg, nil
		}
		if !token_verify.IsManagerUserID(data.MsgData.SendID) {
			if data.MsgData.ContentType <= constant.NotificationEnd && data.MsgData.ContentType >= constant.NotificationBegin {
				return true, 0, "", userIDList
			}
			if !utils.IsContain(data.MsgData.SendID, userIDList) {
				//return returnMsg(&replay, pb, 202, "you are not in group", "", 0)
				return false, 202, "you are not in group", nil
			}
		}
		isMute, err := userIsMuteInGroup(data.MsgData.GroupID, data.MsgData.SendID)
		if err != nil {
			errMsg := data.OperationID + err.Error()
			return false, 223, errMsg, nil
		}
		if isMute {
			return false, 224, "you are muted", nil
		}
		return true, 0, "", userIDList
	case constant.SuperGroupChatType:
		groupInfo, err := rocksCache.GetGroupInfoFromCache(data.MsgData.GroupID)
		if err != nil {
			return false, 201, err.Error(), nil
		}

		if data.MsgData.ContentType == constant.AdvancedRevoke {
			revokeMessage := new(MessageRevoked)
			err := utils.JsonStringToStruct(string(data.MsgData.Content), revokeMessage)
			if err != nil {
				log.Error(data.OperationID, "json unmarshal err:", err.Error())
				return false, 201, err.Error(), nil
			}
			log.Debug(data.OperationID, "revoke message is", *revokeMessage)
			if revokeMessage.RevokerID != revokeMessage.SourceMessageSendID {
				req := pbChat.GetSuperGroupMsgReq{OperationID: data.OperationID, Seq: revokeMessage.Seq, GroupID: data.MsgData.GroupID}
				resp, err := rpc.GetSuperGroupMsg(context.Background(), &req)
				if err != nil {
					log.Error(data.OperationID, "GetSuperGroupMsgReq err:", err.Error())
				} else if resp.ErrCode != 0 {
					log.Error(data.OperationID, "GetSuperGroupMsgReq err:", resp.ErrCode, resp.ErrMsg)
				} else {
					if resp.MsgData != nil && resp.MsgData.ClientMsgID == revokeMessage.ClientMsgID && resp.MsgData.Seq == revokeMessage.Seq {
						revokeMessage.SourceMessageSendTime = resp.MsgData.SendTime
						revokeMessage.SourceMessageSenderNickname = resp.MsgData.SenderNickname
						revokeMessage.SourceMessageSendID = resp.MsgData.SendID
						log.Debug(data.OperationID, "new revoke message is ", revokeMessage)
						data.MsgData.Content = []byte(utils.StructToJsonString(revokeMessage))
					} else {
						return false, 201, errors.New("msg err").Error(), nil
					}
				}
			}
		}
		// TODO: case 不是确保了是SuperGroup了吗？
		if groupInfo.GroupType == constant.SuperGroup {
			return true, 0, "", nil
		} else {
			userIDList, err := utils2.GetGroupMemberUserIDList(data.MsgData.GroupID, data.OperationID)
			if err != nil {
				errMsg := data.OperationID + err.Error()
				log.NewError(data.OperationID, errMsg)
				return false, 201, errMsg, nil
			}
			if !token_verify.IsManagerUserID(data.MsgData.SendID) {
				if data.MsgData.ContentType <= constant.NotificationEnd && data.MsgData.ContentType >= constant.NotificationBegin {
					return true, 0, "", userIDList
				}
				if !utils.IsContain(data.MsgData.SendID, userIDList) {
					//return returnMsg(&replay, pb, 202, "you are not in group", "", 0)
					return false, 202, "you are not in group", nil
				}
			}
			isMute, err := userIsMuteInGroup(data.MsgData.GroupID, data.MsgData.SendID)
			if err != nil {
				errMsg := data.OperationID + err.Error()
				return false, 223, errMsg, nil
			}
			if isMute {
				return false, 224, "you are muted", nil
			}
			return true, 0, "", userIDList
		}
	default:
		return true, 0, "", nil
	}

}
func (rpc *RpcChat) encapsulateMsgData(msg *sdk_ws.MsgData) {
	msg.ServerMsgID = GetMsgID(msg.SendID)
	msg.SendTime = utils.GetCurrentTimestampByMill()
	switch msg.ContentType {
	case constant.Text:
		fallthrough
	case constant.Picture:
		fallthrough
	case constant.Voice:
		fallthrough
	case constant.Video:
		fallthrough
	case constant.File:
		fallthrough
	case constant.AtText:
		fallthrough
	case constant.Merger:
		fallthrough
	case constant.Card:
		fallthrough
	case constant.Location:
		fallthrough
	case constant.Custom:
		fallthrough
	case constant.Quote:
		// 引用类型消息记录	axis
		utils.SetSwitchFromOptions(msg.Options, constant.IsConversationUpdate, true)
		utils.SetSwitchFromOptions(msg.Options, constant.IsUnreadCount, true)
		utils.SetSwitchFromOptions(msg.Options, constant.IsSenderSync, true)
	case constant.Revoke:
		// 撤回类型消息不设置已读提示，且不能离线推送 axis
		utils.SetSwitchFromOptions(msg.Options, constant.IsUnreadCount, false)
		utils.SetSwitchFromOptions(msg.Options, constant.IsOfflinePush, false)
	case constant.HasReadReceipt:
		// 已读回执类型消息 axis
		log.Info("", "this is a test start", msg, msg.Options)
		utils.SetSwitchFromOptions(msg.Options, constant.IsConversationUpdate, false)
		utils.SetSwitchFromOptions(msg.Options, constant.IsSenderConversationUpdate, false)
		utils.SetSwitchFromOptions(msg.Options, constant.IsUnreadCount, false)
		utils.SetSwitchFromOptions(msg.Options, constant.IsOfflinePush, false)
		log.Info("", "this is a test end", msg, msg.Options)
	case constant.Typing:
		utils.SetSwitchFromOptions(msg.Options, constant.IsHistory, false)
		utils.SetSwitchFromOptions(msg.Options, constant.IsPersistent, false)
		utils.SetSwitchFromOptions(msg.Options, constant.IsSenderSync, false)
		utils.SetSwitchFromOptions(msg.Options, constant.IsConversationUpdate, false)
		utils.SetSwitchFromOptions(msg.Options, constant.IsSenderConversationUpdate, false)
		utils.SetSwitchFromOptions(msg.Options, constant.IsUnreadCount, false)
		utils.SetSwitchFromOptions(msg.Options, constant.IsOfflinePush, false)
	}
}
func (rpc *RpcChat) SendMsg(_ context.Context, pb *pbChat.SendMsgReq) (*pbChat.SendMsgResp, error) {
	replay := pbChat.SendMsgResp{}
	log.Info(pb.OperationID, "rpc sendMsg come here ", pb.String())
	flag, errCode, errMsg := isMessageHasReadEnabled(pb)
	if !flag {
		return returnMsg(&replay, pb, errCode, errMsg, "", 0)
	}
	t1 := time.Now()
	rpc.encapsulateMsgData(pb.MsgData)
	log.Debug(pb.OperationID, "encapsulateMsgData ", " cost time: ", time.Since(t1))
	msgToMQSingle := pbChat.MsgDataToMQ{Token: pb.Token, OperationID: pb.OperationID, MsgData: pb.MsgData}
	// callback
	t1 = time.Now()
	callbackResp := callbackMsgModify(pb)
	log.Debug(pb.OperationID, "callbackMsgModify ", callbackResp, "cost time: ", time.Since(t1))
	if callbackResp.ErrCode != 0 {
		log.Error(pb.OperationID, utils.GetSelfFuncName(), "callbackMsgModify resp: ", callbackResp)
	}
	log.NewDebug(pb.OperationID, utils.GetSelfFuncName(), "callbackResp: ", callbackResp)
	if callbackResp.ActionCode != constant.ActionAllow {
		if callbackResp.ErrCode == 0 {
			callbackResp.ErrCode = 201
		}
		log.NewDebug(pb.OperationID, utils.GetSelfFuncName(), "callbackMsgModify result", "end rpc and return", pb.MsgData)
		return returnMsg(&replay, pb, int32(callbackResp.ErrCode), callbackResp.ErrMsg, "", 0)
	}
	switch pb.MsgData.SessionType {
	case constant.SingleChatType:
		promePkg.PromeInc(promePkg.SingleChatMsgRecvSuccessCounter)
		// callback
		t1 = time.Now()
		callbackResp := callbackBeforeSendSingleMsg(pb)
		log.Debug(pb.OperationID, "callbackBeforeSendSingleMsg ", " cost time: ", time.Since(t1))
		if callbackResp.ErrCode != 0 {
			log.NewError(pb.OperationID, utils.GetSelfFuncName(), "callbackBeforeSendSingleMsg resp: ", callbackResp)
		}
		if callbackResp.ActionCode != constant.ActionAllow {
			if callbackResp.ErrCode == 0 {
				callbackResp.ErrCode = 201
			}
			log.NewDebug(pb.OperationID, utils.GetSelfFuncName(), "callbackBeforeSendSingleMsg result", "end rpc and return", callbackResp)
			promePkg.PromeInc(promePkg.SingleChatMsgProcessFailedCounter)
			return returnMsg(&replay, pb, int32(callbackResp.ErrCode), callbackResp.ErrMsg, "", 0)
		}
		t1 = time.Now()
		flag, errCode, errMsg, _ = rpc.messageVerification(pb)
		log.Debug(pb.OperationID, "messageVerification ", flag, " cost time: ", time.Since(t1))
		if !flag {
			return returnMsg(&replay, pb, errCode, errMsg, "", 0)
		}
		t1 = time.Now()
		// 根据接收方的消息接收设置判断是否发送该消息 axis
		isSend := modifyMessageByUserMessageReceiveOpt(pb.MsgData.RecvID, pb.MsgData.SendID, constant.SingleChatType, pb)
		log.Info(pb.OperationID, "modifyMessageByUserMessageReceiveOpt ", " cost time: ", time.Since(t1))
		// 本系统采用的是写扩散的消息收发模型，故会同时往收发两方的信箱中写入消息
		if isSend {
			msgToMQSingle.MsgData = pb.MsgData
			log.NewInfo(msgToMQSingle.OperationID, msgToMQSingle)
			t1 = time.Now()
			// 这里采用key来区分每个用户的信箱的效果要比为每个用户创建一个单独的topic充当信箱的性能要好，通过开启partition的hash分区，确保了
			// 相同key的消息落到同一个partition中，避免了因为使用多个topic的partition而产生的消息乱序问题【因为Kafka是顺序读取每个partition中的msg的】
			err1 := rpc.sendMsgToWriter(&msgToMQSingle, msgToMQSingle.MsgData.RecvID, constant.OnlineStatus)
			log.Info(pb.OperationID, "sendMsgToWriter ", " cost time: ", time.Since(t1))
			if err1 != nil {
				log.NewError(msgToMQSingle.OperationID, "kafka send msg err :RecvID", msgToMQSingle.MsgData.RecvID, msgToMQSingle.String(), err1.Error())
				promePkg.PromeInc(promePkg.SingleChatMsgProcessFailedCounter)
				return returnMsg(&replay, pb, 201, "kafka send msg err", "", 0)
			}
		}
		if msgToMQSingle.MsgData.SendID != msgToMQSingle.MsgData.RecvID { //Filter messages sent to yourself
			t1 = time.Now()
			err2 := rpc.sendMsgToWriter(&msgToMQSingle, msgToMQSingle.MsgData.SendID, constant.OnlineStatus)
			log.Info(pb.OperationID, "sendMsgToWriter ", " cost time: ", time.Since(t1))
			if err2 != nil {
				log.NewError(msgToMQSingle.OperationID, "kafka send msg err:SendID", msgToMQSingle.MsgData.SendID, msgToMQSingle.String())
				promePkg.PromeInc(promePkg.SingleChatMsgProcessFailedCounter)
				return returnMsg(&replay, pb, 201, "kafka send msg err", "", 0)
			}
		}
		// callback
		t1 = time.Now()
		callbackResp = callbackAfterSendSingleMsg(pb)
		log.Info(pb.OperationID, "callbackAfterSendSingleMsg ", " cost time: ", time.Since(t1))
		if callbackResp.ErrCode != 0 {
			log.NewError(pb.OperationID, utils.GetSelfFuncName(), "callbackAfterSendSingleMsg resp: ", callbackResp)
		}
		promePkg.PromeInc(promePkg.SingleChatMsgProcessSuccessCounter)
		return returnMsg(&replay, pb, 0, "", msgToMQSingle.MsgData.ServerMsgID, msgToMQSingle.MsgData.SendTime)
	case constant.GroupChatType:
		// callback
		promePkg.PromeInc(promePkg.GroupChatMsgRecvSuccessCounter)
		callbackResp := callbackBeforeSendGroupMsg(pb)
		if callbackResp.ErrCode != 0 {
			log.NewError(pb.OperationID, utils.GetSelfFuncName(), "callbackBeforeSendGroupMsg resp:", callbackResp)
		}
		if callbackResp.ActionCode != constant.ActionAllow {
			if callbackResp.ErrCode == 0 {
				callbackResp.ErrCode = 201
			}
			log.NewDebug(pb.OperationID, utils.GetSelfFuncName(), "callbackBeforeSendSingleMsg result", "end rpc and return", callbackResp)
			promePkg.PromeInc(promePkg.GroupChatMsgProcessFailedCounter)
			return returnMsg(&replay, pb, int32(callbackResp.ErrCode), callbackResp.ErrMsg, "", 0)
		}
		var memberUserIDList []string
		if flag, errCode, errMsg, memberUserIDList = rpc.messageVerification(pb); !flag {
			promePkg.PromeInc(promePkg.GroupChatMsgProcessFailedCounter)
			return returnMsg(&replay, pb, errCode, errMsg, "", 0)
		}
		log.Debug(pb.OperationID, "GetGroupAllMember userID list", memberUserIDList, "len: ", len(memberUserIDList))
		var addUidList []string
		switch pb.MsgData.ContentType {
		// 被踢用户被动接收踢出通知 axis
		case constant.MemberKickedNotification:
			var tips sdk_ws.TipsComm
			var memberKickedTips sdk_ws.MemberKickedTips
			err := proto.Unmarshal(pb.MsgData.Content, &tips)
			if err != nil {
				log.Error(pb.OperationID, "Unmarshal err", err.Error())
			}
			err = proto.Unmarshal(tips.Detail, &memberKickedTips)
			if err != nil {
				log.Error(pb.OperationID, "Unmarshal err", err.Error())
			}
			log.Info(pb.OperationID, "data is ", memberKickedTips)
			for _, v := range memberKickedTips.KickedUserList {
				addUidList = append(addUidList, v.UserID)
			}
			// 用户主动发送推出群聊通知 axis
		case constant.MemberQuitNotification:
			addUidList = append(addUidList, pb.MsgData.SendID)

		default:
		}
		// 因为memberUserIDList开始存放的数据是所有在群成员id，adUidList存放的是因为被踢或者主动退群而不在群的成员id axis
		if len(addUidList) > 0 {
			memberUserIDList = append(memberUserIDList, addUidList...)
		}
		m := make(map[string][]string, 2)
		m[constant.OnlineStatus] = memberUserIDList
		t1 = time.Now()

		//split  parallel send
		var wg sync.WaitGroup
		var sendTag bool
		var split = 20
		for k, v := range m {
			remain := len(v) % split // 分批发送 axis
			for i := 0; i < len(v)/split; i++ {
				wg.Add(1)
				tmp := valueCopy(pb)
				//	go rpc.sendMsgToGroup(v[i*split:(i+1)*split], *pb, k, &sendTag, &wg)
				go rpc.sendMsgToGroupOptimization(v[i*split:(i+1)*split], tmp, k, &sendTag, &wg)
			}
			if remain > 0 {
				// 处理不满足一批次的消息 axis
				wg.Add(1)
				tmp := valueCopy(pb)
				//	go rpc.sendMsgToGroup(v[split*(len(v)/split):], *pb, k, &sendTag, &wg)
				go rpc.sendMsgToGroupOptimization(v[split*(len(v)/split):], tmp, k, &sendTag, &wg)
			}
		}
		log.Debug(pb.OperationID, "send msg cost time22 ", time.Since(t1), pb.MsgData.ClientMsgID, "uidList : ", len(addUidList))
		//wg.Add(1)
		//go rpc.sendMsgToGroup(addUidList, *pb, constant.OnlineStatus, &sendTag, &wg)
		wg.Wait()
		t1 = time.Now()
		// callback
		callbackResp = callbackAfterSendGroupMsg(pb)
		if callbackResp.ErrCode != 0 {
			log.NewError(pb.OperationID, utils.GetSelfFuncName(), "callbackAfterSendGroupMsg resp: ", callbackResp)
		}
		// TODO: 存在的问题，上述流程中只要一批消息发送成功后sendTag即为true，所以sendTag==true无法保证
		// 所有批次的消息发送成功
		// 发送成功与否后的处理流程  axis
		if !sendTag {
			log.NewWarn(pb.OperationID, "send tag is ", sendTag)
			promePkg.PromeInc(promePkg.GroupChatMsgProcessFailedCounter)
			return returnMsg(&replay, pb, 201, "kafka send msg err", "", 0)
		} else {
			// 如果时@类型消息 axis
			if pb.MsgData.ContentType == constant.AtText {
				go func() {
					var conversationReq pbConversation.ModifyConversationFieldReq
					var tag bool
					var atUserID []string
					conversation := pbConversation.Conversation{
						OwnerUserID:      pb.MsgData.SendID,
						ConversationID:   utils.GetConversationIDBySessionType(pb.MsgData.GroupID, constant.GroupChatType),
						ConversationType: constant.GroupChatType,
						GroupID:          pb.MsgData.GroupID,
					}
					conversationReq.Conversation = &conversation
					conversationReq.OperationID = pb.OperationID
					conversationReq.FieldType = constant.FieldGroupAtType
					// 判断@列表中是否包含@所有人的值 axis
					tagAll := utils.IsContain(constant.AtAllString, pb.MsgData.AtUserIDList)
					if tagAll {
						atUserID = utils.DifferenceString([]string{constant.AtAllString}, pb.MsgData.AtUserIDList)
						if len(atUserID) == 0 { //just @everyone
							conversationReq.UserIDList = memberUserIDList
							conversation.GroupAtType = constant.AtAll
						} else { //@Everyone and @other people
							conversationReq.UserIDList = atUserID
							conversation.GroupAtType = constant.AtAllAtMe
							tag = true
						}
					} else {
						conversationReq.UserIDList = pb.MsgData.AtUserIDList
						conversation.GroupAtType = constant.AtMe
					}
					etcdConn := getcdv3.GetDefaultConn(config.Config.Etcd.EtcdSchema, strings.Join(config.Config.Etcd.EtcdAddr, ","), config.Config.RpcRegisterName.OpenImConversationName, pb.OperationID)
					if etcdConn == nil {
						errMsg := pb.OperationID + "getcdv3.GetDefaultConn == nil"
						log.NewError(pb.OperationID, errMsg)
						return
					}
					client := pbConversation.NewConversationClient(etcdConn)
					conversationReply, err := client.ModifyConversationField(context.Background(), &conversationReq)
					if err != nil {
						log.NewError(conversationReq.OperationID, "ModifyConversationField rpc failed, ", conversationReq.String(), err.Error())
					} else if conversationReply.CommonResp.ErrCode != 0 {
						log.NewError(conversationReq.OperationID, "ModifyConversationField rpc failed, ", conversationReq.String(), conversationReply.String())
					}
					if tag {
						//@Everyone and @other people
						conversationReq.UserIDList = utils.DifferenceString(atUserID, memberUserIDList)
						conversation.GroupAtType = constant.AtAll
						etcdConn := getcdv3.GetDefaultConn(config.Config.Etcd.EtcdSchema, strings.Join(config.Config.Etcd.EtcdAddr, ","), config.Config.RpcRegisterName.OpenImConversationName, pb.OperationID)
						if etcdConn == nil {
							errMsg := pb.OperationID + "getcdv3.GetDefaultConn == nil"
							log.NewError(pb.OperationID, errMsg)
							return
						}
						// 新建会话 axis
						client := pbConversation.NewConversationClient(etcdConn)
						conversationReply, err := client.ModifyConversationField(context.Background(), &conversationReq)
						if err != nil {
							log.NewError(conversationReq.OperationID, "ModifyConversationField rpc failed, ", conversationReq.String(), err.Error())
						} else if conversationReply.CommonResp.ErrCode != 0 {
							log.NewError(conversationReq.OperationID, "ModifyConversationField rpc failed, ", conversationReq.String(), conversationReply.String())
						}
					}
				}()
			}
			log.Debug(pb.OperationID, "send msg cost time3 ", time.Since(t1), pb.MsgData.ClientMsgID)
			promePkg.PromeInc(promePkg.GroupChatMsgProcessSuccessCounter)
			return returnMsg(&replay, pb, 0, "", msgToMQSingle.MsgData.ServerMsgID, msgToMQSingle.MsgData.SendTime)
		}
	case constant.NotificationChatType:
		t1 = time.Now()
		msgToMQSingle.MsgData = pb.MsgData
		log.NewInfo(msgToMQSingle.OperationID, msgToMQSingle)
		err1 := rpc.sendMsgToWriter(&msgToMQSingle, msgToMQSingle.MsgData.RecvID, constant.OnlineStatus)
		if err1 != nil {
			log.NewError(msgToMQSingle.OperationID, "kafka send msg err:RecvID", msgToMQSingle.MsgData.RecvID, msgToMQSingle.String())
			return returnMsg(&replay, pb, 201, "kafka send msg err", "", 0)
		}

		if msgToMQSingle.MsgData.SendID != msgToMQSingle.MsgData.RecvID { //Filter messages sent to yourself
			err2 := rpc.sendMsgToWriter(&msgToMQSingle, msgToMQSingle.MsgData.SendID, constant.OnlineStatus)
			if err2 != nil {
				log.NewError(msgToMQSingle.OperationID, "kafka send msg err:SendID", msgToMQSingle.MsgData.SendID, msgToMQSingle.String())
				return returnMsg(&replay, pb, 201, "kafka send msg err", "", 0)
			}
		}

		log.Debug(pb.OperationID, "send msg cost time ", time.Since(t1), pb.MsgData.ClientMsgID)
		return returnMsg(&replay, pb, 0, "", msgToMQSingle.MsgData.ServerMsgID, msgToMQSingle.MsgData.SendTime)
	case constant.SuperGroupChatType:
		promePkg.PromeInc(promePkg.WorkSuperGroupChatMsgRecvSuccessCounter)
		// callback
		callbackResp := callbackBeforeSendGroupMsg(pb)
		if callbackResp.ErrCode != 0 {
			log.NewError(pb.OperationID, utils.GetSelfFuncName(), "callbackBeforeSendSuperGroupMsg resp: ", callbackResp)
		}
		if callbackResp.ActionCode != constant.ActionAllow {
			if callbackResp.ErrCode == 0 {
				callbackResp.ErrCode = 201
			}
			promePkg.PromeInc(promePkg.WorkSuperGroupChatMsgProcessFailedCounter)
			log.NewDebug(pb.OperationID, utils.GetSelfFuncName(), "callbackBeforeSendSuperGroupMsg result", "end rpc and return", callbackResp)
			return returnMsg(&replay, pb, int32(callbackResp.ErrCode), callbackResp.ErrMsg, "", 0)
		}
		if flag, errCode, errMsg, _ = rpc.messageVerification(pb); !flag {
			promePkg.PromeInc(promePkg.WorkSuperGroupChatMsgProcessFailedCounter)
			return returnMsg(&replay, pb, errCode, errMsg, "", 0)
		}
		msgToMQSingle.MsgData = pb.MsgData
		log.NewInfo(msgToMQSingle.OperationID, msgToMQSingle)
		err1 := rpc.sendMsgToWriter(&msgToMQSingle, msgToMQSingle.MsgData.GroupID, constant.OnlineStatus)
		if err1 != nil {
			log.NewError(msgToMQSingle.OperationID, "kafka send msg err:RecvID", msgToMQSingle.MsgData.RecvID, msgToMQSingle.String())
			promePkg.PromeInc(promePkg.WorkSuperGroupChatMsgProcessFailedCounter)
			return returnMsg(&replay, pb, 201, "kafka send msg err", "", 0)
		}
		// callback
		callbackResp = callbackAfterSendGroupMsg(pb)
		if callbackResp.ErrCode != 0 {
			log.NewError(pb.OperationID, utils.GetSelfFuncName(), "callbackAfterSendSuperGroupMsg resp: ", callbackResp)
		}
		promePkg.PromeInc(promePkg.WorkSuperGroupChatMsgProcessSuccessCounter)
		return returnMsg(&replay, pb, 0, "", msgToMQSingle.MsgData.ServerMsgID, msgToMQSingle.MsgData.SendTime)

	default:
		return returnMsg(&replay, pb, 203, "unknown sessionType", "", 0)
	}
}

func (rpc *RpcChat) sendMsgToWriter(m *pbChat.MsgDataToMQ, key string, status string) error {
	switch status {
	case constant.OnlineStatus:
		//如果是视频通话通知,则直接调用push rpc服务推送到对应用户连接的客户端 axis
		if m.MsgData.ContentType == constant.SignalingNotification {
			rpcPushMsg := pbPush.PushMsgReq{OperationID: m.OperationID, MsgData: m.MsgData, PushToUserID: key}
			grpcConn := getcdv3.GetDefaultConn(config.Config.Etcd.EtcdSchema, strings.Join(config.Config.Etcd.EtcdAddr, ","), config.Config.RpcRegisterName.OpenImPushName, m.OperationID)
			if grpcConn == nil {
				log.Error(rpcPushMsg.OperationID, "rpc dial failed", "push data", rpcPushMsg.String())
				return errors.New("grpcConn is nil")
			}
			msgClient := pbPush.NewPushMsgServiceClient(grpcConn)
			_, err := msgClient.PushMsg(context.Background(), &rpcPushMsg)
			if err != nil {
				log.Error(rpcPushMsg.OperationID, "rpc send failed", rpcPushMsg.OperationID, "push data", rpcPushMsg.String(), "err", err.Error())
				return err
			} else {
				return nil
			}
		}
		// 将消息写入Kafka axis
		pid, offset, err := rpc.messageWriter.SendMessage(m, key, m.OperationID)
		if err != nil {
			log.Error(m.OperationID, "kafka send failed", "send data", m.String(), "pid", pid, "offset", offset, "err", err.Error(), "key", key, status)
		} else {
			//	log.NewWarn(m.OperationID, "sendMsgToWriter   client msgID ", m.MsgData.ClientMsgID)
		}
		return err
	case constant.OfflineStatus:
		pid, offset, err := rpc.messageWriter.SendMessage(m, key, m.OperationID)
		if err != nil {
			log.Error(m.OperationID, "kafka send failed", "send data", m.String(), "pid", pid, "offset", offset, "err", err.Error(), "key", key, status)
		}
		return err
	}
	return errors.New("status error")
}
func GetMsgID(sendID string) string {
	t := time.Now().Format("2006-01-02 15:04:05")
	return utils.Md5(t + "-" + sendID + "-" + strconv.Itoa(rand.Int()))
}

func returnMsg(replay *pbChat.SendMsgResp, pb *pbChat.SendMsgReq, errCode int32, errMsg, serverMsgID string, sendTime int64) (*pbChat.SendMsgResp, error) {
	replay.ErrCode = errCode
	replay.ErrMsg = errMsg
	replay.ServerMsgID = serverMsgID
	replay.ClientMsgID = pb.MsgData.ClientMsgID
	replay.SendTime = sendTime
	return replay, nil
}

func modifyMessageByUserMessageReceiveOpt(userID, sourceID string, sessionType int, pb *pbChat.SendMsgReq) bool {
	opt, err := db.DB.GetUserGlobalMsgRecvOpt(userID) // 获取接收方的全局接收消息设置 axis
	if err != nil {
		log.NewError(pb.OperationID, "GetUserGlobalMsgRecvOpt from redis err", userID, pb.String(), err.Error())
	}
	switch opt {
	case constant.ReceiveMessage:
	case constant.NotReceiveMessage:
		return false
	case constant.ReceiveNotNotifyMessage:
		// 不接收通知类型的消息即不开启离线消息通知推送 axis
		if pb.MsgData.Options == nil {
			pb.MsgData.Options = make(map[string]bool, 10)
		}
		utils.SetSwitchFromOptions(pb.MsgData.Options, constant.IsOfflinePush, false)
		return true
	}
	conversationID := utils.GetConversationIDBySessionType(sourceID, sessionType)
	// 获取接收方于发送用户的消息会话设置 axis
	singleOpt, sErr := db.DB.GetSingleConversationRecvMsgOpt(userID, conversationID)
	if sErr != nil && sErr != go_redis.Nil {
		log.NewError(pb.OperationID, "GetSingleConversationMsgOpt from redis err", conversationID, pb.String(), sErr.Error())
		return true
	}
	switch singleOpt {
	case constant.ReceiveMessage:
		return true
	case constant.NotReceiveMessage:
		if utils.IsContainInt(int(pb.MsgData.ContentType), ExcludeContentType) {
			return true
		}
		return false
	case constant.ReceiveNotNotifyMessage:
		if pb.MsgData.Options == nil {
			pb.MsgData.Options = make(map[string]bool, 10)
		}
		utils.SetSwitchFromOptions(pb.MsgData.Options, constant.IsOfflinePush, false)
		return true
	}

	return true
}

func modifyMessageByUserMessageReceiveOptOptimization(userID, sourceID string, sessionType int, operationID string, options *map[string]bool) bool {
	conversationID := utils.GetConversationIDBySessionType(sourceID, sessionType)
	opt, err := db.DB.GetSingleConversationRecvMsgOpt(userID, conversationID)
	if err != nil && err != go_redis.Nil {
		log.NewError(operationID, "GetSingleConversationMsgOpt from redis err", userID, conversationID, err.Error())
		return true
	}

	switch opt {
	case constant.ReceiveMessage:
		return true
	case constant.NotReceiveMessage:
		return false
	case constant.ReceiveNotNotifyMessage:
		if *options == nil {
			*options = make(map[string]bool, 10)
		}
		utils.SetSwitchFromOptions(*options, constant.IsOfflinePush, false)
		return true
	}
	return true
}

type NotificationMsg struct {
	SendID         string
	RecvID         string
	Content        []byte //  open_im_sdk.TipsComm
	MsgFrom        int32
	ContentType    int32
	SessionType    int32
	OperationID    string
	SenderNickname string
	SenderFaceURL  string
}

func Notification(n *NotificationMsg) {
	var req pbChat.SendMsgReq
	var msg sdk_ws.MsgData
	var offlineInfo sdk_ws.OfflinePushInfo
	var title, desc, ex string
	var pushSwitch, unReadCount bool
	var reliabilityLevel int
	req.OperationID = n.OperationID
	msg.SendID = n.SendID
	msg.RecvID = n.RecvID
	msg.Content = n.Content
	msg.MsgFrom = n.MsgFrom
	msg.ContentType = n.ContentType
	msg.SessionType = n.SessionType
	msg.CreateTime = utils.GetCurrentTimestampByMill()
	// axis 通过当前时间戳和发送者id+随机数生成md5串
	msg.ClientMsgID = utils.GetMsgID(n.SendID)
	msg.Options = make(map[string]bool, 7)
	msg.SenderNickname = n.SenderNickname
	msg.SenderFaceURL = n.SenderFaceURL
	switch n.SessionType {
	case constant.GroupChatType, constant.SuperGroupChatType:
		msg.RecvID = ""
		msg.GroupID = n.RecvID
	}
	// axis IOS系统推送声音以及标记计数
	offlineInfo.IOSBadgeCount = config.Config.IOSPush.BadgeCount
	offlineInfo.IOSPushSound = config.Config.IOSPush.PushSound
	switch msg.ContentType {
	case constant.GroupCreatedNotification:
		pushSwitch = config.Config.Notification.GroupCreated.OfflinePush.PushSwitch
		title = config.Config.Notification.GroupCreated.OfflinePush.Title
		desc = config.Config.Notification.GroupCreated.OfflinePush.Desc
		ex = config.Config.Notification.GroupCreated.OfflinePush.Ext
		reliabilityLevel = config.Config.Notification.GroupCreated.Conversation.ReliabilityLevel
		unReadCount = config.Config.Notification.GroupCreated.Conversation.UnreadCount
	case constant.GroupInfoSetNotification:
		pushSwitch = config.Config.Notification.GroupInfoSet.OfflinePush.PushSwitch
		title = config.Config.Notification.GroupInfoSet.OfflinePush.Title
		desc = config.Config.Notification.GroupInfoSet.OfflinePush.Desc
		ex = config.Config.Notification.GroupInfoSet.OfflinePush.Ext
		// axis reliability 可靠水平
		reliabilityLevel = config.Config.Notification.GroupInfoSet.Conversation.ReliabilityLevel
		unReadCount = config.Config.Notification.GroupInfoSet.Conversation.UnreadCount
	case constant.JoinGroupApplicationNotification:
		pushSwitch = config.Config.Notification.JoinGroupApplication.OfflinePush.PushSwitch
		title = config.Config.Notification.JoinGroupApplication.OfflinePush.Title
		desc = config.Config.Notification.JoinGroupApplication.OfflinePush.Desc
		ex = config.Config.Notification.JoinGroupApplication.OfflinePush.Ext
		reliabilityLevel = config.Config.Notification.JoinGroupApplication.Conversation.ReliabilityLevel
		unReadCount = config.Config.Notification.JoinGroupApplication.Conversation.UnreadCount
	case constant.MemberQuitNotification:
		pushSwitch = config.Config.Notification.MemberQuit.OfflinePush.PushSwitch
		title = config.Config.Notification.MemberQuit.OfflinePush.Title
		desc = config.Config.Notification.MemberQuit.OfflinePush.Desc
		ex = config.Config.Notification.MemberQuit.OfflinePush.Ext
		reliabilityLevel = config.Config.Notification.MemberQuit.Conversation.ReliabilityLevel
		unReadCount = config.Config.Notification.MemberQuit.Conversation.UnreadCount
	case constant.GroupApplicationAcceptedNotification:
		pushSwitch = config.Config.Notification.GroupApplicationAccepted.OfflinePush.PushSwitch
		title = config.Config.Notification.GroupApplicationAccepted.OfflinePush.Title
		desc = config.Config.Notification.GroupApplicationAccepted.OfflinePush.Desc
		ex = config.Config.Notification.GroupApplicationAccepted.OfflinePush.Ext
		reliabilityLevel = config.Config.Notification.GroupApplicationAccepted.Conversation.ReliabilityLevel
		unReadCount = config.Config.Notification.GroupApplicationAccepted.Conversation.UnreadCount
	case constant.GroupApplicationRejectedNotification:
		pushSwitch = config.Config.Notification.GroupApplicationRejected.OfflinePush.PushSwitch
		title = config.Config.Notification.GroupApplicationRejected.OfflinePush.Title
		desc = config.Config.Notification.GroupApplicationRejected.OfflinePush.Desc
		ex = config.Config.Notification.GroupApplicationRejected.OfflinePush.Ext
		reliabilityLevel = config.Config.Notification.GroupApplicationRejected.Conversation.ReliabilityLevel
		unReadCount = config.Config.Notification.GroupApplicationRejected.Conversation.UnreadCount
	case constant.GroupOwnerTransferredNotification:
		pushSwitch = config.Config.Notification.GroupOwnerTransferred.OfflinePush.PushSwitch
		title = config.Config.Notification.GroupOwnerTransferred.OfflinePush.Title
		desc = config.Config.Notification.GroupOwnerTransferred.OfflinePush.Desc
		ex = config.Config.Notification.GroupOwnerTransferred.OfflinePush.Ext
		reliabilityLevel = config.Config.Notification.GroupOwnerTransferred.Conversation.ReliabilityLevel
		unReadCount = config.Config.Notification.GroupOwnerTransferred.Conversation.UnreadCount
	case constant.MemberKickedNotification:
		pushSwitch = config.Config.Notification.MemberKicked.OfflinePush.PushSwitch
		title = config.Config.Notification.MemberKicked.OfflinePush.Title
		desc = config.Config.Notification.MemberKicked.OfflinePush.Desc
		ex = config.Config.Notification.MemberKicked.OfflinePush.Ext
		reliabilityLevel = config.Config.Notification.MemberKicked.Conversation.ReliabilityLevel
		unReadCount = config.Config.Notification.MemberKicked.Conversation.UnreadCount
	case constant.MemberInvitedNotification:
		pushSwitch = config.Config.Notification.MemberInvited.OfflinePush.PushSwitch
		title = config.Config.Notification.MemberInvited.OfflinePush.Title
		desc = config.Config.Notification.MemberInvited.OfflinePush.Desc
		ex = config.Config.Notification.MemberInvited.OfflinePush.Ext
		reliabilityLevel = config.Config.Notification.MemberInvited.Conversation.ReliabilityLevel
		unReadCount = config.Config.Notification.MemberInvited.Conversation.UnreadCount
	case constant.MemberEnterNotification:
		pushSwitch = config.Config.Notification.MemberEnter.OfflinePush.PushSwitch
		title = config.Config.Notification.MemberEnter.OfflinePush.Title
		desc = config.Config.Notification.MemberEnter.OfflinePush.Desc
		ex = config.Config.Notification.MemberEnter.OfflinePush.Ext
		reliabilityLevel = config.Config.Notification.MemberEnter.Conversation.ReliabilityLevel
		unReadCount = config.Config.Notification.MemberEnter.Conversation.UnreadCount
	case constant.UserInfoUpdatedNotification:
		pushSwitch = config.Config.Notification.UserInfoUpdated.OfflinePush.PushSwitch
		title = config.Config.Notification.UserInfoUpdated.OfflinePush.Title
		desc = config.Config.Notification.UserInfoUpdated.OfflinePush.Desc
		ex = config.Config.Notification.UserInfoUpdated.OfflinePush.Ext
		reliabilityLevel = config.Config.Notification.UserInfoUpdated.Conversation.ReliabilityLevel
		unReadCount = config.Config.Notification.UserInfoUpdated.Conversation.UnreadCount
	case constant.FriendApplicationNotification:
		pushSwitch = config.Config.Notification.FriendApplication.OfflinePush.PushSwitch
		title = config.Config.Notification.FriendApplication.OfflinePush.Title
		desc = config.Config.Notification.FriendApplication.OfflinePush.Desc
		ex = config.Config.Notification.FriendApplication.OfflinePush.Ext
		reliabilityLevel = config.Config.Notification.FriendApplication.Conversation.ReliabilityLevel
		unReadCount = config.Config.Notification.FriendApplication.Conversation.UnreadCount
	case constant.FriendApplicationApprovedNotification:
		pushSwitch = config.Config.Notification.FriendApplicationApproved.OfflinePush.PushSwitch
		title = config.Config.Notification.FriendApplicationApproved.OfflinePush.Title
		desc = config.Config.Notification.FriendApplicationApproved.OfflinePush.Desc
		ex = config.Config.Notification.FriendApplicationApproved.OfflinePush.Ext
		reliabilityLevel = config.Config.Notification.FriendApplicationApproved.Conversation.ReliabilityLevel
		unReadCount = config.Config.Notification.FriendApplicationApproved.Conversation.UnreadCount
	case constant.FriendApplicationRejectedNotification:
		pushSwitch = config.Config.Notification.FriendApplicationRejected.OfflinePush.PushSwitch
		title = config.Config.Notification.FriendApplicationRejected.OfflinePush.Title
		desc = config.Config.Notification.FriendApplicationRejected.OfflinePush.Desc
		ex = config.Config.Notification.FriendApplicationRejected.OfflinePush.Ext
		reliabilityLevel = config.Config.Notification.FriendApplicationRejected.Conversation.ReliabilityLevel
		unReadCount = config.Config.Notification.FriendApplicationRejected.Conversation.UnreadCount
	case constant.FriendAddedNotification:
		pushSwitch = config.Config.Notification.FriendAdded.OfflinePush.PushSwitch
		title = config.Config.Notification.FriendAdded.OfflinePush.Title
		desc = config.Config.Notification.FriendAdded.OfflinePush.Desc
		ex = config.Config.Notification.FriendAdded.OfflinePush.Ext
		reliabilityLevel = config.Config.Notification.FriendAdded.Conversation.ReliabilityLevel
		unReadCount = config.Config.Notification.FriendAdded.Conversation.UnreadCount
	case constant.FriendDeletedNotification:
		pushSwitch = config.Config.Notification.FriendDeleted.OfflinePush.PushSwitch
		title = config.Config.Notification.FriendDeleted.OfflinePush.Title
		desc = config.Config.Notification.FriendDeleted.OfflinePush.Desc
		ex = config.Config.Notification.FriendDeleted.OfflinePush.Ext
		reliabilityLevel = config.Config.Notification.FriendDeleted.Conversation.ReliabilityLevel
		unReadCount = config.Config.Notification.FriendDeleted.Conversation.UnreadCount
	case constant.FriendRemarkSetNotification:
		pushSwitch = config.Config.Notification.FriendRemarkSet.OfflinePush.PushSwitch
		title = config.Config.Notification.FriendRemarkSet.OfflinePush.Title
		desc = config.Config.Notification.FriendRemarkSet.OfflinePush.Desc
		ex = config.Config.Notification.FriendRemarkSet.OfflinePush.Ext
		reliabilityLevel = config.Config.Notification.FriendRemarkSet.Conversation.ReliabilityLevel
		unReadCount = config.Config.Notification.FriendRemarkSet.Conversation.UnreadCount
	case constant.BlackAddedNotification:
		pushSwitch = config.Config.Notification.BlackAdded.OfflinePush.PushSwitch
		title = config.Config.Notification.BlackAdded.OfflinePush.Title
		desc = config.Config.Notification.BlackAdded.OfflinePush.Desc
		ex = config.Config.Notification.BlackAdded.OfflinePush.Ext
		reliabilityLevel = config.Config.Notification.BlackAdded.Conversation.ReliabilityLevel
		unReadCount = config.Config.Notification.BlackAdded.Conversation.UnreadCount
	case constant.BlackDeletedNotification:
		pushSwitch = config.Config.Notification.BlackDeleted.OfflinePush.PushSwitch
		title = config.Config.Notification.BlackDeleted.OfflinePush.Title
		desc = config.Config.Notification.BlackDeleted.OfflinePush.Desc
		ex = config.Config.Notification.BlackDeleted.OfflinePush.Ext
		reliabilityLevel = config.Config.Notification.BlackDeleted.Conversation.ReliabilityLevel
		unReadCount = config.Config.Notification.BlackDeleted.Conversation.UnreadCount
	case constant.ConversationOptChangeNotification:
		pushSwitch = config.Config.Notification.ConversationOptUpdate.OfflinePush.PushSwitch
		title = config.Config.Notification.ConversationOptUpdate.OfflinePush.Title
		desc = config.Config.Notification.ConversationOptUpdate.OfflinePush.Desc
		ex = config.Config.Notification.ConversationOptUpdate.OfflinePush.Ext
		reliabilityLevel = config.Config.Notification.ConversationOptUpdate.Conversation.ReliabilityLevel
		unReadCount = config.Config.Notification.ConversationOptUpdate.Conversation.UnreadCount

	case constant.GroupDismissedNotification:
		pushSwitch = config.Config.Notification.GroupDismissed.OfflinePush.PushSwitch
		title = config.Config.Notification.GroupDismissed.OfflinePush.Title
		desc = config.Config.Notification.GroupDismissed.OfflinePush.Desc
		ex = config.Config.Notification.GroupDismissed.OfflinePush.Ext
		reliabilityLevel = config.Config.Notification.GroupDismissed.Conversation.ReliabilityLevel
		unReadCount = config.Config.Notification.GroupDismissed.Conversation.UnreadCount

	case constant.GroupMutedNotification:
		pushSwitch = config.Config.Notification.GroupMuted.OfflinePush.PushSwitch
		title = config.Config.Notification.GroupMuted.OfflinePush.Title
		desc = config.Config.Notification.GroupMuted.OfflinePush.Desc
		ex = config.Config.Notification.GroupMuted.OfflinePush.Ext
		reliabilityLevel = config.Config.Notification.GroupMuted.Conversation.ReliabilityLevel
		unReadCount = config.Config.Notification.GroupMuted.Conversation.UnreadCount

	case constant.GroupCancelMutedNotification:
		pushSwitch = config.Config.Notification.GroupCancelMuted.OfflinePush.PushSwitch
		title = config.Config.Notification.GroupCancelMuted.OfflinePush.Title
		desc = config.Config.Notification.GroupCancelMuted.OfflinePush.Desc
		ex = config.Config.Notification.GroupCancelMuted.OfflinePush.Ext
		reliabilityLevel = config.Config.Notification.GroupCancelMuted.Conversation.ReliabilityLevel
		unReadCount = config.Config.Notification.GroupCancelMuted.Conversation.UnreadCount

	case constant.GroupMemberMutedNotification:
		pushSwitch = config.Config.Notification.GroupMemberMuted.OfflinePush.PushSwitch
		title = config.Config.Notification.GroupMemberMuted.OfflinePush.Title
		desc = config.Config.Notification.GroupMemberMuted.OfflinePush.Desc
		ex = config.Config.Notification.GroupMemberMuted.OfflinePush.Ext
		reliabilityLevel = config.Config.Notification.GroupMemberMuted.Conversation.ReliabilityLevel
		unReadCount = config.Config.Notification.GroupMemberMuted.Conversation.UnreadCount

	case constant.GroupMemberCancelMutedNotification:
		pushSwitch = config.Config.Notification.GroupMemberCancelMuted.OfflinePush.PushSwitch
		title = config.Config.Notification.GroupMemberCancelMuted.OfflinePush.Title
		desc = config.Config.Notification.GroupMemberCancelMuted.OfflinePush.Desc
		ex = config.Config.Notification.GroupMemberCancelMuted.OfflinePush.Ext
		reliabilityLevel = config.Config.Notification.GroupMemberCancelMuted.Conversation.ReliabilityLevel
		unReadCount = config.Config.Notification.GroupMemberCancelMuted.Conversation.UnreadCount

	case constant.GroupMemberInfoSetNotification:
		pushSwitch = config.Config.Notification.GroupMemberInfoSet.OfflinePush.PushSwitch
		title = config.Config.Notification.GroupMemberInfoSet.OfflinePush.Title
		desc = config.Config.Notification.GroupMemberInfoSet.OfflinePush.Desc
		ex = config.Config.Notification.GroupMemberInfoSet.OfflinePush.Ext
		reliabilityLevel = config.Config.Notification.GroupMemberInfoSet.Conversation.ReliabilityLevel
		unReadCount = config.Config.Notification.GroupMemberInfoSet.Conversation.UnreadCount

	case constant.OrganizationChangedNotification:
		pushSwitch = config.Config.Notification.OrganizationChanged.OfflinePush.PushSwitch
		title = config.Config.Notification.OrganizationChanged.OfflinePush.Title
		desc = config.Config.Notification.OrganizationChanged.OfflinePush.Desc
		ex = config.Config.Notification.OrganizationChanged.OfflinePush.Ext
		reliabilityLevel = config.Config.Notification.OrganizationChanged.Conversation.ReliabilityLevel
		unReadCount = config.Config.Notification.OrganizationChanged.Conversation.UnreadCount

	case constant.WorkMomentNotification:
		pushSwitch = config.Config.Notification.WorkMomentsNotification.OfflinePush.PushSwitch
		title = config.Config.Notification.WorkMomentsNotification.OfflinePush.Title
		desc = config.Config.Notification.WorkMomentsNotification.OfflinePush.Desc
		ex = config.Config.Notification.WorkMomentsNotification.OfflinePush.Ext
		reliabilityLevel = config.Config.Notification.WorkMomentsNotification.Conversation.ReliabilityLevel
		unReadCount = config.Config.Notification.WorkMomentsNotification.Conversation.UnreadCount

	case constant.ConversationPrivateChatNotification:
		pushSwitch = config.Config.Notification.ConversationSetPrivate.OfflinePush.PushSwitch
		title = config.Config.Notification.ConversationSetPrivate.OfflinePush.Title
		desc = config.Config.Notification.ConversationSetPrivate.OfflinePush.Desc
		ex = config.Config.Notification.ConversationSetPrivate.OfflinePush.Ext
		reliabilityLevel = config.Config.Notification.ConversationSetPrivate.Conversation.ReliabilityLevel
		unReadCount = config.Config.Notification.ConversationSetPrivate.Conversation.UnreadCount
	case constant.DeleteMessageNotification:
		reliabilityLevel = constant.ReliableNotificationNoMsg
	case constant.ConversationUnreadNotification, constant.SuperGroupUpdateNotification:
		reliabilityLevel = constant.UnreliableNotification
	}
	switch reliabilityLevel {
	case constant.UnreliableNotification:
		utils.SetSwitchFromOptions(msg.Options, constant.IsHistory, false)
		utils.SetSwitchFromOptions(msg.Options, constant.IsPersistent, false)
		utils.SetSwitchFromOptions(msg.Options, constant.IsConversationUpdate, false)
		utils.SetSwitchFromOptions(msg.Options, constant.IsSenderConversationUpdate, false)
	case constant.ReliableNotificationNoMsg:
		utils.SetSwitchFromOptions(msg.Options, constant.IsConversationUpdate, false)
		utils.SetSwitchFromOptions(msg.Options, constant.IsSenderConversationUpdate, false)
	case constant.ReliableNotificationMsg:

	}
	utils.SetSwitchFromOptions(msg.Options, constant.IsUnreadCount, unReadCount)
	utils.SetSwitchFromOptions(msg.Options, constant.IsOfflinePush, pushSwitch)
	offlineInfo.Title = title
	offlineInfo.Desc = desc
	offlineInfo.Ex = ex
	msg.OfflinePushInfo = &offlineInfo
	req.MsgData = &msg
	etcdConn := getcdv3.GetDefaultConn(config.Config.Etcd.EtcdSchema, strings.Join(config.Config.Etcd.EtcdAddr, ","), config.Config.RpcRegisterName.OpenImMsgName, req.OperationID)
	if etcdConn == nil {
		errMsg := req.OperationID + "getcdv3.GetDefaultConn == nil"
		log.NewError(req.OperationID, errMsg)
		return
	}

	client := pbChat.NewMsgClient(etcdConn)
	reply, err := client.SendMsg(context.Background(), &req)
	if err != nil {
		log.NewError(req.OperationID, "SendMsg rpc failed, ", req.String(), err.Error())
	} else if reply.ErrCode != 0 {
		log.NewError(req.OperationID, "SendMsg rpc failed, ", req.String(), reply.ErrCode, reply.ErrMsg)
	}
}

func getOnlineAndOfflineUserIDList(memberList []string, m map[string][]string, operationID string) {
	var onllUserIDList, offlUserIDList []string
	var wsResult []*pbRelay.GetUsersOnlineStatusResp_SuccessResult
	req := &pbRelay.GetUsersOnlineStatusReq{}
	req.UserIDList = memberList
	req.OperationID = operationID
	// 这里表明，该函数只能系统管理员才能使用？ axis
	req.OpUserID = config.Config.Manager.AppManagerUid[0]
	flag := false
	// 获取relay【msg gateway】的所有可用rpc服务 axis
	grpcCons := getcdv3.GetDefaultGatewayConn4Unique(config.Config.Etcd.EtcdSchema, strings.Join(config.Config.Etcd.EtcdAddr, ","), operationID)
	for _, v := range grpcCons {
		client := pbRelay.NewRelayClient(v)
		reply, err := client.GetUsersOnlineStatus(context.Background(), req)
		if err != nil {
			log.NewError(operationID, "GetUsersOnlineStatus rpc  err", req.String(), err.Error())
			continue
		} else {
			if reply.ErrCode == 0 {
				wsResult = append(wsResult, reply.SuccessResult...)
			}
		}
	}
	log.NewInfo(operationID, "call GetUsersOnlineStatus rpc server is success", wsResult)
	//Online data merge of each node
	for _, v1 := range memberList {
		flag = false

		for _, v2 := range wsResult {
			if v2.UserID == v1 {
				flag = true
				onllUserIDList = append(onllUserIDList, v1)
			}

		}
		if !flag {
			offlUserIDList = append(offlUserIDList, v1)
		}
	}
	m[constant.OnlineStatus] = onllUserIDList
	m[constant.OfflineStatus] = offlUserIDList
}

func valueCopy(pb *pbChat.SendMsgReq) *pbChat.SendMsgReq {
	offlinePushInfo := sdk_ws.OfflinePushInfo{}
	if pb.MsgData.OfflinePushInfo != nil {
		// 添加取指针值符号，值复制  axis
		offlinePushInfo = *pb.MsgData.OfflinePushInfo
	}
	msgData := sdk_ws.MsgData{}
	msgData = *pb.MsgData
	msgData.OfflinePushInfo = &offlinePushInfo

	options := make(map[string]bool, 10)
	for key, value := range pb.MsgData.Options {
		options[key] = value
	}
	msgData.Options = options
	return &pbChat.SendMsgReq{Token: pb.Token, OperationID: pb.OperationID, MsgData: &msgData}
}

func (rpc *RpcChat) sendMsgToGroupOptimization(list []string, groupPB *pbChat.SendMsgReq, status string, sendTag *bool, wg *sync.WaitGroup) {
	msgToMQGroup := pbChat.MsgDataToMQ{Token: groupPB.Token, OperationID: groupPB.OperationID, MsgData: groupPB.MsgData}
	tempOptions := make(map[string]bool, 1)
	// TODO: 这个函数的整个处理过程，groupPB.MsgData.Options的值都没有变过，那
	// 取出后用重新赋值是什么操作。 axis
	for k, v := range groupPB.MsgData.Options {
		tempOptions[k] = v
	}
	for _, v := range list {
		groupPB.MsgData.RecvID = v
		options := make(map[string]bool, 1)
		for k, v := range tempOptions {
			options[k] = v
		}
		groupPB.MsgData.Options = options
		// 根据消息接收方的用户的消息接收配置，判断是否发送消息 axis
		isSend := modifyMessageByUserMessageReceiveOpt(v, groupPB.MsgData.GroupID, constant.GroupChatType, groupPB)
		if isSend {
			if v == "" || groupPB.MsgData.SendID == "" {
				log.Error(msgToMQGroup.OperationID, "sendMsgToGroupOptimization userID nil ", msgToMQGroup.String())
				continue
			}
			err := rpc.sendMsgToWriter(&msgToMQGroup, v, status)
			if err != nil {
				log.NewError(msgToMQGroup.OperationID, "kafka send msg err:UserId", v, msgToMQGroup.String())
			} else {
				*sendTag = true
			}
		} else {
			log.Debug(groupPB.OperationID, "not sendMsgToWriter, ", v)
		}
	}
	wg.Done()
}
