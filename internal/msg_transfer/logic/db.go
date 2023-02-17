package logic

import (
	"Open_IM/pkg/common/db"
	"Open_IM/pkg/common/log"
	pbMsg "Open_IM/pkg/proto/msg"
	"Open_IM/pkg/utils"
)

// saveUserChat 将从Kafka中取出的消息写入mongodb中 axis
func saveUserChat(uid string, msg *pbMsg.MsgDataToMQ) error {
	time := utils.GetCurrentTimestampByMill()
	// 增加对应用户的信箱中的消息偏移量 axis
	seq, err := db.DB.IncrUserSeq(uid)
	if err != nil {
		log.NewError(msg.OperationID, "data insert to redis err", err.Error(), msg.String())
		return err
	}
	msg.MsgData.Seq = uint32(seq)
	pbSaveData := pbMsg.MsgDataToDB{}
	pbSaveData.MsgData = msg.MsgData
	log.NewInfo(msg.OperationID, "IncrUserSeq cost time", utils.GetCurrentTimestampByMill()-time)
	return db.DB.SaveUserChatMongo2(uid, pbSaveData.MsgData.SendTime, &pbSaveData)
	//	return db.DB.SaveUserChatMongo2(uid, pbSaveData.MsgData.SendTime, &pbSaveData)
}

// saveUserChatList 将Kafka中取出的消息利用Pipeline批量写入redis中 axis
func saveUserChatList(userID string, msgList []*pbMsg.MsgDataToMQ, operationID string) (error, uint64) {
	log.Info(operationID, utils.GetSelfFuncName(), "args ", userID, len(msgList))
	//return db.DB.BatchInsertChat(userID, msgList, operationID)
	return db.DB.BatchInsertChat2Cache(userID, msgList, operationID)
}
