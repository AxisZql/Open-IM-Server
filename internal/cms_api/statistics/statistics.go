package statistics

import (
	"Open_IM/pkg/cms_api_struct"
	"Open_IM/pkg/common/config"
	"Open_IM/pkg/common/log"
	"Open_IM/pkg/grpc-etcdv3/getcdv3"
	admin "Open_IM/pkg/proto/admin_cms"
	"Open_IM/pkg/utils"
	"context"
	"net/http"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
)

// GetMessageStatistics 获取单聊和群聊在指定时间跨度的消息产生数分析. [axis]
func GetMessagesStatistics(c *gin.Context) {
	var (
		req   cms_api_struct.GetMessageStatisticsRequest
		resp  cms_api_struct.GetMessageStatisticsResponse
		reqPb admin.GetMessageStatisticsReq
	)
	reqPb.StatisticsReq = &admin.StatisticsReq{}
	if err := c.BindJSON(&req); err != nil {
		log.NewError(req.OperationID, utils.GetSelfFuncName(), "BindJSON failed ", err.Error())
		c.JSON(http.StatusBadRequest, gin.H{"errCode": 400, "errMsg": err.Error()})
		return
	}
	reqPb.OperationID = utils.OperationIDGenerator()
	log.NewInfo(reqPb.OperationID, utils.GetSelfFuncName(), "req: ", req)
	utils.CopyStructFields(&reqPb.StatisticsReq, &req)
	etcdConn := getcdv3.GetDefaultConn(config.Config.Etcd.EtcdSchema, strings.Join(config.Config.Etcd.EtcdAddr, ","), config.Config.RpcRegisterName.OpenImAdminCMSName, reqPb.OperationID)
	if etcdConn == nil {
		errMsg := reqPb.OperationID + "getcdv3.GetDefaultConn == nil"
		log.NewError(reqPb.OperationID, errMsg)
		c.JSON(http.StatusInternalServerError, gin.H{"errCode": 500, "errMsg": errMsg})
		return
	}
	client := admin.NewAdminCMSClient(etcdConn)
	ctx, cancel := context.WithTimeout(context.TODO(), time.Second*100)
	defer cancel()
	respPb, err := client.GetMessageStatistics(ctx, &reqPb)
	if err != nil {
		log.NewError(reqPb.OperationID, utils.GetSelfFuncName(), "GetMessageStatistics failed", err.Error())
		c.JSON(http.StatusInternalServerError, gin.H{"errCode": 400, "errMsg": err.Error()})
		return
	}

	resp.GroupMessageNum = int(respPb.GroupMessageNum)
	resp.PrivateMessageNum = int(respPb.PrivateMessageNum)
	for _, v := range respPb.PrivateMessageNumList {
		resp.PrivateMessageNumList = append(resp.PrivateMessageNumList, struct {
			Date       string `json:"date"`
			MessageNum int    `json:"messageNum"`
		}{
			Date:       v.Date,
			MessageNum: int(v.Num),
		})
	}
	for _, v := range respPb.GroupMessageNumList {
		resp.GroupMessageNumList = append(resp.GroupMessageNumList, struct {
			Date       string `json:"date"`
			MessageNum int    `json:"messageNum"`
		}{
			Date:       v.Date,
			MessageNum: int(v.Num),
		})
	}
	log.NewInfo(reqPb.OperationID, utils.GetSelfFuncName(), "resp: ", resp)
	c.JSON(http.StatusOK, gin.H{"errCode": respPb.CommonResp.ErrCode, "errMsg": respPb.CommonResp.ErrMsg, "data": resp})
}

// GetUserStatistics 获取目标时间跨度上的活跃用户、新增用户、用户总数的变化数据.[axis]
func GetUserStatistics(c *gin.Context) {
	var (
		req   cms_api_struct.GetUserStatisticsRequest
		resp  cms_api_struct.GetUserStatisticsResponse
		reqPb admin.GetUserStatisticsReq
	)
	reqPb.StatisticsReq = &admin.StatisticsReq{}
	if err := c.BindJSON(&req); err != nil {
		log.NewError(req.OperationID, utils.GetSelfFuncName(), "BindJSON failed ", err.Error())
		c.JSON(http.StatusBadRequest, gin.H{"errCode": 400, "errMsg": err.Error()})
		return
	}
	reqPb.OperationID = utils.OperationIDGenerator()
	log.NewInfo(reqPb.OperationID, utils.GetSelfFuncName(), "req: ", req)
	utils.CopyStructFields(&reqPb.StatisticsReq, &req)
	etcdConn := getcdv3.GetDefaultConn(config.Config.Etcd.EtcdSchema, strings.Join(config.Config.Etcd.EtcdAddr, ","), config.Config.RpcRegisterName.OpenImAdminCMSName, reqPb.OperationID)
	if etcdConn == nil {
		errMsg := reqPb.OperationID + "getcdv3.GetDefaultConn == nil"
		log.NewError(reqPb.OperationID, errMsg)
		c.JSON(http.StatusInternalServerError, gin.H{"errCode": 500, "errMsg": errMsg})
		return
	}
	client := admin.NewAdminCMSClient(etcdConn)
	ctx, cancel := context.WithTimeout(context.TODO(), time.Second*100)
	defer cancel()
	respPb, err := client.GetUserStatistics(ctx, &reqPb)
	if err != nil {
		log.NewError(reqPb.OperationID, utils.GetSelfFuncName(), "GetUserStatistics failed", err.Error(), reqPb.String())
		c.JSON(http.StatusBadRequest, gin.H{"errCode": 500, "errMsg": err.Error()})
		return
	}
	resp.ActiveUserNum = int(respPb.ActiveUserNum)
	resp.IncreaseUserNum = int(respPb.IncreaseUserNum)
	resp.TotalUserNum = int(respPb.TotalUserNum)
	for _, v := range respPb.ActiveUserNumList {
		resp.ActiveUserNumList = append(resp.ActiveUserNumList, struct {
			Date          string `json:"date"`
			ActiveUserNum int    `json:"activeUserNum"`
		}{
			Date:          v.Date,
			ActiveUserNum: int(v.Num),
		})
	}
	for _, v := range respPb.IncreaseUserNumList {
		resp.IncreaseUserNumList = append(resp.IncreaseUserNumList, struct {
			Date            string `json:"date"`
			IncreaseUserNum int    `json:"increaseUserNum"`
		}{
			Date:            v.Date,
			IncreaseUserNum: int(v.Num),
		})
	}
	for _, v := range respPb.TotalUserNumList {
		resp.TotalUserNumList = append(resp.TotalUserNumList, struct {
			Date         string `json:"date"`
			TotalUserNum int    `json:"totalUserNum"`
		}{
			Date:         v.Date,
			TotalUserNum: int(v.Num),
		})
	}
	log.NewInfo(reqPb.OperationID, utils.GetSelfFuncName(), "resp: ", resp)
	c.JSON(http.StatusOK, gin.H{"errCode": respPb.CommonResp.ErrCode, "errMsg": respPb.CommonResp.ErrMsg, "data": resp})
}

func GetGroupStatistics(c *gin.Context) {
	var (
		req   cms_api_struct.GetGroupStatisticsRequest
		resp  cms_api_struct.GetGroupStatisticsResponse
		reqPb admin.GetGroupStatisticsReq
	)
	reqPb.StatisticsReq = &admin.StatisticsReq{}
	if err := c.BindJSON(&req); err != nil {
		log.NewError(req.OperationID, "BindJSON failed ", err.Error())
		c.JSON(http.StatusBadRequest, gin.H{"errCode": 400, "errMsg": err.Error()})
		return
	}
	reqPb.OperationID = utils.OperationIDGenerator()
	log.NewInfo(reqPb.OperationID, utils.GetSelfFuncName(), "req: ", req)
	utils.CopyStructFields(&reqPb.StatisticsReq, &req)
	etcdConn := getcdv3.GetDefaultConn(config.Config.Etcd.EtcdSchema, strings.Join(config.Config.Etcd.EtcdAddr, ","), config.Config.RpcRegisterName.OpenImAdminCMSName, reqPb.OperationID)
	if etcdConn == nil {
		errMsg := reqPb.OperationID + "getcdv3.GetDefaultConn == nil"
		log.NewError(reqPb.OperationID, errMsg)
		c.JSON(http.StatusInternalServerError, gin.H{"errCode": 500, "errMsg": errMsg})
		return
	}
	client := admin.NewAdminCMSClient(etcdConn)
	ctx, cancel := context.WithTimeout(context.TODO(), time.Second*100)
	defer cancel()
	respPb, err := client.GetGroupStatistics(ctx, &reqPb)
	if err != nil {
		log.NewError(reqPb.OperationID, utils.GetSelfFuncName(), "GetGroupStatistics failed", err.Error())
		c.JSON(http.StatusBadRequest, gin.H{"errCode": 500, "errMsg": err.Error()})
		return
	}
	// utils.CopyStructFields(&resp, respPb)
	resp.IncreaseGroupNum = int(respPb.GetIncreaseGroupNum())
	resp.TotalGroupNum = int(respPb.GetTotalGroupNum())
	for _, v := range respPb.IncreaseGroupNumList {
		resp.IncreaseGroupNumList = append(resp.IncreaseGroupNumList,
			struct {
				Date             string `json:"date"`
				IncreaseGroupNum int    `json:"increaseGroupNum"`
			}{
				Date:             v.Date,
				IncreaseGroupNum: int(v.Num),
			})
	}
	for _, v := range respPb.TotalGroupNumList {
		resp.TotalGroupNumList = append(resp.TotalGroupNumList,
			struct {
				Date          string `json:"date"`
				TotalGroupNum int    `json:"totalGroupNum"`
			}{
				Date:          v.Date,
				TotalGroupNum: int(v.Num),
			})

	}
	log.NewInfo(reqPb.OperationID, utils.GetSelfFuncName(), "resp: ", resp)
	c.JSON(http.StatusOK, gin.H{"errCode": respPb.CommonResp.ErrCode, "errMsg": respPb.CommonResp.ErrMsg, "data": resp})
}

// GetActiveUser 获取一定时间跨度上的活跃用户消息，已经每个活跃用户在此期间的发发消息量. [axis]
func GetActiveUser(c *gin.Context) {
	var (
		req   cms_api_struct.GetActiveUserRequest
		resp  cms_api_struct.GetActiveUserResponse
		reqPb admin.GetActiveUserReq
	)
	reqPb.StatisticsReq = &admin.StatisticsReq{}
	if err := c.BindJSON(&req); err != nil {
		log.NewError(req.OperationID, "BindJSON failed ", err.Error())
		c.JSON(http.StatusBadRequest, gin.H{"errCode": 400, "errMsg": err.Error()})
		return
	}
	reqPb.OperationID = utils.OperationIDGenerator()
	log.NewInfo(reqPb.OperationID, utils.GetSelfFuncName(), "req: ", req)
	utils.CopyStructFields(&reqPb.StatisticsReq, req)
	etcdConn := getcdv3.GetDefaultConn(config.Config.Etcd.EtcdSchema, strings.Join(config.Config.Etcd.EtcdAddr, ","), config.Config.RpcRegisterName.OpenImAdminCMSName, reqPb.OperationID)
	if etcdConn == nil {
		errMsg := reqPb.OperationID + "getcdv3.GetDefaultConn == nil"
		log.NewError(reqPb.OperationID, errMsg)
		c.JSON(http.StatusInternalServerError, gin.H{"errCode": 500, "errMsg": errMsg})
		return
	}
	client := admin.NewAdminCMSClient(etcdConn)
	ctx, cancel := context.WithTimeout(context.TODO(), time.Second*100)
	defer cancel()
	respPb, err := client.GetActiveUser(ctx, &reqPb)
	if err != nil {
		log.NewError(reqPb.OperationID, utils.GetSelfFuncName(), "GetActiveUser failed ", err.Error())
		c.JSON(http.StatusBadRequest, gin.H{"errCode": 500, "errMsg": err.Error()})
		return
	}
	utils.CopyStructFields(&resp.ActiveUserList, respPb.Users)
	log.NewInfo(reqPb.OperationID, utils.GetSelfFuncName(), "resp: ", resp)
	c.JSON(http.StatusOK, gin.H{"errCode": respPb.CommonResp.ErrCode, "errMsg": respPb.CommonResp.ErrMsg, "data": resp})
}

// GetActiveGroup 获取一定时间跨度上的活跃群聊信息，已经对应群聊在该时间内的消息量. [axis]
func GetActiveGroup(c *gin.Context) {
	var (
		req   cms_api_struct.GetActiveGroupRequest
		resp  cms_api_struct.GetActiveGroupResponse
		reqPb admin.GetActiveGroupReq
	)
	reqPb.StatisticsReq = &admin.StatisticsReq{}
	if err := c.BindJSON(&req); err != nil {
		log.NewError(req.OperationID, utils.GetSelfFuncName(), "BindJSON failed ", err.Error())
		c.JSON(http.StatusBadRequest, gin.H{"errCode": 400, "errMsg": err.Error()})
		return
	}
	reqPb.OperationID = utils.OperationIDGenerator()
	log.NewInfo(reqPb.OperationID, utils.GetSelfFuncName(), "req: ", req)
	utils.CopyStructFields(&reqPb.StatisticsReq, req)
	etcdConn := getcdv3.GetDefaultConn(config.Config.Etcd.EtcdSchema, strings.Join(config.Config.Etcd.EtcdAddr, ","), config.Config.RpcRegisterName.OpenImAdminCMSName, reqPb.OperationID)
	if etcdConn == nil {
		errMsg := reqPb.OperationID + "getcdv3.GetDefaultConn == nil"
		log.NewError(reqPb.OperationID, errMsg)
		c.JSON(http.StatusInternalServerError, gin.H{"errCode": 500, "errMsg": errMsg})
		return
	}
	client := admin.NewAdminCMSClient(etcdConn)
	ctx, cancel := context.WithTimeout(context.TODO(), time.Second*100)
	defer cancel()
	respPb, err := client.GetActiveGroup(ctx, &reqPb)
	if err != nil {
		log.NewError(reqPb.OperationID, utils.GetSelfFuncName(), "GetActiveGroup failed ", err.Error())
		c.JSON(http.StatusBadRequest, gin.H{"errCode": 500, "errMsg": err.Error()})
		return
	}
	for _, group := range respPb.Groups {
		resp.ActiveGroupList = append(resp.ActiveGroupList, struct {
			GroupName  string `json:"groupName"`
			GroupId    string `json:"groupID"`
			MessageNum int    `json:"messageNum"`
		}{
			GroupName:  group.GroupName,
			GroupId:    group.GroupId,
			MessageNum: int(group.MessageNum),
		})
	}
	log.NewInfo(reqPb.OperationID, utils.GetSelfFuncName(), "resp: ", resp)
	c.JSON(http.StatusOK, gin.H{"errCode": respPb.CommonResp.ErrCode, "errMsg": respPb.CommonResp.ErrMsg, "data": resp})
}
