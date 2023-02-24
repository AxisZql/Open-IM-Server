package register

import (
	"Open_IM/pkg/common/constant"
	imdb "Open_IM/pkg/common/db/mysql_model/im_mysql_model"
	"Open_IM/pkg/common/log"
	"Open_IM/pkg/utils"
	"errors"
	"net/http"

	"github.com/gin-gonic/gin"
	"gorm.io/gorm"
)

type CheckLoginLimitReq struct {
	OperationID string `json:"operationID"`
	UserID      string `json:"userID"`
}

type CheckLoginLimitResp struct {
}

func CheckLoginLimit(c *gin.Context) {
	req := CheckLoginLimitReq{}
	if err := c.BindJSON(&req); err != nil {
		log.NewInfo(req.OperationID, utils.GetSelfFuncName(), err.Error())
		c.JSON(http.StatusBadRequest, gin.H{"errCode": 400, "errMsg": err.Error()})
		return
	}
	ip := c.Request.Header.Get("X-Forward-For")
	if ip == "" {
		ip = c.ClientIP()
	}
	log.NewDebug(req.OperationID, utils.GetSelfFuncName(), "IP: ", ip)
	// TODO: IP limit check. this data belong hot data that cache in redis maybe better . [axis]
	// TODO: 这步查询个人感觉没必要 axis
	user, err := imdb.GetUserIPLimit(req.UserID) // 差user_ip_lists表，该表是记录对应用户只能再对应ip下登录的规则表 axis
	if err != nil && !errors.Is(gorm.ErrRecordNotFound, err) {
		errMsg := req.OperationID + " imdb.GetUserByUserID failed " + err.Error() + req.UserID
		log.NewError(req.OperationID, errMsg)
		c.JSON(http.StatusBadRequest, gin.H{"errCode": constant.ErrDB.ErrCode, "errMsg": errMsg})
		return
	}

	// insert or update current user ip information.[axis]
	if err := imdb.UpdateIpRecord(req.UserID, ip); err != nil {
		log.NewError(req.OperationID, err.Error(), req.UserID, ip)
		c.JSON(http.StatusInternalServerError, gin.H{"errCode": constant.ErrDB.ErrCode, "errMsg": err.Error()})
		return
	}

	var Limited bool
	var LimitError error
	// 指定账户指定ip才能登录
	// TODO:可能该接口只用于检测管理员登录时的ip，故此类指定ip登录的操作类似微信公众平台的ip白名单功能 axis
	Limited, LimitError = imdb.IsLimitUserLoginIp(user.UserID, ip)
	if LimitError != nil {
		log.NewError(req.OperationID, utils.GetSelfFuncName(), LimitError, ip)
		c.JSON(http.StatusBadRequest, gin.H{"errCode": constant.ErrDB.ErrCode, "errMsg": LimitError})
		return
	}
	if Limited {
		log.NewInfo(req.OperationID, utils.GetSelfFuncName(), Limited, ip, req.UserID)
		c.JSON(http.StatusBadRequest, gin.H{"errCode": constant.LoginLimit, "errMsg": "user ip limited Login"})
		return
	}

	// 该ip不能登录
	Limited, LimitError = imdb.IsLimitLoginIp(ip)
	if LimitError != nil {
		log.NewError(req.OperationID, utils.GetSelfFuncName(), LimitError, ip)
		c.JSON(http.StatusBadRequest, gin.H{"errCode": constant.ErrDB.ErrCode, "errMsg": LimitError})
		return
	}
	if Limited {
		log.NewInfo(req.OperationID, utils.GetSelfFuncName(), Limited, ip, req.UserID)
		c.JSON(http.StatusBadRequest, gin.H{"errCode": constant.LoginLimit, "errMsg": "ip limited Login"})
		return
	}

	Limited, LimitError = imdb.UserIsBlock(user.UserID)
	if LimitError != nil {
		log.NewError(req.OperationID, utils.GetSelfFuncName(), LimitError, user.UserID)
		c.JSON(http.StatusBadRequest, gin.H{"errCode": constant.ErrDB.ErrCode, "errMsg": LimitError})
		return
	}
	if Limited {
		log.NewInfo(req.OperationID, utils.GetSelfFuncName(), Limited, ip, req.UserID)
		c.JSON(http.StatusBadRequest, gin.H{"errCode": constant.LoginLimit, "errMsg": "user is block"})
		return
	}
	c.JSON(http.StatusOK, gin.H{"errCode": 0, "errMsg": ""})
}
