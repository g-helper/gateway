package gatewaystaff

import (
	"errors"

	"github.com/g-helper/gateway"
	"github.com/nats-io/nats.go"
)

type QueueService interface {
	CheckPermission(req CheckPermissionReq) (res *CheckPermissionRes, err error)
}

type UnimplementedStaffServer struct{}

func (UnimplementedStaffServer) CheckPermission(req CheckPermissionReq) (res *CheckPermissionRes, err error) {
	return nil, errors.New("method GetUserInfo not implemented")
}

func _Staff_Add_CheckPermission(q interface{}) nats.MsgHandler {
	queueStaff := q.(QueueService)
	return func(msg *nats.Msg) {
		res, err := queueStaff.CheckPermission(gateway.ConvertData[CheckPermissionReq](msg.Data))
		if err != nil {
			_ = gateway.GetServer().Response(msg, nil, err.Error())
			return
		}
		gateway.GetServer().Response(msg, res, "")
		return
	}
}
