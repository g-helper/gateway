package gatewaystaff

import "github.com/g-helper/gateway"

type ClientInterface interface {
	CheckPermission(req CheckPermissionReq) (res CheckPermissionRes, err error)
}

func NewClient() ClientInterface {
	return &client{}
}

type client struct {
}

// CheckPermission ...
func (c client) CheckPermission(req CheckPermissionReq) (res CheckPermissionRes, err error) {
	return gateway.ClientRequest[CheckPermissionReq, CheckPermissionRes](gateway.QueueName.Staff.CheckPermission, req)
}
