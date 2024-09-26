package gatewaystaff

type StaffServerInterface interface {
	CheckPermission(cb func(req *CheckPermissionReq) (*CheckPermissionRes, error)) error
}

type StaffClientInterface interface {
	CheckPermission(req CheckPermissionReq) (*CheckPermissionRes, error)
}
