package gatewaystaff

// CheckPermissionReq ...
type CheckPermissionReq struct {
	Token      string   `json:"token"`
	Permission []string `json:"permission"`
}

// CheckPermissionRes ...
type CheckPermissionRes struct {
	IsAccess   bool     `json:"isAccess"`
	Permission []string `json:"permission"`
}
