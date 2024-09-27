package gatewaystaff

import (
	"github.com/g-helper/gateway"
)

var serviceDesc = gateway.ServiceDesc{
	Queues: []gateway.QueueDesc{
		{
			Subject: gateway.QueueName.Staff.CheckPermission,
			Worker:  gateway.Worker.Staff,
			Handle:  _Staff_Add_CheckPermission,
		},
	},
}

// RegisterServer ...
func RegisterServer(queue QueueService) error {
	return serviceDesc.Register(gateway.GetServer(), queue)
}
