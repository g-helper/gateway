package gateway

var (
	Worker = struct {
		Staff string
	}{
		Staff: "staff",
	}

	QueueName = struct {
		Staff StaffQueue
	}{
		Staff: StaffQueue{
			CheckPermission: "staff:check.permission",
		},
	}
)

type StaffQueue struct {
	CheckPermission string
}
