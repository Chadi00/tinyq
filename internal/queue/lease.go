package queue

import "time"

const leaseTTL = 30 * time.Second

type Lease struct{
	Job *Job
	Token string
	ExpiresAt time.Time
}
