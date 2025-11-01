package rmq

import (
	"strings"
)

type Prefix string

func (p Prefix) String(args ...string) string {
	if len(args) == 0 {
		return string(p)
	}
	return string(p) + ":" + strings.Join(args, ":")
}
