package tests

import (
  "github.com/lxfontes/roxter"
  "testing"
)

func Test_Proxy(t *testing.T) {
  roxter.NewProxy("127.0.0.1:11211")
}
