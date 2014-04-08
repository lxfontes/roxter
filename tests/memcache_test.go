package tests

import (
  "github.com/lxfontes/roxter"
  "testing"
)

func Test_Proxy(t *testing.T) {
  p := roxter.NewProxy("127.0.0.1:11211")
  p.MaxIdle = 8
  p.ListenAndServe(":11212")
}
