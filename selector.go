package roxter

// https://github.com/bradfitz/gomemcache/blob/master/memcache/selector.go

import (
  "hash/crc32"
  "net"
  "strings"
  "sync"
)

type ServerSelector interface {
  PickServer(key string) (net.Addr, error)
  Monitor()
}

type ServerList struct {
  lk    sync.RWMutex
  addrs []net.Addr
}

func (self *ServerList) SetServers(servers ...string) error {
  naddr := make([]net.Addr, len(servers))
  for i, server := range servers {
    if strings.Contains(server, "/") {
      addr, err := net.ResolveUnixAddr("unix", server)
      if err != nil {
        return err
      }
      naddr[i] = addr
    } else {
      tcpaddr, err := net.ResolveTCPAddr("tcp", server)
      if err != nil {
        return err
      }
      naddr[i] = tcpaddr
    }
  }

  self.lk.Lock()
  defer self.lk.Unlock()
  self.addrs = naddr
  return nil
}

func (self *ServerList) PickServer(key string) (net.Addr, error) {
  self.lk.RLock()
  defer self.lk.RUnlock()

  if len(self.addrs) == 0 {
    return nil, ErrNoServers
  }

  cs := crc32.ChecksumIEEE([]byte(key))
  return self.addrs[cs%uint32(len(self.addrs))], nil
}

func (self *ServerList) Monitor() {

}
