package main

import (
  "errors"
  "flag"
  "fmt"
  "github.com/lxfontes/roxter"
  "log"
  "net"
  "strings"
  "sync"
  "time"
)

type KetamaServer struct {
  addr  net.Addr
  alive bool
}

type KetamaList struct {
  ring  *roxter.HashRing
  mtx   sync.RWMutex
  addrs map[string]*KetamaServer
}

func (self *KetamaList) PickServer(key string) (net.Addr, error) {
  self.mtx.RLock()
  defer self.mtx.RUnlock()

  if self.ring.Length == 0 {
    return nil, roxter.ErrNoServers
  }

  h := self.ring.Hash(key)
  return self.addrs[h].addr, nil
}

func (self *KetamaList) rebalance() {
  self.mtx.Lock()
  defer self.mtx.Unlock()

  log.Println("Rebalancing")

  ring := roxter.NewRing(200)
  for k, s := range self.addrs {
    if s.alive {
      log.Println(k, "added to ring")
      ring.AddNode(k, 1)
    }
  }
  ring.Bake()
  self.ring = ring
}

func (self *KetamaList) AddServer(name, server string) error {
  self.mtx.Lock()
  defer self.mtx.Unlock()
  if strings.Contains(server, "/") {
    addr, err := net.ResolveUnixAddr("unix", server)
    if err != nil {
      return err
    }
    self.addrs[name] = &KetamaServer{
      addr:  addr,
      alive: false,
    }
  } else {
    tcpaddr, err := net.ResolveTCPAddr("tcp", server)
    if err != nil {
      return err
    }
    self.addrs[name] = &KetamaServer{
      addr:  tcpaddr,
      alive: false,
    }
  }

  return nil
}

func (self *KetamaList) Monitor() {
  for k, _ := range self.addrs {
    go self.monitorServer(k)
  }
}

func (self *KetamaList) monitorServer(name string) {
  server := self.addrs[name]
  for {
    alive := false
    mc, err := roxter.Dial(server.addr)
    if err == nil {
      alive = mc.Ping()
      mc.Close()
    }

    if alive != server.alive {
      server.alive = alive
      state := "dead"
      if alive {
        state = "alive"
      }
      log.Println(name, "is", state)
      self.rebalance()
    }

    time.Sleep(1 * time.Second)
  }
}

func NewKetamaList() *KetamaList {
  k := new(KetamaList)
  k.addrs = make(map[string]*KetamaServer)
  return k
}

type serverlist struct {
  l map[string]string
}

func (s *serverlist) Set(value string) error {
  if !strings.Contains(value, ":") {
    return errors.New("Invalid server format")
  }

  s.l[value] = value

  return nil
}

func (s *serverlist) String() string {
  return fmt.Sprint(*s)
}

func main() {
  var servers serverlist
  servers.l = make(map[string]string)

  var endpoint string

  var maxIdle int

  flag.Var(&servers, "server", "memcache server names and addresses (example: '127.0.0.1:11211')")
  flag.StringVar(&endpoint, "bind", ":11212", "proxy bind address")
  flag.IntVar(&maxIdle, "idle", 10, "maximum idle connections per server")
  flag.Parse()

  if len(servers.l) < 1 {
    fmt.Println("No serverlist")
    flag.Usage()
    return
  }

  ss := NewKetamaList()

  for name, server := range servers.l {
    ss.AddServer(name, server)
  }

  ss.Monitor()

  proxy := roxter.NewProxyFromSelector(ss)
  proxy.MaxIdle = maxIdle

  proxy.ListenAndServe(endpoint)
}
