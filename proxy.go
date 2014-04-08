package roxter

import (
  "errors"
  "net"
  "sync"
)

var (
  ErrNotFound  = errors.New("mc: not found")
  ErrNoServers = errors.New("mc: no servers available")
  ErrTimeout   = errors.New("mc: connection timeout")
)

type Proxy struct {
  selector ServerSelector
  mtx      sync.Mutex
  freeconn map[string][]*MCConn
  MaxIdle  int
}

func NewProxy(servers ...string) *Proxy {
  ss := new(ServerList)
  ss.SetServers(servers...)
  return NewProxyFromSelector(ss)
}

func NewProxyFromSelector(ss ServerSelector) *Proxy {
  p := new(Proxy)
  p.selector = ss
  p.freeconn = make(map[string][]*MCConn)
  return p
}

func (self *Proxy) ListenAndServe(addr string) error {
  l, err := net.Listen("tcp", addr)

  if err != nil {
    return err
  }

  self.selector.Monitor()

  return self.Serve(l)
}

func (self *Proxy) Serve(l net.Listener) error {
  defer l.Close()
  for {
    rw, err := l.Accept()
    if err != nil {
      return err
    }

    c, err := self.newClient(rw)
    if err != nil {
      continue
    }

    go c.serve()
  }
  return nil
}

func (self *Proxy) newClient(rwc net.Conn) (*MCConn, error) {
  c := new(MCConn)
  c.addr = rwc.RemoteAddr()
  c.nc = rwc
  c.proxy = self

  return c, nil
}

func (self *Proxy) getFreeConn(addr net.Addr) (*MCConn, bool) {
  self.mtx.Lock()
  defer self.mtx.Unlock()

  freelist, ok := self.freeconn[addr.String()]
  if !ok || len(freelist) == 0 {
    return nil, false
  }

  conn := freelist[len(freelist)-1]
  self.freeconn[addr.String()] = freelist[:len(freelist)-1]

  return conn, true
}

func (self *Proxy) putFreeConn(conn *MCConn, ierr error) {
  self.mtx.Lock()
  defer self.mtx.Unlock()

  freelist := self.freeconn[conn.addr.String()]
  if len(freelist) >= self.MaxIdle || ierr != nil {
    conn.Close()
    return
  }

  self.freeconn[conn.addr.String()] = append(freelist, conn)
}

func (self *Proxy) getConn(addr net.Addr) (*MCConn, error) {
  conn, ok := self.getFreeConn(addr)
  if ok == true {
    return conn, nil
  }

  return Dial(addr)
}
