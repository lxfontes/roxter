package roxter

import (
  "bytes"
  "encoding/binary"
  "io"
  "net"
  "sync"
  "time"
)

/*
   Byte/     0       |       1       |       2       |       3       |
      /              |               |               |               |
     |0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|
     +---------------+---------------+---------------+---------------+
    0/ HEADER                                                        /
     /                                                               /
     /                                                               /
     /                                                               /
     +---------------+---------------+---------------+---------------+
   24/ COMMAND-SPECIFIC EXTRAS (as needed)                           /
    +/  (note length in the extras length header field)              /
     +---------------+---------------+---------------+---------------+
    m/ Key (as needed)                                               /
    +/  (note length in key length header field)                     /
     +---------------+---------------+---------------+---------------+
    n/ Value (as needed)                                             /
    +/  (note length is total body length header field, minus        /
    +/   sum of the extras and key length body fields)               /
     +---------------+---------------+---------------+---------------+
     Total 24 bytes
*/

type mcheader struct {
  Magic     uint8
  Op        uint8
  KeyLen    uint16
  ExtrasLen uint8
  DataType  uint8
  Status    uint16
  BodyLen   uint32
  Opaque    uint32
  CAS       uint64
}

type MCMessage struct {
  Header mcheader
  Extras []byte
  Key    string
  Value  []byte
}

type MCConn struct {
  nc    io.ReadWriteCloser
  addr  net.Addr
  mtx   sync.Mutex
  proxy *Proxy
}

func (self *MCConn) serve() {
  for {
    var req MCMessage
    err := req.Unmarshal(self.nc)
    if err != nil {
      break
    }

    addr, err := self.proxy.selector.PickServer(req.Key)

    if err != nil {
      break
    }

    server, err := self.proxy.getConn(addr)

    if err != nil {
      break
    }

    resp, err := server.RunSingle(&req)

    self.proxy.putFreeConn(server, err)

    if err != nil {
      break
    }

    buf := new(bytes.Buffer)

    err = resp.Marshal(buf)

    if err != nil {
      break
    }

    self.nc.Write(buf.Bytes())
  }
  self.Close()
}

const DefaultTimeout = time.Duration(100) * time.Millisecond

func Dial(addr net.Addr) (*MCConn, error) {
  type connError struct {
    nc  net.Conn
    err error
  }
  ch := make(chan connError)

  go func() {
    nc, err := net.Dial(addr.Network(), addr.String())
    ch <- connError{nc, err}
  }()

  select {
  case ce := <-ch:
    //tcp stack returned on time
    if ce.err != nil {
      return nil, ce.err
    }
    mc := &MCConn{
      nc:   ce.nc,
      addr: addr,
    }
    return mc, nil
  case <-time.After(DefaultTimeout):
    // catch late connection
    go func() {
      ce := <-ch
      if ce.err == nil {
        ce.nc.Close()
      }
    }()
  }

  return nil, ErrTimeout
}

func (self *MCConn) Close() error {
  return self.nc.Close()
}

// not meant to guarantee that memcache is not full
// simple ping
func (self *MCConn) Ping() bool {
  req := new(MCMessage)
  req.Header.Magic = 0x80
  req.Header.Op = 0x0a

  _, err := self.RunSingle(req)
  if err != nil {
    return false
  }

  return true
}

func (self *MCMessage) Marshal(buf *bytes.Buffer) error {

  valLen := uint16(len(self.Value))
  self.Header.ExtrasLen = uint8(len(self.Extras))

  self.Header.KeyLen = uint16(len(self.Key))
  self.Header.BodyLen = uint32(self.Header.KeyLen + valLen + uint16(self.Header.ExtrasLen))

  //sequence is headers , extras, key, value
  if err := binary.Write(buf, binary.BigEndian, self.Header); err != nil {
    return err
  }

  if _, err := buf.Write(self.Extras); err != nil {
    return err
  }

  if _, err := io.WriteString(buf, self.Key); err != nil {
    return err
  }

  if _, err := buf.Write(self.Value); err != nil {
    return err
  }

  return nil
}

func (self *MCMessage) Unmarshal(nc io.Reader) error {

  if err := binary.Read(nc, binary.BigEndian, &self.Header); err != nil {
    return err
  }

  retBody := make([]byte, self.Header.BodyLen)
  if _, err := io.ReadFull(nc, retBody); err != nil {
    return err
  }

  retBuf := bytes.NewBuffer(retBody)
  retValLen := int(uint32(self.Header.BodyLen) - uint32(self.Header.ExtrasLen) - uint32(self.Header.KeyLen))

  self.Extras = retBuf.Next(int(self.Header.ExtrasLen))
  self.Key = string(retBuf.Next(int(self.Header.KeyLen)))
  self.Value = retBuf.Next(int(retValLen))

  return nil
}

func (self *MCConn) RunSingle(req *MCMessage) (*MCMessage, error) {
  self.mtx.Lock()
  defer self.mtx.Unlock()

  buf := new(bytes.Buffer)

  if err := req.Marshal(buf); err != nil {
    return nil, err
  }

  //TODO: catch half writes ?
  _, err := buf.WriteTo(self.nc)
  if err != nil {
    return nil, err
  }

  var resp = new(MCMessage)

  if err := resp.Unmarshal(self.nc); err != nil {
    return nil, err
  }

  return resp, nil
}
