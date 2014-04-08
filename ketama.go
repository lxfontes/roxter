package roxter

// https://raw.githubusercontent.com/mncaudill/ketama/master/ketama.go
// adapted to export fields

import (
  "crypto/sha1"
  "sort"
  "strconv"
)

type node struct {
  node string
  hash uint
}

type tickArray []node

func (p tickArray) Len() int           { return len(p) }
func (p tickArray) Less(i, j int) bool { return p[i].hash < p[j].hash }
func (p tickArray) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }
func (p tickArray) Sort()              { sort.Sort(p) }

type HashRing struct {
  DefaultSpots int
  Ticks        tickArray
  Length       int
}

func NewRing(n int) (h *HashRing) {
  h = new(HashRing)
  h.DefaultSpots = n
  return
}

// Adds a new node to a hash ring
// n: name of the server
// s: multiplier for default number of ticks (useful when one cache node has more resources, like RAM, than another)
func (h *HashRing) AddNode(n string, s int) {
  tSpots := h.DefaultSpots * s
  hash := sha1.New()
  for i := 1; i <= tSpots; i++ {
    hash.Write([]byte(n + ":" + strconv.Itoa(i)))
    hashBytes := hash.Sum(nil)

    n := &node{
      node: n,
      hash: uint(hashBytes[19]) | uint(hashBytes[18])<<8 | uint(hashBytes[17])<<16 | uint(hashBytes[16])<<24,
    }

    h.Ticks = append(h.Ticks, *n)
    hash.Reset()
  }
}

func (h *HashRing) Bake() {
  h.Ticks.Sort()
  h.Length = len(h.Ticks)
}

func (h *HashRing) Hash(s string) string {
  hash := sha1.New()
  hash.Write([]byte(s))
  hashBytes := hash.Sum(nil)
  v := uint(hashBytes[19]) | uint(hashBytes[18])<<8 | uint(hashBytes[17])<<16 | uint(hashBytes[16])<<24
  i := sort.Search(h.Length, func(i int) bool { return h.Ticks[i].hash >= v })

  if i == h.Length {
    i = 0
  }

  return h.Ticks[i].node
}
