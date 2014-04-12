package tests

import (
  "fmt"
  "github.com/lxfontes/roxter"
  "github.com/stathat/consistent"
  "testing"
)

func Test_RingStatic(t *testing.T) {
  r := roxter.NewRing(100)
  r.AddNode("node1", 1)
  r.AddNode("node2", 1)
  r.AddNode("node3", 1)
  r.Bake()

  // these are statically mapped and totally random
  k1 := r.Hash("key1")
  k2 := r.Hash("keyd")
  k3 := r.Hash("key2")

  if k1 != "node1" {
    t.Fatal("Invalid node assignment: k1")
  }

  if k2 != "node2" {
    t.Fatal("Invalid node assignment: k2")
  }

  if k3 != "node3" {
    t.Fatal("Invalid node assignment: k3")
  }

}

func Test_RingConsistency(t *testing.T) {
  r := roxter.NewRing(100)
  r.AddNode("node1", 1)
  r.AddNode("node2", 1)
  r.AddNode("node3", 1)
  r.Bake()

  // these are statically mapped and totally random
  k1 := r.Hash("key1")
  k2 := r.Hash("keyd")
  k3 := r.Hash("key2")

  if k1 != "node1" || k2 != "node2" || k3 != "node3" {
    t.Fatal("Consistency failed: first pass")
  }

  //adding nodes out of order, but keeping naming consistent
  r = roxter.NewRing(100)
  r.AddNode("node1", 1)
  r.AddNode("node3", 1)
  r.AddNode("node2", 1)
  r.Bake()

  k1 = r.Hash("key1")
  k2 = r.Hash("keyd")
  k3 = r.Hash("key2")

  if k1 != "node1" || k2 != "node2" || k3 != "node3" {
    t.Fatal("Consistency failed: second pass", k1, k2, k3)
  }

}

func Test_RingRebalance(t *testing.T) {
  r := roxter.NewRing(100)
  r.AddNode("node1", 1)
  r.AddNode("node2", 1)
  r.AddNode("node3", 1)
  r.Bake()

  // these are statically mapped and totally random
  k1 := r.Hash("key1")
  k2 := r.Hash("keyd")
  k3 := r.Hash("key2")

  if k1 != "node1" || k2 != "node2" || k3 != "node3" {
    t.Fatal("Rebalance failed: first pass")
  }

  //adding nodes out of order, but keeping naming consistent
  r = roxter.NewRing(100)
  r.AddNode("node1", 1)
  r.AddNode("node2", 1)
  r.Bake()

  k1 = r.Hash("key1")
  k2 = r.Hash("keyd")
  //k3 should rebalance
  k3 = r.Hash("key2")

  if k1 != "node1" || k2 != "node2" || k3 != "node1" {
    t.Fatal("Rebalance failed: second pass", k1, k2, k3)
  }

}

func Benchmark_Ketama(b *testing.B) {

  r := roxter.NewRing(100)
  r.AddNode("node1", 1)
  r.AddNode("node2", 1)
  r.AddNode("node3", 1)
  r.Bake()

  for i := 0; i < b.N; i++ {
    r.Hash(fmt.Sprint("hash", i))
  }

}

func Benchmark_StatHat(b *testing.B) {
  c := consistent.New()
  c.Add("node1")
  c.Add("node2")
  c.Add("node3")

  for n := 0; n < b.N; n++ {
    b.Log(c.Get(fmt.Sprint("hash", n)))
  }
}
