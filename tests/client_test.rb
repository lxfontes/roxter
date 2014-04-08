#!/usr/bin/env ruby
# coding: utf-8
require 'dalli'
require 'benchmark'

def getcon(addr)
  options = { namespace: 'app_v1' }
  Dalli::Client.new(addr, options)
end

requests = 500
clients = 100

puts("Starting #{clients} running #{requests} each. Total #{requests * clients}.")

Benchmark.bm do |x|
  proxy_conns = []
  mc_conns = []

  clients.times do
    proxy_conns << getcon('localhost:11212')
    mc_conns << getcon('localhost:11211')
  end

  x.report('mset:') do
    tg = ThreadGroup.new
    mc_conns.each do |conn|
      th = Thread.new do
        requests.times { |i| conn.set("conn#{i}", i) }
      end
      tg.add(th)
    end
    tg.list.each do |th|
      th.join
    end
  end

  x.report('mget:') do
    tg = ThreadGroup.new
    mc_conns.each do |conn|
      th = Thread.new do
        requests.times { |i| conn.get("conn#{i}") }
      end
      tg.add(th)
    end
    tg.list.each do |th|
      th.join
    end
  end

  x.report('pset:') do
    tg = ThreadGroup.new
    proxy_conns.each do |conn|
      th = Thread.new do
        requests.times { |i| conn.set("pconn#{i}", i) }
      end
      tg.add(th)
    end
    tg.list.each do |th|
      th.join
    end
  end

  x.report('pget:') do
    tg = ThreadGroup.new
    proxy_conns.each do |conn|
      th = Thread.new do
        requests.times { |i| conn.get("pconn#{i}") }
      end
      tg.add(th)
    end
    tg.list.each do |th|
      th.join
    end
  end

end
