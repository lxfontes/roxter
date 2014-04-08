# roxter

Memcached proxy, focused on:
- Speed
- Sharding
- Memcache Binary Protocol


## So, what ? why ?

This is heavily inspired by moxi from couchbase, however we prefer to use plain 
memcached and have a bit more control of our data.

Unfortunately the most used memcached gem for rails (dalli) and the most 
performing proxy (nutcracker) don't talk to each other.

The proxy keeps a list of active memcached servers, rebalancing keys in case they go kaput.


## Features left behind

- SASL / PLAIN authentication
- ASCII protocol (check twitter's nutcracker if you need this)

## Installation

    go get -u github.com/lxfontes/roxter/roxter

## Usage

Assuming 2 memcache servers running on localhost 11210 and 11211:

    roxter -bind ":11212" -server 127.0.0.1:11210 -server 127.0.0.1:11211

This will setup a listener on 11212 and split keys between 11210 and 11211.

## ACK

- @bradfitz - Selector Idea
- @mncaudill - Ketama
- @uken
