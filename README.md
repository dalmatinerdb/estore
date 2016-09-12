# estore

`estore` short for event-store. It is a pure Erlang store for mostly ordered time indexed events or logs.


## Design

It is build around the concept of using sequential reads and writes and allow for distributed storage (like `riak_core`). `estore` archives this by a few mechanisms.

### Mostly ordered writes

For once wrote are allowed to be out of order for a certain grace period this allows to use a simple append even if events don't come in perfect order. This means that for reads we need to read a bit between the requested time.

For example with a grace periods of `30s` when reading between `21:01` and `21:02` we would read up to `21:02.30` discarding every event that has a timestamp over `21:02`.

### Reconciliation file

While the grace periods does cover most events that come out of order sometimes that is not enough. In this cases they are put in the reconciliation file. The file does not guarantee any order.


This has the downside that during a read all elements in the recon file. However so far tests have shown that the recon file remains fairly small in real life conditions even with many events arriving from multiple servers.

### Indexing

`estore` keeps an index file that keeps 'most' indexes for events written to the event store file. The file is not guaranteed or required to keep indexes for every entry. However having the indexes allows to avoid scanning the store file.

The fact that indexing is not required to be 100% means that this can later be tuned and optimized.


### Event IDs

Each event is required to carry a 160 bit (size of a sha1 sum) ID. those ID's allow that in the case of a distributed use events can be repaired without accidentally duplicating or reduplicating events.

It allows to have two equal events with the same timestamp and content and still repair them.

### Store vs. File

The `estore` has two interfaces. Once the `estore` itself wich allows keeping files smaller it shards multiple `efiles` based on time range, that way it keeps the recon files as small as possible. It also works well when distributing and sharding by time range.

The `efile` is the 'simple' implementation of the above mention concepts.


## Build

That's rather simple.


```bash
$ rebar3 compile
```

## Usage

The normal interface is `estore` so this section will discuss only that, however the `efile` interface is near identical.

```erlang

%%  Create a estore with a 1 day and a grace period of 1 minute.
T0 = erlang:system_time(nano_seconds),
{ok, E1} = estore:new("exmaplestore", [{file_size, {1, d}}, {grace, {1, m}}]).
{ok, E2} = estore:append([estore:event(<<"my event">>)], E1),
{ok, Es, E3} = estore:read(T0, erlang:system_time(nano_seconds), E2).
```