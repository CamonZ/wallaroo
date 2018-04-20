/*

Copyright 2017 The Wallaroo Authors.

Licensed as a Wallaroo Enterprise file under the Wallaroo Community
License (the "License"); you may not use this file except in compliance with
the License. You may obtain a copy of the License at

     https://github.com/wallaroolabs/wallaroo/blob/master/LICENSE

*/

use "collections"
use "net"
use "wallaroo/core/common"
use "wallaroo/core/initialization"
use "wallaroo/core/invariant"
use "wallaroo/core/messages"
use "wallaroo/ent/network"
use "wallaroo/ent/router_registry"
use "wallaroo_labs/collection_helpers"
use "wallaroo_labs/mort"

class Autoscale
  """
  TODO: This is currently a work in progress. We should eventually move the
    protocol logic from RouterRegistry to this class. For the time being, it
    only handles the first two phases of join autoscale events.

  Phases:
    Initial: _WaitingForAutoscale: Wait for !@ to be called to initiate
      autoscale. At that point, we either begin join or shrink protocol.

    JOIN:
    1) _WaitingForJoiners: Waiting for provided number of workers to connect
    2) _WaitingForJoinerInitialization: Waiting for all joiners to initialize
    3) _WaitingForConnections: Waiting for current workers to connect to
      joiners
    TODO: Handle the remaining phases, which are currently handled by
      RouterRegistry.

    SHRINK:
    TODO: Handle shrink autoscale phases, which are currently handled by
      RouterRegistry.
  """
  let _auth: AmbientAuth
  let _router_registry: RouterRegistry ref
  let _connections: Connections
  var _phase: AutoscalePhase = _EmptyAutoscalePhase

  new create(auth: AmbientAuth, rr: RouterRegistry ref,
    connections: Connections)
  =>
    _auth = auth
    _router_registry = rr
    _connections = connections
    _phase = _WaitingForAutoscale(this)

  fun ref worker_join(conn: TCPConnection, worker: String,
    worker_count: USize, local_topology: LocalTopology,
    current_worker_count: USize)
  =>
    _phase.worker_join(conn, worker, worker_count, local_topology,
      current_worker_count)

  fun ref wait_for_joiners(conn: TCPConnection, worker: String,
    worker_count: USize, local_topology: LocalTopology,
    current_worker_count: USize)
  =>
    _phase = _WaitingForJoiners(_auth, this, _router_registry, worker_count,
      current_worker_count)
    _phase.worker_join(conn, worker, worker_count, local_topology,
      current_worker_count)

  fun ref wait_for_joiner_initialization(joining_worker_count: USize,
    initialized_workers: SetIs[String], current_worker_count: USize)
  =>
    _phase = _WaitingForJoinerInitialization(this, joining_worker_count,
      initialized_workers, current_worker_count)

  fun ref wait_for_connections(new_workers: Array[String] val,
    current_worker_count: USize)
  =>
    _phase = _WaitingForConnections(this, new_workers, current_worker_count)

  fun ref autoscale_complete() =>
    _phase = _WaitingForAutoscale(this)

  fun ref joining_worker_initialized(worker: String) =>
    _phase.joining_worker_initialized(worker)

  fun ref worker_connected_to_joining_workers(worker: String) =>
    _phase.worker_connected_to_joining_workers(worker)

  fun notify_current_workers_of_joining_addresses(
    new_workers: Array[String] val)
  =>
    _connections.notify_current_workers_of_joining_addresses(new_workers)

  fun notify_joining_workers_of_joining_addresses(
    new_workers: Array[String] val)
  =>
    _connections.notify_joining_workers_of_joining_addresses(new_workers)

  fun ref migrate_onto_new_workers(new_workers: Array[String] val) =>
    // TODO: For now, we're handing control of the join protocol over to
    // RouterRegistry at this point. Eventually, we should manage the
    // entire protocol.
    _phase = _MigrationInProgress

    _router_registry.stop_the_world_for_grow_migration(new_workers)

  fun send_control_to_cluster(msg: Array[ByteSeq] val) =>
    _connections.send_control_to_cluster(msg)

  fun send_control_to_cluster_with_exclusions(msg: Array[ByteSeq] val,
    exceptions: Array[String] val)
  =>
    _connections.send_control_to_cluster_with_exclusions(msg, exceptions)

///////////////////
// Autoscale Phases
///////////////////
trait AutoscalePhase
  fun ref worker_join(conn: TCPConnection, worker: String,
    worker_count: USize, local_topology: LocalTopology,
    current_worker_count: USize)
  =>
    Fail()

  fun ref joining_worker_initialized(worker: String) =>
    ifdef debug then
      @printf[I32](("Joining worker reported as initialized, but we're " +
        "not waiting for it. This should mean that either we weren't the " +
        "worker contacted for the join or we're also joining.\n").cstring())
    end

  fun ref worker_connected_to_joining_workers(worker: String) =>
    Fail()

class _EmptyAutoscalePhase is AutoscalePhase

class _WaitingForAutoscale is AutoscalePhase
  let _autoscale: Autoscale ref

  new create(autoscale: Autoscale ref) =>
    _autoscale = autoscale

  fun ref worker_join(conn: TCPConnection, worker: String,
    worker_count: USize, local_topology: LocalTopology,
    current_worker_count: USize)
  =>
    _autoscale.wait_for_joiners(conn, worker, worker_count, local_topology,
      current_worker_count)

class _WaitingForJoiners is AutoscalePhase
  let _auth: AmbientAuth
  let _autoscale: Autoscale ref
  let _router_registry: RouterRegistry ref
  let _joining_worker_count: USize
  let _connected_joiners: SetIs[String] = _connected_joiners.create()
  let _initialized_workers: SetIs[String] = _initialized_workers.create()
  let _current_worker_count: USize

  new create(auth: AmbientAuth, autoscale: Autoscale ref,
    rr: RouterRegistry ref, joining_worker_count: USize,
    current_worker_count: USize)
  =>
    _auth = auth
    _autoscale = autoscale
    _router_registry = rr
    _joining_worker_count = joining_worker_count
    _current_worker_count = current_worker_count
    @printf[I32](("AUTOSCALE: Waiting for %s joining workers. Current " +
      "cluster size: %s\n").cstring(),
      _joining_worker_count.string().cstring(),
      _current_worker_count.string().cstring())

  fun ref worker_join(conn: TCPConnection, worker: String,
    worker_count: USize, local_topology: LocalTopology,
    current_worker_count: USize)
  =>
    if worker_count != _joining_worker_count then
      @printf[I32]("Join error: Joining worker supplied invalid worker count\n"
        .cstring())
      let error_msg = "All joining workers must supply the same worker " +
        "count. Current pending count is " + _joining_worker_count.string() +
        ". You supplied " + worker_count.string() + "."
      try
        let msg = ChannelMsgEncoder.inform_join_error(error_msg, _auth)?
        conn.writev(msg)
      else
        Fail()
      end
    elseif worker_count < 1 then
      @printf[I32](("Join error: Joining worker supplied a worker count " +
        "less than 1\n").cstring())
      let error_msg = "Joining worker must supply a worker count greater " +
        "than 0."
      try
        let msg = ChannelMsgEncoder.inform_join_error(error_msg, _auth)?
        conn.writev(msg)
      else
        Fail()
      end
    else
      _router_registry.inform_joining_worker(conn, worker, local_topology)
      _connected_joiners.set(worker)
      if _connected_joiners.size() == _joining_worker_count then
        _autoscale.wait_for_joiner_initialization(_joining_worker_count,
          _initialized_workers, _current_worker_count)
      end
    end

  fun ref joining_worker_initialized(worker: String) =>
    // It's possible some workers will be initialized when we're still in
    // this phase. We need to keep track of this to hand off that info to
    // the next phase.
    _initialized_workers.set(worker)
    if _initialized_workers.size() >= _joining_worker_count then
      // We should have already transitioned to the next phase before this.
      Fail()
    end

class _WaitingForJoinerInitialization is AutoscalePhase
  let _autoscale: Autoscale ref
  let _joining_worker_count: USize
  var _initialized_joining_workers: SetIs[String]
  let _current_worker_count: USize

  new create(autoscale: Autoscale ref, joining_worker_count: USize,
    initialized_workers: SetIs[String], current_worker_count: USize)
  =>
    ifdef debug then
      // When this phase begins, at least one joining worker should still
      // have not notified us it was initialized.
      Invariant(initialized_workers.size() < joining_worker_count)
    end
    _autoscale = autoscale
    _joining_worker_count = joining_worker_count
    _initialized_joining_workers = initialized_workers
    _current_worker_count = current_worker_count
    @printf[I32](("AUTOSCALE: Waiting for %s joining workers to initialize. " +
      "Already initialized: %s. Current cluster size is %s\n").cstring(),
      _joining_worker_count.string().cstring(),
      _initialized_joining_workers.size().string().cstring(),
      _current_worker_count.string().cstring())

  fun ref joining_worker_initialized(worker: String) =>
    _initialized_joining_workers.set(worker)
    if _initialized_joining_workers.size() == _joining_worker_count then
      let nws = recover trn Array[String] end
      for w in _initialized_joining_workers.values() do
        nws.push(w)
      end
      let new_workers = consume val nws
      _autoscale.notify_joining_workers_of_joining_addresses(new_workers)
      _autoscale.notify_current_workers_of_joining_addresses(new_workers)
      _autoscale.wait_for_connections(new_workers, _current_worker_count)
    end

class _WaitingForConnections is AutoscalePhase
  let _autoscale: Autoscale ref
  let _new_workers: Array[String] val
  let _connecting_worker_count: USize
  let _connected_workers: SetIs[String] = _connected_workers.create()

  new create(autoscale: Autoscale ref, new_workers: Array[String] val,
    current_worker_count: USize)
  =>
    _autoscale = autoscale
    _new_workers = new_workers
    _connecting_worker_count = current_worker_count - 1
    if _connecting_worker_count == 0 then
      // Single worker cluster. We're ready to migrate already.
      _autoscale.migrate_onto_new_workers(_new_workers)
    else
      @printf[I32](("AUTOSCALE: Waiting for %s current workers to connect " +
        "to joining workers.\n").cstring(),
        _connecting_worker_count.string().cstring())
    end

  fun ref worker_connected_to_joining_workers(worker: String) =>
    _connected_workers.set(worker)
    if _connected_workers.size() == _connecting_worker_count then
      _autoscale.migrate_onto_new_workers(_new_workers)
    end

class _MigrationInProgress is AutoscalePhase
  """
  During this phase, we've handed off responsibility for autoscale migration to
  the RouterRegistry.
  """
  new create() =>
    @printf[I32]("AUTOSCALE: Preparing for join migration.\n".cstring())
