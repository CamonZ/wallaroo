/*

Copyright 2017 The Wallaroo Authors.

Licensed as a Wallaroo Enterprise file under the Wallaroo Community
License (the "License"); you may not use this file except in compliance with
the License. You may obtain a copy of the License at

     https://github.com/wallaroolabs/wallaroo/blob/master/LICENSE

*/

use "buffered"
use "collections"
use "files"
use "wallaroo/core/common"
use "wallaroo/ent/router_registry"
use "wallaroo_labs/mort"
use "wallaroo/core/initialization"
use "wallaroo/core/messages"
use "wallaroo/core/topology"

interface tag Resilient
  be log_replay_finished()
  be replay_log_entry(uid: U128, frac_ids: FractionalMessageId,
    statechange_id: U64, payload: ByteSeq)
  be initialize_seq_id_on_recovery(seq_id: SeqId)
  be log_flushed(low_watermark: SeqId)

  // Log-rotation
  be snapshot_state()


class val EventLogConfig
  let log_dir: (FilePath | AmbientAuth | None)
  let filename: (String val | None)
  let logging_batch_size: USize
  let backend_file_length: (USize | None)
  let log_rotation: Bool
  let suffix: String

  new val create(log_dir': (FilePath | AmbientAuth | None) = None,
    filename': (String val | None) = None,
    logging_batch_size': USize = 10,
    backend_file_length': (USize | None) = None,
    log_rotation': Bool = false,
    suffix': String = ".evlog")
  =>
    filename = filename'
    log_dir = log_dir'
    logging_batch_size = logging_batch_size'
    backend_file_length = backend_file_length'
    log_rotation = log_rotation'
    suffix = suffix'

actor EventLog
  let _resilients: Map[StepId, Resilient] = _resilients.create()
  let _backend: Backend
  let _replay_complete_markers: Map[U64, Bool] =
    _replay_complete_markers.create()
  let _config: EventLogConfig
  var num_encoded: USize = 0
  var _flush_waiting: USize = 0
  var _initialized: Bool = false
  var _recovery: (Recovery | None) = None
  var _resilients_to_snapshot: SetIs[StepId] = _resilients_to_snapshot.create()
  var _router_registry: (RouterRegistry | None) = None
  var _rotating: Bool = false
  var _backend_bytes_after_snapshot: USize

  new create(event_log_config: EventLogConfig = EventLogConfig()) =>
    _config = event_log_config
    _backend = match _config.filename
      | let f: String val =>
        try
          if _config.log_rotation then
            match _config.log_dir
            | let ld: FilePath =>
              RotatingFileBackend(ld, f, _config.suffix, this,
                _config.backend_file_length)?
            else
              Fail()
              DummyBackend(this)
            end
          else
            match _config.log_dir
            | let ld: FilePath =>
              FileBackend(FilePath(ld, f)?, this)
            | let ld: AmbientAuth =>
              FileBackend(FilePath(ld, f)?, this)
            else
              Fail()
              DummyBackend(this)
            end
          end
        else
          DummyBackend(this)
        end
      else
        DummyBackend(this)
      end
    _backend_bytes_after_snapshot = _backend.bytes_written()

  be set_router_registry(router_registry: RouterRegistry) =>
    _router_registry = router_registry

  be start_pipeline_logging(initializer: LocalTopologyInitializer) =>
    _initialized = true
    initializer.report_event_log_ready_to_work()

  be start_log_replay(recovery: Recovery) =>
    _recovery = recovery
    _backend.start_log_replay()

  be log_replay_finished() =>
    for r in _resilients.values() do
      r.log_replay_finished()
    end

    match _recovery
    | let r: Recovery =>
      r.log_replay_finished()
    else
      Fail()
    end

  be replay_log_entry(resilient_id: StepId,
    uid: U128, frac_ids: FractionalMessageId,
    statechange_id: U64, payload: ByteSeq val)
  =>
    try
      _resilients(resilient_id)?.replay_log_entry(uid, frac_ids,
        statechange_id, payload)
    else
      @printf[I32](("FATAL: Unable to replay event log, because a replay " +
        "buffer has disappeared").cstring())
      Fail()
    end

  be initialize_seq_ids(seq_ids: Map[StepId, SeqId] val) =>
    for (resilient_id, seq_id) in seq_ids.pairs() do
      try
        _resilients(resilient_id)?.initialize_seq_id_on_recovery(seq_id)
      else
        @printf[I32](("Could not initialize seq id " + seq_id.string() +
          ". Resilient " + resilient_id.string() + " does not exist\n")
          .cstring())
        Fail()
      end
    end

  be register_resilient(resilient: Resilient, id: StepId) =>
    _resilients(id) = resilient

  be queue_log_entry(resilient_id: StepId, uid: U128,
    frac_ids: FractionalMessageId, statechange_id: U64, seq_id: U64,
    payload: Array[ByteSeq] val)
  =>
    _queue_log_entry(resilient_id, uid, frac_ids, statechange_id, seq_id,
      payload)

  fun ref _queue_log_entry(resilient_id: StepId, uid: U128,
    frac_ids: FractionalMessageId,
    statechange_id: U64, seq_id: U64,
    payload: Array[ByteSeq] val, force_write: Bool = false)
  =>
    ifdef "resilience" then
      // add to backend buffer after encoding
      // encode right away to amortize encoding cost per entry when received
      // as opposed to when writing a batch to disk
      _backend.encode_entry((false, resilient_id, uid, frac_ids, statechange_id,
        seq_id, payload))

      num_encoded = num_encoded + 1

      if (num_encoded == _config.logging_batch_size) or force_write then
        //write buffer to disk
        write_log()
      end
    else
      None
    end

  fun ref write_log() =>
    try
      num_encoded = 0

      // write buffer to disk
      _backend.write()?
    else
      @printf[I32]("error writing log entries to disk!\n".cstring())
      Fail()
    end

  be flush_buffer(resilient_id: StepId, low_watermark: U64) =>
    _flush_buffer(resilient_id, low_watermark)

  fun ref _flush_buffer(resilient_id: StepId, low_watermark: U64) =>
    ifdef "trace" then
      @printf[I32](("flush_buffer for id: " + resilient_id.string() + "\n\n")
        .cstring())
    end

    try
      // Add low watermark ack to buffer
      _backend.encode_entry((true, resilient_id, 0, None, 0, low_watermark,
        recover Array[ByteSeq] end))

      num_encoded = num_encoded + 1
      _flush_waiting = _flush_waiting + 1
      //write buffer to disk
      write_log()

      // if (_flush_waiting % 50) == 0 then
      //   //sync any written data to disk
      //   _backend.sync()
      //   _backend.datasync()
      // end

      _resilients(resilient_id)?.log_flushed(low_watermark)
    else
      @printf[I32]("Errror writing/flushing/syncing ack to disk!\n".cstring())
      Fail()
    end

  be snapshot_state(resilient_id: StepId, uid: U128,
    statechange_id: U64, seq_id: U64,
    payload: Array[ByteSeq] val)
  =>
    ifdef "trace" then
      @printf[I32](("Snapshotting state for resilient " + resilient_id.string()
        + "\n").cstring())
    end
    if _resilients_to_snapshot.contains(resilient_id) then
      _resilients_to_snapshot.unset(resilient_id)
    else
      @printf[I32](("Error writing snapshot to logfile. StepId not in set " +
        "of expected resilients!\n").cstring())
      Fail()
    end

    // Note: calling _flush_buffer relies on the assumption that everything
    // is acked by now, which isn't being validated here.
    // This should be addressed by
    // https://github.com/WallarooLabs/wallaroo/issues/1132
    _flush_buffer(resilient_id, seq_id)
    _queue_log_entry(resilient_id, uid, None, statechange_id, seq_id,
      payload, true)
    if _resilients_to_snapshot.size() == 0 then
      rotation_complete()
    end

  be start_rotation() =>
    if _rotating then
      @printf[I32](("Event log rotation already ongoing. Rotate log request "
        + "ignrored.\n").cstring())
    elseif _backend.bytes_written() > _backend_bytes_after_snapshot then
      @printf[I32]("Starting event log rotation.\n".cstring())
      _rotating = true
      match _router_registry
      | let r: RouterRegistry =>
        r.rotate_log_file()
      else
        Fail()
      end
    else
      @printf[I32](("Event log does not contain new data. Rotate log request"
        + " ignored.\n").cstring())
    end

  be rotate_file() =>
    @printf[I32]("Snapshotting %d resilients to new log file.\n".cstring(),
      _resilients.size())
    match _router_registry
    | None =>
      Fail()
    end
    _rotate_file()
    _resilients_to_snapshot = _resilients_to_snapshot.create()
    for v in _resilients.keys() do
      _resilients_to_snapshot.set(v)
    end
    for r in _resilients.values() do
      r.snapshot_state()
    end

  fun ref _rotate_file() =>
    try
      match _backend
      | let b: RotatingFileBackend => b.rotate_file()?
      else
        @printf[I32](("Unsupported operation requested on log Backend: " +
                      "'rotate_file'. Request ignored.\n").cstring())
      end
    else
      @printf[I32]("Error rotating log file!\n".cstring())
      Fail()
    end

  fun ref rotation_complete() =>
    @printf[I32]("Resilients snapshotting to new log file complete.\n"
      .cstring())
    try
      _backend.sync()?
      _backend.datasync()?
      _backend_bytes_after_snapshot = _backend.bytes_written()
    else
      Fail()
    end
    _rotating = false
    match _router_registry
    | let r: RouterRegistry => r.rotation_complete()
    else
      Fail()
    end
