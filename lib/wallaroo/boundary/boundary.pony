use "assert"
use "buffered"
use "collections"
use "net"
use "time"
use "sendence/bytes"
use "sendence/guid"
use "sendence/wall-clock"
use "wallaroo/fail"
use "wallaroo/initialization"
use "wallaroo/invariant"
use "wallaroo/messages"
use "wallaroo/metrics"
use "wallaroo/routing"
use "wallaroo/tcp-sink"
use "wallaroo/topology"

use @pony_asio_event_create[AsioEventID](owner: AsioEventNotify, fd: U32,
  flags: U32, nsec: U64, noisy: Bool, auto_resub: Bool)
use @pony_asio_event_fd[U32](event: AsioEventID)
use @pony_asio_event_unsubscribe[None](event: AsioEventID)
use @pony_asio_event_resubscribe_read[None](event: AsioEventID)
use @pony_asio_event_resubscribe_write[None](event: AsioEventID)
use @pony_asio_event_destroy[None](event: AsioEventID)


class OutgoingBoundaryBuilder
  fun apply(auth: AmbientAuth, worker_name: String,
    reporter: MetricsReporter iso, host: String, service: String):
      OutgoingBoundary
  =>
    OutgoingBoundary(auth, worker_name, consume reporter, host,
      service)

actor OutgoingBoundary is (CreditFlowConsumer & RunnableStep & Initializable)
  // Steplike
  // let _encoder: EncoderWrapper val
  let _wb: Writer = Writer
  let _metrics_reporter: MetricsReporter

  // CreditFlow
  var _upstreams: Array[Producer] = _upstreams.create()
  var _mute_outstanding: Bool = false
  var _max_distributable_credits: ISize = 350_000
  var _distributable_credits: ISize = _max_distributable_credits
  let _permanent_max_credit_response: ISize = 1024
  var _max_credit_response: ISize = _permanent_max_credit_response
  var _minimum_credit_response: ISize = 250
  var _waiting_producers: Array[Producer] = _waiting_producers.create()

  // TCP
  var _notify: _OutgoingBoundaryNotify
  var _read_buf: Array[U8] iso
  var _next_size: USize
  let _max_size: USize
  var _connect_count: U32 = 0
  var _fd: U32 = -1
  var _in_sent: Bool = false
  var _expect: USize = 0
  var _connected: Bool = false
  var _closed: Bool = false
  var _writeable: Bool = false
  var _event: AsioEventID = AsioEvent.none()
  embed _pending: List[(ByteSeq, USize)] = _pending.create()
  embed _pending_writev: Array[USize] = _pending_writev.create()
  var _pending_writev_total: USize = 0
  var _shutdown_peer: Bool = false
  var _readable: Bool = false
  var _read_len: USize = 0
  var _shutdown: Bool = false
  var _muted: Bool = false
  var _expect_read_buf: Reader = Reader

  // Connection, Acking and Replay
  let _auth: AmbientAuth
  let _worker_name: String
  var _step_id: U128 = 0
  let _host: String
  let _service: String
  let _from: String
  let _queue: Array[Array[ByteSeq] val] = _queue.create()
  var _lowest_queue_id: U64 = 0
  // TODO: this should go away and TerminusRoute entirely takes
  // over seq_id generation whether there is resilience or not.
  var _seq_id: U64 = 1

  // Origin (Resilience)
  let _terminus_route: TerminusRoute = TerminusRoute

  new create(auth: AmbientAuth, worker_name: String,
    metrics_reporter: MetricsReporter iso, host: String, service: String,
    from: String = "", init_size: USize = 64, max_size: USize = 16384)
  =>
    """
    Connect via IPv4 or IPv6. If `from` is a non-empty string, the connection
    will be made from the specified interface.
    """
    // _encoder = encoder_wrapper
    _auth = auth
    _notify = BoundaryNotify(_auth)
    _worker_name = worker_name
    _host = host
    _service = service
    _from = from
    _metrics_reporter = consume metrics_reporter
    _read_buf = recover Array[U8].undefined(init_size) end
    _next_size = init_size
    _max_size = 65_536

  //
  // Application startup lifecycle event
  //

  be application_begin_reporting(initializer: LocalTopologyInitializer) =>
    initializer.report_created(this)

  be application_created(initializer: LocalTopologyInitializer,
    omni_router: OmniRouter val)
  =>
    _connect_count = @pony_os_connect_tcp[U32](this,
      _host.cstring(), _service.cstring(),
      _from.cstring())
    _notify_connecting()

    @printf[I32](("Connected OutgoingBoundary to " + _host + ":" + _service + "\n").cstring())

    // If connecting failed, we should handle here
    initializer.report_initialized(this)

  be application_initialized(initializer: LocalTopologyInitializer) =>
    try
      if _step_id == 0 then
        Fail()
      end

      let connect_msg = ChannelMsgEncoder.data_connect(_worker_name, _step_id,
        _auth)
      writev(connect_msg)
    else
      Fail()
    end

    initializer.report_ready_to_work(this)

  be application_ready_to_work(initializer: LocalTopologyInitializer) =>
    None

  be register_step_id(step_id: U128) =>
    _step_id = step_id

  be run[D: Any val](metric_name: String, pipeline_time_spent: U64, data: D,
    origin: Producer, msg_uid: U128,
    frac_ids: None, seq_id: SeqId, route_id: RouteId,
    latest_ts: U64, metrics_id: U16, worker_ingress_ts: U64)
  =>
    // Run should never be called on an OutgoingBoundary
    Fail()

  be replay_run[D: Any val](metric_name: String, pipeline_time_spent: U64,
    data: D, origin: Producer, msg_uid: U128,
    frac_ids: None, incoming_seq_id: SeqId, route_id: RouteId,
    latest_ts: U64, metrics_id: U16, worker_ingress_ts: U64)
  =>
    // Should never be called on an OutgoingBoundary
    Fail()

  // TODO: open question: how do we reconnect if our external system goes away?
  be forward(delivery_msg: ReplayableDeliveryMsg val, pipeline_time_spent: U64,
    i_origin: Producer, i_seq_id: SeqId, i_route_id: RouteId, latest_ts: U64,
    metrics_id: U16, worker_ingress_ts: U64)
  =>
    let metric_name = delivery_msg.metric_name()
    let i_frac_ids = delivery_msg.frac_ids()
    let msg_uid = delivery_msg.msg_uid()

    ifdef "trace" then
      @printf[I32]("Rcvd message at OutgoingBoundary\n".cstring())
    end

    let my_latest_ts = ifdef "detailed-metrics" then
        Time.nanos()
      else
        latest_ts
      end

    let new_metrics_id = ifdef "detailed-metrics" then
        _metrics_reporter.step_metric(metric_name, "Before receive at boundary",
          metrics_id, latest_ts, my_latest_ts)
        metrics_id + 2
      else
        metrics_id
      end

    try
      let seq_id = ifdef "resilience" then
        _terminus_route.terminate(i_origin, i_route_id, i_seq_id)
      else
        _seq_id = _seq_id + 1
      end

      let outgoing_msg = ChannelMsgEncoder.data_channel(delivery_msg,
        pipeline_time_spent + (Time.nanos() - worker_ingress_ts),
        seq_id, _wb, _auth, WallClock.nanoseconds(),
        new_metrics_id, metric_name)
        _add_to_upstream_backup(outgoing_msg)

      _writev(outgoing_msg)

      let end_ts = Time.nanos()

      ifdef "detailed-metrics" then
        _metrics_reporter.step_metric(metric_name,
          "Before sending to next worker", metrics_id + 1,
          my_latest_ts, end_ts)
      end

      _metrics_reporter.worker_metric(metric_name, end_ts - worker_ingress_ts)
    else
      Fail()
    end

    _maybe_mute_or_unmute_upstreams()

  be writev(data: Array[ByteSeq] val) =>
    _writev(data)

  fun ref ack(seq_id: SeqId) =>
    ifdef debug then
      Invariant(seq_id > _lowest_queue_id)
    end

    ifdef "trace" then
      @printf[I32](
        "OutgoingBoundary: got ack from downstream worker\n".cstring())
    end

    let flush_count: USize = (seq_id - _lowest_queue_id).usize()
    _queue.remove(0, flush_count)
    _lowest_queue_id = _lowest_queue_id + flush_count.u64()

    ifdef "backpressure" then
      recoup_credits(flush_count.isize())
    end

    ifdef "resilience" then
      _terminus_route.receive_ack(seq_id)
    end

  be replay_msgs() =>
    for msg in _queue.values() do
      try
        _writev(ChannelMsgEncoder.replay(msg, _auth))
      else
        Fail()
      end
    end
    try
      _writev(ChannelMsgEncoder.replay_complete(_worker_name, _auth))
    else
      Fail()
    end

  be update_router(router: Router val) =>
    """
    No-op: OutgoingBoundary has no router
    """
    None

  be dispose() =>
    """
    Gracefully shuts down the sink. Allows all pending writes
    to be sent but any writes that arrive after this will be
    silently discarded and not acknowleged.
    """
    close()

  //
  // CREDIT FLOW
  be register_producer(producer: Producer) =>
    ifdef debug then
      Invariant(not _upstreams.contains(producer))
    end

    _upstreams.push(producer)
    ifdef "backpressure" then
      _calculate_max_credit_response()
    end

  be unregister_producer(producer: Producer, credits_returned: ISize) =>
    ifdef debug then
      Invariant(_upstreams.contains(producer))
    end

    try
      let i = _upstreams.find(producer)
      _upstreams.delete(i)
      ifdef "backpressure" then
        recoup_credits(credits_returned)
      end
    end
    ifdef "backpressure" then
      _calculate_max_credit_response()
    end

  fun ref _calculate_max_credit_response() =>
    _max_credit_response = if _upstreams.size() > 0 then
      let portion = _max_distributable_credits / _upstreams.size().isize()
      portion.min(_permanent_max_credit_response)
    else
      _permanent_max_credit_response
    end

    if (_max_credit_response < _minimum_credit_response) then
      Fail()
    end

  be credit_request(from: Producer) =>
    """
    Receive a credit request from a producer. For speed purposes, we assume
    the producer is already registered with us.

    Even if we have credits available in the "distributable pool", we can't
    give them out if our outgoing socket is in a non-sendable state. The
    following are non-sendable states:

    - Not connected
    - Closed
    - Not currently writeable

    By not giving out credits when we are "not currently writable", we can
    implement backpressure without having to inform our upstream producers
    to stop sending. They only send when they have credits. If they run out
    and we are experiencing backpressure, they don't get any more.
    """
    ifdef debug then
      Invariant(_upstreams.contains(from))
    end

    if _can_distribute_credits() and (_waiting_producers.size() == 0) then
      _distribute_credits_to(from)
    else
      _waiting_producers.push(from)
    end

  fun ref _can_distribute_credits(): Bool =>
    _can_send() and _above_minimum_response_level()

  fun ref _maybe_distribute_credits() =>
    while (_waiting_producers.size() > 0) and _can_distribute_credits() do
      try
        let producer = _waiting_producers.shift()
        _distribute_credits_to(producer)
      else
        Fail()
      end
    end

  fun ref _distribute_credits_to(producer: Producer) =>
    ifdef debug then
      Invariant(_can_distribute_credits())
    end

    let give_out =
      _distributable_credits
        .min(_max_credit_response)
        .max(_minimum_credit_response)

    ifdef debug then
      Invariant(give_out >= _minimum_credit_response)
    end

    ifdef "credit_trace" then
      @printf[I32]((
        "OutgoingBoundary: Credits requested." +
        " Giving %llu out of %llu\n"
        ).cstring(),
        give_out, _distributable_credits)
    end

    producer.receive_credits(give_out, this)
    _distributable_credits = _distributable_credits - give_out

  be return_credits(credits: ISize) =>
    recoup_credits(credits)

  fun ref recoup_credits(recoup: ISize) =>
    _distributable_credits = _distributable_credits + recoup
    ifdef debug then
      Invariant(_distributable_credits <= _max_distributable_credits)
    end
    _maybe_distribute_credits()

    ifdef "credit_trace" then
      @printf[I32]("OutgoingBoundary: recouped %llu credits. Now at %llu\n".cstring(), recoup, _distributable_credits)
    end

  fun _above_minimum_response_level(): Bool =>
    _distributable_credits >= _minimum_credit_response
  //
  // TCP
 be _event_notify(event: AsioEventID, flags: U32, arg: U32) =>
    """
    Handle socket events.
    """
    if event isnt _event then
      if AsioEvent.writeable(flags) then
        // A connection has completed.
        var fd = @pony_asio_event_fd(event)
        _connect_count = _connect_count - 1

        if not _connected and not _closed then
          // We don't have a connection yet.
          if @pony_os_connected[Bool](fd) then
            // The connection was successful, make it ours.
            _fd = fd
            _event = event
            _connected = true
            _writeable = true

            _notify.connected(this)

            ifdef not windows then
              if _pending_writes() then
                //sent all data; release backpressure
                _release_backpressure()
              end
            end

          else
            // The connection failed, unsubscribe the event and close.
            @pony_asio_event_unsubscribe(event)
            @pony_os_socket_close[None](fd)
            _notify_connecting()
          end
        else
          // We're already connected, unsubscribe the event and close.
          @pony_asio_event_unsubscribe(event)
          @pony_os_socket_close[None](fd)
        end
      else
        // It's not our event.
        if AsioEvent.disposable(flags) then
          // It's disposable, so dispose of it.
          @pony_asio_event_destroy(event)
        end
      end
    else
      // At this point, it's our event.
      if AsioEvent.writeable(flags) then
        _writeable = true
        ifdef not windows then
          if _pending_writes() then
            //sent all data; release backpressure
            _release_backpressure()
          end
        end

      end

      if AsioEvent.readable(flags) then
        _readable = true
        _pending_reads()
      end

      if AsioEvent.disposable(flags) then
        @pony_asio_event_destroy(event)
        _event = AsioEvent.none()
      end

      _try_shutdown()
    end

  fun ref _writev(data: ByteSeqIter)
  =>
    """
    Write a sequence of sequences of bytes.
    """
    _in_sent = true

    var data_size: USize = 0
    for bytes in _notify.sentv(this, data).values() do
      _pending_writev.push(bytes.cpointer().usize()).push(bytes.size())
      _pending_writev_total = _pending_writev_total + bytes.size()
      _pending.push((bytes, 0))
      data_size = data_size + bytes.size()
    end

    _pending_writes()

    _in_sent = false

  fun ref _write_final(data: ByteSeq) =>
    """
    Write as much as possible to the socket. Set _writeable to false if not
    everything was written. On an error, close the connection. This is for
    data that has already been transformed by the notifier.
    """
    _pending_writev.push(data.cpointer().usize()).push(data.size())
    _pending_writev_total = _pending_writev_total + data.size()

    _pending.push((data, 0))
    _pending_writes()

  fun ref _notify_connecting() =>
    """
    Inform the notifier that we're connecting.
    """
    if _connect_count > 0 then
      _notify.connecting(this, _connect_count)
    else
      _notify.connect_failed(this)
      _hard_close()
    end

  fun ref close() =>
    """
    Perform a graceful shutdown. Don't accept new writes, but don't finish
    closing until we get a zero length read.
    """
    _closed = true
    _try_shutdown()

  fun ref _try_shutdown() =>
    """
    If we have closed and we have no remaining writes or pending connections,
    then shutdown.
    """
    if not _closed then
      return
    end

    if
      not _shutdown and
      (_connect_count == 0) and
      (_pending_writev_total == 0)
    then
      _shutdown = true

      if _connected then
        @pony_os_socket_shutdown[None](_fd)
      else
        _shutdown_peer = true
      end
    end

    if _connected and _shutdown and _shutdown_peer then
      _hard_close()
    end

  fun ref _hard_close() =>
    """
    When an error happens, do a non-graceful close.
    """
    if not _connected then
      return
    end

    _connected = false
    _closed = true
    _shutdown = true
    _shutdown_peer = true

    // Unsubscribe immediately and drop all pending writes.
    @pony_asio_event_unsubscribe(_event)
    _pending_writev.clear()
    _pending.clear()
    _pending_writev_total = 0
    _readable = false
    _writeable = false
    ifdef linux then
      AsioEvent.set_readable(_event, false)
      AsioEvent.set_writeable(_event, false)
    end

    @pony_os_socket_close[None](_fd)
    _fd = -1

    _notify.closed(this)

  fun ref _pending_reads() =>
    """
    Unless this connection is currently muted, read while data is available,
    guessing the next packet length as we go. If we read 4 kb of data, send
    ourself a resume message and stop reading, to avoid starving other actors.
    """
    try
      var sum: USize = 0
      var received_called: USize = 0

      while _readable and not _shutdown_peer do
        if _muted then
          return
        end

        // Read as much data as possible.
        let len = @pony_os_recv[USize](
          _event,
          _read_buf.cpointer().usize() + _read_len,
          _read_buf.size() - _read_len) ?

        match len
        | 0 =>
          // Would block, try again later.
          ifdef linux then
            // this is safe because asio thread isn't currently subscribed
            // for a read event so will not be writing to the readable flag
            AsioEvent.set_readable(_event, false)
            _readable = false
            @pony_asio_event_resubscribe_read(_event)
          else
            _readable = false
          end
          return
        | _next_size =>
          // Increase the read buffer size.
          _next_size = _max_size.min(_next_size * 2)
        end

        _read_len = _read_len + len

        if _read_len >= _expect then
          let data = _read_buf = recover Array[U8] end
          data.truncate(_read_len)
          _read_len = 0

          received_called = received_called + 1
          if not _notify.received(this, consume data,
            received_called)
          then
            _read_buf_size()
            _read_again()
            return
          else
            _read_buf_size()
          end

          sum = sum + len

          if sum >= _max_size then
            // If we've read _max_size, yield and read again later.
            _read_again()
            return
          end
        end
      end
    else
      // The socket has been closed from the other side.
      _shutdown_peer = true
      close()
    end

  be _read_again() =>
    """
    Resume reading.
    """
    _pending_reads()

  fun ref _pending_writes(): Bool =>
    """
    Send pending data. If any data can't be sent, keep it and mark as not
    writeable. On an error, dispose of the connection. Returns whether
    it sent all pending data or not.
    """
    // TODO: Make writev_batch_size user configurable
    let writev_batch_size: USize = @pony_os_writev_max[I32]().usize()
    var num_to_send: USize = 0
    var bytes_to_send: USize = 0
    var bytes_sent: USize = 0
    while _writeable and (_pending_writev_total > 0) do
      try
        //determine number of bytes and buffers to send
        if (_pending_writev.size()/2) < writev_batch_size then
          num_to_send = _pending_writev.size()/2
          bytes_to_send = _pending_writev_total
        else
          //have more buffers than a single writev can handle
          //iterate over buffers being sent to add up total
          num_to_send = writev_batch_size
          bytes_to_send = 0
          for d in Range[USize](1, num_to_send*2, 2) do
            bytes_to_send = bytes_to_send + _pending_writev(d)
          end
        end

        // Write as much data as possible.
        var len = @pony_os_writev[USize](_event,
          _pending_writev.cpointer(), num_to_send) ?

        // keep track of how many bytes we sent
        bytes_sent = bytes_sent + len

        if len < bytes_to_send then
          while len > 0 do
            let iov_p = _pending_writev(0)
            let iov_s = _pending_writev(1)
            if iov_s <= len then
              len = len - iov_s
              _pending_writev.shift()
              _pending_writev.shift()
              _pending.shift()
              _pending_writev_total = _pending_writev_total - iov_s
            else
              _pending_writev.update(0, iov_p+len)
              _pending_writev.update(1, iov_s-len)
              _pending_writev_total = _pending_writev_total - len
              len = 0
            end
          end
          _apply_backpressure()
        else
          // sent all data we requested in this batch
          _pending_writev_total = _pending_writev_total - bytes_to_send
          if _pending_writev_total == 0 then
            _pending_writev.clear()
            _pending.clear()

            return true
          else
            for d in Range[USize](0, num_to_send, 1) do
              _pending_writev.shift()
              _pending_writev.shift()
              _pending.shift()
            end

          end
        end
      else
        // Non-graceful shutdown on error.
        _hard_close()
      end
    end

    false

  fun ref _read_buf_size() =>
    """
    Resize the read buffer.
    """
    if _expect != 0 then
      _read_buf.undefined(_expect)
    else
      _read_buf.undefined(_next_size)
    end

  fun ref expect(qty: USize = 0) =>
    """
    A `received` call on the notifier must contain exactly `qty` bytes. If
    `qty` is zero, the call can contain any amount of data. This has no effect
    if called in the `sent` notifier callback.
    """
    if not _in_sent then
      _expect = _notify.expect(this, qty)
      _read_buf_size()
    end

  fun local_address(): IPAddress =>
    """
    Return the local IP address.
    """
    let ip = recover IPAddress end
    @pony_os_sockname[Bool](_fd, ip)
    ip

  fun ref set_nodelay(state: Bool) =>
    """
    Turn Nagle on/off. Defaults to on. This can only be set on a connected
    socket.
    """
    if _connected then
      @pony_os_nodelay[None](_fd, state)
    end

  fun ref _add_to_upstream_backup(msg: Array[ByteSeq] val) =>
    _queue.push(msg)
    _maybe_mute_or_unmute_upstreams()

  fun ref _apply_backpressure() =>
    _writeable = false
    ifdef linux then
      // this is safe because asio thread isn't currently subscribed
      // for a write event so will not be writing to the readable flag
      AsioEvent.set_writeable(_event, false)
      @pony_asio_event_resubscribe_write(_event)
    end
    _notify.throttled(this)
    _maybe_mute_or_unmute_upstreams()

  fun ref _release_backpressure() =>
    _notify.unthrottled(this)
    _maybe_distribute_credits()
    _maybe_mute_or_unmute_upstreams()

  fun ref _maybe_mute_or_unmute_upstreams() =>
    if _mute_outstanding then
      if _can_send() then
        _unmute_upstreams()
      end
    else
      if not _can_send() then
        _mute_upstreams()
      end
    end

  fun ref _mute_upstreams() =>
    for u in _upstreams.values() do
      u.mute(this)
    end
    _mute_outstanding = true

  fun ref _unmute_upstreams() =>
    for u in _upstreams.values() do
      u.unmute(this)
    end
    _mute_outstanding = false

  fun _can_send(): Bool =>
    _connected and
      _writeable and
      not _closed and
      not _backup_queue_is_overflowing()

  fun _backup_queue_is_overflowing(): Bool =>
    _queue.size() >= 16_384

interface _OutgoingBoundaryNotify
  fun ref connecting(conn: OutgoingBoundary ref, count: U32) =>
    """
    Called if name resolution succeeded for a TCPConnection and we are now
    waiting for a connection to the server to succeed. The count is the number
    of connections we're trying. The notifier will be informed each time the
    count changes, until a connection is made or connect_failed() is called.
    """
    None

  fun ref connected(conn: OutgoingBoundary ref) =>
    """
    Called when we have successfully connected to the server.
    """
    None

  fun ref connect_failed(conn: OutgoingBoundary ref) =>
    """
    Called when we have failed to connect to all possible addresses for the
    server. At this point, the connection will never be established.
    """
    Fail()

  fun ref sentv(conn: OutgoingBoundary ref, data: ByteSeqIter): ByteSeqIter =>
    """
    Called when multiple chunks of data are sent to the connection in a single
    call. This gives the notifier an opportunity to modify the sent data chunks
    before they are written. To swallow the send, return an empty
    Array[String].
    """
    data

  fun ref received(conn: OutgoingBoundary ref, data: Array[U8] iso,
    times: USize): Bool
  =>
    """
    Called when new data is received on the connection. Return true if you
    want to continue receiving messages without yielding until you read
    max_size on the TCPConnection.  Return false to cause the TCPConnection
    to yield now.

    `times` parameter is the number of times this method has been called during
    this behavior. Starts at 1.
    """
    true

  fun ref expect(conn: OutgoingBoundary ref, qty: USize): USize =>
    """
    Called when the connection has been told to expect a certain quantity of
    bytes. This allows nested notifiers to change the expected quantity, which
    allows a lower level protocol to handle any framing (e.g. SSL).
    """
    qty

  fun ref closed(conn: OutgoingBoundary ref) =>
    """
    Called when the connection is closed.
    """
    None

  fun ref throttled(conn: OutgoingBoundary ref) =>
    """
    Called when the connection starts experiencing TCP backpressure. You should
    respond to this by pausing additional calls to `write` and `writev` until
    you are informed that pressure has been released. Failure to respond to
    the `throttled` notification will result in outgoing data queuing in the
    connection and increasing memory usage.
    """
    None

  fun ref unthrottled(conn: OutgoingBoundary ref) =>
    """
    Called when the connection stops experiencing TCP backpressure. Upon
    receiving this notification, you should feel free to start making calls to
    `write` and `writev` again.
    """
    None

class BoundaryNotify is _OutgoingBoundaryNotify
  let _auth: AmbientAuth
  var _header: Bool = true

  new create(auth: AmbientAuth) =>
    _auth = auth

  fun ref received(conn: OutgoingBoundary ref, data: Array[U8] iso,
    times: USize): Bool
  =>
    if _header then
      try
        let expect = Bytes.to_u32(data(0), data(1), data(2), data(3)).usize()

        conn.expect(expect)
        _header = false
      end
      true
    else
      ifdef "trace" then
        @printf[I32]("Rcvd msg at OutgoingBoundary\n".cstring())
      end
      match ChannelMsgDecoder(consume data, _auth)
      | let aw: AckWatermarkMsg val =>
        conn.ack(aw.seq_id)
      else
        @printf[I32]("Unknown Wallaroo data message type received at OutgoingBoundary.\n".cstring()
)      end

      conn.expect(4)
      _header = true

      ifdef linux then
        true
      else
        false
      end
    end

  fun ref connecting(conn: OutgoingBoundary ref, count: U32) =>
    """
    Called if name resolution succeeded for a TCPConnection and we are now
    waiting for a connection to the server to succeed. The count is the number
    of connections we're trying. The notifier will be informed each time the
    count changes, until a connection is made or connect_failed() is called.
    """
    @printf[I32]("BoundaryNotify: connecting\n\n".cstring())

  fun ref connected(conn: OutgoingBoundary ref) =>
    """
    Called when we have successfully connected to the server.
    """
    @printf[I32]("BoundaryNotify: connected\n\n".cstring())
    conn.set_nodelay(true)
    conn.expect(4)

  fun ref closed(conn: OutgoingBoundary ref) =>
    @printf[I32]("BoundaryNotify: closed\n\n".cstring())

  fun ref connect_failed(conn: OutgoingBoundary ref) =>
    """
    Called when we have failed to connect to all possible addresses for the
    server. At this point, the connection will never be established.
    """
    @printf[I32]("BoundaryNotify: connect_failed\n\n".cstring())

   fun ref throttled(conn: OutgoingBoundary ref) =>
    """
    Called when the connection starts experiencing TCP backpressure. You should
    respond to this by pausing additional calls to `write` and `writev` until
    you are informed that pressure has been released. Failure to respond to
    the `throttled` notification will result in outgoing data queuing in the
    connection and increasing memory usage.
    """
    @printf[I32]("BoundaryNotify: throttled\n\n".cstring())

  fun ref unthrottled(conn: OutgoingBoundary ref) =>
    """
    Called when the connection stops experiencing TCP backpressure. Upon
    receiving this notification, you should feel free to start making calls to
    `write` and `writev` again.
    """
    @printf[I32]("BoundaryNotify: unthrottled\n\n".cstring())
