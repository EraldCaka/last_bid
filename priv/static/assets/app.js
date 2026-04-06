(() => {
  // ../deps/phoenix/priv/static/phoenix.mjs
  var closure = (value) => {
    if (typeof value === "function") {
      return value;
    } else {
      let closure2 = function() {
        return value;
      };
      return closure2;
    }
  };
  var globalSelf = typeof self !== "undefined" ? self : null;
  var phxWindow = typeof window !== "undefined" ? window : null;
  var global = globalSelf || phxWindow || global;
  var DEFAULT_VSN = "2.0.0";
  var SOCKET_STATES = { connecting: 0, open: 1, closing: 2, closed: 3 };
  var DEFAULT_TIMEOUT = 1e4;
  var WS_CLOSE_NORMAL = 1e3;
  var CHANNEL_STATES = {
    closed: "closed",
    errored: "errored",
    joined: "joined",
    joining: "joining",
    leaving: "leaving"
  };
  var CHANNEL_EVENTS = {
    close: "phx_close",
    error: "phx_error",
    join: "phx_join",
    reply: "phx_reply",
    leave: "phx_leave"
  };
  var TRANSPORTS = {
    longpoll: "longpoll",
    websocket: "websocket"
  };
  var XHR_STATES = {
    complete: 4
  };
  var Push = class {
    constructor(channel, event, payload, timeout) {
      this.channel = channel;
      this.event = event;
      this.payload = payload || function() {
        return {};
      };
      this.receivedResp = null;
      this.timeout = timeout;
      this.timeoutTimer = null;
      this.recHooks = [];
      this.sent = false;
    }
    /**
     *
     * @param {number} timeout
     */
    resend(timeout) {
      this.timeout = timeout;
      this.reset();
      this.send();
    }
    /**
     *
     */
    send() {
      if (this.hasReceived("timeout")) {
        return;
      }
      this.startTimeout();
      this.sent = true;
      this.channel.socket.push({
        topic: this.channel.topic,
        event: this.event,
        payload: this.payload(),
        ref: this.ref,
        join_ref: this.channel.joinRef()
      });
    }
    /**
     *
     * @param {*} status
     * @param {*} callback
     */
    receive(status, callback) {
      if (this.hasReceived(status)) {
        callback(this.receivedResp.response);
      }
      this.recHooks.push({ status, callback });
      return this;
    }
    /**
     * @private
     */
    reset() {
      this.cancelRefEvent();
      this.ref = null;
      this.refEvent = null;
      this.receivedResp = null;
      this.sent = false;
    }
    /**
     * @private
     */
    matchReceive({ status, response, _ref }) {
      this.recHooks.filter((h) => h.status === status).forEach((h) => h.callback(response));
    }
    /**
     * @private
     */
    cancelRefEvent() {
      if (!this.refEvent) {
        return;
      }
      this.channel.off(this.refEvent);
    }
    /**
     * @private
     */
    cancelTimeout() {
      clearTimeout(this.timeoutTimer);
      this.timeoutTimer = null;
    }
    /**
     * @private
     */
    startTimeout() {
      if (this.timeoutTimer) {
        this.cancelTimeout();
      }
      this.ref = this.channel.socket.makeRef();
      this.refEvent = this.channel.replyEventName(this.ref);
      this.channel.on(this.refEvent, (payload) => {
        this.cancelRefEvent();
        this.cancelTimeout();
        this.receivedResp = payload;
        this.matchReceive(payload);
      });
      this.timeoutTimer = setTimeout(() => {
        this.trigger("timeout", {});
      }, this.timeout);
    }
    /**
     * @private
     */
    hasReceived(status) {
      return this.receivedResp && this.receivedResp.status === status;
    }
    /**
     * @private
     */
    trigger(status, response) {
      this.channel.trigger(this.refEvent, { status, response });
    }
  };
  var Timer = class {
    constructor(callback, timerCalc) {
      this.callback = callback;
      this.timerCalc = timerCalc;
      this.timer = null;
      this.tries = 0;
    }
    reset() {
      this.tries = 0;
      clearTimeout(this.timer);
    }
    /**
     * Cancels any previous scheduleTimeout and schedules callback
     */
    scheduleTimeout() {
      clearTimeout(this.timer);
      this.timer = setTimeout(() => {
        this.tries = this.tries + 1;
        this.callback();
      }, this.timerCalc(this.tries + 1));
    }
  };
  var Channel = class {
    constructor(topic, params, socket) {
      this.state = CHANNEL_STATES.closed;
      this.topic = topic;
      this.params = closure(params || {});
      this.socket = socket;
      this.bindings = [];
      this.bindingRef = 0;
      this.timeout = this.socket.timeout;
      this.joinedOnce = false;
      this.joinPush = new Push(this, CHANNEL_EVENTS.join, this.params, this.timeout);
      this.pushBuffer = [];
      this.stateChangeRefs = [];
      this.rejoinTimer = new Timer(() => {
        if (this.socket.isConnected()) {
          this.rejoin();
        }
      }, this.socket.rejoinAfterMs);
      this.stateChangeRefs.push(this.socket.onError(() => this.rejoinTimer.reset()));
      this.stateChangeRefs.push(
        this.socket.onOpen(() => {
          this.rejoinTimer.reset();
          if (this.isErrored()) {
            this.rejoin();
          }
        })
      );
      this.joinPush.receive("ok", () => {
        this.state = CHANNEL_STATES.joined;
        this.rejoinTimer.reset();
        this.pushBuffer.forEach((pushEvent) => pushEvent.send());
        this.pushBuffer = [];
      });
      this.joinPush.receive("error", () => {
        this.state = CHANNEL_STATES.errored;
        if (this.socket.isConnected()) {
          this.rejoinTimer.scheduleTimeout();
        }
      });
      this.onClose(() => {
        this.rejoinTimer.reset();
        if (this.socket.hasLogger())
          this.socket.log("channel", `close ${this.topic} ${this.joinRef()}`);
        this.state = CHANNEL_STATES.closed;
        this.socket.remove(this);
      });
      this.onError((reason) => {
        if (this.socket.hasLogger())
          this.socket.log("channel", `error ${this.topic}`, reason);
        if (this.isJoining()) {
          this.joinPush.reset();
        }
        this.state = CHANNEL_STATES.errored;
        if (this.socket.isConnected()) {
          this.rejoinTimer.scheduleTimeout();
        }
      });
      this.joinPush.receive("timeout", () => {
        if (this.socket.hasLogger())
          this.socket.log("channel", `timeout ${this.topic} (${this.joinRef()})`, this.joinPush.timeout);
        let leavePush = new Push(this, CHANNEL_EVENTS.leave, closure({}), this.timeout);
        leavePush.send();
        this.state = CHANNEL_STATES.errored;
        this.joinPush.reset();
        if (this.socket.isConnected()) {
          this.rejoinTimer.scheduleTimeout();
        }
      });
      this.on(CHANNEL_EVENTS.reply, (payload, ref) => {
        this.trigger(this.replyEventName(ref), payload);
      });
    }
    /**
     * Join the channel
     * @param {integer} timeout
     * @returns {Push}
     */
    join(timeout = this.timeout) {
      if (this.joinedOnce) {
        throw new Error("tried to join multiple times. 'join' can only be called a single time per channel instance");
      } else {
        this.timeout = timeout;
        this.joinedOnce = true;
        this.rejoin();
        return this.joinPush;
      }
    }
    /**
     * Hook into channel close
     * @param {Function} callback
     */
    onClose(callback) {
      this.on(CHANNEL_EVENTS.close, callback);
    }
    /**
     * Hook into channel errors
     * @param {Function} callback
     */
    onError(callback) {
      return this.on(CHANNEL_EVENTS.error, (reason) => callback(reason));
    }
    /**
     * Subscribes on channel events
     *
     * Subscription returns a ref counter, which can be used later to
     * unsubscribe the exact event listener
     *
     * @example
     * const ref1 = channel.on("event", do_stuff)
     * const ref2 = channel.on("event", do_other_stuff)
     * channel.off("event", ref1)
     * // Since unsubscription, do_stuff won't fire,
     * // while do_other_stuff will keep firing on the "event"
     *
     * @param {string} event
     * @param {Function} callback
     * @returns {integer} ref
     */
    on(event, callback) {
      let ref = this.bindingRef++;
      this.bindings.push({ event, ref, callback });
      return ref;
    }
    /**
     * Unsubscribes off of channel events
     *
     * Use the ref returned from a channel.on() to unsubscribe one
     * handler, or pass nothing for the ref to unsubscribe all
     * handlers for the given event.
     *
     * @example
     * // Unsubscribe the do_stuff handler
     * const ref1 = channel.on("event", do_stuff)
     * channel.off("event", ref1)
     *
     * // Unsubscribe all handlers from event
     * channel.off("event")
     *
     * @param {string} event
     * @param {integer} ref
     */
    off(event, ref) {
      this.bindings = this.bindings.filter((bind) => {
        return !(bind.event === event && (typeof ref === "undefined" || ref === bind.ref));
      });
    }
    /**
     * @private
     */
    canPush() {
      return this.socket.isConnected() && this.isJoined();
    }
    /**
     * Sends a message `event` to phoenix with the payload `payload`.
     * Phoenix receives this in the `handle_in(event, payload, socket)`
     * function. if phoenix replies or it times out (default 10000ms),
     * then optionally the reply can be received.
     *
     * @example
     * channel.push("event")
     *   .receive("ok", payload => console.log("phoenix replied:", payload))
     *   .receive("error", err => console.log("phoenix errored", err))
     *   .receive("timeout", () => console.log("timed out pushing"))
     * @param {string} event
     * @param {Object} payload
     * @param {number} [timeout]
     * @returns {Push}
     */
    push(event, payload, timeout = this.timeout) {
      payload = payload || {};
      if (!this.joinedOnce) {
        throw new Error(`tried to push '${event}' to '${this.topic}' before joining. Use channel.join() before pushing events`);
      }
      let pushEvent = new Push(this, event, function() {
        return payload;
      }, timeout);
      if (this.canPush()) {
        pushEvent.send();
      } else {
        pushEvent.startTimeout();
        this.pushBuffer.push(pushEvent);
      }
      return pushEvent;
    }
    /** Leaves the channel
     *
     * Unsubscribes from server events, and
     * instructs channel to terminate on server
     *
     * Triggers onClose() hooks
     *
     * To receive leave acknowledgements, use the `receive`
     * hook to bind to the server ack, ie:
     *
     * @example
     * channel.leave().receive("ok", () => alert("left!") )
     *
     * @param {integer} timeout
     * @returns {Push}
     */
    leave(timeout = this.timeout) {
      this.rejoinTimer.reset();
      this.joinPush.cancelTimeout();
      this.state = CHANNEL_STATES.leaving;
      let onClose = () => {
        if (this.socket.hasLogger())
          this.socket.log("channel", `leave ${this.topic}`);
        this.trigger(CHANNEL_EVENTS.close, "leave");
      };
      let leavePush = new Push(this, CHANNEL_EVENTS.leave, closure({}), timeout);
      leavePush.receive("ok", () => onClose()).receive("timeout", () => onClose());
      leavePush.send();
      if (!this.canPush()) {
        leavePush.trigger("ok", {});
      }
      return leavePush;
    }
    /**
     * Overridable message hook
     *
     * Receives all events for specialized message handling
     * before dispatching to the channel callbacks.
     *
     * Must return the payload, modified or unmodified
     * @param {string} event
     * @param {Object} payload
     * @param {integer} ref
     * @returns {Object}
     */
    onMessage(_event, payload, _ref) {
      return payload;
    }
    /**
     * @private
     */
    isMember(topic, event, payload, joinRef) {
      if (this.topic !== topic) {
        return false;
      }
      if (joinRef && joinRef !== this.joinRef()) {
        if (this.socket.hasLogger())
          this.socket.log("channel", "dropping outdated message", { topic, event, payload, joinRef });
        return false;
      } else {
        return true;
      }
    }
    /**
     * @private
     */
    joinRef() {
      return this.joinPush.ref;
    }
    /**
     * @private
     */
    rejoin(timeout = this.timeout) {
      if (this.isLeaving()) {
        return;
      }
      this.socket.leaveOpenTopic(this.topic);
      this.state = CHANNEL_STATES.joining;
      this.joinPush.resend(timeout);
    }
    /**
     * @private
     */
    trigger(event, payload, ref, joinRef) {
      let handledPayload = this.onMessage(event, payload, ref, joinRef);
      if (payload && !handledPayload) {
        throw new Error("channel onMessage callbacks must return the payload, modified or unmodified");
      }
      let eventBindings = this.bindings.filter((bind) => bind.event === event);
      for (let i = 0; i < eventBindings.length; i++) {
        let bind = eventBindings[i];
        bind.callback(handledPayload, ref, joinRef || this.joinRef());
      }
    }
    /**
     * @private
     */
    replyEventName(ref) {
      return `chan_reply_${ref}`;
    }
    /**
     * @private
     */
    isClosed() {
      return this.state === CHANNEL_STATES.closed;
    }
    /**
     * @private
     */
    isErrored() {
      return this.state === CHANNEL_STATES.errored;
    }
    /**
     * @private
     */
    isJoined() {
      return this.state === CHANNEL_STATES.joined;
    }
    /**
     * @private
     */
    isJoining() {
      return this.state === CHANNEL_STATES.joining;
    }
    /**
     * @private
     */
    isLeaving() {
      return this.state === CHANNEL_STATES.leaving;
    }
  };
  var Ajax = class {
    static request(method, endPoint, accept, body, timeout, ontimeout, callback) {
      if (global.XDomainRequest) {
        let req = new global.XDomainRequest();
        return this.xdomainRequest(req, method, endPoint, body, timeout, ontimeout, callback);
      } else {
        let req = new global.XMLHttpRequest();
        return this.xhrRequest(req, method, endPoint, accept, body, timeout, ontimeout, callback);
      }
    }
    static xdomainRequest(req, method, endPoint, body, timeout, ontimeout, callback) {
      req.timeout = timeout;
      req.open(method, endPoint);
      req.onload = () => {
        let response = this.parseJSON(req.responseText);
        callback && callback(response);
      };
      if (ontimeout) {
        req.ontimeout = ontimeout;
      }
      req.onprogress = () => {
      };
      req.send(body);
      return req;
    }
    static xhrRequest(req, method, endPoint, accept, body, timeout, ontimeout, callback) {
      req.open(method, endPoint, true);
      req.timeout = timeout;
      req.setRequestHeader("Content-Type", accept);
      req.onerror = () => callback && callback(null);
      req.onreadystatechange = () => {
        if (req.readyState === XHR_STATES.complete && callback) {
          let response = this.parseJSON(req.responseText);
          callback(response);
        }
      };
      if (ontimeout) {
        req.ontimeout = ontimeout;
      }
      req.send(body);
      return req;
    }
    static parseJSON(resp) {
      if (!resp || resp === "") {
        return null;
      }
      try {
        return JSON.parse(resp);
      } catch (e) {
        console && console.log("failed to parse JSON response", resp);
        return null;
      }
    }
    static serialize(obj, parentKey) {
      let queryStr = [];
      for (var key in obj) {
        if (!Object.prototype.hasOwnProperty.call(obj, key)) {
          continue;
        }
        let paramKey = parentKey ? `${parentKey}[${key}]` : key;
        let paramVal = obj[key];
        if (typeof paramVal === "object") {
          queryStr.push(this.serialize(paramVal, paramKey));
        } else {
          queryStr.push(encodeURIComponent(paramKey) + "=" + encodeURIComponent(paramVal));
        }
      }
      return queryStr.join("&");
    }
    static appendParams(url, params) {
      if (Object.keys(params).length === 0) {
        return url;
      }
      let prefix = url.match(/\?/) ? "&" : "?";
      return `${url}${prefix}${this.serialize(params)}`;
    }
  };
  var arrayBufferToBase64 = (buffer) => {
    let binary = "";
    let bytes = new Uint8Array(buffer);
    let len = bytes.byteLength;
    for (let i = 0; i < len; i++) {
      binary += String.fromCharCode(bytes[i]);
    }
    return btoa(binary);
  };
  var LongPoll = class {
    constructor(endPoint) {
      this.endPoint = null;
      this.token = null;
      this.skipHeartbeat = true;
      this.reqs = /* @__PURE__ */ new Set();
      this.awaitingBatchAck = false;
      this.currentBatch = null;
      this.currentBatchTimer = null;
      this.batchBuffer = [];
      this.onopen = function() {
      };
      this.onerror = function() {
      };
      this.onmessage = function() {
      };
      this.onclose = function() {
      };
      this.pollEndpoint = this.normalizeEndpoint(endPoint);
      this.readyState = SOCKET_STATES.connecting;
      setTimeout(() => this.poll(), 0);
    }
    normalizeEndpoint(endPoint) {
      return endPoint.replace("ws://", "http://").replace("wss://", "https://").replace(new RegExp("(.*)/" + TRANSPORTS.websocket), "$1/" + TRANSPORTS.longpoll);
    }
    endpointURL() {
      return Ajax.appendParams(this.pollEndpoint, { token: this.token });
    }
    closeAndRetry(code, reason, wasClean) {
      this.close(code, reason, wasClean);
      this.readyState = SOCKET_STATES.connecting;
    }
    ontimeout() {
      this.onerror("timeout");
      this.closeAndRetry(1005, "timeout", false);
    }
    isActive() {
      return this.readyState === SOCKET_STATES.open || this.readyState === SOCKET_STATES.connecting;
    }
    poll() {
      this.ajax("GET", "application/json", null, () => this.ontimeout(), (resp) => {
        if (resp) {
          var { status, token, messages } = resp;
          this.token = token;
        } else {
          status = 0;
        }
        switch (status) {
          case 200:
            messages.forEach((msg) => {
              setTimeout(() => this.onmessage({ data: msg }), 0);
            });
            this.poll();
            break;
          case 204:
            this.poll();
            break;
          case 410:
            this.readyState = SOCKET_STATES.open;
            this.onopen({});
            this.poll();
            break;
          case 403:
            this.onerror(403);
            this.close(1008, "forbidden", false);
            break;
          case 0:
          case 500:
            this.onerror(500);
            this.closeAndRetry(1011, "internal server error", 500);
            break;
          default:
            throw new Error(`unhandled poll status ${status}`);
        }
      });
    }
    // we collect all pushes within the current event loop by
    // setTimeout 0, which optimizes back-to-back procedural
    // pushes against an empty buffer
    send(body) {
      if (typeof body !== "string") {
        body = arrayBufferToBase64(body);
      }
      if (this.currentBatch) {
        this.currentBatch.push(body);
      } else if (this.awaitingBatchAck) {
        this.batchBuffer.push(body);
      } else {
        this.currentBatch = [body];
        this.currentBatchTimer = setTimeout(() => {
          this.batchSend(this.currentBatch);
          this.currentBatch = null;
        }, 0);
      }
    }
    batchSend(messages) {
      this.awaitingBatchAck = true;
      this.ajax("POST", "application/x-ndjson", messages.join("\n"), () => this.onerror("timeout"), (resp) => {
        this.awaitingBatchAck = false;
        if (!resp || resp.status !== 200) {
          this.onerror(resp && resp.status);
          this.closeAndRetry(1011, "internal server error", false);
        } else if (this.batchBuffer.length > 0) {
          this.batchSend(this.batchBuffer);
          this.batchBuffer = [];
        }
      });
    }
    close(code, reason, wasClean) {
      for (let req of this.reqs) {
        req.abort();
      }
      this.readyState = SOCKET_STATES.closed;
      let opts = Object.assign({ code: 1e3, reason: void 0, wasClean: true }, { code, reason, wasClean });
      this.batchBuffer = [];
      clearTimeout(this.currentBatchTimer);
      this.currentBatchTimer = null;
      if (typeof CloseEvent !== "undefined") {
        this.onclose(new CloseEvent("close", opts));
      } else {
        this.onclose(opts);
      }
    }
    ajax(method, contentType, body, onCallerTimeout, callback) {
      let req;
      let ontimeout = () => {
        this.reqs.delete(req);
        onCallerTimeout();
      };
      req = Ajax.request(method, this.endpointURL(), contentType, body, this.timeout, ontimeout, (resp) => {
        this.reqs.delete(req);
        if (this.isActive()) {
          callback(resp);
        }
      });
      this.reqs.add(req);
    }
  };
  var serializer_default = {
    HEADER_LENGTH: 1,
    META_LENGTH: 4,
    KINDS: { push: 0, reply: 1, broadcast: 2 },
    encode(msg, callback) {
      if (msg.payload.constructor === ArrayBuffer) {
        return callback(this.binaryEncode(msg));
      } else {
        let payload = [msg.join_ref, msg.ref, msg.topic, msg.event, msg.payload];
        return callback(JSON.stringify(payload));
      }
    },
    decode(rawPayload, callback) {
      if (rawPayload.constructor === ArrayBuffer) {
        return callback(this.binaryDecode(rawPayload));
      } else {
        let [join_ref, ref, topic, event, payload] = JSON.parse(rawPayload);
        return callback({ join_ref, ref, topic, event, payload });
      }
    },
    // private
    binaryEncode(message) {
      let { join_ref, ref, event, topic, payload } = message;
      let metaLength = this.META_LENGTH + join_ref.length + ref.length + topic.length + event.length;
      let header = new ArrayBuffer(this.HEADER_LENGTH + metaLength);
      let view = new DataView(header);
      let offset = 0;
      view.setUint8(offset++, this.KINDS.push);
      view.setUint8(offset++, join_ref.length);
      view.setUint8(offset++, ref.length);
      view.setUint8(offset++, topic.length);
      view.setUint8(offset++, event.length);
      Array.from(join_ref, (char) => view.setUint8(offset++, char.charCodeAt(0)));
      Array.from(ref, (char) => view.setUint8(offset++, char.charCodeAt(0)));
      Array.from(topic, (char) => view.setUint8(offset++, char.charCodeAt(0)));
      Array.from(event, (char) => view.setUint8(offset++, char.charCodeAt(0)));
      var combined = new Uint8Array(header.byteLength + payload.byteLength);
      combined.set(new Uint8Array(header), 0);
      combined.set(new Uint8Array(payload), header.byteLength);
      return combined.buffer;
    },
    binaryDecode(buffer) {
      let view = new DataView(buffer);
      let kind = view.getUint8(0);
      let decoder = new TextDecoder();
      switch (kind) {
        case this.KINDS.push:
          return this.decodePush(buffer, view, decoder);
        case this.KINDS.reply:
          return this.decodeReply(buffer, view, decoder);
        case this.KINDS.broadcast:
          return this.decodeBroadcast(buffer, view, decoder);
      }
    },
    decodePush(buffer, view, decoder) {
      let joinRefSize = view.getUint8(1);
      let topicSize = view.getUint8(2);
      let eventSize = view.getUint8(3);
      let offset = this.HEADER_LENGTH + this.META_LENGTH - 1;
      let joinRef = decoder.decode(buffer.slice(offset, offset + joinRefSize));
      offset = offset + joinRefSize;
      let topic = decoder.decode(buffer.slice(offset, offset + topicSize));
      offset = offset + topicSize;
      let event = decoder.decode(buffer.slice(offset, offset + eventSize));
      offset = offset + eventSize;
      let data = buffer.slice(offset, buffer.byteLength);
      return { join_ref: joinRef, ref: null, topic, event, payload: data };
    },
    decodeReply(buffer, view, decoder) {
      let joinRefSize = view.getUint8(1);
      let refSize = view.getUint8(2);
      let topicSize = view.getUint8(3);
      let eventSize = view.getUint8(4);
      let offset = this.HEADER_LENGTH + this.META_LENGTH;
      let joinRef = decoder.decode(buffer.slice(offset, offset + joinRefSize));
      offset = offset + joinRefSize;
      let ref = decoder.decode(buffer.slice(offset, offset + refSize));
      offset = offset + refSize;
      let topic = decoder.decode(buffer.slice(offset, offset + topicSize));
      offset = offset + topicSize;
      let event = decoder.decode(buffer.slice(offset, offset + eventSize));
      offset = offset + eventSize;
      let data = buffer.slice(offset, buffer.byteLength);
      let payload = { status: event, response: data };
      return { join_ref: joinRef, ref, topic, event: CHANNEL_EVENTS.reply, payload };
    },
    decodeBroadcast(buffer, view, decoder) {
      let topicSize = view.getUint8(1);
      let eventSize = view.getUint8(2);
      let offset = this.HEADER_LENGTH + 2;
      let topic = decoder.decode(buffer.slice(offset, offset + topicSize));
      offset = offset + topicSize;
      let event = decoder.decode(buffer.slice(offset, offset + eventSize));
      offset = offset + eventSize;
      let data = buffer.slice(offset, buffer.byteLength);
      return { join_ref: null, ref: null, topic, event, payload: data };
    }
  };
  var Socket = class {
    constructor(endPoint, opts = {}) {
      this.stateChangeCallbacks = { open: [], close: [], error: [], message: [] };
      this.channels = [];
      this.sendBuffer = [];
      this.ref = 0;
      this.timeout = opts.timeout || DEFAULT_TIMEOUT;
      this.transport = opts.transport || global.WebSocket || LongPoll;
      this.primaryPassedHealthCheck = false;
      this.longPollFallbackMs = opts.longPollFallbackMs;
      this.fallbackTimer = null;
      this.sessionStore = opts.sessionStorage || global && global.sessionStorage;
      this.establishedConnections = 0;
      this.defaultEncoder = serializer_default.encode.bind(serializer_default);
      this.defaultDecoder = serializer_default.decode.bind(serializer_default);
      this.closeWasClean = false;
      this.disconnecting = false;
      this.binaryType = opts.binaryType || "arraybuffer";
      this.connectClock = 1;
      if (this.transport !== LongPoll) {
        this.encode = opts.encode || this.defaultEncoder;
        this.decode = opts.decode || this.defaultDecoder;
      } else {
        this.encode = this.defaultEncoder;
        this.decode = this.defaultDecoder;
      }
      let awaitingConnectionOnPageShow = null;
      if (phxWindow && phxWindow.addEventListener) {
        phxWindow.addEventListener("pagehide", (_e) => {
          if (this.conn) {
            this.disconnect();
            awaitingConnectionOnPageShow = this.connectClock;
          }
        });
        phxWindow.addEventListener("pageshow", (_e) => {
          if (awaitingConnectionOnPageShow === this.connectClock) {
            awaitingConnectionOnPageShow = null;
            this.connect();
          }
        });
      }
      this.heartbeatIntervalMs = opts.heartbeatIntervalMs || 3e4;
      this.rejoinAfterMs = (tries) => {
        if (opts.rejoinAfterMs) {
          return opts.rejoinAfterMs(tries);
        } else {
          return [1e3, 2e3, 5e3][tries - 1] || 1e4;
        }
      };
      this.reconnectAfterMs = (tries) => {
        if (opts.reconnectAfterMs) {
          return opts.reconnectAfterMs(tries);
        } else {
          return [10, 50, 100, 150, 200, 250, 500, 1e3, 2e3][tries - 1] || 5e3;
        }
      };
      this.logger = opts.logger || null;
      if (!this.logger && opts.debug) {
        this.logger = (kind, msg, data) => {
          console.log(`${kind}: ${msg}`, data);
        };
      }
      this.longpollerTimeout = opts.longpollerTimeout || 2e4;
      this.params = closure(opts.params || {});
      this.endPoint = `${endPoint}/${TRANSPORTS.websocket}`;
      this.vsn = opts.vsn || DEFAULT_VSN;
      this.heartbeatTimeoutTimer = null;
      this.heartbeatTimer = null;
      this.pendingHeartbeatRef = null;
      this.reconnectTimer = new Timer(() => {
        this.teardown(() => this.connect());
      }, this.reconnectAfterMs);
    }
    /**
     * Returns the LongPoll transport reference
     */
    getLongPollTransport() {
      return LongPoll;
    }
    /**
     * Disconnects and replaces the active transport
     *
     * @param {Function} newTransport - The new transport class to instantiate
     *
     */
    replaceTransport(newTransport) {
      this.connectClock++;
      this.closeWasClean = true;
      clearTimeout(this.fallbackTimer);
      this.reconnectTimer.reset();
      if (this.conn) {
        this.conn.close();
        this.conn = null;
      }
      this.transport = newTransport;
    }
    /**
     * Returns the socket protocol
     *
     * @returns {string}
     */
    protocol() {
      return location.protocol.match(/^https/) ? "wss" : "ws";
    }
    /**
     * The fully qualified socket url
     *
     * @returns {string}
     */
    endPointURL() {
      let uri = Ajax.appendParams(
        Ajax.appendParams(this.endPoint, this.params()),
        { vsn: this.vsn }
      );
      if (uri.charAt(0) !== "/") {
        return uri;
      }
      if (uri.charAt(1) === "/") {
        return `${this.protocol()}:${uri}`;
      }
      return `${this.protocol()}://${location.host}${uri}`;
    }
    /**
     * Disconnects the socket
     *
     * See https://developer.mozilla.org/en-US/docs/Web/API/CloseEvent#Status_codes for valid status codes.
     *
     * @param {Function} callback - Optional callback which is called after socket is disconnected.
     * @param {integer} code - A status code for disconnection (Optional).
     * @param {string} reason - A textual description of the reason to disconnect. (Optional)
     */
    disconnect(callback, code, reason) {
      this.connectClock++;
      this.disconnecting = true;
      this.closeWasClean = true;
      clearTimeout(this.fallbackTimer);
      this.reconnectTimer.reset();
      this.teardown(() => {
        this.disconnecting = false;
        callback && callback();
      }, code, reason);
    }
    /**
     *
     * @param {Object} params - The params to send when connecting, for example `{user_id: userToken}`
     *
     * Passing params to connect is deprecated; pass them in the Socket constructor instead:
     * `new Socket("/socket", {params: {user_id: userToken}})`.
     */
    connect(params) {
      if (params) {
        console && console.log("passing params to connect is deprecated. Instead pass :params to the Socket constructor");
        this.params = closure(params);
      }
      if (this.conn && !this.disconnecting) {
        return;
      }
      if (this.longPollFallbackMs && this.transport !== LongPoll) {
        this.connectWithFallback(LongPoll, this.longPollFallbackMs);
      } else {
        this.transportConnect();
      }
    }
    /**
     * Logs the message. Override `this.logger` for specialized logging. noops by default
     * @param {string} kind
     * @param {string} msg
     * @param {Object} data
     */
    log(kind, msg, data) {
      this.logger && this.logger(kind, msg, data);
    }
    /**
     * Returns true if a logger has been set on this socket.
     */
    hasLogger() {
      return this.logger !== null;
    }
    /**
     * Registers callbacks for connection open events
     *
     * @example socket.onOpen(function(){ console.info("the socket was opened") })
     *
     * @param {Function} callback
     */
    onOpen(callback) {
      let ref = this.makeRef();
      this.stateChangeCallbacks.open.push([ref, callback]);
      return ref;
    }
    /**
     * Registers callbacks for connection close events
     * @param {Function} callback
     */
    onClose(callback) {
      let ref = this.makeRef();
      this.stateChangeCallbacks.close.push([ref, callback]);
      return ref;
    }
    /**
     * Registers callbacks for connection error events
     *
     * @example socket.onError(function(error){ alert("An error occurred") })
     *
     * @param {Function} callback
     */
    onError(callback) {
      let ref = this.makeRef();
      this.stateChangeCallbacks.error.push([ref, callback]);
      return ref;
    }
    /**
     * Registers callbacks for connection message events
     * @param {Function} callback
     */
    onMessage(callback) {
      let ref = this.makeRef();
      this.stateChangeCallbacks.message.push([ref, callback]);
      return ref;
    }
    /**
     * Pings the server and invokes the callback with the RTT in milliseconds
     * @param {Function} callback
     *
     * Returns true if the ping was pushed or false if unable to be pushed.
     */
    ping(callback) {
      if (!this.isConnected()) {
        return false;
      }
      let ref = this.makeRef();
      let startTime = Date.now();
      this.push({ topic: "phoenix", event: "heartbeat", payload: {}, ref });
      let onMsgRef = this.onMessage((msg) => {
        if (msg.ref === ref) {
          this.off([onMsgRef]);
          callback(Date.now() - startTime);
        }
      });
      return true;
    }
    /**
     * @private
     */
    transportConnect() {
      this.connectClock++;
      this.closeWasClean = false;
      this.conn = new this.transport(this.endPointURL());
      this.conn.binaryType = this.binaryType;
      this.conn.timeout = this.longpollerTimeout;
      this.conn.onopen = () => this.onConnOpen();
      this.conn.onerror = (error) => this.onConnError(error);
      this.conn.onmessage = (event) => this.onConnMessage(event);
      this.conn.onclose = (event) => this.onConnClose(event);
    }
    getSession(key) {
      return this.sessionStore && this.sessionStore.getItem(key);
    }
    storeSession(key, val) {
      this.sessionStore && this.sessionStore.setItem(key, val);
    }
    connectWithFallback(fallbackTransport, fallbackThreshold = 2500) {
      clearTimeout(this.fallbackTimer);
      let established = false;
      let primaryTransport = true;
      let openRef, errorRef;
      let fallback = (reason) => {
        this.log("transport", `falling back to ${fallbackTransport.name}...`, reason);
        this.off([openRef, errorRef]);
        primaryTransport = false;
        this.replaceTransport(fallbackTransport);
        this.transportConnect();
      };
      if (this.getSession(`phx:fallback:${fallbackTransport.name}`)) {
        return fallback("memorized");
      }
      this.fallbackTimer = setTimeout(fallback, fallbackThreshold);
      errorRef = this.onError((reason) => {
        this.log("transport", "error", reason);
        if (primaryTransport && !established) {
          clearTimeout(this.fallbackTimer);
          fallback(reason);
        }
      });
      this.onOpen(() => {
        established = true;
        if (!primaryTransport) {
          if (!this.primaryPassedHealthCheck) {
            this.storeSession(`phx:fallback:${fallbackTransport.name}`, "true");
          }
          return this.log("transport", `established ${fallbackTransport.name} fallback`);
        }
        clearTimeout(this.fallbackTimer);
        this.fallbackTimer = setTimeout(fallback, fallbackThreshold);
        this.ping((rtt) => {
          this.log("transport", "connected to primary after", rtt);
          this.primaryPassedHealthCheck = true;
          clearTimeout(this.fallbackTimer);
        });
      });
      this.transportConnect();
    }
    clearHeartbeats() {
      clearTimeout(this.heartbeatTimer);
      clearTimeout(this.heartbeatTimeoutTimer);
    }
    onConnOpen() {
      if (this.hasLogger())
        this.log("transport", `${this.transport.name} connected to ${this.endPointURL()}`);
      this.closeWasClean = false;
      this.disconnecting = false;
      this.establishedConnections++;
      this.flushSendBuffer();
      this.reconnectTimer.reset();
      this.resetHeartbeat();
      this.stateChangeCallbacks.open.forEach(([, callback]) => callback());
    }
    /**
     * @private
     */
    heartbeatTimeout() {
      if (this.pendingHeartbeatRef) {
        this.pendingHeartbeatRef = null;
        if (this.hasLogger()) {
          this.log("transport", "heartbeat timeout. Attempting to re-establish connection");
        }
        this.triggerChanError();
        this.closeWasClean = false;
        this.teardown(() => this.reconnectTimer.scheduleTimeout(), WS_CLOSE_NORMAL, "heartbeat timeout");
      }
    }
    resetHeartbeat() {
      if (this.conn && this.conn.skipHeartbeat) {
        return;
      }
      this.pendingHeartbeatRef = null;
      this.clearHeartbeats();
      this.heartbeatTimer = setTimeout(() => this.sendHeartbeat(), this.heartbeatIntervalMs);
    }
    teardown(callback, code, reason) {
      if (!this.conn) {
        return callback && callback();
      }
      let connectClock = this.connectClock;
      this.waitForBufferDone(() => {
        if (connectClock !== this.connectClock) {
          return;
        }
        if (this.conn) {
          if (code) {
            this.conn.close(code, reason || "");
          } else {
            this.conn.close();
          }
        }
        this.waitForSocketClosed(() => {
          if (connectClock !== this.connectClock) {
            return;
          }
          if (this.conn) {
            this.conn.onopen = function() {
            };
            this.conn.onerror = function() {
            };
            this.conn.onmessage = function() {
            };
            this.conn.onclose = function() {
            };
            this.conn = null;
          }
          callback && callback();
        });
      });
    }
    waitForBufferDone(callback, tries = 1) {
      if (tries === 5 || !this.conn || !this.conn.bufferedAmount) {
        callback();
        return;
      }
      setTimeout(() => {
        this.waitForBufferDone(callback, tries + 1);
      }, 150 * tries);
    }
    waitForSocketClosed(callback, tries = 1) {
      if (tries === 5 || !this.conn || this.conn.readyState === SOCKET_STATES.closed) {
        callback();
        return;
      }
      setTimeout(() => {
        this.waitForSocketClosed(callback, tries + 1);
      }, 150 * tries);
    }
    onConnClose(event) {
      let closeCode = event && event.code;
      if (this.hasLogger())
        this.log("transport", "close", event);
      this.triggerChanError();
      this.clearHeartbeats();
      if (!this.closeWasClean && closeCode !== 1e3) {
        this.reconnectTimer.scheduleTimeout();
      }
      this.stateChangeCallbacks.close.forEach(([, callback]) => callback(event));
    }
    /**
     * @private
     */
    onConnError(error) {
      if (this.hasLogger())
        this.log("transport", error);
      let transportBefore = this.transport;
      let establishedBefore = this.establishedConnections;
      this.stateChangeCallbacks.error.forEach(([, callback]) => {
        callback(error, transportBefore, establishedBefore);
      });
      if (transportBefore === this.transport || establishedBefore > 0) {
        this.triggerChanError();
      }
    }
    /**
     * @private
     */
    triggerChanError() {
      this.channels.forEach((channel) => {
        if (!(channel.isErrored() || channel.isLeaving() || channel.isClosed())) {
          channel.trigger(CHANNEL_EVENTS.error);
        }
      });
    }
    /**
     * @returns {string}
     */
    connectionState() {
      switch (this.conn && this.conn.readyState) {
        case SOCKET_STATES.connecting:
          return "connecting";
        case SOCKET_STATES.open:
          return "open";
        case SOCKET_STATES.closing:
          return "closing";
        default:
          return "closed";
      }
    }
    /**
     * @returns {boolean}
     */
    isConnected() {
      return this.connectionState() === "open";
    }
    /**
     * @private
     *
     * @param {Channel}
     */
    remove(channel) {
      this.off(channel.stateChangeRefs);
      this.channels = this.channels.filter((c) => c !== channel);
    }
    /**
     * Removes `onOpen`, `onClose`, `onError,` and `onMessage` registrations.
     *
     * @param {refs} - list of refs returned by calls to
     *                 `onOpen`, `onClose`, `onError,` and `onMessage`
     */
    off(refs) {
      for (let key in this.stateChangeCallbacks) {
        this.stateChangeCallbacks[key] = this.stateChangeCallbacks[key].filter(([ref]) => {
          return refs.indexOf(ref) === -1;
        });
      }
    }
    /**
     * Initiates a new channel for the given topic
     *
     * @param {string} topic
     * @param {Object} chanParams - Parameters for the channel
     * @returns {Channel}
     */
    channel(topic, chanParams = {}) {
      let chan = new Channel(topic, chanParams, this);
      this.channels.push(chan);
      return chan;
    }
    /**
     * @param {Object} data
     */
    push(data) {
      if (this.hasLogger()) {
        let { topic, event, payload, ref, join_ref } = data;
        this.log("push", `${topic} ${event} (${join_ref}, ${ref})`, payload);
      }
      if (this.isConnected()) {
        this.encode(data, (result) => this.conn.send(result));
      } else {
        this.sendBuffer.push(() => this.encode(data, (result) => this.conn.send(result)));
      }
    }
    /**
     * Return the next message ref, accounting for overflows
     * @returns {string}
     */
    makeRef() {
      let newRef = this.ref + 1;
      if (newRef === this.ref) {
        this.ref = 0;
      } else {
        this.ref = newRef;
      }
      return this.ref.toString();
    }
    sendHeartbeat() {
      if (this.pendingHeartbeatRef && !this.isConnected()) {
        return;
      }
      this.pendingHeartbeatRef = this.makeRef();
      this.push({ topic: "phoenix", event: "heartbeat", payload: {}, ref: this.pendingHeartbeatRef });
      this.heartbeatTimeoutTimer = setTimeout(() => this.heartbeatTimeout(), this.heartbeatIntervalMs);
    }
    flushSendBuffer() {
      if (this.isConnected() && this.sendBuffer.length > 0) {
        this.sendBuffer.forEach((callback) => callback());
        this.sendBuffer = [];
      }
    }
    onConnMessage(rawMessage) {
      this.decode(rawMessage.data, (msg) => {
        let { topic, event, payload, ref, join_ref } = msg;
        if (ref && ref === this.pendingHeartbeatRef) {
          this.clearHeartbeats();
          this.pendingHeartbeatRef = null;
          this.heartbeatTimer = setTimeout(() => this.sendHeartbeat(), this.heartbeatIntervalMs);
        }
        if (this.hasLogger())
          this.log("receive", `${payload.status || ""} ${topic} ${event} ${ref && "(" + ref + ")" || ""}`, payload);
        for (let i = 0; i < this.channels.length; i++) {
          const channel = this.channels[i];
          if (!channel.isMember(topic, event, payload, join_ref)) {
            continue;
          }
          channel.trigger(event, payload, ref, join_ref);
        }
        for (let i = 0; i < this.stateChangeCallbacks.message.length; i++) {
          let [, callback] = this.stateChangeCallbacks.message[i];
          callback(msg);
        }
      });
    }
    leaveOpenTopic(topic) {
      let dupChannel = this.channels.find((c) => c.topic === topic && (c.isJoined() || c.isJoining()));
      if (dupChannel) {
        if (this.hasLogger())
          this.log("transport", `leaving duplicate topic "${topic}"`);
        dupChannel.leave();
      }
    }
  };

  // js/app.js
  var config = window.DarkPool || {};
  function boot(cfg) {
    if (!cfg.socketToken) {
      console.debug("[DarkPool] No socket token found \u2014 WebSocket not started.");
      return;
    }
    const socket = new Socket("/socket", {
      params: { token: cfg.socketToken }
    });
    socket.connect();
    const lobbyMgr = new LobbyManager(socket, cfg);
    lobbyMgr.init();
    if (cfg.matchId) {
      const matchMgr = new MatchManager(socket, cfg, lobbyMgr);
      matchMgr.init();
    }
  }
  var Toast = {
    show(msg, type = "info", duration = 4e3) {
      const icons = {
        info: "\u2139",
        success: "\u2713",
        warning: "\u26A0",
        error: "\u2715"
      };
      let container = document.getElementById("toast-container");
      if (!container) {
        container = document.createElement("div");
        container.id = "toast-container";
        container.style.position = "fixed";
        container.style.top = "16px";
        container.style.right = "16px";
        container.style.zIndex = "9999";
        container.style.display = "flex";
        container.style.flexDirection = "column";
        container.style.gap = "8px";
        document.body.appendChild(container);
      }
      const el = document.createElement("div");
      el.className = `toast ${type}`;
      el.style.padding = "10px 12px";
      el.style.borderRadius = "10px";
      el.style.border = "1px solid rgba(255,255,255,.12)";
      el.style.background = "#111827";
      el.style.color = "#e5e7eb";
      el.style.fontSize = "13px";
      el.style.fontFamily = "ui-monospace, SFMono-Regular, Menlo, monospace";
      el.style.display = "flex";
      el.style.alignItems = "center";
      el.style.gap = "8px";
      el.style.cursor = "pointer";
      el.innerHTML = `
      <span style="flex-shrink:0">${icons[type] || "\u2022"}</span>
      <span>${escapeHtml(msg)}</span>
    `;
      container.appendChild(el);
      const timer = setTimeout(() => el.remove(), duration);
      el.addEventListener("click", () => {
        clearTimeout(timer);
        el.remove();
      });
    }
  };
  var SparklineChart = class {
    constructor(containerId, options = {}) {
      this.container = document.getElementById(containerId);
      this.history = [];
      this.maxPoints = options.maxPoints || 20;
      this.w = options.width || 80;
      this.h = options.height || 28;
      if (this.container)
        this._createSvg();
    }
    _createSvg() {
      this.svg = document.createElementNS("http://www.w3.org/2000/svg", "svg");
      this.svg.setAttribute("viewBox", `0 0 ${this.w} ${this.h}`);
      this.svg.setAttribute("width", this.w);
      this.svg.setAttribute("height", this.h);
      const gradId = `sg-${Math.random().toString(36).slice(2)}`;
      this.svg.innerHTML = `
      <defs>
        <linearGradient id="${gradId}" x1="0" y1="0" x2="0" y2="1">
          <stop offset="0%" stop-color="var(--spark-color,#10b981)" stop-opacity="0.35"/>
          <stop offset="100%" stop-color="var(--spark-color,#10b981)" stop-opacity="0"/>
        </linearGradient>
      </defs>
      <path class="area" fill="url(#${gradId})"></path>
      <polyline class="line" fill="none" stroke="var(--spark-color,#10b981)" stroke-width="2"></polyline>
    `;
      this.gradId = gradId;
      this.container.innerHTML = "";
      this.container.appendChild(this.svg);
    }
    reset(values = []) {
      this.history = [];
      values.forEach((v) => this.push(v));
    }
    push(price) {
      const p = parseFloat(price);
      if (Number.isNaN(p))
        return;
      this.history.push(p);
      if (this.history.length > this.maxPoints)
        this.history.shift();
      this._render();
    }
    setColor(isUp) {
      if (!this.svg)
        return;
      const c = isUp ? "#10b981" : "#ef4444";
      this.svg.style.setProperty("--spark-color", c);
      const firstStop = this.svg.querySelector("stop");
      if (firstStop)
        firstStop.setAttribute("stop-color", c);
    }
    _render() {
      if (!this.svg || this.history.length === 0)
        return;
      const min = Math.min(...this.history);
      const max = Math.max(...this.history);
      const range = max - min || 1;
      const pad = 2;
      const step = this.history.length > 1 ? (this.w - pad * 2) / (this.history.length - 1) : 0;
      const points = this.history.map((v, i) => {
        const x = pad + i * step;
        const y = pad + (1 - (v - min) / range) * (this.h - pad * 2);
        return `${x.toFixed(2)},${y.toFixed(2)}`;
      });
      const line = this.svg.querySelector(".line");
      const area = this.svg.querySelector(".area");
      if (line)
        line.setAttribute("points", points.join(" "));
      if (area && points.length > 0) {
        const first = points[0].split(",");
        const last = points[points.length - 1].split(",");
        area.setAttribute(
          "d",
          `M${first[0]},${this.h - pad} L${points.join(" L")} L${last[0]},${this.h - pad} Z`
        );
      }
    }
  };
  var DetailChart = class {
    constructor(canvasId) {
      var _a;
      this.canvas = document.getElementById(canvasId);
      this.ctx = (_a = this.canvas) == null ? void 0 : _a.getContext("2d");
    }
    render(points) {
      if (!this.canvas || !this.ctx)
        return;
      const rect = this.canvas.getBoundingClientRect();
      const w = Math.max(420, Math.floor(rect.width || 420));
      const h = Math.max(260, Math.floor(rect.height || 260));
      this.canvas.width = w;
      this.canvas.height = h;
      const ctx = this.ctx;
      ctx.clearRect(0, 0, w, h);
      if (!points || points.length === 0)
        return;
      const values = points.map((p) => p.price);
      const min = Math.min(...values);
      const max = Math.max(...values);
      const range = max - min || 1;
      const padL = 44;
      const padR = 18;
      const padT = 18;
      const padB = 30;
      ctx.strokeStyle = "rgba(255,255,255,0.08)";
      ctx.lineWidth = 1;
      for (let i = 0; i < 4; i++) {
        const y = padT + (h - padT - padB) / 3 * i;
        ctx.beginPath();
        ctx.moveTo(padL, y);
        ctx.lineTo(w - padR, y);
        ctx.stroke();
      }
      ctx.beginPath();
      points.forEach((p, i) => {
        const x = padL + i / Math.max(1, points.length - 1) * (w - padL - padR);
        const y = h - padB - (p.price - min) / range * (h - padT - padB);
        if (i === 0)
          ctx.moveTo(x, y);
        else
          ctx.lineTo(x, y);
      });
      ctx.strokeStyle = "#10b981";
      ctx.lineWidth = 3;
      ctx.stroke();
      points.forEach((p, i) => {
        const x = padL + i / Math.max(1, points.length - 1) * (w - padL - padR);
        const y = h - padB - (p.price - min) / range * (h - padT - padB);
        ctx.beginPath();
        ctx.arc(x, y, 3, 0, Math.PI * 2);
        ctx.fillStyle = "#10b981";
        ctx.fill();
      });
      ctx.fillStyle = "#9ca3af";
      ctx.font = "12px ui-monospace, monospace";
      ctx.fillText(`$${max.toFixed(2)}`, 6, 14);
      ctx.fillText(`$${min.toFixed(2)}`, 6, h - 10);
      points.forEach((p, i) => {
        const x = padL + i / Math.max(1, points.length - 1) * (w - padL - padR);
        ctx.fillText(String(p.round), x - 3, h - 8);
      });
    }
  };
  var LobbyManager = class {
    constructor(socket, cfg) {
      this.socket = socket;
      this.cfg = cfg;
      this.channel = null;
      this.joined = false;
    }
    init() {
      this.channel = this.socket.channel("lobby:general", {});
      this.channel.on(
        "open_matches",
        (p) => this._updateMatchList(p.matches || [])
      );
      this.channel.on("match_created", (match) => this._addOrUpdateMatch(match));
      this.channel.on("match_updated", (match) => this._addOrUpdateMatch(match));
      this.channel.on("match_started", (p) => this._onMatchStarted(p.match_id));
      this.channel.on("presence_state", (state) => this._onPresenceState(state));
      this.channel.on("presence_diff", (diff) => this._onPresenceDiff(diff));
      this.channel.join().receive("ok", () => {
        this.joined = true;
        this._wireJoinButtons(document);
      }).receive("error", () => {
        this._wireJoinButtons(document);
      });
    }
    push(event, payload) {
      if (this.channel)
        return this.channel.push(event, payload);
    }
    _wireJoinButtons(root = document) {
      root.querySelectorAll(".join-match-btn:not([data-wired])").forEach((btn) => {
        btn.dataset.wired = "1";
        btn.addEventListener("click", (e) => {
          var _a;
          e.preventDefault();
          const matchId = btn.dataset.matchId || ((_a = btn.closest("[data-match-id]")) == null ? void 0 : _a.dataset.matchId);
          if (!matchId)
            return;
          this._joinMatch(matchId, btn);
        });
      });
    }
    _joinMatch(matchId, btn) {
      const original = btn.textContent;
      btn.disabled = true;
      btn.textContent = "Joining...";
      this.channel.push("join_match", { match_id: matchId }).receive("ok", () => {
        window.location.href = `/matches/${matchId}`;
      }).receive("error", (err) => {
        btn.disabled = false;
        btn.textContent = original;
        Toast.show(
          `Could not join: ${(err == null ? void 0 : err.reason) || "unknown error"}`,
          "error"
        );
      }).receive("timeout", () => {
        btn.disabled = false;
        btn.textContent = original;
        Toast.show("Join timed out. Please try again.", "warning");
      });
    }
    _addOrUpdateMatch(match) {
      var _a;
      const list = document.getElementById("open-matches-list");
      if (!list || !match)
        return;
      (_a = document.getElementById("no-matches-placeholder")) == null ? void 0 : _a.remove();
      const existing = list.querySelector(`[data-match-id="${match.id}"]`);
      const card = this._buildMatchCard(match);
      if (existing)
        existing.replaceWith(card);
      else
        list.insertAdjacentElement("afterbegin", card);
      this._wireJoinButtons(list);
    }
    _buildMatchCard(match) {
      const div = document.createElement("div");
      div.className = "card card-hover p-4 match-card fade-in-up";
      div.dataset.matchId = match.id;
      const isFull = (match.player_count || 0) >= match.max_players;
      div.innerHTML = `
      <div class="flex items-center justify-between">
        <div class="flex items-center gap-3">
          <div class="w-9 h-9 rounded-lg bg-emerald-500/10 border border-emerald-500/20 flex items-center justify-center shrink-0">
            <svg class="w-4 h-4 text-emerald-400" fill="none" viewBox="0 0 24 24" stroke="currentColor" stroke-width="2">
              <polyline points="22 7 13.5 15.5 8.5 10.5 2 17"></polyline>
            </svg>
          </div>
          <div>
            <div class="font-semibold text-sm text-white font-mono">${escapeHtml(match.name)}</div>
            <div class="text-xs text-gray-500 mt-0.5">
              hosted by <span class="text-gray-400">${escapeHtml(match.host_username || "\u2014")}</span>
            </div>
          </div>
        </div>
        <div class="flex items-center gap-3">
          <div class="text-right">
            <div class="text-xs font-mono text-gray-400">
              <span class="text-white font-semibold">${match.player_count || 0}</span>/${match.max_players || 4}
            </div>
          </div>
          ${isFull ? `<span class="btn-ghost text-xs py-1.5 px-3 opacity-40 pointer-events-none">Full</span>` : `<button type="button" data-match-id="${escapeHtml(match.id)}" class="join-match-btn btn-primary text-xs py-1.5 px-4">Join</button>`}
        </div>
      </div>
    `;
      return div;
    }
    _updateMatchList(matches) {
      const list = document.getElementById("open-matches-list");
      if (!list)
        return;
      if (!matches.length)
        return;
      list.innerHTML = "";
      matches.forEach((m) => list.appendChild(this._buildMatchCard(m)));
      this._wireJoinButtons(list);
    }
    _onMatchStarted(matchId) {
      const card = document.querySelector(
        `#open-matches-list [data-match-id="${matchId}"]`
      );
      if (card)
        card.remove();
    }
    _onPresenceState(state) {
      const list = document.getElementById("lobby-presence-list");
      const counter = document.getElementById("lobby-online-count");
      if (!list)
        return;
      const users = [];
      Object.values(state || {}).forEach((entry) => {
        var _a;
        const meta = (_a = entry.metas) == null ? void 0 : _a[0];
        if (meta == null ? void 0 : meta.username)
          users.push(meta.username);
      });
      if (counter)
        counter.textContent = String(users.length);
      this._renderPresenceList(list, users);
    }
    _onPresenceDiff(diff) {
      const list = document.getElementById("lobby-presence-list");
      if (!list)
        return;
      const current = /* @__PURE__ */ new Map();
      list.querySelectorAll("[data-presence-user]").forEach((el) => {
        current.set(el.dataset.presenceUser, el);
      });
      Object.values((diff == null ? void 0 : diff.joins) || {}).forEach((entry) => {
        var _a;
        const meta = (_a = entry.metas) == null ? void 0 : _a[0];
        if (!(meta == null ? void 0 : meta.username) || current.has(meta.username))
          return;
        const el = this._buildPresenceRow(meta.username);
        list.appendChild(el);
        current.set(meta.username, el);
      });
      Object.values((diff == null ? void 0 : diff.leaves) || {}).forEach((entry) => {
        var _a;
        const meta = (_a = entry.metas) == null ? void 0 : _a[0];
        if (!(meta == null ? void 0 : meta.username))
          return;
        const el = current.get(meta.username);
        if (el) {
          el.remove();
          current.delete(meta.username);
        }
      });
      const counter = document.getElementById("lobby-online-count");
      if (counter)
        counter.textContent = String(current.size);
      if (current.size === 0) {
        list.innerHTML = `<p class="text-xs text-gray-600 font-mono">Nobody else online.</p>`;
      }
    }
    _renderPresenceList(list, users) {
      if (!users.length) {
        list.innerHTML = `<p class="text-xs text-gray-600 font-mono">Nobody else online.</p>`;
        return;
      }
      list.innerHTML = "";
      users.forEach((u) => list.appendChild(this._buildPresenceRow(u)));
    }
    _buildPresenceRow(username) {
      const el = document.createElement("div");
      el.className = "flex items-center gap-2 fade-in-up";
      el.dataset.presenceUser = username;
      el.innerHTML = `
      <div class="presence-dot"></div>
      <span class="text-xs font-mono text-gray-300">${escapeHtml(username)}</span>
    `;
      return el;
    }
  };
  var MatchManager = class {
    constructor(socket, cfg, lobbyMgr) {
      this.socket = socket;
      this.cfg = cfg;
      this.lobbyMgr = lobbyMgr;
      this.channel = null;
      this.players = [];
      this.companies = [];
      this.myState = null;
      this.phase = null;
      this.countdown = null;
      this.phaseOverlayTimer = null;
      this.prevPrices = {};
      this.sparklineCharts = {};
      this.priceHistory = {};
      this.selectedTicker = null;
      this.detailChart = new DetailChart("stock-detail-chart");
    }
    init() {
      this.channel = this.socket.channel(`match:${this.cfg.matchId}`, {});
      this.channel.on("state_updated", (p) => this._onStateUpdated(p));
      this.channel.on("phase_changed", (p) => this._onPhaseChanged(p));
      this.channel.on("private_state", (p) => this._onPrivateState(p));
      this.channel.on("private_events", (p) => this._onPrivateEvents(p));
      this.channel.on("new_message", (m) => this._appendChat(m));
      this.channel.on("match_finished", (p) => this._onMatchFinished(p));
      this.channel.on("presence_state", (s) => this._onPresenceState(s));
      this.channel.on("presence_diff", (d) => this._onPresenceDiff(d));
      this.channel.on(
        "waiting_room_updated",
        (p) => this._onWaitingRoomUpdated(p)
      );
      this.channel.join().receive("ok", () => {
      }).receive("error", ({ reason }) => {
        Toast.show(`Could not join match: ${reason}`, "error");
      });
      this._wireControls();
    }
    _wireControls() {
      var _a, _b, _c, _d, _e;
      const startBtn = document.getElementById("start-match-btn");
      if (startBtn) {
        startBtn.addEventListener("click", (e) => {
          var _a2, _b2;
          e.preventDefault();
          startBtn.disabled = true;
          startBtn.textContent = "Starting...";
          (_b2 = (_a2 = this.lobbyMgr.push("start_match", { match_id: this.cfg.matchId })) == null ? void 0 : _a2.receive("ok", () => {
            startBtn.remove();
          })) == null ? void 0 : _b2.receive("error", (err) => {
            startBtn.disabled = false;
            startBtn.textContent = "Start Match";
            Toast.show(
              `Could not start: ${(err == null ? void 0 : err.reason) || "unknown error"}`,
              "error"
            );
          });
        });
      }
      document.querySelectorAll(".action-btn").forEach((btn) => {
        btn.addEventListener("click", (e) => {
          e.preventDefault();
          if (btn.disabled)
            return;
          this._openActionModal(btn.dataset);
        });
      });
      (_a = document.getElementById("copy-invite-btn")) == null ? void 0 : _a.addEventListener("click", (e) => {
        var _a2;
        e.preventDefault();
        const url = window.location.href;
        (_a2 = navigator.clipboard) == null ? void 0 : _a2.writeText(url).then(() => Toast.show("Invite link copied", "success")).catch(() => window.prompt("Copy this invite link:", url));
      });
      (_b = document.getElementById("modal-close")) == null ? void 0 : _b.addEventListener("click", (e) => {
        e.preventDefault();
        this._closeModal();
      });
      (_c = document.getElementById("action-modal")) == null ? void 0 : _c.addEventListener("click", (e) => {
        if (e.target === e.currentTarget)
          this._closeModal();
      });
      (_d = document.getElementById("modal-submit")) == null ? void 0 : _d.addEventListener("click", (e) => {
        e.preventDefault();
        this._submitAction();
      });
      (_e = document.getElementById("input-quantity")) == null ? void 0 : _e.addEventListener("keydown", (e) => {
        if (e.key === "Enter") {
          e.preventDefault();
          this._submitAction();
        }
      });
      const sendBtn = document.getElementById("chat-send");
      const chatInput = document.getElementById("chat-input");
      if (sendBtn && chatInput) {
        sendBtn.addEventListener("click", (e) => {
          e.preventDefault();
          this._sendChatMessage();
        });
        chatInput.addEventListener("keydown", (e) => {
          if (e.key === "Enter" && !e.shiftKey) {
            e.preventDefault();
            this._sendChatMessage();
          }
        });
      }
    }
    // ── state updates ────────────────────────────────────────────────
    _onStateUpdated(payload) {
      this.players = payload.players || [];
      this.companies = payload.companies || [];
      this.phase = payload.phase;
      this._renderRound(payload.round, payload.total_rounds);
      this._renderPhase(payload.phase);
      this._renderPlayers(payload.players || []);
      this._renderMarket(payload.companies || [], payload.round);
      this._renderActionPanel(payload.phase);
      this._renderEvents(payload.public_events || []);
      this._renderNews(payload.public_events || []);
      this._updateChatIndicator(payload.phase);
      if (payload.phase === "negotiation" && payload.negotiation_deadline) {
        this._startCountdown(new Date(payload.negotiation_deadline));
      } else if (payload.phase !== "negotiation") {
        this._clearCountdown();
      }
    }
    _onPhaseChanged(payload) {
      this.phase = payload.phase;
      this._renderRound(payload.round, payload.total_rounds);
      this._renderPhase(payload.phase);
      this._renderActionPanel(payload.phase);
      this._updateChatIndicator(payload.phase);
      this._showPhaseOverlay(payload.phase);
      if (payload.phase === "action_submission") {
        this._clearCountdown();
        this._hide(document.getElementById("action-submitted-badge"));
        document.querySelectorAll(".action-btn").forEach((b) => {
          b.disabled = false;
          b.classList.remove("submitted");
        });
      }
      if (payload.phase === "negotiation" && payload.negotiation_deadline) {
        this._startCountdown(new Date(payload.negotiation_deadline));
      } else if (payload.phase !== "negotiation") {
        this._clearCountdown();
      }
    }
    _onPrivateState(state) {
      this.myState = state;
      this._renderMyStats(state);
    }
    _onPrivateEvents(payload) {
      (payload.events || []).forEach((ev) => {
        const msg = ev.message || ev.headline || ev.type || "Event";
        Toast.show(msg, ev.positive ? "success" : "info");
      });
    }
    _onMatchFinished(payload) {
      this._clearCountdown();
      this._renderLeaderboard(payload.leaderboard || []);
      this._showPhaseOverlay("finished");
      this._hide(document.getElementById("action-panel"));
      this._show(document.getElementById("leaderboard-panel"), "block");
      Toast.show("Match over! Final rankings are in.", "info", 8e3);
    }
    // ── presence ─────────────────────────────────────────────────────
    _onPresenceState(state) {
      Object.entries(state || {}).forEach(([userId]) => {
        this._setPlayerOnline(userId, true);
      });
    }
    _onPresenceDiff(diff) {
      Object.entries((diff == null ? void 0 : diff.joins) || {}).forEach(
        ([userId]) => this._setPlayerOnline(userId, true)
      );
      Object.entries((diff == null ? void 0 : diff.leaves) || {}).forEach(
        ([userId]) => this._setPlayerOnline(userId, false)
      );
    }
    _onWaitingRoomUpdated(payload) {
      if (payload.players) {
        this.players = payload.players;
        this._renderPlayers(payload.players);
      }
    }
    _setPlayerOnline(userId, online) {
      const row = document.querySelector(`[data-player-id="${userId}"]`);
      const dot = row == null ? void 0 : row.querySelector(".presence-dot");
      if (dot)
        dot.classList.toggle("offline", !online);
    }
    // ── rendering ────────────────────────────────────────────────────
    _renderRound(round, total) {
      const el = document.getElementById("current-round");
      if (el)
        el.textContent = round || "\u2014";
      const lastRound = document.getElementById("detail-last-round");
      if (lastRound)
        lastRound.textContent = round || "1";
    }
    _renderPhase(phase) {
      const container = document.getElementById("phase-badge-container");
      if (!container)
        return;
      container.innerHTML = `<span class="phase-badge phase-${phase || "waiting"}">${phaseLabel(phase)}</span>`;
    }
    _renderActionPanel(phase) {
      const panel = document.getElementById("action-panel");
      if (!panel)
        return;
      if (phase === "action_submission")
        this._show(panel, "block");
      else
        this._hide(panel);
    }
    _renderPlayers(players) {
      const list = document.getElementById("players-list");
      if (!list || !players)
        return;
      list.innerHTML = "";
      players.slice().sort((a, b) => (a.seat_number || 0) - (b.seat_number || 0)).forEach((p) => {
        const row = document.createElement("div");
        row.className = "flex items-center gap-2.5";
        row.dataset.playerId = p.user_id;
        row.innerHTML = `
          <div class="presence-dot offline"></div>
          <div class="flex-1 min-w-0">
            <span class="text-sm text-gray-300 font-mono truncate">${escapeHtml(p.username)}</span>
          </div>
          <span class="text-xs font-mono text-amber-400">${(p.regulatory_heat || 0) > 0 ? `\u2696${p.regulatory_heat}` : ""}</span>
          <span class="text-xs font-mono text-blue-400">${p.liquidity_frozen ? "\u2744" : ""}</span>
          <span class="text-xs font-mono text-emerald-400">${p.has_submitted ? "\u2713" : ""}</span>
        `;
        list.appendChild(row);
      });
    }
    _renderMyStats(state) {
      this._show(document.getElementById("my-stats"), "block");
      const cash = parseFloat(state.cash || 0);
      const netWorth = parseFloat(state.net_worth || 0);
      const cashEl = document.getElementById("my-cash");
      const nwEl = document.getElementById("my-networth");
      const heatEl = document.getElementById("my-heat");
      if (cashEl)
        cashEl.textContent = `$${cash.toFixed(2)}`;
      if (nwEl)
        nwEl.textContent = `$${netWorth.toFixed(2)}`;
      if (heatEl)
        heatEl.textContent = state.regulatory_heat || 0;
      if (state.liquidity_frozen)
        this._show(document.getElementById("my-frozen-badge"), "block");
      else
        this._hide(document.getElementById("my-frozen-badge"));
      const holdingsEl = document.getElementById("my-holdings");
      if (holdingsEl) {
        holdingsEl.innerHTML = "";
        Object.entries(state.portfolio || {}).forEach(([ticker, qty]) => {
          const row = document.createElement("div");
          row.className = "flex justify-between text-xs font-mono";
          row.innerHTML = `<span class="text-gray-500">${escapeHtml(ticker)}</span><span class="text-gray-300">${qty} sh</span>`;
          holdingsEl.appendChild(row);
        });
      }
    }
    _renderMarket(companies, round) {
      const list = document.getElementById("companies-list");
      if (!list)
        return;
      list.innerHTML = "";
      companies.forEach((c) => {
        const price = parseFloat(c.price || 0);
        const prev = this.prevPrices[c.ticker];
        const isUp = prev === void 0 ? true : price >= prev;
        this.prevPrices[c.ticker] = price;
        if (!this.priceHistory[c.ticker])
          this.priceHistory[c.ticker] = [];
        const history = this.priceHistory[c.ticker];
        const last = history[history.length - 1];
        if (!last || last.round !== round || last.price !== price) {
          history.push({
            round: round || 1,
            price,
            regulatory_heat: c.regulatory_heat || 0
          });
        }
        const row = document.createElement("div");
        row.className = `ticker-row ${this.selectedTicker === c.ticker ? "selected" : ""}`;
        row.dataset.ticker = c.ticker;
        const chartId = `chart-${c.ticker}`;
        const pct = prev === void 0 || prev === 0 ? "" : `${price >= prev ? "+" : ""}${((price - prev) / prev * 100).toFixed(2)}%`;
        row.innerHTML = `
        <span class="font-mono font-bold text-sm ticker-symbol">${escapeHtml(c.ticker)}</span>
        <span class="text-xs text-gray-500 truncate ticker-name">${escapeHtml(c.name || "")}</span>
        <div class="text-right">
          <div class="font-mono font-semibold text-sm ticker-price" style="color:${isUp ? "#10b981" : "#ef4444"}">$${price.toFixed(2)}</div>
          <div class="text-xs font-mono ticker-change mt-0.5" style="color:${isUp ? "#10b981" : "#ef4444"}">${pct}</div>
        </div>
        <div id="${chartId}" class="flex items-center justify-end" style="height:28px;width:80px"></div>
        <div></div>
      `;
        row.addEventListener("click", () => {
          this.selectedTicker = c.ticker;
          this._renderMarket(this.companies, round);
          this._renderStockDetail(c.ticker);
        });
        list.appendChild(row);
        requestAnimationFrame(() => {
          if (!this.sparklineCharts[c.ticker]) {
            this.sparklineCharts[c.ticker] = new SparklineChart(chartId, {
              width: 80,
              height: 28
            });
          }
          this.sparklineCharts[c.ticker].reset(
            this.priceHistory[c.ticker].map((p) => p.price)
          );
          this.sparklineCharts[c.ticker].setColor(isUp);
        });
      });
      if (!this.selectedTicker && companies[0]) {
        this.selectedTicker = companies[0].ticker;
      }
      if (this.selectedTicker) {
        this._renderStockDetail(this.selectedTicker);
      }
    }
    _renderStockDetail(ticker) {
      var _a, _b, _c, _d, _e;
      const company = this.companies.find((c) => c.ticker === ticker);
      const history = this.priceHistory[ticker] || [];
      if (!company)
        return;
      this._hide(document.getElementById("stock-detail-empty"));
      this._show(document.getElementById("stock-detail-panel"), "block");
      const currentPrice = parseFloat(company.price || 0);
      const firstPrice = (_b = (_a = history[0]) == null ? void 0 : _a.price) != null ? _b : currentPrice;
      const lastPrice = (_d = (_c = history[history.length - 1]) == null ? void 0 : _c.price) != null ? _d : currentPrice;
      const pct = (lastPrice - firstPrice) / Math.max(firstPrice, 1e-4) * 100;
      const title = document.getElementById("selected-stock-title");
      const meta = document.getElementById("selected-stock-meta");
      const current = document.getElementById("detail-current-price");
      const change = document.getElementById("detail-round-change");
      const heat = document.getElementById("detail-reg-heat");
      const count = document.getElementById("detail-history-count");
      const lastRound = document.getElementById("detail-last-round");
      const summary = document.getElementById("detail-history-summary");
      if (title)
        title.textContent = `${ticker} \xB7 ${company.name}`;
      if (meta)
        meta.textContent = `Round-by-round movement for ${ticker}`;
      if (current)
        current.textContent = `$${currentPrice.toFixed(2)}`;
      if (change) {
        change.textContent = `${pct >= 0 ? "+" : ""}${pct.toFixed(2)}%`;
        change.style.color = pct >= 0 ? "#10b981" : "#ef4444";
      }
      if (heat)
        heat.textContent = String(company.regulatory_heat || 0);
      if (count)
        count.textContent = String(history.length);
      if (lastRound)
        lastRound.textContent = String(((_e = history[history.length - 1]) == null ? void 0 : _e.round) || 1);
      if (summary)
        summary.textContent = `${history.length} price points tracked`;
      const historyList = document.getElementById("stock-history-list");
      if (historyList) {
        historyList.innerHTML = "";
        if (!history.length) {
          historyList.innerHTML = `<p class="text-xs text-gray-600 font-mono text-center py-4">Waiting for price history\u2026</p>`;
        } else {
          history.forEach((item, idx) => {
            const prev = idx > 0 ? history[idx - 1].price : item.price;
            const dir = item.price > prev ? "up" : item.price < prev ? "down" : "flat";
            const row = document.createElement("div");
            row.className = `history-item ${dir}`;
            row.innerHTML = `
            <div class="flex items-center justify-between">
              <span class="text-xs font-mono text-gray-400">Round ${item.round}</span>
              <span class="text-xs font-mono ${dir === "up" ? "text-emerald-400" : dir === "down" ? "text-red-400" : "text-gray-400"}">$${item.price.toFixed(2)}</span>
            </div>
          `;
            historyList.appendChild(row);
          });
        }
      }
      this.detailChart.render(history);
    }
    _renderEvents(events) {
      const box = document.getElementById("events-feed");
      if (!box)
        return;
      box.innerHTML = "";
      if (!events.length) {
        box.innerHTML = `<p class="text-xs text-gray-600 font-mono text-center py-2">No events yet.</p>`;
        return;
      }
      events.forEach((ev) => {
        const item = document.createElement("div");
        item.className = "history-item";
        if (ev.type === "news_event") {
          item.innerHTML = `
          <div class="text-xs font-mono text-emerald-400 mb-1">NEWS</div>
          <div class="text-xs text-gray-300">${escapeHtml(ev.headline || "")}</div>
        `;
        } else if (ev.type === "price_updated") {
          item.innerHTML = `
          <div class="text-xs font-mono text-cyan-400 mb-1">${escapeHtml(ev.ticker || "")}</div>
          <div class="text-xs text-gray-300">Price now $${parseFloat(ev.price || 0).toFixed(2)}</div>
        `;
        } else {
          item.innerHTML = `<div class="text-xs text-gray-300">${escapeHtml(ev.headline || JSON.stringify(ev))}</div>`;
        }
        box.appendChild(item);
      });
    }
    _renderNews(events) {
      const box = document.getElementById("news-feed");
      if (!box)
        return;
      const news = (events || []).filter(
        (ev) => ev.type === "news_event" || ev.type === "price_updated"
      );
      box.innerHTML = "";
      if (!news.length) {
        box.innerHTML = `<p class="text-xs text-gray-600 font-mono text-center py-4">No news yet.</p>`;
        return;
      }
      news.forEach((ev) => {
        const item = document.createElement("div");
        item.className = `news-item ${ev.type === "news_event" ? (ev.impact || 0) >= 0 ? "positive" : "negative" : "neutral"}`;
        if (ev.type === "news_event") {
          item.innerHTML = `
          <div class="flex items-center justify-between mb-1">
            <span class="text-[10px] font-mono uppercase tracking-widest text-emerald-400">Round ${ev.round || "\u2014"}</span>
            <span class="text-[10px] font-mono text-gray-500">${escapeHtml((ev.targets || []).join(", "))}</span>
          </div>
          <div class="text-sm text-gray-200">${escapeHtml(ev.headline || "")}</div>
        `;
        } else {
          item.innerHTML = `
          <div class="flex items-center justify-between">
            <span class="text-xs font-mono text-cyan-400">${escapeHtml(ev.ticker || "")}</span>
            <span class="text-xs font-mono text-white">$${parseFloat(ev.price || 0).toFixed(2)}</span>
          </div>
        `;
        }
        box.appendChild(item);
      });
    }
    _updateChatIndicator(phase) {
      const el = document.getElementById("chat-phase-indicator");
      if (!el)
        return;
      const open = ["negotiation", "disclosure", "finished"].includes(phase);
      el.textContent = open ? "Open" : "Closed";
      el.style.color = open ? "#10b981" : "#6b7280";
    }
    _showPhaseOverlay(phase) {
      const overlay = document.getElementById("phase-overlay");
      const badge = document.getElementById("phase-overlay-badge");
      const desc = document.getElementById("phase-overlay-desc");
      if (!overlay || !badge)
        return;
      const descs = {
        news: "Market news just landed.",
        action_submission: "Submit your move.",
        negotiation: "Chat is open.",
        resolution: "Resolving actions.",
        disclosure: "Results posted.",
        finished: "Final rankings ready."
      };
      badge.textContent = phaseLabel(phase);
      badge.className = `phase-badge phase-${phase} text-2xl px-8 py-4 font-mono tracking-widest uppercase`;
      if (desc)
        desc.textContent = descs[phase] || "";
      this._show(overlay, "flex");
      clearTimeout(this.phaseOverlayTimer);
      if (phase !== "finished") {
        this.phaseOverlayTimer = setTimeout(() => this._hide(overlay), 2500);
      }
    }
    // ── countdown ────────────────────────────────────────────────────
    _startCountdown(deadline) {
      this._clearCountdown();
      const el = document.getElementById("countdown-timer");
      const valEl = document.getElementById("countdown-value");
      if (!el || !valEl)
        return;
      this._show(el, "flex");
      const tick = () => {
        const diff = Math.max(
          0,
          Math.floor((deadline.getTime() - Date.now()) / 1e3)
        );
        const m = Math.floor(diff / 60);
        const s = diff % 60;
        valEl.textContent = `${m}:${String(s).padStart(2, "0")}`;
        el.classList.toggle("countdown-urgent", diff <= 10 && diff > 0);
        if (diff <= 0)
          this._clearCountdown();
      };
      tick();
      this.countdown = setInterval(tick, 1e3);
    }
    _clearCountdown() {
      if (this.countdown)
        clearInterval(this.countdown);
      this.countdown = null;
      const el = document.getElementById("countdown-timer");
      if (el) {
        this._hide(el);
        el.classList.remove("countdown-urgent");
      }
    }
    // ── leaderboard ──────────────────────────────────────────────────
    _renderLeaderboard(leaderboard) {
      const list = document.getElementById("leaderboard-list");
      if (!list)
        return;
      list.innerHTML = "";
      leaderboard.forEach((entry, i) => {
        const div = document.createElement("div");
        div.className = "flex items-center gap-3 p-3 rounded-lg card fade-in-up";
        const medals = ["\u{1F947}", "\u{1F948}", "\u{1F949}"];
        div.innerHTML = `
        <span class="text-xl w-8 text-center">${medals[i] || `#${i + 1}`}</span>
        <span class="font-mono font-semibold text-sm text-white flex-1">${escapeHtml(entry.username)}</span>
        <span class="font-mono text-sm ${i === 0 ? "text-emerald-400 font-bold" : "text-gray-300"}">
          $${parseFloat(entry.net_worth || 0).toLocaleString("en-US", {
          minimumFractionDigits: 2,
          maximumFractionDigits: 2
        })}
        </span>
      `;
        list.appendChild(div);
      });
    }
    // ── action modal ─────────────────────────────────────────────────
    _openActionModal(dataset) {
      const modal = document.getElementById("action-modal");
      const titleEl = document.getElementById("modal-title");
      const descEl = document.getElementById("modal-desc");
      const errEl = document.getElementById("modal-error");
      const tickerDiv = document.getElementById("field-ticker");
      const qtyDiv = document.getElementById("field-quantity");
      const targetDiv = document.getElementById("field-target");
      const costDiv = document.getElementById("cost-estimate");
      const tickerBtns = document.getElementById("ticker-buttons");
      const targetBtns = document.getElementById("target-buttons");
      const qtyInput = document.getElementById("input-quantity");
      if (!modal)
        return;
      if (titleEl)
        titleEl.textContent = dataset.label || dataset.action || "";
      if (descEl)
        descEl.textContent = dataset.desc || "";
      if (errEl) {
        errEl.textContent = "";
        this._hide(errEl);
      }
      const needsTicker = dataset.needsTicker === "true";
      const needsQty = dataset.needsQuantity === "true";
      const needsTarget = dataset.needsTarget === "true";
      needsTicker ? this._show(tickerDiv, "block") : this._hide(tickerDiv);
      needsQty ? this._show(qtyDiv, "block") : this._hide(qtyDiv);
      needsTarget ? this._show(targetDiv, "block") : this._hide(targetDiv);
      this._hide(costDiv);
      if (needsTicker && tickerBtns) {
        tickerBtns.innerHTML = "";
        this.companies.forEach((c) => {
          const btn = document.createElement("button");
          btn.type = "button";
          btn.className = "ticker-pill";
          btn.dataset.ticker = c.ticker;
          btn.innerHTML = `<span class="font-bold">${escapeHtml(c.ticker)}</span>
                         <span class="text-gray-500 text-xs ml-1">$${parseFloat(c.price).toFixed(2)}</span>`;
          btn.addEventListener("click", (e) => {
            e.preventDefault();
            tickerBtns.querySelectorAll(".ticker-pill").forEach((b) => b.classList.remove("selected"));
            btn.classList.add("selected");
            if (needsQty)
              this._updateCostEstimate(dataset.action, c.ticker, qtyInput == null ? void 0 : qtyInput.value);
          });
          tickerBtns.appendChild(btn);
        });
      }
      if (needsTarget && targetBtns) {
        targetBtns.innerHTML = "";
        this.players.forEach((p) => {
          if (String(p.user_id) === String(this.cfg.userId))
            return;
          const btn = document.createElement("button");
          btn.type = "button";
          btn.className = "player-pill";
          btn.dataset.targetId = p.user_id;
          btn.innerHTML = `
          <div class="w-2 h-2 rounded-full ${p.liquidity_frozen ? "bg-blue-400" : "bg-emerald-400"}"></div>
          <span class="font-mono text-sm text-gray-200 flex-1">${escapeHtml(p.username)}</span>
          ${p.liquidity_frozen ? `<span class="text-xs text-blue-400 font-mono">\u2744 frozen</span>` : ""}
          ${p.regulatory_heat > 0 ? `<span class="text-xs text-amber-400 font-mono">\u2696 ${p.regulatory_heat}</span>` : ""}
        `;
          btn.addEventListener("click", (e) => {
            e.preventDefault();
            targetBtns.querySelectorAll(".player-pill").forEach((b) => b.classList.remove("selected"));
            btn.classList.add("selected");
          });
          targetBtns.appendChild(btn);
        });
      }
      if (qtyInput) {
        qtyInput.value = "";
        qtyInput.oninput = () => {
          var _a;
          const ticker = (_a = tickerBtns == null ? void 0 : tickerBtns.querySelector(".selected")) == null ? void 0 : _a.dataset.ticker;
          this._updateCostEstimate(dataset.action, ticker, qtyInput.value);
        };
      }
      const form = document.getElementById("action-form");
      if (form)
        form.dataset.actionType = dataset.action;
      this._show(modal, "flex");
    }
    _closeModal() {
      this._hide(document.getElementById("action-modal"));
      this._hide(document.getElementById("modal-error"));
    }
    _updateCostEstimate(action, ticker, qty) {
      const costDiv = document.getElementById("cost-estimate");
      const costValEl = document.getElementById("cost-value");
      const cashRemEl = document.getElementById("cash-remaining");
      if (!costDiv || !ticker || !qty || !["buy", "short", "acquire_stake"].includes(action)) {
        this._hide(costDiv);
        return;
      }
      const company = this.companies.find((c) => c.ticker === ticker);
      if (!company) {
        this._hide(costDiv);
        return;
      }
      const price = parseFloat(company.price);
      const q = parseInt(qty, 10) || 0;
      const total = price * q;
      const myCash = this.myState ? parseFloat(this.myState.cash) : 0;
      this._show(costDiv, "block");
      if (costValEl) {
        costValEl.textContent = `$${total.toLocaleString("en-US", {
          minimumFractionDigits: 2,
          maximumFractionDigits: 2
        })}`;
      }
      if (cashRemEl) {
        cashRemEl.textContent = `$${Math.max(0, myCash - total).toLocaleString(
          "en-US",
          {
            minimumFractionDigits: 2,
            maximumFractionDigits: 2
          }
        )}`;
        cashRemEl.style.color = myCash - total < 0 ? "#ef4444" : "#d1d5db";
      }
    }
    _submitAction() {
      const form = document.getElementById("action-form");
      const submitBtn = document.getElementById("modal-submit");
      if (!form)
        return;
      const actionType = form.dataset.actionType;
      if (!actionType)
        return;
      const params = {};
      const selectedTicker = document.querySelector(
        "#ticker-buttons .ticker-pill.selected"
      );
      if (selectedTicker)
        params.ticker = selectedTicker.dataset.ticker;
      const qtyInput = document.getElementById("input-quantity");
      const qtyParent = document.getElementById("field-quantity");
      const qtyVisible = qtyParent && qtyParent.style.display !== "none";
      if (qtyVisible && qtyInput) {
        const q = parseInt(qtyInput.value, 10);
        if (!q || q <= 0) {
          this._showModalError("Please enter a valid quantity.");
          return;
        }
        params.quantity = q;
      }
      const selectedTarget = document.querySelector(
        "#target-buttons .player-pill.selected"
      );
      if (selectedTarget)
        params.target_user_id = selectedTarget.dataset.targetId;
      const tickerParent = document.getElementById("field-ticker");
      if (tickerParent && tickerParent.style.display !== "none" && !params.ticker) {
        this._showModalError("Please select a company.");
        return;
      }
      const targetParent = document.getElementById("field-target");
      if (targetParent && targetParent.style.display !== "none" && !params.target_user_id) {
        this._showModalError("Please select a target player.");
        return;
      }
      submitBtn.disabled = true;
      submitBtn.textContent = "Submitting...";
      this.channel.push("submit_action", { action_type: actionType, params }).receive("ok", () => {
        this._closeModal();
        submitBtn.disabled = false;
        submitBtn.textContent = "Confirm Action";
        document.querySelectorAll(".action-btn").forEach((b) => {
          b.disabled = true;
          b.classList.add("submitted");
        });
        this._show(document.getElementById("action-submitted-badge"), "flex");
        Toast.show("Action submitted! Waiting for other players.", "success");
      }).receive("error", (e) => {
        submitBtn.disabled = false;
        submitBtn.textContent = "Confirm Action";
        const reason = typeof e.reason === "string" ? e.reason : JSON.stringify(e.reason);
        this._showModalError("Rejected: " + reason);
      });
    }
    _showModalError(msg) {
      const el = document.getElementById("modal-error");
      if (!el)
        return;
      el.textContent = msg;
      this._show(el, "block");
    }
    _show(el, display = "block") {
      if (!el)
        return;
      el.classList.remove("hidden");
      el.style.display = display;
    }
    _hide(el) {
      if (!el)
        return;
      el.classList.add("hidden");
      el.style.display = "none";
    }
    // ── chat ─────────────────────────────────────────────────────────
    _sendChatMessage() {
      const input = document.getElementById("chat-input");
      if (!input)
        return;
      const content = input.value.trim();
      if (!content)
        return;
      this.channel.push("send_message", { content }).receive("ok", () => {
        input.value = "";
      }).receive("error", (e) => {
        Toast.show(`Message rejected: ${JSON.stringify(e.reason)}`, "error");
      });
    }
    _appendChat(msg) {
      const container = document.getElementById("chat-messages");
      if (!container)
        return;
      const placeholder = container.querySelector("p");
      if (placeholder && placeholder.classList.contains("text-center")) {
        placeholder.remove();
      }
      const el = document.createElement("div");
      el.className = "flex gap-1.5 fade-in-up";
      const time = msg.inserted_at ? new Date(msg.inserted_at).toLocaleTimeString([], {
        hour: "2-digit",
        minute: "2-digit"
      }) : "";
      el.innerHTML = `
      <span class="text-gray-600 font-mono text-xs shrink-0 mt-0.5">${time}</span>
      <div class="min-w-0">
        <span class="font-mono text-xs font-semibold text-emerald-400">${escapeHtml(msg.username || "?")}:</span>
        <span class="text-gray-300 text-xs ml-1 break-words">${escapeHtml(msg.content)}</span>
      </div>
    `;
      container.appendChild(el);
      container.scrollTop = container.scrollHeight;
    }
  };
  function phaseLabel(phase) {
    const labels = {
      waiting: "WAITING",
      news: "NEWS",
      action_submission: "SUBMIT ACTION",
      negotiation: "NEGOTIATION",
      resolution: "RESOLUTION",
      disclosure: "DISCLOSURE",
      finished: "FINISHED"
    };
    return labels[phase] || String(phase || "\u2014").replaceAll("_", " ").toUpperCase();
  }
  function escapeHtml(str) {
    return String(str != null ? str : "").replaceAll("&", "&amp;").replaceAll("<", "&lt;").replaceAll(">", "&gt;").replaceAll('"', "&quot;").replaceAll("'", "&#39;");
  }
  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", () => boot(config), {
      once: true
    });
  } else {
    boot(config);
  }
})();
//# sourceMappingURL=data:application/json;base64,ewogICJ2ZXJzaW9uIjogMywKICAic291cmNlcyI6IFsiLi4vLi4vLi4vZGVwcy9waG9lbml4L2Fzc2V0cy9qcy9waG9lbml4L3V0aWxzLmpzIiwgIi4uLy4uLy4uL2RlcHMvcGhvZW5peC9hc3NldHMvanMvcGhvZW5peC9jb25zdGFudHMuanMiLCAiLi4vLi4vLi4vZGVwcy9waG9lbml4L2Fzc2V0cy9qcy9waG9lbml4L3B1c2guanMiLCAiLi4vLi4vLi4vZGVwcy9waG9lbml4L2Fzc2V0cy9qcy9waG9lbml4L3RpbWVyLmpzIiwgIi4uLy4uLy4uL2RlcHMvcGhvZW5peC9hc3NldHMvanMvcGhvZW5peC9jaGFubmVsLmpzIiwgIi4uLy4uLy4uL2RlcHMvcGhvZW5peC9hc3NldHMvanMvcGhvZW5peC9hamF4LmpzIiwgIi4uLy4uLy4uL2RlcHMvcGhvZW5peC9hc3NldHMvanMvcGhvZW5peC9sb25ncG9sbC5qcyIsICIuLi8uLi8uLi9kZXBzL3Bob2VuaXgvYXNzZXRzL2pzL3Bob2VuaXgvcHJlc2VuY2UuanMiLCAiLi4vLi4vLi4vZGVwcy9waG9lbml4L2Fzc2V0cy9qcy9waG9lbml4L3NlcmlhbGl6ZXIuanMiLCAiLi4vLi4vLi4vZGVwcy9waG9lbml4L2Fzc2V0cy9qcy9waG9lbml4L3NvY2tldC5qcyIsICIuLi8uLi8uLi9hc3NldHMvanMvYXBwLmpzIl0sCiAgInNvdXJjZXNDb250ZW50IjogWyIvLyB3cmFwcyB2YWx1ZSBpbiBjbG9zdXJlIG9yIHJldHVybnMgY2xvc3VyZVxuZXhwb3J0IGxldCBjbG9zdXJlID0gKHZhbHVlKSA9PiB7XG4gIGlmKHR5cGVvZiB2YWx1ZSA9PT0gXCJmdW5jdGlvblwiKXtcbiAgICByZXR1cm4gdmFsdWVcbiAgfSBlbHNlIHtcbiAgICBsZXQgY2xvc3VyZSA9IGZ1bmN0aW9uICgpeyByZXR1cm4gdmFsdWUgfVxuICAgIHJldHVybiBjbG9zdXJlXG4gIH1cbn1cbiIsICJleHBvcnQgY29uc3QgZ2xvYmFsU2VsZiA9IHR5cGVvZiBzZWxmICE9PSBcInVuZGVmaW5lZFwiID8gc2VsZiA6IG51bGxcbmV4cG9ydCBjb25zdCBwaHhXaW5kb3cgPSB0eXBlb2Ygd2luZG93ICE9PSBcInVuZGVmaW5lZFwiID8gd2luZG93IDogbnVsbFxuZXhwb3J0IGNvbnN0IGdsb2JhbCA9IGdsb2JhbFNlbGYgfHwgcGh4V2luZG93IHx8IGdsb2JhbFxuZXhwb3J0IGNvbnN0IERFRkFVTFRfVlNOID0gXCIyLjAuMFwiXG5leHBvcnQgY29uc3QgU09DS0VUX1NUQVRFUyA9IHtjb25uZWN0aW5nOiAwLCBvcGVuOiAxLCBjbG9zaW5nOiAyLCBjbG9zZWQ6IDN9XG5leHBvcnQgY29uc3QgREVGQVVMVF9USU1FT1VUID0gMTAwMDBcbmV4cG9ydCBjb25zdCBXU19DTE9TRV9OT1JNQUwgPSAxMDAwXG5leHBvcnQgY29uc3QgQ0hBTk5FTF9TVEFURVMgPSB7XG4gIGNsb3NlZDogXCJjbG9zZWRcIixcbiAgZXJyb3JlZDogXCJlcnJvcmVkXCIsXG4gIGpvaW5lZDogXCJqb2luZWRcIixcbiAgam9pbmluZzogXCJqb2luaW5nXCIsXG4gIGxlYXZpbmc6IFwibGVhdmluZ1wiLFxufVxuZXhwb3J0IGNvbnN0IENIQU5ORUxfRVZFTlRTID0ge1xuICBjbG9zZTogXCJwaHhfY2xvc2VcIixcbiAgZXJyb3I6IFwicGh4X2Vycm9yXCIsXG4gIGpvaW46IFwicGh4X2pvaW5cIixcbiAgcmVwbHk6IFwicGh4X3JlcGx5XCIsXG4gIGxlYXZlOiBcInBoeF9sZWF2ZVwiXG59XG5cbmV4cG9ydCBjb25zdCBUUkFOU1BPUlRTID0ge1xuICBsb25ncG9sbDogXCJsb25ncG9sbFwiLFxuICB3ZWJzb2NrZXQ6IFwid2Vic29ja2V0XCJcbn1cbmV4cG9ydCBjb25zdCBYSFJfU1RBVEVTID0ge1xuICBjb21wbGV0ZTogNFxufVxuIiwgIi8qKlxuICogSW5pdGlhbGl6ZXMgdGhlIFB1c2hcbiAqIEBwYXJhbSB7Q2hhbm5lbH0gY2hhbm5lbCAtIFRoZSBDaGFubmVsXG4gKiBAcGFyYW0ge3N0cmluZ30gZXZlbnQgLSBUaGUgZXZlbnQsIGZvciBleGFtcGxlIGBcInBoeF9qb2luXCJgXG4gKiBAcGFyYW0ge09iamVjdH0gcGF5bG9hZCAtIFRoZSBwYXlsb2FkLCBmb3IgZXhhbXBsZSBge3VzZXJfaWQ6IDEyM31gXG4gKiBAcGFyYW0ge251bWJlcn0gdGltZW91dCAtIFRoZSBwdXNoIHRpbWVvdXQgaW4gbWlsbGlzZWNvbmRzXG4gKi9cbmV4cG9ydCBkZWZhdWx0IGNsYXNzIFB1c2gge1xuICBjb25zdHJ1Y3RvcihjaGFubmVsLCBldmVudCwgcGF5bG9hZCwgdGltZW91dCl7XG4gICAgdGhpcy5jaGFubmVsID0gY2hhbm5lbFxuICAgIHRoaXMuZXZlbnQgPSBldmVudFxuICAgIHRoaXMucGF5bG9hZCA9IHBheWxvYWQgfHwgZnVuY3Rpb24gKCl7IHJldHVybiB7fSB9XG4gICAgdGhpcy5yZWNlaXZlZFJlc3AgPSBudWxsXG4gICAgdGhpcy50aW1lb3V0ID0gdGltZW91dFxuICAgIHRoaXMudGltZW91dFRpbWVyID0gbnVsbFxuICAgIHRoaXMucmVjSG9va3MgPSBbXVxuICAgIHRoaXMuc2VudCA9IGZhbHNlXG4gIH1cblxuICAvKipcbiAgICpcbiAgICogQHBhcmFtIHtudW1iZXJ9IHRpbWVvdXRcbiAgICovXG4gIHJlc2VuZCh0aW1lb3V0KXtcbiAgICB0aGlzLnRpbWVvdXQgPSB0aW1lb3V0XG4gICAgdGhpcy5yZXNldCgpXG4gICAgdGhpcy5zZW5kKClcbiAgfVxuXG4gIC8qKlxuICAgKlxuICAgKi9cbiAgc2VuZCgpe1xuICAgIGlmKHRoaXMuaGFzUmVjZWl2ZWQoXCJ0aW1lb3V0XCIpKXsgcmV0dXJuIH1cbiAgICB0aGlzLnN0YXJ0VGltZW91dCgpXG4gICAgdGhpcy5zZW50ID0gdHJ1ZVxuICAgIHRoaXMuY2hhbm5lbC5zb2NrZXQucHVzaCh7XG4gICAgICB0b3BpYzogdGhpcy5jaGFubmVsLnRvcGljLFxuICAgICAgZXZlbnQ6IHRoaXMuZXZlbnQsXG4gICAgICBwYXlsb2FkOiB0aGlzLnBheWxvYWQoKSxcbiAgICAgIHJlZjogdGhpcy5yZWYsXG4gICAgICBqb2luX3JlZjogdGhpcy5jaGFubmVsLmpvaW5SZWYoKVxuICAgIH0pXG4gIH1cblxuICAvKipcbiAgICpcbiAgICogQHBhcmFtIHsqfSBzdGF0dXNcbiAgICogQHBhcmFtIHsqfSBjYWxsYmFja1xuICAgKi9cbiAgcmVjZWl2ZShzdGF0dXMsIGNhbGxiYWNrKXtcbiAgICBpZih0aGlzLmhhc1JlY2VpdmVkKHN0YXR1cykpe1xuICAgICAgY2FsbGJhY2sodGhpcy5yZWNlaXZlZFJlc3AucmVzcG9uc2UpXG4gICAgfVxuXG4gICAgdGhpcy5yZWNIb29rcy5wdXNoKHtzdGF0dXMsIGNhbGxiYWNrfSlcbiAgICByZXR1cm4gdGhpc1xuICB9XG5cbiAgLyoqXG4gICAqIEBwcml2YXRlXG4gICAqL1xuICByZXNldCgpe1xuICAgIHRoaXMuY2FuY2VsUmVmRXZlbnQoKVxuICAgIHRoaXMucmVmID0gbnVsbFxuICAgIHRoaXMucmVmRXZlbnQgPSBudWxsXG4gICAgdGhpcy5yZWNlaXZlZFJlc3AgPSBudWxsXG4gICAgdGhpcy5zZW50ID0gZmFsc2VcbiAgfVxuXG4gIC8qKlxuICAgKiBAcHJpdmF0ZVxuICAgKi9cbiAgbWF0Y2hSZWNlaXZlKHtzdGF0dXMsIHJlc3BvbnNlLCBfcmVmfSl7XG4gICAgdGhpcy5yZWNIb29rcy5maWx0ZXIoaCA9PiBoLnN0YXR1cyA9PT0gc3RhdHVzKVxuICAgICAgLmZvckVhY2goaCA9PiBoLmNhbGxiYWNrKHJlc3BvbnNlKSlcbiAgfVxuXG4gIC8qKlxuICAgKiBAcHJpdmF0ZVxuICAgKi9cbiAgY2FuY2VsUmVmRXZlbnQoKXtcbiAgICBpZighdGhpcy5yZWZFdmVudCl7IHJldHVybiB9XG4gICAgdGhpcy5jaGFubmVsLm9mZih0aGlzLnJlZkV2ZW50KVxuICB9XG5cbiAgLyoqXG4gICAqIEBwcml2YXRlXG4gICAqL1xuICBjYW5jZWxUaW1lb3V0KCl7XG4gICAgY2xlYXJUaW1lb3V0KHRoaXMudGltZW91dFRpbWVyKVxuICAgIHRoaXMudGltZW91dFRpbWVyID0gbnVsbFxuICB9XG5cbiAgLyoqXG4gICAqIEBwcml2YXRlXG4gICAqL1xuICBzdGFydFRpbWVvdXQoKXtcbiAgICBpZih0aGlzLnRpbWVvdXRUaW1lcil7IHRoaXMuY2FuY2VsVGltZW91dCgpIH1cbiAgICB0aGlzLnJlZiA9IHRoaXMuY2hhbm5lbC5zb2NrZXQubWFrZVJlZigpXG4gICAgdGhpcy5yZWZFdmVudCA9IHRoaXMuY2hhbm5lbC5yZXBseUV2ZW50TmFtZSh0aGlzLnJlZilcblxuICAgIHRoaXMuY2hhbm5lbC5vbih0aGlzLnJlZkV2ZW50LCBwYXlsb2FkID0+IHtcbiAgICAgIHRoaXMuY2FuY2VsUmVmRXZlbnQoKVxuICAgICAgdGhpcy5jYW5jZWxUaW1lb3V0KClcbiAgICAgIHRoaXMucmVjZWl2ZWRSZXNwID0gcGF5bG9hZFxuICAgICAgdGhpcy5tYXRjaFJlY2VpdmUocGF5bG9hZClcbiAgICB9KVxuXG4gICAgdGhpcy50aW1lb3V0VGltZXIgPSBzZXRUaW1lb3V0KCgpID0+IHtcbiAgICAgIHRoaXMudHJpZ2dlcihcInRpbWVvdXRcIiwge30pXG4gICAgfSwgdGhpcy50aW1lb3V0KVxuICB9XG5cbiAgLyoqXG4gICAqIEBwcml2YXRlXG4gICAqL1xuICBoYXNSZWNlaXZlZChzdGF0dXMpe1xuICAgIHJldHVybiB0aGlzLnJlY2VpdmVkUmVzcCAmJiB0aGlzLnJlY2VpdmVkUmVzcC5zdGF0dXMgPT09IHN0YXR1c1xuICB9XG5cbiAgLyoqXG4gICAqIEBwcml2YXRlXG4gICAqL1xuICB0cmlnZ2VyKHN0YXR1cywgcmVzcG9uc2Upe1xuICAgIHRoaXMuY2hhbm5lbC50cmlnZ2VyKHRoaXMucmVmRXZlbnQsIHtzdGF0dXMsIHJlc3BvbnNlfSlcbiAgfVxufVxuIiwgIi8qKlxuICpcbiAqIENyZWF0ZXMgYSB0aW1lciB0aGF0IGFjY2VwdHMgYSBgdGltZXJDYWxjYCBmdW5jdGlvbiB0byBwZXJmb3JtXG4gKiBjYWxjdWxhdGVkIHRpbWVvdXQgcmV0cmllcywgc3VjaCBhcyBleHBvbmVudGlhbCBiYWNrb2ZmLlxuICpcbiAqIEBleGFtcGxlXG4gKiBsZXQgcmVjb25uZWN0VGltZXIgPSBuZXcgVGltZXIoKCkgPT4gdGhpcy5jb25uZWN0KCksIGZ1bmN0aW9uKHRyaWVzKXtcbiAqICAgcmV0dXJuIFsxMDAwLCA1MDAwLCAxMDAwMF1bdHJpZXMgLSAxXSB8fCAxMDAwMFxuICogfSlcbiAqIHJlY29ubmVjdFRpbWVyLnNjaGVkdWxlVGltZW91dCgpIC8vIGZpcmVzIGFmdGVyIDEwMDBcbiAqIHJlY29ubmVjdFRpbWVyLnNjaGVkdWxlVGltZW91dCgpIC8vIGZpcmVzIGFmdGVyIDUwMDBcbiAqIHJlY29ubmVjdFRpbWVyLnJlc2V0KClcbiAqIHJlY29ubmVjdFRpbWVyLnNjaGVkdWxlVGltZW91dCgpIC8vIGZpcmVzIGFmdGVyIDEwMDBcbiAqXG4gKiBAcGFyYW0ge0Z1bmN0aW9ufSBjYWxsYmFja1xuICogQHBhcmFtIHtGdW5jdGlvbn0gdGltZXJDYWxjXG4gKi9cbmV4cG9ydCBkZWZhdWx0IGNsYXNzIFRpbWVyIHtcbiAgY29uc3RydWN0b3IoY2FsbGJhY2ssIHRpbWVyQ2FsYyl7XG4gICAgdGhpcy5jYWxsYmFjayA9IGNhbGxiYWNrXG4gICAgdGhpcy50aW1lckNhbGMgPSB0aW1lckNhbGNcbiAgICB0aGlzLnRpbWVyID0gbnVsbFxuICAgIHRoaXMudHJpZXMgPSAwXG4gIH1cblxuICByZXNldCgpe1xuICAgIHRoaXMudHJpZXMgPSAwXG4gICAgY2xlYXJUaW1lb3V0KHRoaXMudGltZXIpXG4gIH1cblxuICAvKipcbiAgICogQ2FuY2VscyBhbnkgcHJldmlvdXMgc2NoZWR1bGVUaW1lb3V0IGFuZCBzY2hlZHVsZXMgY2FsbGJhY2tcbiAgICovXG4gIHNjaGVkdWxlVGltZW91dCgpe1xuICAgIGNsZWFyVGltZW91dCh0aGlzLnRpbWVyKVxuXG4gICAgdGhpcy50aW1lciA9IHNldFRpbWVvdXQoKCkgPT4ge1xuICAgICAgdGhpcy50cmllcyA9IHRoaXMudHJpZXMgKyAxXG4gICAgICB0aGlzLmNhbGxiYWNrKClcbiAgICB9LCB0aGlzLnRpbWVyQ2FsYyh0aGlzLnRyaWVzICsgMSkpXG4gIH1cbn1cbiIsICJpbXBvcnQge2Nsb3N1cmV9IGZyb20gXCIuL3V0aWxzXCJcbmltcG9ydCB7XG4gIENIQU5ORUxfRVZFTlRTLFxuICBDSEFOTkVMX1NUQVRFUyxcbn0gZnJvbSBcIi4vY29uc3RhbnRzXCJcblxuaW1wb3J0IFB1c2ggZnJvbSBcIi4vcHVzaFwiXG5pbXBvcnQgVGltZXIgZnJvbSBcIi4vdGltZXJcIlxuXG4vKipcbiAqXG4gKiBAcGFyYW0ge3N0cmluZ30gdG9waWNcbiAqIEBwYXJhbSB7KE9iamVjdHxmdW5jdGlvbil9IHBhcmFtc1xuICogQHBhcmFtIHtTb2NrZXR9IHNvY2tldFxuICovXG5leHBvcnQgZGVmYXVsdCBjbGFzcyBDaGFubmVsIHtcbiAgY29uc3RydWN0b3IodG9waWMsIHBhcmFtcywgc29ja2V0KXtcbiAgICB0aGlzLnN0YXRlID0gQ0hBTk5FTF9TVEFURVMuY2xvc2VkXG4gICAgdGhpcy50b3BpYyA9IHRvcGljXG4gICAgdGhpcy5wYXJhbXMgPSBjbG9zdXJlKHBhcmFtcyB8fCB7fSlcbiAgICB0aGlzLnNvY2tldCA9IHNvY2tldFxuICAgIHRoaXMuYmluZGluZ3MgPSBbXVxuICAgIHRoaXMuYmluZGluZ1JlZiA9IDBcbiAgICB0aGlzLnRpbWVvdXQgPSB0aGlzLnNvY2tldC50aW1lb3V0XG4gICAgdGhpcy5qb2luZWRPbmNlID0gZmFsc2VcbiAgICB0aGlzLmpvaW5QdXNoID0gbmV3IFB1c2godGhpcywgQ0hBTk5FTF9FVkVOVFMuam9pbiwgdGhpcy5wYXJhbXMsIHRoaXMudGltZW91dClcbiAgICB0aGlzLnB1c2hCdWZmZXIgPSBbXVxuICAgIHRoaXMuc3RhdGVDaGFuZ2VSZWZzID0gW11cblxuICAgIHRoaXMucmVqb2luVGltZXIgPSBuZXcgVGltZXIoKCkgPT4ge1xuICAgICAgaWYodGhpcy5zb2NrZXQuaXNDb25uZWN0ZWQoKSl7IHRoaXMucmVqb2luKCkgfVxuICAgIH0sIHRoaXMuc29ja2V0LnJlam9pbkFmdGVyTXMpXG4gICAgdGhpcy5zdGF0ZUNoYW5nZVJlZnMucHVzaCh0aGlzLnNvY2tldC5vbkVycm9yKCgpID0+IHRoaXMucmVqb2luVGltZXIucmVzZXQoKSkpXG4gICAgdGhpcy5zdGF0ZUNoYW5nZVJlZnMucHVzaCh0aGlzLnNvY2tldC5vbk9wZW4oKCkgPT4ge1xuICAgICAgdGhpcy5yZWpvaW5UaW1lci5yZXNldCgpXG4gICAgICBpZih0aGlzLmlzRXJyb3JlZCgpKXsgdGhpcy5yZWpvaW4oKSB9XG4gICAgfSlcbiAgICApXG4gICAgdGhpcy5qb2luUHVzaC5yZWNlaXZlKFwib2tcIiwgKCkgPT4ge1xuICAgICAgdGhpcy5zdGF0ZSA9IENIQU5ORUxfU1RBVEVTLmpvaW5lZFxuICAgICAgdGhpcy5yZWpvaW5UaW1lci5yZXNldCgpXG4gICAgICB0aGlzLnB1c2hCdWZmZXIuZm9yRWFjaChwdXNoRXZlbnQgPT4gcHVzaEV2ZW50LnNlbmQoKSlcbiAgICAgIHRoaXMucHVzaEJ1ZmZlciA9IFtdXG4gICAgfSlcbiAgICB0aGlzLmpvaW5QdXNoLnJlY2VpdmUoXCJlcnJvclwiLCAoKSA9PiB7XG4gICAgICB0aGlzLnN0YXRlID0gQ0hBTk5FTF9TVEFURVMuZXJyb3JlZFxuICAgICAgaWYodGhpcy5zb2NrZXQuaXNDb25uZWN0ZWQoKSl7IHRoaXMucmVqb2luVGltZXIuc2NoZWR1bGVUaW1lb3V0KCkgfVxuICAgIH0pXG4gICAgdGhpcy5vbkNsb3NlKCgpID0+IHtcbiAgICAgIHRoaXMucmVqb2luVGltZXIucmVzZXQoKVxuICAgICAgaWYodGhpcy5zb2NrZXQuaGFzTG9nZ2VyKCkpIHRoaXMuc29ja2V0LmxvZyhcImNoYW5uZWxcIiwgYGNsb3NlICR7dGhpcy50b3BpY30gJHt0aGlzLmpvaW5SZWYoKX1gKVxuICAgICAgdGhpcy5zdGF0ZSA9IENIQU5ORUxfU1RBVEVTLmNsb3NlZFxuICAgICAgdGhpcy5zb2NrZXQucmVtb3ZlKHRoaXMpXG4gICAgfSlcbiAgICB0aGlzLm9uRXJyb3IocmVhc29uID0+IHtcbiAgICAgIGlmKHRoaXMuc29ja2V0Lmhhc0xvZ2dlcigpKSB0aGlzLnNvY2tldC5sb2coXCJjaGFubmVsXCIsIGBlcnJvciAke3RoaXMudG9waWN9YCwgcmVhc29uKVxuICAgICAgaWYodGhpcy5pc0pvaW5pbmcoKSl7IHRoaXMuam9pblB1c2gucmVzZXQoKSB9XG4gICAgICB0aGlzLnN0YXRlID0gQ0hBTk5FTF9TVEFURVMuZXJyb3JlZFxuICAgICAgaWYodGhpcy5zb2NrZXQuaXNDb25uZWN0ZWQoKSl7IHRoaXMucmVqb2luVGltZXIuc2NoZWR1bGVUaW1lb3V0KCkgfVxuICAgIH0pXG4gICAgdGhpcy5qb2luUHVzaC5yZWNlaXZlKFwidGltZW91dFwiLCAoKSA9PiB7XG4gICAgICBpZih0aGlzLnNvY2tldC5oYXNMb2dnZXIoKSkgdGhpcy5zb2NrZXQubG9nKFwiY2hhbm5lbFwiLCBgdGltZW91dCAke3RoaXMudG9waWN9ICgke3RoaXMuam9pblJlZigpfSlgLCB0aGlzLmpvaW5QdXNoLnRpbWVvdXQpXG4gICAgICBsZXQgbGVhdmVQdXNoID0gbmV3IFB1c2godGhpcywgQ0hBTk5FTF9FVkVOVFMubGVhdmUsIGNsb3N1cmUoe30pLCB0aGlzLnRpbWVvdXQpXG4gICAgICBsZWF2ZVB1c2guc2VuZCgpXG4gICAgICB0aGlzLnN0YXRlID0gQ0hBTk5FTF9TVEFURVMuZXJyb3JlZFxuICAgICAgdGhpcy5qb2luUHVzaC5yZXNldCgpXG4gICAgICBpZih0aGlzLnNvY2tldC5pc0Nvbm5lY3RlZCgpKXsgdGhpcy5yZWpvaW5UaW1lci5zY2hlZHVsZVRpbWVvdXQoKSB9XG4gICAgfSlcbiAgICB0aGlzLm9uKENIQU5ORUxfRVZFTlRTLnJlcGx5LCAocGF5bG9hZCwgcmVmKSA9PiB7XG4gICAgICB0aGlzLnRyaWdnZXIodGhpcy5yZXBseUV2ZW50TmFtZShyZWYpLCBwYXlsb2FkKVxuICAgIH0pXG4gIH1cblxuICAvKipcbiAgICogSm9pbiB0aGUgY2hhbm5lbFxuICAgKiBAcGFyYW0ge2ludGVnZXJ9IHRpbWVvdXRcbiAgICogQHJldHVybnMge1B1c2h9XG4gICAqL1xuICBqb2luKHRpbWVvdXQgPSB0aGlzLnRpbWVvdXQpe1xuICAgIGlmKHRoaXMuam9pbmVkT25jZSl7XG4gICAgICB0aHJvdyBuZXcgRXJyb3IoXCJ0cmllZCB0byBqb2luIG11bHRpcGxlIHRpbWVzLiAnam9pbicgY2FuIG9ubHkgYmUgY2FsbGVkIGEgc2luZ2xlIHRpbWUgcGVyIGNoYW5uZWwgaW5zdGFuY2VcIilcbiAgICB9IGVsc2Uge1xuICAgICAgdGhpcy50aW1lb3V0ID0gdGltZW91dFxuICAgICAgdGhpcy5qb2luZWRPbmNlID0gdHJ1ZVxuICAgICAgdGhpcy5yZWpvaW4oKVxuICAgICAgcmV0dXJuIHRoaXMuam9pblB1c2hcbiAgICB9XG4gIH1cblxuICAvKipcbiAgICogSG9vayBpbnRvIGNoYW5uZWwgY2xvc2VcbiAgICogQHBhcmFtIHtGdW5jdGlvbn0gY2FsbGJhY2tcbiAgICovXG4gIG9uQ2xvc2UoY2FsbGJhY2spe1xuICAgIHRoaXMub24oQ0hBTk5FTF9FVkVOVFMuY2xvc2UsIGNhbGxiYWNrKVxuICB9XG5cbiAgLyoqXG4gICAqIEhvb2sgaW50byBjaGFubmVsIGVycm9yc1xuICAgKiBAcGFyYW0ge0Z1bmN0aW9ufSBjYWxsYmFja1xuICAgKi9cbiAgb25FcnJvcihjYWxsYmFjayl7XG4gICAgcmV0dXJuIHRoaXMub24oQ0hBTk5FTF9FVkVOVFMuZXJyb3IsIHJlYXNvbiA9PiBjYWxsYmFjayhyZWFzb24pKVxuICB9XG5cbiAgLyoqXG4gICAqIFN1YnNjcmliZXMgb24gY2hhbm5lbCBldmVudHNcbiAgICpcbiAgICogU3Vic2NyaXB0aW9uIHJldHVybnMgYSByZWYgY291bnRlciwgd2hpY2ggY2FuIGJlIHVzZWQgbGF0ZXIgdG9cbiAgICogdW5zdWJzY3JpYmUgdGhlIGV4YWN0IGV2ZW50IGxpc3RlbmVyXG4gICAqXG4gICAqIEBleGFtcGxlXG4gICAqIGNvbnN0IHJlZjEgPSBjaGFubmVsLm9uKFwiZXZlbnRcIiwgZG9fc3R1ZmYpXG4gICAqIGNvbnN0IHJlZjIgPSBjaGFubmVsLm9uKFwiZXZlbnRcIiwgZG9fb3RoZXJfc3R1ZmYpXG4gICAqIGNoYW5uZWwub2ZmKFwiZXZlbnRcIiwgcmVmMSlcbiAgICogLy8gU2luY2UgdW5zdWJzY3JpcHRpb24sIGRvX3N0dWZmIHdvbid0IGZpcmUsXG4gICAqIC8vIHdoaWxlIGRvX290aGVyX3N0dWZmIHdpbGwga2VlcCBmaXJpbmcgb24gdGhlIFwiZXZlbnRcIlxuICAgKlxuICAgKiBAcGFyYW0ge3N0cmluZ30gZXZlbnRcbiAgICogQHBhcmFtIHtGdW5jdGlvbn0gY2FsbGJhY2tcbiAgICogQHJldHVybnMge2ludGVnZXJ9IHJlZlxuICAgKi9cbiAgb24oZXZlbnQsIGNhbGxiYWNrKXtcbiAgICBsZXQgcmVmID0gdGhpcy5iaW5kaW5nUmVmKytcbiAgICB0aGlzLmJpbmRpbmdzLnB1c2goe2V2ZW50LCByZWYsIGNhbGxiYWNrfSlcbiAgICByZXR1cm4gcmVmXG4gIH1cblxuICAvKipcbiAgICogVW5zdWJzY3JpYmVzIG9mZiBvZiBjaGFubmVsIGV2ZW50c1xuICAgKlxuICAgKiBVc2UgdGhlIHJlZiByZXR1cm5lZCBmcm9tIGEgY2hhbm5lbC5vbigpIHRvIHVuc3Vic2NyaWJlIG9uZVxuICAgKiBoYW5kbGVyLCBvciBwYXNzIG5vdGhpbmcgZm9yIHRoZSByZWYgdG8gdW5zdWJzY3JpYmUgYWxsXG4gICAqIGhhbmRsZXJzIGZvciB0aGUgZ2l2ZW4gZXZlbnQuXG4gICAqXG4gICAqIEBleGFtcGxlXG4gICAqIC8vIFVuc3Vic2NyaWJlIHRoZSBkb19zdHVmZiBoYW5kbGVyXG4gICAqIGNvbnN0IHJlZjEgPSBjaGFubmVsLm9uKFwiZXZlbnRcIiwgZG9fc3R1ZmYpXG4gICAqIGNoYW5uZWwub2ZmKFwiZXZlbnRcIiwgcmVmMSlcbiAgICpcbiAgICogLy8gVW5zdWJzY3JpYmUgYWxsIGhhbmRsZXJzIGZyb20gZXZlbnRcbiAgICogY2hhbm5lbC5vZmYoXCJldmVudFwiKVxuICAgKlxuICAgKiBAcGFyYW0ge3N0cmluZ30gZXZlbnRcbiAgICogQHBhcmFtIHtpbnRlZ2VyfSByZWZcbiAgICovXG4gIG9mZihldmVudCwgcmVmKXtcbiAgICB0aGlzLmJpbmRpbmdzID0gdGhpcy5iaW5kaW5ncy5maWx0ZXIoKGJpbmQpID0+IHtcbiAgICAgIHJldHVybiAhKGJpbmQuZXZlbnQgPT09IGV2ZW50ICYmICh0eXBlb2YgcmVmID09PSBcInVuZGVmaW5lZFwiIHx8IHJlZiA9PT0gYmluZC5yZWYpKVxuICAgIH0pXG4gIH1cblxuICAvKipcbiAgICogQHByaXZhdGVcbiAgICovXG4gIGNhblB1c2goKXsgcmV0dXJuIHRoaXMuc29ja2V0LmlzQ29ubmVjdGVkKCkgJiYgdGhpcy5pc0pvaW5lZCgpIH1cblxuICAvKipcbiAgICogU2VuZHMgYSBtZXNzYWdlIGBldmVudGAgdG8gcGhvZW5peCB3aXRoIHRoZSBwYXlsb2FkIGBwYXlsb2FkYC5cbiAgICogUGhvZW5peCByZWNlaXZlcyB0aGlzIGluIHRoZSBgaGFuZGxlX2luKGV2ZW50LCBwYXlsb2FkLCBzb2NrZXQpYFxuICAgKiBmdW5jdGlvbi4gaWYgcGhvZW5peCByZXBsaWVzIG9yIGl0IHRpbWVzIG91dCAoZGVmYXVsdCAxMDAwMG1zKSxcbiAgICogdGhlbiBvcHRpb25hbGx5IHRoZSByZXBseSBjYW4gYmUgcmVjZWl2ZWQuXG4gICAqXG4gICAqIEBleGFtcGxlXG4gICAqIGNoYW5uZWwucHVzaChcImV2ZW50XCIpXG4gICAqICAgLnJlY2VpdmUoXCJva1wiLCBwYXlsb2FkID0+IGNvbnNvbGUubG9nKFwicGhvZW5peCByZXBsaWVkOlwiLCBwYXlsb2FkKSlcbiAgICogICAucmVjZWl2ZShcImVycm9yXCIsIGVyciA9PiBjb25zb2xlLmxvZyhcInBob2VuaXggZXJyb3JlZFwiLCBlcnIpKVxuICAgKiAgIC5yZWNlaXZlKFwidGltZW91dFwiLCAoKSA9PiBjb25zb2xlLmxvZyhcInRpbWVkIG91dCBwdXNoaW5nXCIpKVxuICAgKiBAcGFyYW0ge3N0cmluZ30gZXZlbnRcbiAgICogQHBhcmFtIHtPYmplY3R9IHBheWxvYWRcbiAgICogQHBhcmFtIHtudW1iZXJ9IFt0aW1lb3V0XVxuICAgKiBAcmV0dXJucyB7UHVzaH1cbiAgICovXG4gIHB1c2goZXZlbnQsIHBheWxvYWQsIHRpbWVvdXQgPSB0aGlzLnRpbWVvdXQpe1xuICAgIHBheWxvYWQgPSBwYXlsb2FkIHx8IHt9XG4gICAgaWYoIXRoaXMuam9pbmVkT25jZSl7XG4gICAgICB0aHJvdyBuZXcgRXJyb3IoYHRyaWVkIHRvIHB1c2ggJyR7ZXZlbnR9JyB0byAnJHt0aGlzLnRvcGljfScgYmVmb3JlIGpvaW5pbmcuIFVzZSBjaGFubmVsLmpvaW4oKSBiZWZvcmUgcHVzaGluZyBldmVudHNgKVxuICAgIH1cbiAgICBsZXQgcHVzaEV2ZW50ID0gbmV3IFB1c2godGhpcywgZXZlbnQsIGZ1bmN0aW9uICgpeyByZXR1cm4gcGF5bG9hZCB9LCB0aW1lb3V0KVxuICAgIGlmKHRoaXMuY2FuUHVzaCgpKXtcbiAgICAgIHB1c2hFdmVudC5zZW5kKClcbiAgICB9IGVsc2Uge1xuICAgICAgcHVzaEV2ZW50LnN0YXJ0VGltZW91dCgpXG4gICAgICB0aGlzLnB1c2hCdWZmZXIucHVzaChwdXNoRXZlbnQpXG4gICAgfVxuXG4gICAgcmV0dXJuIHB1c2hFdmVudFxuICB9XG5cbiAgLyoqIExlYXZlcyB0aGUgY2hhbm5lbFxuICAgKlxuICAgKiBVbnN1YnNjcmliZXMgZnJvbSBzZXJ2ZXIgZXZlbnRzLCBhbmRcbiAgICogaW5zdHJ1Y3RzIGNoYW5uZWwgdG8gdGVybWluYXRlIG9uIHNlcnZlclxuICAgKlxuICAgKiBUcmlnZ2VycyBvbkNsb3NlKCkgaG9va3NcbiAgICpcbiAgICogVG8gcmVjZWl2ZSBsZWF2ZSBhY2tub3dsZWRnZW1lbnRzLCB1c2UgdGhlIGByZWNlaXZlYFxuICAgKiBob29rIHRvIGJpbmQgdG8gdGhlIHNlcnZlciBhY2ssIGllOlxuICAgKlxuICAgKiBAZXhhbXBsZVxuICAgKiBjaGFubmVsLmxlYXZlKCkucmVjZWl2ZShcIm9rXCIsICgpID0+IGFsZXJ0KFwibGVmdCFcIikgKVxuICAgKlxuICAgKiBAcGFyYW0ge2ludGVnZXJ9IHRpbWVvdXRcbiAgICogQHJldHVybnMge1B1c2h9XG4gICAqL1xuICBsZWF2ZSh0aW1lb3V0ID0gdGhpcy50aW1lb3V0KXtcbiAgICB0aGlzLnJlam9pblRpbWVyLnJlc2V0KClcbiAgICB0aGlzLmpvaW5QdXNoLmNhbmNlbFRpbWVvdXQoKVxuXG4gICAgdGhpcy5zdGF0ZSA9IENIQU5ORUxfU1RBVEVTLmxlYXZpbmdcbiAgICBsZXQgb25DbG9zZSA9ICgpID0+IHtcbiAgICAgIGlmKHRoaXMuc29ja2V0Lmhhc0xvZ2dlcigpKSB0aGlzLnNvY2tldC5sb2coXCJjaGFubmVsXCIsIGBsZWF2ZSAke3RoaXMudG9waWN9YClcbiAgICAgIHRoaXMudHJpZ2dlcihDSEFOTkVMX0VWRU5UUy5jbG9zZSwgXCJsZWF2ZVwiKVxuICAgIH1cbiAgICBsZXQgbGVhdmVQdXNoID0gbmV3IFB1c2godGhpcywgQ0hBTk5FTF9FVkVOVFMubGVhdmUsIGNsb3N1cmUoe30pLCB0aW1lb3V0KVxuICAgIGxlYXZlUHVzaC5yZWNlaXZlKFwib2tcIiwgKCkgPT4gb25DbG9zZSgpKVxuICAgICAgLnJlY2VpdmUoXCJ0aW1lb3V0XCIsICgpID0+IG9uQ2xvc2UoKSlcbiAgICBsZWF2ZVB1c2guc2VuZCgpXG4gICAgaWYoIXRoaXMuY2FuUHVzaCgpKXsgbGVhdmVQdXNoLnRyaWdnZXIoXCJva1wiLCB7fSkgfVxuXG4gICAgcmV0dXJuIGxlYXZlUHVzaFxuICB9XG5cbiAgLyoqXG4gICAqIE92ZXJyaWRhYmxlIG1lc3NhZ2UgaG9va1xuICAgKlxuICAgKiBSZWNlaXZlcyBhbGwgZXZlbnRzIGZvciBzcGVjaWFsaXplZCBtZXNzYWdlIGhhbmRsaW5nXG4gICAqIGJlZm9yZSBkaXNwYXRjaGluZyB0byB0aGUgY2hhbm5lbCBjYWxsYmFja3MuXG4gICAqXG4gICAqIE11c3QgcmV0dXJuIHRoZSBwYXlsb2FkLCBtb2RpZmllZCBvciB1bm1vZGlmaWVkXG4gICAqIEBwYXJhbSB7c3RyaW5nfSBldmVudFxuICAgKiBAcGFyYW0ge09iamVjdH0gcGF5bG9hZFxuICAgKiBAcGFyYW0ge2ludGVnZXJ9IHJlZlxuICAgKiBAcmV0dXJucyB7T2JqZWN0fVxuICAgKi9cbiAgb25NZXNzYWdlKF9ldmVudCwgcGF5bG9hZCwgX3JlZil7IHJldHVybiBwYXlsb2FkIH1cblxuICAvKipcbiAgICogQHByaXZhdGVcbiAgICovXG4gIGlzTWVtYmVyKHRvcGljLCBldmVudCwgcGF5bG9hZCwgam9pblJlZil7XG4gICAgaWYodGhpcy50b3BpYyAhPT0gdG9waWMpeyByZXR1cm4gZmFsc2UgfVxuXG4gICAgaWYoam9pblJlZiAmJiBqb2luUmVmICE9PSB0aGlzLmpvaW5SZWYoKSl7XG4gICAgICBpZih0aGlzLnNvY2tldC5oYXNMb2dnZXIoKSkgdGhpcy5zb2NrZXQubG9nKFwiY2hhbm5lbFwiLCBcImRyb3BwaW5nIG91dGRhdGVkIG1lc3NhZ2VcIiwge3RvcGljLCBldmVudCwgcGF5bG9hZCwgam9pblJlZn0pXG4gICAgICByZXR1cm4gZmFsc2VcbiAgICB9IGVsc2Uge1xuICAgICAgcmV0dXJuIHRydWVcbiAgICB9XG4gIH1cblxuICAvKipcbiAgICogQHByaXZhdGVcbiAgICovXG4gIGpvaW5SZWYoKXsgcmV0dXJuIHRoaXMuam9pblB1c2gucmVmIH1cblxuICAvKipcbiAgICogQHByaXZhdGVcbiAgICovXG4gIHJlam9pbih0aW1lb3V0ID0gdGhpcy50aW1lb3V0KXtcbiAgICBpZih0aGlzLmlzTGVhdmluZygpKXsgcmV0dXJuIH1cbiAgICB0aGlzLnNvY2tldC5sZWF2ZU9wZW5Ub3BpYyh0aGlzLnRvcGljKVxuICAgIHRoaXMuc3RhdGUgPSBDSEFOTkVMX1NUQVRFUy5qb2luaW5nXG4gICAgdGhpcy5qb2luUHVzaC5yZXNlbmQodGltZW91dClcbiAgfVxuXG4gIC8qKlxuICAgKiBAcHJpdmF0ZVxuICAgKi9cbiAgdHJpZ2dlcihldmVudCwgcGF5bG9hZCwgcmVmLCBqb2luUmVmKXtcbiAgICBsZXQgaGFuZGxlZFBheWxvYWQgPSB0aGlzLm9uTWVzc2FnZShldmVudCwgcGF5bG9hZCwgcmVmLCBqb2luUmVmKVxuICAgIGlmKHBheWxvYWQgJiYgIWhhbmRsZWRQYXlsb2FkKXsgdGhyb3cgbmV3IEVycm9yKFwiY2hhbm5lbCBvbk1lc3NhZ2UgY2FsbGJhY2tzIG11c3QgcmV0dXJuIHRoZSBwYXlsb2FkLCBtb2RpZmllZCBvciB1bm1vZGlmaWVkXCIpIH1cblxuICAgIGxldCBldmVudEJpbmRpbmdzID0gdGhpcy5iaW5kaW5ncy5maWx0ZXIoYmluZCA9PiBiaW5kLmV2ZW50ID09PSBldmVudClcblxuICAgIGZvcihsZXQgaSA9IDA7IGkgPCBldmVudEJpbmRpbmdzLmxlbmd0aDsgaSsrKXtcbiAgICAgIGxldCBiaW5kID0gZXZlbnRCaW5kaW5nc1tpXVxuICAgICAgYmluZC5jYWxsYmFjayhoYW5kbGVkUGF5bG9hZCwgcmVmLCBqb2luUmVmIHx8IHRoaXMuam9pblJlZigpKVxuICAgIH1cbiAgfVxuXG4gIC8qKlxuICAgKiBAcHJpdmF0ZVxuICAgKi9cbiAgcmVwbHlFdmVudE5hbWUocmVmKXsgcmV0dXJuIGBjaGFuX3JlcGx5XyR7cmVmfWAgfVxuXG4gIC8qKlxuICAgKiBAcHJpdmF0ZVxuICAgKi9cbiAgaXNDbG9zZWQoKXsgcmV0dXJuIHRoaXMuc3RhdGUgPT09IENIQU5ORUxfU1RBVEVTLmNsb3NlZCB9XG5cbiAgLyoqXG4gICAqIEBwcml2YXRlXG4gICAqL1xuICBpc0Vycm9yZWQoKXsgcmV0dXJuIHRoaXMuc3RhdGUgPT09IENIQU5ORUxfU1RBVEVTLmVycm9yZWQgfVxuXG4gIC8qKlxuICAgKiBAcHJpdmF0ZVxuICAgKi9cbiAgaXNKb2luZWQoKXsgcmV0dXJuIHRoaXMuc3RhdGUgPT09IENIQU5ORUxfU1RBVEVTLmpvaW5lZCB9XG5cbiAgLyoqXG4gICAqIEBwcml2YXRlXG4gICAqL1xuICBpc0pvaW5pbmcoKXsgcmV0dXJuIHRoaXMuc3RhdGUgPT09IENIQU5ORUxfU1RBVEVTLmpvaW5pbmcgfVxuXG4gIC8qKlxuICAgKiBAcHJpdmF0ZVxuICAgKi9cbiAgaXNMZWF2aW5nKCl7IHJldHVybiB0aGlzLnN0YXRlID09PSBDSEFOTkVMX1NUQVRFUy5sZWF2aW5nIH1cbn1cbiIsICJpbXBvcnQge1xuICBnbG9iYWwsXG4gIFhIUl9TVEFURVNcbn0gZnJvbSBcIi4vY29uc3RhbnRzXCJcblxuZXhwb3J0IGRlZmF1bHQgY2xhc3MgQWpheCB7XG5cbiAgc3RhdGljIHJlcXVlc3QobWV0aG9kLCBlbmRQb2ludCwgYWNjZXB0LCBib2R5LCB0aW1lb3V0LCBvbnRpbWVvdXQsIGNhbGxiYWNrKXtcbiAgICBpZihnbG9iYWwuWERvbWFpblJlcXVlc3Qpe1xuICAgICAgbGV0IHJlcSA9IG5ldyBnbG9iYWwuWERvbWFpblJlcXVlc3QoKSAvLyBJRTgsIElFOVxuICAgICAgcmV0dXJuIHRoaXMueGRvbWFpblJlcXVlc3QocmVxLCBtZXRob2QsIGVuZFBvaW50LCBib2R5LCB0aW1lb3V0LCBvbnRpbWVvdXQsIGNhbGxiYWNrKVxuICAgIH0gZWxzZSB7XG4gICAgICBsZXQgcmVxID0gbmV3IGdsb2JhbC5YTUxIdHRwUmVxdWVzdCgpIC8vIElFNyssIEZpcmVmb3gsIENocm9tZSwgT3BlcmEsIFNhZmFyaVxuICAgICAgcmV0dXJuIHRoaXMueGhyUmVxdWVzdChyZXEsIG1ldGhvZCwgZW5kUG9pbnQsIGFjY2VwdCwgYm9keSwgdGltZW91dCwgb250aW1lb3V0LCBjYWxsYmFjaylcbiAgICB9XG4gIH1cblxuICBzdGF0aWMgeGRvbWFpblJlcXVlc3QocmVxLCBtZXRob2QsIGVuZFBvaW50LCBib2R5LCB0aW1lb3V0LCBvbnRpbWVvdXQsIGNhbGxiYWNrKXtcbiAgICByZXEudGltZW91dCA9IHRpbWVvdXRcbiAgICByZXEub3BlbihtZXRob2QsIGVuZFBvaW50KVxuICAgIHJlcS5vbmxvYWQgPSAoKSA9PiB7XG4gICAgICBsZXQgcmVzcG9uc2UgPSB0aGlzLnBhcnNlSlNPTihyZXEucmVzcG9uc2VUZXh0KVxuICAgICAgY2FsbGJhY2sgJiYgY2FsbGJhY2socmVzcG9uc2UpXG4gICAgfVxuICAgIGlmKG9udGltZW91dCl7IHJlcS5vbnRpbWVvdXQgPSBvbnRpbWVvdXQgfVxuXG4gICAgLy8gV29yayBhcm91bmQgYnVnIGluIElFOSB0aGF0IHJlcXVpcmVzIGFuIGF0dGFjaGVkIG9ucHJvZ3Jlc3MgaGFuZGxlclxuICAgIHJlcS5vbnByb2dyZXNzID0gKCkgPT4geyB9XG5cbiAgICByZXEuc2VuZChib2R5KVxuICAgIHJldHVybiByZXFcbiAgfVxuXG4gIHN0YXRpYyB4aHJSZXF1ZXN0KHJlcSwgbWV0aG9kLCBlbmRQb2ludCwgYWNjZXB0LCBib2R5LCB0aW1lb3V0LCBvbnRpbWVvdXQsIGNhbGxiYWNrKXtcbiAgICByZXEub3BlbihtZXRob2QsIGVuZFBvaW50LCB0cnVlKVxuICAgIHJlcS50aW1lb3V0ID0gdGltZW91dFxuICAgIHJlcS5zZXRSZXF1ZXN0SGVhZGVyKFwiQ29udGVudC1UeXBlXCIsIGFjY2VwdClcbiAgICByZXEub25lcnJvciA9ICgpID0+IGNhbGxiYWNrICYmIGNhbGxiYWNrKG51bGwpXG4gICAgcmVxLm9ucmVhZHlzdGF0ZWNoYW5nZSA9ICgpID0+IHtcbiAgICAgIGlmKHJlcS5yZWFkeVN0YXRlID09PSBYSFJfU1RBVEVTLmNvbXBsZXRlICYmIGNhbGxiYWNrKXtcbiAgICAgICAgbGV0IHJlc3BvbnNlID0gdGhpcy5wYXJzZUpTT04ocmVxLnJlc3BvbnNlVGV4dClcbiAgICAgICAgY2FsbGJhY2socmVzcG9uc2UpXG4gICAgICB9XG4gICAgfVxuICAgIGlmKG9udGltZW91dCl7IHJlcS5vbnRpbWVvdXQgPSBvbnRpbWVvdXQgfVxuXG4gICAgcmVxLnNlbmQoYm9keSlcbiAgICByZXR1cm4gcmVxXG4gIH1cblxuICBzdGF0aWMgcGFyc2VKU09OKHJlc3Ape1xuICAgIGlmKCFyZXNwIHx8IHJlc3AgPT09IFwiXCIpeyByZXR1cm4gbnVsbCB9XG5cbiAgICB0cnkge1xuICAgICAgcmV0dXJuIEpTT04ucGFyc2UocmVzcClcbiAgICB9IGNhdGNoIChlKXtcbiAgICAgIGNvbnNvbGUgJiYgY29uc29sZS5sb2coXCJmYWlsZWQgdG8gcGFyc2UgSlNPTiByZXNwb25zZVwiLCByZXNwKVxuICAgICAgcmV0dXJuIG51bGxcbiAgICB9XG4gIH1cblxuICBzdGF0aWMgc2VyaWFsaXplKG9iaiwgcGFyZW50S2V5KXtcbiAgICBsZXQgcXVlcnlTdHIgPSBbXVxuICAgIGZvcih2YXIga2V5IGluIG9iail7XG4gICAgICBpZighT2JqZWN0LnByb3RvdHlwZS5oYXNPd25Qcm9wZXJ0eS5jYWxsKG9iaiwga2V5KSl7IGNvbnRpbnVlIH1cbiAgICAgIGxldCBwYXJhbUtleSA9IHBhcmVudEtleSA/IGAke3BhcmVudEtleX1bJHtrZXl9XWAgOiBrZXlcbiAgICAgIGxldCBwYXJhbVZhbCA9IG9ialtrZXldXG4gICAgICBpZih0eXBlb2YgcGFyYW1WYWwgPT09IFwib2JqZWN0XCIpe1xuICAgICAgICBxdWVyeVN0ci5wdXNoKHRoaXMuc2VyaWFsaXplKHBhcmFtVmFsLCBwYXJhbUtleSkpXG4gICAgICB9IGVsc2Uge1xuICAgICAgICBxdWVyeVN0ci5wdXNoKGVuY29kZVVSSUNvbXBvbmVudChwYXJhbUtleSkgKyBcIj1cIiArIGVuY29kZVVSSUNvbXBvbmVudChwYXJhbVZhbCkpXG4gICAgICB9XG4gICAgfVxuICAgIHJldHVybiBxdWVyeVN0ci5qb2luKFwiJlwiKVxuICB9XG5cbiAgc3RhdGljIGFwcGVuZFBhcmFtcyh1cmwsIHBhcmFtcyl7XG4gICAgaWYoT2JqZWN0LmtleXMocGFyYW1zKS5sZW5ndGggPT09IDApeyByZXR1cm4gdXJsIH1cblxuICAgIGxldCBwcmVmaXggPSB1cmwubWF0Y2goL1xcPy8pID8gXCImXCIgOiBcIj9cIlxuICAgIHJldHVybiBgJHt1cmx9JHtwcmVmaXh9JHt0aGlzLnNlcmlhbGl6ZShwYXJhbXMpfWBcbiAgfVxufVxuIiwgImltcG9ydCB7XG4gIFNPQ0tFVF9TVEFURVMsXG4gIFRSQU5TUE9SVFNcbn0gZnJvbSBcIi4vY29uc3RhbnRzXCJcblxuaW1wb3J0IEFqYXggZnJvbSBcIi4vYWpheFwiXG5cbmxldCBhcnJheUJ1ZmZlclRvQmFzZTY0ID0gKGJ1ZmZlcikgPT4ge1xuICBsZXQgYmluYXJ5ID0gXCJcIlxuICBsZXQgYnl0ZXMgPSBuZXcgVWludDhBcnJheShidWZmZXIpXG4gIGxldCBsZW4gPSBieXRlcy5ieXRlTGVuZ3RoXG4gIGZvcihsZXQgaSA9IDA7IGkgPCBsZW47IGkrKyl7IGJpbmFyeSArPSBTdHJpbmcuZnJvbUNoYXJDb2RlKGJ5dGVzW2ldKSB9XG4gIHJldHVybiBidG9hKGJpbmFyeSlcbn1cblxuZXhwb3J0IGRlZmF1bHQgY2xhc3MgTG9uZ1BvbGwge1xuXG4gIGNvbnN0cnVjdG9yKGVuZFBvaW50KXtcbiAgICB0aGlzLmVuZFBvaW50ID0gbnVsbFxuICAgIHRoaXMudG9rZW4gPSBudWxsXG4gICAgdGhpcy5za2lwSGVhcnRiZWF0ID0gdHJ1ZVxuICAgIHRoaXMucmVxcyA9IG5ldyBTZXQoKVxuICAgIHRoaXMuYXdhaXRpbmdCYXRjaEFjayA9IGZhbHNlXG4gICAgdGhpcy5jdXJyZW50QmF0Y2ggPSBudWxsXG4gICAgdGhpcy5jdXJyZW50QmF0Y2hUaW1lciA9IG51bGxcbiAgICB0aGlzLmJhdGNoQnVmZmVyID0gW11cbiAgICB0aGlzLm9ub3BlbiA9IGZ1bmN0aW9uICgpeyB9IC8vIG5vb3BcbiAgICB0aGlzLm9uZXJyb3IgPSBmdW5jdGlvbiAoKXsgfSAvLyBub29wXG4gICAgdGhpcy5vbm1lc3NhZ2UgPSBmdW5jdGlvbiAoKXsgfSAvLyBub29wXG4gICAgdGhpcy5vbmNsb3NlID0gZnVuY3Rpb24gKCl7IH0gLy8gbm9vcFxuICAgIHRoaXMucG9sbEVuZHBvaW50ID0gdGhpcy5ub3JtYWxpemVFbmRwb2ludChlbmRQb2ludClcbiAgICB0aGlzLnJlYWR5U3RhdGUgPSBTT0NLRVRfU1RBVEVTLmNvbm5lY3RpbmdcbiAgICAvLyB3ZSBtdXN0IHdhaXQgZm9yIHRoZSBjYWxsZXIgdG8gZmluaXNoIHNldHRpbmcgdXAgb3VyIGNhbGxiYWNrcyBhbmQgdGltZW91dCBwcm9wZXJ0aWVzXG4gICAgc2V0VGltZW91dCgoKSA9PiB0aGlzLnBvbGwoKSwgMClcbiAgfVxuXG4gIG5vcm1hbGl6ZUVuZHBvaW50KGVuZFBvaW50KXtcbiAgICByZXR1cm4gKGVuZFBvaW50XG4gICAgICAucmVwbGFjZShcIndzOi8vXCIsIFwiaHR0cDovL1wiKVxuICAgICAgLnJlcGxhY2UoXCJ3c3M6Ly9cIiwgXCJodHRwczovL1wiKVxuICAgICAgLnJlcGxhY2UobmV3IFJlZ0V4cChcIiguKilcXC9cIiArIFRSQU5TUE9SVFMud2Vic29ja2V0KSwgXCIkMS9cIiArIFRSQU5TUE9SVFMubG9uZ3BvbGwpKVxuICB9XG5cbiAgZW5kcG9pbnRVUkwoKXtcbiAgICByZXR1cm4gQWpheC5hcHBlbmRQYXJhbXModGhpcy5wb2xsRW5kcG9pbnQsIHt0b2tlbjogdGhpcy50b2tlbn0pXG4gIH1cblxuICBjbG9zZUFuZFJldHJ5KGNvZGUsIHJlYXNvbiwgd2FzQ2xlYW4pe1xuICAgIHRoaXMuY2xvc2UoY29kZSwgcmVhc29uLCB3YXNDbGVhbilcbiAgICB0aGlzLnJlYWR5U3RhdGUgPSBTT0NLRVRfU1RBVEVTLmNvbm5lY3RpbmdcbiAgfVxuXG4gIG9udGltZW91dCgpe1xuICAgIHRoaXMub25lcnJvcihcInRpbWVvdXRcIilcbiAgICB0aGlzLmNsb3NlQW5kUmV0cnkoMTAwNSwgXCJ0aW1lb3V0XCIsIGZhbHNlKVxuICB9XG5cbiAgaXNBY3RpdmUoKXsgcmV0dXJuIHRoaXMucmVhZHlTdGF0ZSA9PT0gU09DS0VUX1NUQVRFUy5vcGVuIHx8IHRoaXMucmVhZHlTdGF0ZSA9PT0gU09DS0VUX1NUQVRFUy5jb25uZWN0aW5nIH1cblxuICBwb2xsKCl7XG4gICAgdGhpcy5hamF4KFwiR0VUXCIsIFwiYXBwbGljYXRpb24vanNvblwiLCBudWxsLCAoKSA9PiB0aGlzLm9udGltZW91dCgpLCByZXNwID0+IHtcbiAgICAgIGlmKHJlc3Ape1xuICAgICAgICB2YXIge3N0YXR1cywgdG9rZW4sIG1lc3NhZ2VzfSA9IHJlc3BcbiAgICAgICAgdGhpcy50b2tlbiA9IHRva2VuXG4gICAgICB9IGVsc2Uge1xuICAgICAgICBzdGF0dXMgPSAwXG4gICAgICB9XG5cbiAgICAgIHN3aXRjaChzdGF0dXMpe1xuICAgICAgICBjYXNlIDIwMDpcbiAgICAgICAgICBtZXNzYWdlcy5mb3JFYWNoKG1zZyA9PiB7XG4gICAgICAgICAgICAvLyBUYXNrcyBhcmUgd2hhdCB0aGluZ3MgbGlrZSBldmVudCBoYW5kbGVycywgc2V0VGltZW91dCBjYWxsYmFja3MsXG4gICAgICAgICAgICAvLyBwcm9taXNlIHJlc29sdmVzIGFuZCBtb3JlIGFyZSBydW4gd2l0aGluLlxuICAgICAgICAgICAgLy8gSW4gbW9kZXJuIGJyb3dzZXJzLCB0aGVyZSBhcmUgdHdvIGRpZmZlcmVudCBraW5kcyBvZiB0YXNrcyxcbiAgICAgICAgICAgIC8vIG1pY3JvdGFza3MgYW5kIG1hY3JvdGFza3MuXG4gICAgICAgICAgICAvLyBNaWNyb3Rhc2tzIGFyZSBtYWlubHkgdXNlZCBmb3IgUHJvbWlzZXMsIHdoaWxlIG1hY3JvdGFza3MgYXJlXG4gICAgICAgICAgICAvLyB1c2VkIGZvciBldmVyeXRoaW5nIGVsc2UuXG4gICAgICAgICAgICAvLyBNaWNyb3Rhc2tzIGFsd2F5cyBoYXZlIHByaW9yaXR5IG92ZXIgbWFjcm90YXNrcy4gSWYgdGhlIEpTIGVuZ2luZVxuICAgICAgICAgICAgLy8gaXMgbG9va2luZyBmb3IgYSB0YXNrIHRvIHJ1biwgaXQgd2lsbCBhbHdheXMgdHJ5IHRvIGVtcHR5IHRoZVxuICAgICAgICAgICAgLy8gbWljcm90YXNrIHF1ZXVlIGJlZm9yZSBhdHRlbXB0aW5nIHRvIHJ1biBhbnl0aGluZyBmcm9tIHRoZVxuICAgICAgICAgICAgLy8gbWFjcm90YXNrIHF1ZXVlLlxuICAgICAgICAgICAgLy9cbiAgICAgICAgICAgIC8vIEZvciB0aGUgV2ViU29ja2V0IHRyYW5zcG9ydCwgbWVzc2FnZXMgYWx3YXlzIGFycml2ZSBpbiB0aGVpciBvd25cbiAgICAgICAgICAgIC8vIGV2ZW50LiBUaGlzIG1lYW5zIHRoYXQgaWYgYW55IHByb21pc2VzIGFyZSByZXNvbHZlZCBmcm9tIHdpdGhpbixcbiAgICAgICAgICAgIC8vIHRoZWlyIGNhbGxiYWNrcyB3aWxsIGFsd2F5cyBmaW5pc2ggZXhlY3V0aW9uIGJ5IHRoZSB0aW1lIHRoZVxuICAgICAgICAgICAgLy8gbmV4dCBtZXNzYWdlIGV2ZW50IGhhbmRsZXIgaXMgcnVuLlxuICAgICAgICAgICAgLy9cbiAgICAgICAgICAgIC8vIEluIG9yZGVyIHRvIGVtdWxhdGUgdGhpcyBiZWhhdmlvdXIsIHdlIG5lZWQgdG8gbWFrZSBzdXJlIGVhY2hcbiAgICAgICAgICAgIC8vIG9ubWVzc2FnZSBoYW5kbGVyIGlzIHJ1biB3aXRoaW4gaXRzIG93biBtYWNyb3Rhc2suXG4gICAgICAgICAgICBzZXRUaW1lb3V0KCgpID0+IHRoaXMub25tZXNzYWdlKHtkYXRhOiBtc2d9KSwgMClcbiAgICAgICAgICB9KVxuICAgICAgICAgIHRoaXMucG9sbCgpXG4gICAgICAgICAgYnJlYWtcbiAgICAgICAgY2FzZSAyMDQ6XG4gICAgICAgICAgdGhpcy5wb2xsKClcbiAgICAgICAgICBicmVha1xuICAgICAgICBjYXNlIDQxMDpcbiAgICAgICAgICB0aGlzLnJlYWR5U3RhdGUgPSBTT0NLRVRfU1RBVEVTLm9wZW5cbiAgICAgICAgICB0aGlzLm9ub3Blbih7fSlcbiAgICAgICAgICB0aGlzLnBvbGwoKVxuICAgICAgICAgIGJyZWFrXG4gICAgICAgIGNhc2UgNDAzOlxuICAgICAgICAgIHRoaXMub25lcnJvcig0MDMpXG4gICAgICAgICAgdGhpcy5jbG9zZSgxMDA4LCBcImZvcmJpZGRlblwiLCBmYWxzZSlcbiAgICAgICAgICBicmVha1xuICAgICAgICBjYXNlIDA6XG4gICAgICAgIGNhc2UgNTAwOlxuICAgICAgICAgIHRoaXMub25lcnJvcig1MDApXG4gICAgICAgICAgdGhpcy5jbG9zZUFuZFJldHJ5KDEwMTEsIFwiaW50ZXJuYWwgc2VydmVyIGVycm9yXCIsIDUwMClcbiAgICAgICAgICBicmVha1xuICAgICAgICBkZWZhdWx0OiB0aHJvdyBuZXcgRXJyb3IoYHVuaGFuZGxlZCBwb2xsIHN0YXR1cyAke3N0YXR1c31gKVxuICAgICAgfVxuICAgIH0pXG4gIH1cblxuICAvLyB3ZSBjb2xsZWN0IGFsbCBwdXNoZXMgd2l0aGluIHRoZSBjdXJyZW50IGV2ZW50IGxvb3AgYnlcbiAgLy8gc2V0VGltZW91dCAwLCB3aGljaCBvcHRpbWl6ZXMgYmFjay10by1iYWNrIHByb2NlZHVyYWxcbiAgLy8gcHVzaGVzIGFnYWluc3QgYW4gZW1wdHkgYnVmZmVyXG5cbiAgc2VuZChib2R5KXtcbiAgICBpZih0eXBlb2YoYm9keSkgIT09IFwic3RyaW5nXCIpeyBib2R5ID0gYXJyYXlCdWZmZXJUb0Jhc2U2NChib2R5KSB9XG4gICAgaWYodGhpcy5jdXJyZW50QmF0Y2gpe1xuICAgICAgdGhpcy5jdXJyZW50QmF0Y2gucHVzaChib2R5KVxuICAgIH0gZWxzZSBpZih0aGlzLmF3YWl0aW5nQmF0Y2hBY2spe1xuICAgICAgdGhpcy5iYXRjaEJ1ZmZlci5wdXNoKGJvZHkpXG4gICAgfSBlbHNlIHtcbiAgICAgIHRoaXMuY3VycmVudEJhdGNoID0gW2JvZHldXG4gICAgICB0aGlzLmN1cnJlbnRCYXRjaFRpbWVyID0gc2V0VGltZW91dCgoKSA9PiB7XG4gICAgICAgIHRoaXMuYmF0Y2hTZW5kKHRoaXMuY3VycmVudEJhdGNoKVxuICAgICAgICB0aGlzLmN1cnJlbnRCYXRjaCA9IG51bGxcbiAgICAgIH0sIDApXG4gICAgfVxuICB9XG5cbiAgYmF0Y2hTZW5kKG1lc3NhZ2VzKXtcbiAgICB0aGlzLmF3YWl0aW5nQmF0Y2hBY2sgPSB0cnVlXG4gICAgdGhpcy5hamF4KFwiUE9TVFwiLCBcImFwcGxpY2F0aW9uL3gtbmRqc29uXCIsIG1lc3NhZ2VzLmpvaW4oXCJcXG5cIiksICgpID0+IHRoaXMub25lcnJvcihcInRpbWVvdXRcIiksIHJlc3AgPT4ge1xuICAgICAgdGhpcy5hd2FpdGluZ0JhdGNoQWNrID0gZmFsc2VcbiAgICAgIGlmKCFyZXNwIHx8IHJlc3Auc3RhdHVzICE9PSAyMDApe1xuICAgICAgICB0aGlzLm9uZXJyb3IocmVzcCAmJiByZXNwLnN0YXR1cylcbiAgICAgICAgdGhpcy5jbG9zZUFuZFJldHJ5KDEwMTEsIFwiaW50ZXJuYWwgc2VydmVyIGVycm9yXCIsIGZhbHNlKVxuICAgICAgfSBlbHNlIGlmKHRoaXMuYmF0Y2hCdWZmZXIubGVuZ3RoID4gMCl7XG4gICAgICAgIHRoaXMuYmF0Y2hTZW5kKHRoaXMuYmF0Y2hCdWZmZXIpXG4gICAgICAgIHRoaXMuYmF0Y2hCdWZmZXIgPSBbXVxuICAgICAgfVxuICAgIH0pXG4gIH1cblxuICBjbG9zZShjb2RlLCByZWFzb24sIHdhc0NsZWFuKXtcbiAgICBmb3IobGV0IHJlcSBvZiB0aGlzLnJlcXMpeyByZXEuYWJvcnQoKSB9XG4gICAgdGhpcy5yZWFkeVN0YXRlID0gU09DS0VUX1NUQVRFUy5jbG9zZWRcbiAgICBsZXQgb3B0cyA9IE9iamVjdC5hc3NpZ24oe2NvZGU6IDEwMDAsIHJlYXNvbjogdW5kZWZpbmVkLCB3YXNDbGVhbjogdHJ1ZX0sIHtjb2RlLCByZWFzb24sIHdhc0NsZWFufSlcbiAgICB0aGlzLmJhdGNoQnVmZmVyID0gW11cbiAgICBjbGVhclRpbWVvdXQodGhpcy5jdXJyZW50QmF0Y2hUaW1lcilcbiAgICB0aGlzLmN1cnJlbnRCYXRjaFRpbWVyID0gbnVsbFxuICAgIGlmKHR5cGVvZihDbG9zZUV2ZW50KSAhPT0gXCJ1bmRlZmluZWRcIil7XG4gICAgICB0aGlzLm9uY2xvc2UobmV3IENsb3NlRXZlbnQoXCJjbG9zZVwiLCBvcHRzKSlcbiAgICB9IGVsc2Uge1xuICAgICAgdGhpcy5vbmNsb3NlKG9wdHMpXG4gICAgfVxuICB9XG5cbiAgYWpheChtZXRob2QsIGNvbnRlbnRUeXBlLCBib2R5LCBvbkNhbGxlclRpbWVvdXQsIGNhbGxiYWNrKXtcbiAgICBsZXQgcmVxXG4gICAgbGV0IG9udGltZW91dCA9ICgpID0+IHtcbiAgICAgIHRoaXMucmVxcy5kZWxldGUocmVxKVxuICAgICAgb25DYWxsZXJUaW1lb3V0KClcbiAgICB9XG4gICAgcmVxID0gQWpheC5yZXF1ZXN0KG1ldGhvZCwgdGhpcy5lbmRwb2ludFVSTCgpLCBjb250ZW50VHlwZSwgYm9keSwgdGhpcy50aW1lb3V0LCBvbnRpbWVvdXQsIHJlc3AgPT4ge1xuICAgICAgdGhpcy5yZXFzLmRlbGV0ZShyZXEpXG4gICAgICBpZih0aGlzLmlzQWN0aXZlKCkpeyBjYWxsYmFjayhyZXNwKSB9XG4gICAgfSlcbiAgICB0aGlzLnJlcXMuYWRkKHJlcSlcbiAgfVxufVxuIiwgIi8qKlxuICogSW5pdGlhbGl6ZXMgdGhlIFByZXNlbmNlXG4gKiBAcGFyYW0ge0NoYW5uZWx9IGNoYW5uZWwgLSBUaGUgQ2hhbm5lbFxuICogQHBhcmFtIHtPYmplY3R9IG9wdHMgLSBUaGUgb3B0aW9ucyxcbiAqICAgICAgICBmb3IgZXhhbXBsZSBge2V2ZW50czoge3N0YXRlOiBcInN0YXRlXCIsIGRpZmY6IFwiZGlmZlwifX1gXG4gKi9cbmV4cG9ydCBkZWZhdWx0IGNsYXNzIFByZXNlbmNlIHtcblxuICBjb25zdHJ1Y3RvcihjaGFubmVsLCBvcHRzID0ge30pe1xuICAgIGxldCBldmVudHMgPSBvcHRzLmV2ZW50cyB8fCB7c3RhdGU6IFwicHJlc2VuY2Vfc3RhdGVcIiwgZGlmZjogXCJwcmVzZW5jZV9kaWZmXCJ9XG4gICAgdGhpcy5zdGF0ZSA9IHt9XG4gICAgdGhpcy5wZW5kaW5nRGlmZnMgPSBbXVxuICAgIHRoaXMuY2hhbm5lbCA9IGNoYW5uZWxcbiAgICB0aGlzLmpvaW5SZWYgPSBudWxsXG4gICAgdGhpcy5jYWxsZXIgPSB7XG4gICAgICBvbkpvaW46IGZ1bmN0aW9uICgpeyB9LFxuICAgICAgb25MZWF2ZTogZnVuY3Rpb24gKCl7IH0sXG4gICAgICBvblN5bmM6IGZ1bmN0aW9uICgpeyB9XG4gICAgfVxuXG4gICAgdGhpcy5jaGFubmVsLm9uKGV2ZW50cy5zdGF0ZSwgbmV3U3RhdGUgPT4ge1xuICAgICAgbGV0IHtvbkpvaW4sIG9uTGVhdmUsIG9uU3luY30gPSB0aGlzLmNhbGxlclxuXG4gICAgICB0aGlzLmpvaW5SZWYgPSB0aGlzLmNoYW5uZWwuam9pblJlZigpXG4gICAgICB0aGlzLnN0YXRlID0gUHJlc2VuY2Uuc3luY1N0YXRlKHRoaXMuc3RhdGUsIG5ld1N0YXRlLCBvbkpvaW4sIG9uTGVhdmUpXG5cbiAgICAgIHRoaXMucGVuZGluZ0RpZmZzLmZvckVhY2goZGlmZiA9PiB7XG4gICAgICAgIHRoaXMuc3RhdGUgPSBQcmVzZW5jZS5zeW5jRGlmZih0aGlzLnN0YXRlLCBkaWZmLCBvbkpvaW4sIG9uTGVhdmUpXG4gICAgICB9KVxuICAgICAgdGhpcy5wZW5kaW5nRGlmZnMgPSBbXVxuICAgICAgb25TeW5jKClcbiAgICB9KVxuXG4gICAgdGhpcy5jaGFubmVsLm9uKGV2ZW50cy5kaWZmLCBkaWZmID0+IHtcbiAgICAgIGxldCB7b25Kb2luLCBvbkxlYXZlLCBvblN5bmN9ID0gdGhpcy5jYWxsZXJcblxuICAgICAgaWYodGhpcy5pblBlbmRpbmdTeW5jU3RhdGUoKSl7XG4gICAgICAgIHRoaXMucGVuZGluZ0RpZmZzLnB1c2goZGlmZilcbiAgICAgIH0gZWxzZSB7XG4gICAgICAgIHRoaXMuc3RhdGUgPSBQcmVzZW5jZS5zeW5jRGlmZih0aGlzLnN0YXRlLCBkaWZmLCBvbkpvaW4sIG9uTGVhdmUpXG4gICAgICAgIG9uU3luYygpXG4gICAgICB9XG4gICAgfSlcbiAgfVxuXG4gIG9uSm9pbihjYWxsYmFjayl7IHRoaXMuY2FsbGVyLm9uSm9pbiA9IGNhbGxiYWNrIH1cblxuICBvbkxlYXZlKGNhbGxiYWNrKXsgdGhpcy5jYWxsZXIub25MZWF2ZSA9IGNhbGxiYWNrIH1cblxuICBvblN5bmMoY2FsbGJhY2speyB0aGlzLmNhbGxlci5vblN5bmMgPSBjYWxsYmFjayB9XG5cbiAgbGlzdChieSl7IHJldHVybiBQcmVzZW5jZS5saXN0KHRoaXMuc3RhdGUsIGJ5KSB9XG5cbiAgaW5QZW5kaW5nU3luY1N0YXRlKCl7XG4gICAgcmV0dXJuICF0aGlzLmpvaW5SZWYgfHwgKHRoaXMuam9pblJlZiAhPT0gdGhpcy5jaGFubmVsLmpvaW5SZWYoKSlcbiAgfVxuXG4gIC8vIGxvd2VyLWxldmVsIHB1YmxpYyBzdGF0aWMgQVBJXG5cbiAgLyoqXG4gICAqIFVzZWQgdG8gc3luYyB0aGUgbGlzdCBvZiBwcmVzZW5jZXMgb24gdGhlIHNlcnZlclxuICAgKiB3aXRoIHRoZSBjbGllbnQncyBzdGF0ZS4gQW4gb3B0aW9uYWwgYG9uSm9pbmAgYW5kIGBvbkxlYXZlYCBjYWxsYmFjayBjYW5cbiAgICogYmUgcHJvdmlkZWQgdG8gcmVhY3QgdG8gY2hhbmdlcyBpbiB0aGUgY2xpZW50J3MgbG9jYWwgcHJlc2VuY2VzIGFjcm9zc1xuICAgKiBkaXNjb25uZWN0cyBhbmQgcmVjb25uZWN0cyB3aXRoIHRoZSBzZXJ2ZXIuXG4gICAqXG4gICAqIEByZXR1cm5zIHtQcmVzZW5jZX1cbiAgICovXG4gIHN0YXRpYyBzeW5jU3RhdGUoY3VycmVudFN0YXRlLCBuZXdTdGF0ZSwgb25Kb2luLCBvbkxlYXZlKXtcbiAgICBsZXQgc3RhdGUgPSB0aGlzLmNsb25lKGN1cnJlbnRTdGF0ZSlcbiAgICBsZXQgam9pbnMgPSB7fVxuICAgIGxldCBsZWF2ZXMgPSB7fVxuXG4gICAgdGhpcy5tYXAoc3RhdGUsIChrZXksIHByZXNlbmNlKSA9PiB7XG4gICAgICBpZighbmV3U3RhdGVba2V5XSl7XG4gICAgICAgIGxlYXZlc1trZXldID0gcHJlc2VuY2VcbiAgICAgIH1cbiAgICB9KVxuICAgIHRoaXMubWFwKG5ld1N0YXRlLCAoa2V5LCBuZXdQcmVzZW5jZSkgPT4ge1xuICAgICAgbGV0IGN1cnJlbnRQcmVzZW5jZSA9IHN0YXRlW2tleV1cbiAgICAgIGlmKGN1cnJlbnRQcmVzZW5jZSl7XG4gICAgICAgIGxldCBuZXdSZWZzID0gbmV3UHJlc2VuY2UubWV0YXMubWFwKG0gPT4gbS5waHhfcmVmKVxuICAgICAgICBsZXQgY3VyUmVmcyA9IGN1cnJlbnRQcmVzZW5jZS5tZXRhcy5tYXAobSA9PiBtLnBoeF9yZWYpXG4gICAgICAgIGxldCBqb2luZWRNZXRhcyA9IG5ld1ByZXNlbmNlLm1ldGFzLmZpbHRlcihtID0+IGN1clJlZnMuaW5kZXhPZihtLnBoeF9yZWYpIDwgMClcbiAgICAgICAgbGV0IGxlZnRNZXRhcyA9IGN1cnJlbnRQcmVzZW5jZS5tZXRhcy5maWx0ZXIobSA9PiBuZXdSZWZzLmluZGV4T2YobS5waHhfcmVmKSA8IDApXG4gICAgICAgIGlmKGpvaW5lZE1ldGFzLmxlbmd0aCA+IDApe1xuICAgICAgICAgIGpvaW5zW2tleV0gPSBuZXdQcmVzZW5jZVxuICAgICAgICAgIGpvaW5zW2tleV0ubWV0YXMgPSBqb2luZWRNZXRhc1xuICAgICAgICB9XG4gICAgICAgIGlmKGxlZnRNZXRhcy5sZW5ndGggPiAwKXtcbiAgICAgICAgICBsZWF2ZXNba2V5XSA9IHRoaXMuY2xvbmUoY3VycmVudFByZXNlbmNlKVxuICAgICAgICAgIGxlYXZlc1trZXldLm1ldGFzID0gbGVmdE1ldGFzXG4gICAgICAgIH1cbiAgICAgIH0gZWxzZSB7XG4gICAgICAgIGpvaW5zW2tleV0gPSBuZXdQcmVzZW5jZVxuICAgICAgfVxuICAgIH0pXG4gICAgcmV0dXJuIHRoaXMuc3luY0RpZmYoc3RhdGUsIHtqb2luczogam9pbnMsIGxlYXZlczogbGVhdmVzfSwgb25Kb2luLCBvbkxlYXZlKVxuICB9XG5cbiAgLyoqXG4gICAqXG4gICAqIFVzZWQgdG8gc3luYyBhIGRpZmYgb2YgcHJlc2VuY2Ugam9pbiBhbmQgbGVhdmVcbiAgICogZXZlbnRzIGZyb20gdGhlIHNlcnZlciwgYXMgdGhleSBoYXBwZW4uIExpa2UgYHN5bmNTdGF0ZWAsIGBzeW5jRGlmZmBcbiAgICogYWNjZXB0cyBvcHRpb25hbCBgb25Kb2luYCBhbmQgYG9uTGVhdmVgIGNhbGxiYWNrcyB0byByZWFjdCB0byBhIHVzZXJcbiAgICogam9pbmluZyBvciBsZWF2aW5nIGZyb20gYSBkZXZpY2UuXG4gICAqXG4gICAqIEByZXR1cm5zIHtQcmVzZW5jZX1cbiAgICovXG4gIHN0YXRpYyBzeW5jRGlmZihzdGF0ZSwgZGlmZiwgb25Kb2luLCBvbkxlYXZlKXtcbiAgICBsZXQge2pvaW5zLCBsZWF2ZXN9ID0gdGhpcy5jbG9uZShkaWZmKVxuICAgIGlmKCFvbkpvaW4peyBvbkpvaW4gPSBmdW5jdGlvbiAoKXsgfSB9XG4gICAgaWYoIW9uTGVhdmUpeyBvbkxlYXZlID0gZnVuY3Rpb24gKCl7IH0gfVxuXG4gICAgdGhpcy5tYXAoam9pbnMsIChrZXksIG5ld1ByZXNlbmNlKSA9PiB7XG4gICAgICBsZXQgY3VycmVudFByZXNlbmNlID0gc3RhdGVba2V5XVxuICAgICAgc3RhdGVba2V5XSA9IHRoaXMuY2xvbmUobmV3UHJlc2VuY2UpXG4gICAgICBpZihjdXJyZW50UHJlc2VuY2Upe1xuICAgICAgICBsZXQgam9pbmVkUmVmcyA9IHN0YXRlW2tleV0ubWV0YXMubWFwKG0gPT4gbS5waHhfcmVmKVxuICAgICAgICBsZXQgY3VyTWV0YXMgPSBjdXJyZW50UHJlc2VuY2UubWV0YXMuZmlsdGVyKG0gPT4gam9pbmVkUmVmcy5pbmRleE9mKG0ucGh4X3JlZikgPCAwKVxuICAgICAgICBzdGF0ZVtrZXldLm1ldGFzLnVuc2hpZnQoLi4uY3VyTWV0YXMpXG4gICAgICB9XG4gICAgICBvbkpvaW4oa2V5LCBjdXJyZW50UHJlc2VuY2UsIG5ld1ByZXNlbmNlKVxuICAgIH0pXG4gICAgdGhpcy5tYXAobGVhdmVzLCAoa2V5LCBsZWZ0UHJlc2VuY2UpID0+IHtcbiAgICAgIGxldCBjdXJyZW50UHJlc2VuY2UgPSBzdGF0ZVtrZXldXG4gICAgICBpZighY3VycmVudFByZXNlbmNlKXsgcmV0dXJuIH1cbiAgICAgIGxldCByZWZzVG9SZW1vdmUgPSBsZWZ0UHJlc2VuY2UubWV0YXMubWFwKG0gPT4gbS5waHhfcmVmKVxuICAgICAgY3VycmVudFByZXNlbmNlLm1ldGFzID0gY3VycmVudFByZXNlbmNlLm1ldGFzLmZpbHRlcihwID0+IHtcbiAgICAgICAgcmV0dXJuIHJlZnNUb1JlbW92ZS5pbmRleE9mKHAucGh4X3JlZikgPCAwXG4gICAgICB9KVxuICAgICAgb25MZWF2ZShrZXksIGN1cnJlbnRQcmVzZW5jZSwgbGVmdFByZXNlbmNlKVxuICAgICAgaWYoY3VycmVudFByZXNlbmNlLm1ldGFzLmxlbmd0aCA9PT0gMCl7XG4gICAgICAgIGRlbGV0ZSBzdGF0ZVtrZXldXG4gICAgICB9XG4gICAgfSlcbiAgICByZXR1cm4gc3RhdGVcbiAgfVxuXG4gIC8qKlxuICAgKiBSZXR1cm5zIHRoZSBhcnJheSBvZiBwcmVzZW5jZXMsIHdpdGggc2VsZWN0ZWQgbWV0YWRhdGEuXG4gICAqXG4gICAqIEBwYXJhbSB7T2JqZWN0fSBwcmVzZW5jZXNcbiAgICogQHBhcmFtIHtGdW5jdGlvbn0gY2hvb3NlclxuICAgKlxuICAgKiBAcmV0dXJucyB7UHJlc2VuY2V9XG4gICAqL1xuICBzdGF0aWMgbGlzdChwcmVzZW5jZXMsIGNob29zZXIpe1xuICAgIGlmKCFjaG9vc2VyKXsgY2hvb3NlciA9IGZ1bmN0aW9uIChrZXksIHByZXMpeyByZXR1cm4gcHJlcyB9IH1cblxuICAgIHJldHVybiB0aGlzLm1hcChwcmVzZW5jZXMsIChrZXksIHByZXNlbmNlKSA9PiB7XG4gICAgICByZXR1cm4gY2hvb3NlcihrZXksIHByZXNlbmNlKVxuICAgIH0pXG4gIH1cblxuICAvLyBwcml2YXRlXG5cbiAgc3RhdGljIG1hcChvYmosIGZ1bmMpe1xuICAgIHJldHVybiBPYmplY3QuZ2V0T3duUHJvcGVydHlOYW1lcyhvYmopLm1hcChrZXkgPT4gZnVuYyhrZXksIG9ialtrZXldKSlcbiAgfVxuXG4gIHN0YXRpYyBjbG9uZShvYmopeyByZXR1cm4gSlNPTi5wYXJzZShKU09OLnN0cmluZ2lmeShvYmopKSB9XG59XG4iLCAiLyogVGhlIGRlZmF1bHQgc2VyaWFsaXplciBmb3IgZW5jb2RpbmcgYW5kIGRlY29kaW5nIG1lc3NhZ2VzICovXG5pbXBvcnQge1xuICBDSEFOTkVMX0VWRU5UU1xufSBmcm9tIFwiLi9jb25zdGFudHNcIlxuXG5leHBvcnQgZGVmYXVsdCB7XG4gIEhFQURFUl9MRU5HVEg6IDEsXG4gIE1FVEFfTEVOR1RIOiA0LFxuICBLSU5EUzoge3B1c2g6IDAsIHJlcGx5OiAxLCBicm9hZGNhc3Q6IDJ9LFxuXG4gIGVuY29kZShtc2csIGNhbGxiYWNrKXtcbiAgICBpZihtc2cucGF5bG9hZC5jb25zdHJ1Y3RvciA9PT0gQXJyYXlCdWZmZXIpe1xuICAgICAgcmV0dXJuIGNhbGxiYWNrKHRoaXMuYmluYXJ5RW5jb2RlKG1zZykpXG4gICAgfSBlbHNlIHtcbiAgICAgIGxldCBwYXlsb2FkID0gW21zZy5qb2luX3JlZiwgbXNnLnJlZiwgbXNnLnRvcGljLCBtc2cuZXZlbnQsIG1zZy5wYXlsb2FkXVxuICAgICAgcmV0dXJuIGNhbGxiYWNrKEpTT04uc3RyaW5naWZ5KHBheWxvYWQpKVxuICAgIH1cbiAgfSxcblxuICBkZWNvZGUocmF3UGF5bG9hZCwgY2FsbGJhY2spe1xuICAgIGlmKHJhd1BheWxvYWQuY29uc3RydWN0b3IgPT09IEFycmF5QnVmZmVyKXtcbiAgICAgIHJldHVybiBjYWxsYmFjayh0aGlzLmJpbmFyeURlY29kZShyYXdQYXlsb2FkKSlcbiAgICB9IGVsc2Uge1xuICAgICAgbGV0IFtqb2luX3JlZiwgcmVmLCB0b3BpYywgZXZlbnQsIHBheWxvYWRdID0gSlNPTi5wYXJzZShyYXdQYXlsb2FkKVxuICAgICAgcmV0dXJuIGNhbGxiYWNrKHtqb2luX3JlZiwgcmVmLCB0b3BpYywgZXZlbnQsIHBheWxvYWR9KVxuICAgIH1cbiAgfSxcblxuICAvLyBwcml2YXRlXG5cbiAgYmluYXJ5RW5jb2RlKG1lc3NhZ2Upe1xuICAgIGxldCB7am9pbl9yZWYsIHJlZiwgZXZlbnQsIHRvcGljLCBwYXlsb2FkfSA9IG1lc3NhZ2VcbiAgICBsZXQgbWV0YUxlbmd0aCA9IHRoaXMuTUVUQV9MRU5HVEggKyBqb2luX3JlZi5sZW5ndGggKyByZWYubGVuZ3RoICsgdG9waWMubGVuZ3RoICsgZXZlbnQubGVuZ3RoXG4gICAgbGV0IGhlYWRlciA9IG5ldyBBcnJheUJ1ZmZlcih0aGlzLkhFQURFUl9MRU5HVEggKyBtZXRhTGVuZ3RoKVxuICAgIGxldCB2aWV3ID0gbmV3IERhdGFWaWV3KGhlYWRlcilcbiAgICBsZXQgb2Zmc2V0ID0gMFxuXG4gICAgdmlldy5zZXRVaW50OChvZmZzZXQrKywgdGhpcy5LSU5EUy5wdXNoKSAvLyBraW5kXG4gICAgdmlldy5zZXRVaW50OChvZmZzZXQrKywgam9pbl9yZWYubGVuZ3RoKVxuICAgIHZpZXcuc2V0VWludDgob2Zmc2V0KyssIHJlZi5sZW5ndGgpXG4gICAgdmlldy5zZXRVaW50OChvZmZzZXQrKywgdG9waWMubGVuZ3RoKVxuICAgIHZpZXcuc2V0VWludDgob2Zmc2V0KyssIGV2ZW50Lmxlbmd0aClcbiAgICBBcnJheS5mcm9tKGpvaW5fcmVmLCBjaGFyID0+IHZpZXcuc2V0VWludDgob2Zmc2V0KyssIGNoYXIuY2hhckNvZGVBdCgwKSkpXG4gICAgQXJyYXkuZnJvbShyZWYsIGNoYXIgPT4gdmlldy5zZXRVaW50OChvZmZzZXQrKywgY2hhci5jaGFyQ29kZUF0KDApKSlcbiAgICBBcnJheS5mcm9tKHRvcGljLCBjaGFyID0+IHZpZXcuc2V0VWludDgob2Zmc2V0KyssIGNoYXIuY2hhckNvZGVBdCgwKSkpXG4gICAgQXJyYXkuZnJvbShldmVudCwgY2hhciA9PiB2aWV3LnNldFVpbnQ4KG9mZnNldCsrLCBjaGFyLmNoYXJDb2RlQXQoMCkpKVxuXG4gICAgdmFyIGNvbWJpbmVkID0gbmV3IFVpbnQ4QXJyYXkoaGVhZGVyLmJ5dGVMZW5ndGggKyBwYXlsb2FkLmJ5dGVMZW5ndGgpXG4gICAgY29tYmluZWQuc2V0KG5ldyBVaW50OEFycmF5KGhlYWRlciksIDApXG4gICAgY29tYmluZWQuc2V0KG5ldyBVaW50OEFycmF5KHBheWxvYWQpLCBoZWFkZXIuYnl0ZUxlbmd0aClcblxuICAgIHJldHVybiBjb21iaW5lZC5idWZmZXJcbiAgfSxcblxuICBiaW5hcnlEZWNvZGUoYnVmZmVyKXtcbiAgICBsZXQgdmlldyA9IG5ldyBEYXRhVmlldyhidWZmZXIpXG4gICAgbGV0IGtpbmQgPSB2aWV3LmdldFVpbnQ4KDApXG4gICAgbGV0IGRlY29kZXIgPSBuZXcgVGV4dERlY29kZXIoKVxuICAgIHN3aXRjaChraW5kKXtcbiAgICAgIGNhc2UgdGhpcy5LSU5EUy5wdXNoOiByZXR1cm4gdGhpcy5kZWNvZGVQdXNoKGJ1ZmZlciwgdmlldywgZGVjb2RlcilcbiAgICAgIGNhc2UgdGhpcy5LSU5EUy5yZXBseTogcmV0dXJuIHRoaXMuZGVjb2RlUmVwbHkoYnVmZmVyLCB2aWV3LCBkZWNvZGVyKVxuICAgICAgY2FzZSB0aGlzLktJTkRTLmJyb2FkY2FzdDogcmV0dXJuIHRoaXMuZGVjb2RlQnJvYWRjYXN0KGJ1ZmZlciwgdmlldywgZGVjb2RlcilcbiAgICB9XG4gIH0sXG5cbiAgZGVjb2RlUHVzaChidWZmZXIsIHZpZXcsIGRlY29kZXIpe1xuICAgIGxldCBqb2luUmVmU2l6ZSA9IHZpZXcuZ2V0VWludDgoMSlcbiAgICBsZXQgdG9waWNTaXplID0gdmlldy5nZXRVaW50OCgyKVxuICAgIGxldCBldmVudFNpemUgPSB2aWV3LmdldFVpbnQ4KDMpXG4gICAgbGV0IG9mZnNldCA9IHRoaXMuSEVBREVSX0xFTkdUSCArIHRoaXMuTUVUQV9MRU5HVEggLSAxIC8vIHB1c2hlcyBoYXZlIG5vIHJlZlxuICAgIGxldCBqb2luUmVmID0gZGVjb2Rlci5kZWNvZGUoYnVmZmVyLnNsaWNlKG9mZnNldCwgb2Zmc2V0ICsgam9pblJlZlNpemUpKVxuICAgIG9mZnNldCA9IG9mZnNldCArIGpvaW5SZWZTaXplXG4gICAgbGV0IHRvcGljID0gZGVjb2Rlci5kZWNvZGUoYnVmZmVyLnNsaWNlKG9mZnNldCwgb2Zmc2V0ICsgdG9waWNTaXplKSlcbiAgICBvZmZzZXQgPSBvZmZzZXQgKyB0b3BpY1NpemVcbiAgICBsZXQgZXZlbnQgPSBkZWNvZGVyLmRlY29kZShidWZmZXIuc2xpY2Uob2Zmc2V0LCBvZmZzZXQgKyBldmVudFNpemUpKVxuICAgIG9mZnNldCA9IG9mZnNldCArIGV2ZW50U2l6ZVxuICAgIGxldCBkYXRhID0gYnVmZmVyLnNsaWNlKG9mZnNldCwgYnVmZmVyLmJ5dGVMZW5ndGgpXG4gICAgcmV0dXJuIHtqb2luX3JlZjogam9pblJlZiwgcmVmOiBudWxsLCB0b3BpYzogdG9waWMsIGV2ZW50OiBldmVudCwgcGF5bG9hZDogZGF0YX1cbiAgfSxcblxuICBkZWNvZGVSZXBseShidWZmZXIsIHZpZXcsIGRlY29kZXIpe1xuICAgIGxldCBqb2luUmVmU2l6ZSA9IHZpZXcuZ2V0VWludDgoMSlcbiAgICBsZXQgcmVmU2l6ZSA9IHZpZXcuZ2V0VWludDgoMilcbiAgICBsZXQgdG9waWNTaXplID0gdmlldy5nZXRVaW50OCgzKVxuICAgIGxldCBldmVudFNpemUgPSB2aWV3LmdldFVpbnQ4KDQpXG4gICAgbGV0IG9mZnNldCA9IHRoaXMuSEVBREVSX0xFTkdUSCArIHRoaXMuTUVUQV9MRU5HVEhcbiAgICBsZXQgam9pblJlZiA9IGRlY29kZXIuZGVjb2RlKGJ1ZmZlci5zbGljZShvZmZzZXQsIG9mZnNldCArIGpvaW5SZWZTaXplKSlcbiAgICBvZmZzZXQgPSBvZmZzZXQgKyBqb2luUmVmU2l6ZVxuICAgIGxldCByZWYgPSBkZWNvZGVyLmRlY29kZShidWZmZXIuc2xpY2Uob2Zmc2V0LCBvZmZzZXQgKyByZWZTaXplKSlcbiAgICBvZmZzZXQgPSBvZmZzZXQgKyByZWZTaXplXG4gICAgbGV0IHRvcGljID0gZGVjb2Rlci5kZWNvZGUoYnVmZmVyLnNsaWNlKG9mZnNldCwgb2Zmc2V0ICsgdG9waWNTaXplKSlcbiAgICBvZmZzZXQgPSBvZmZzZXQgKyB0b3BpY1NpemVcbiAgICBsZXQgZXZlbnQgPSBkZWNvZGVyLmRlY29kZShidWZmZXIuc2xpY2Uob2Zmc2V0LCBvZmZzZXQgKyBldmVudFNpemUpKVxuICAgIG9mZnNldCA9IG9mZnNldCArIGV2ZW50U2l6ZVxuICAgIGxldCBkYXRhID0gYnVmZmVyLnNsaWNlKG9mZnNldCwgYnVmZmVyLmJ5dGVMZW5ndGgpXG4gICAgbGV0IHBheWxvYWQgPSB7c3RhdHVzOiBldmVudCwgcmVzcG9uc2U6IGRhdGF9XG4gICAgcmV0dXJuIHtqb2luX3JlZjogam9pblJlZiwgcmVmOiByZWYsIHRvcGljOiB0b3BpYywgZXZlbnQ6IENIQU5ORUxfRVZFTlRTLnJlcGx5LCBwYXlsb2FkOiBwYXlsb2FkfVxuICB9LFxuXG4gIGRlY29kZUJyb2FkY2FzdChidWZmZXIsIHZpZXcsIGRlY29kZXIpe1xuICAgIGxldCB0b3BpY1NpemUgPSB2aWV3LmdldFVpbnQ4KDEpXG4gICAgbGV0IGV2ZW50U2l6ZSA9IHZpZXcuZ2V0VWludDgoMilcbiAgICBsZXQgb2Zmc2V0ID0gdGhpcy5IRUFERVJfTEVOR1RIICsgMlxuICAgIGxldCB0b3BpYyA9IGRlY29kZXIuZGVjb2RlKGJ1ZmZlci5zbGljZShvZmZzZXQsIG9mZnNldCArIHRvcGljU2l6ZSkpXG4gICAgb2Zmc2V0ID0gb2Zmc2V0ICsgdG9waWNTaXplXG4gICAgbGV0IGV2ZW50ID0gZGVjb2Rlci5kZWNvZGUoYnVmZmVyLnNsaWNlKG9mZnNldCwgb2Zmc2V0ICsgZXZlbnRTaXplKSlcbiAgICBvZmZzZXQgPSBvZmZzZXQgKyBldmVudFNpemVcbiAgICBsZXQgZGF0YSA9IGJ1ZmZlci5zbGljZShvZmZzZXQsIGJ1ZmZlci5ieXRlTGVuZ3RoKVxuXG4gICAgcmV0dXJuIHtqb2luX3JlZjogbnVsbCwgcmVmOiBudWxsLCB0b3BpYzogdG9waWMsIGV2ZW50OiBldmVudCwgcGF5bG9hZDogZGF0YX1cbiAgfVxufVxuIiwgImltcG9ydCB7XG4gIGdsb2JhbCxcbiAgcGh4V2luZG93LFxuICBDSEFOTkVMX0VWRU5UUyxcbiAgREVGQVVMVF9USU1FT1VULFxuICBERUZBVUxUX1ZTTixcbiAgU09DS0VUX1NUQVRFUyxcbiAgVFJBTlNQT1JUUyxcbiAgV1NfQ0xPU0VfTk9STUFMXG59IGZyb20gXCIuL2NvbnN0YW50c1wiXG5cbmltcG9ydCB7XG4gIGNsb3N1cmVcbn0gZnJvbSBcIi4vdXRpbHNcIlxuXG5pbXBvcnQgQWpheCBmcm9tIFwiLi9hamF4XCJcbmltcG9ydCBDaGFubmVsIGZyb20gXCIuL2NoYW5uZWxcIlxuaW1wb3J0IExvbmdQb2xsIGZyb20gXCIuL2xvbmdwb2xsXCJcbmltcG9ydCBTZXJpYWxpemVyIGZyb20gXCIuL3NlcmlhbGl6ZXJcIlxuaW1wb3J0IFRpbWVyIGZyb20gXCIuL3RpbWVyXCJcblxuLyoqIEluaXRpYWxpemVzIHRoZSBTb2NrZXQgKlxuICpcbiAqIEZvciBJRTggc3VwcG9ydCB1c2UgYW4gRVM1LXNoaW0gKGh0dHBzOi8vZ2l0aHViLmNvbS9lcy1zaGltcy9lczUtc2hpbSlcbiAqXG4gKiBAcGFyYW0ge3N0cmluZ30gZW5kUG9pbnQgLSBUaGUgc3RyaW5nIFdlYlNvY2tldCBlbmRwb2ludCwgaWUsIGBcIndzOi8vZXhhbXBsZS5jb20vc29ja2V0XCJgLFxuICogICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgIGBcIndzczovL2V4YW1wbGUuY29tXCJgXG4gKiAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgYFwiL3NvY2tldFwiYCAoaW5oZXJpdGVkIGhvc3QgJiBwcm90b2NvbClcbiAqIEBwYXJhbSB7T2JqZWN0fSBbb3B0c10gLSBPcHRpb25hbCBjb25maWd1cmF0aW9uXG4gKiBAcGFyYW0ge0Z1bmN0aW9ufSBbb3B0cy50cmFuc3BvcnRdIC0gVGhlIFdlYnNvY2tldCBUcmFuc3BvcnQsIGZvciBleGFtcGxlIFdlYlNvY2tldCBvciBQaG9lbml4LkxvbmdQb2xsLlxuICpcbiAqIERlZmF1bHRzIHRvIFdlYlNvY2tldCB3aXRoIGF1dG9tYXRpYyBMb25nUG9sbCBmYWxsYmFjayBpZiBXZWJTb2NrZXQgaXMgbm90IGRlZmluZWQuXG4gKiBUbyBmYWxsYmFjayB0byBMb25nUG9sbCB3aGVuIFdlYlNvY2tldCBhdHRlbXB0cyBmYWlsLCB1c2UgYGxvbmdQb2xsRmFsbGJhY2tNczogMjUwMGAuXG4gKlxuICogQHBhcmFtIHtGdW5jdGlvbn0gW29wdHMubG9uZ1BvbGxGYWxsYmFja01zXSAtIFRoZSBtaWxsaXNlY29uZCB0aW1lIHRvIGF0dGVtcHQgdGhlIHByaW1hcnkgdHJhbnNwb3J0XG4gKiBiZWZvcmUgZmFsbGluZyBiYWNrIHRvIHRoZSBMb25nUG9sbCB0cmFuc3BvcnQuIERpc2FibGVkIGJ5IGRlZmF1bHQuXG4gKlxuICogQHBhcmFtIHtGdW5jdGlvbn0gW29wdHMuZGVidWddIC0gV2hlbiB0cnVlLCBlbmFibGVzIGRlYnVnIGxvZ2dpbmcuIERlZmF1bHQgZmFsc2UuXG4gKlxuICogQHBhcmFtIHtGdW5jdGlvbn0gW29wdHMuZW5jb2RlXSAtIFRoZSBmdW5jdGlvbiB0byBlbmNvZGUgb3V0Z29pbmcgbWVzc2FnZXMuXG4gKlxuICogRGVmYXVsdHMgdG8gSlNPTiBlbmNvZGVyLlxuICpcbiAqIEBwYXJhbSB7RnVuY3Rpb259IFtvcHRzLmRlY29kZV0gLSBUaGUgZnVuY3Rpb24gdG8gZGVjb2RlIGluY29taW5nIG1lc3NhZ2VzLlxuICpcbiAqIERlZmF1bHRzIHRvIEpTT046XG4gKlxuICogYGBgamF2YXNjcmlwdFxuICogKHBheWxvYWQsIGNhbGxiYWNrKSA9PiBjYWxsYmFjayhKU09OLnBhcnNlKHBheWxvYWQpKVxuICogYGBgXG4gKlxuICogQHBhcmFtIHtudW1iZXJ9IFtvcHRzLnRpbWVvdXRdIC0gVGhlIGRlZmF1bHQgdGltZW91dCBpbiBtaWxsaXNlY29uZHMgdG8gdHJpZ2dlciBwdXNoIHRpbWVvdXRzLlxuICpcbiAqIERlZmF1bHRzIGBERUZBVUxUX1RJTUVPVVRgXG4gKiBAcGFyYW0ge251bWJlcn0gW29wdHMuaGVhcnRiZWF0SW50ZXJ2YWxNc10gLSBUaGUgbWlsbGlzZWMgaW50ZXJ2YWwgdG8gc2VuZCBhIGhlYXJ0YmVhdCBtZXNzYWdlXG4gKiBAcGFyYW0ge251bWJlcn0gW29wdHMucmVjb25uZWN0QWZ0ZXJNc10gLSBUaGUgb3B0aW9uYWwgZnVuY3Rpb24gdGhhdCByZXR1cm5zIHRoZSBtaWxsaXNlY1xuICogc29ja2V0IHJlY29ubmVjdCBpbnRlcnZhbC5cbiAqXG4gKiBEZWZhdWx0cyB0byBzdGVwcGVkIGJhY2tvZmYgb2Y6XG4gKlxuICogYGBgamF2YXNjcmlwdFxuICogZnVuY3Rpb24odHJpZXMpe1xuICogICByZXR1cm4gWzEwLCA1MCwgMTAwLCAxNTAsIDIwMCwgMjUwLCA1MDAsIDEwMDAsIDIwMDBdW3RyaWVzIC0gMV0gfHwgNTAwMFxuICogfVxuICogYGBgYFxuICpcbiAqIEBwYXJhbSB7bnVtYmVyfSBbb3B0cy5yZWpvaW5BZnRlck1zXSAtIFRoZSBvcHRpb25hbCBmdW5jdGlvbiB0aGF0IHJldHVybnMgdGhlIG1pbGxpc2VjXG4gKiByZWpvaW4gaW50ZXJ2YWwgZm9yIGluZGl2aWR1YWwgY2hhbm5lbHMuXG4gKlxuICogYGBgamF2YXNjcmlwdFxuICogZnVuY3Rpb24odHJpZXMpe1xuICogICByZXR1cm4gWzEwMDAsIDIwMDAsIDUwMDBdW3RyaWVzIC0gMV0gfHwgMTAwMDBcbiAqIH1cbiAqIGBgYGBcbiAqXG4gKiBAcGFyYW0ge0Z1bmN0aW9ufSBbb3B0cy5sb2dnZXJdIC0gVGhlIG9wdGlvbmFsIGZ1bmN0aW9uIGZvciBzcGVjaWFsaXplZCBsb2dnaW5nLCBpZTpcbiAqXG4gKiBgYGBqYXZhc2NyaXB0XG4gKiBmdW5jdGlvbihraW5kLCBtc2csIGRhdGEpIHtcbiAqICAgY29uc29sZS5sb2coYCR7a2luZH06ICR7bXNnfWAsIGRhdGEpXG4gKiB9XG4gKiBgYGBcbiAqXG4gKiBAcGFyYW0ge251bWJlcn0gW29wdHMubG9uZ3BvbGxlclRpbWVvdXRdIC0gVGhlIG1heGltdW0gdGltZW91dCBvZiBhIGxvbmcgcG9sbCBBSkFYIHJlcXVlc3QuXG4gKlxuICogRGVmYXVsdHMgdG8gMjBzIChkb3VibGUgdGhlIHNlcnZlciBsb25nIHBvbGwgdGltZXIpLlxuICpcbiAqIEBwYXJhbSB7KE9iamVjdHxmdW5jdGlvbil9IFtvcHRzLnBhcmFtc10gLSBUaGUgb3B0aW9uYWwgcGFyYW1zIHRvIHBhc3Mgd2hlbiBjb25uZWN0aW5nXG4gKiBAcGFyYW0ge3N0cmluZ30gW29wdHMuYmluYXJ5VHlwZV0gLSBUaGUgYmluYXJ5IHR5cGUgdG8gdXNlIGZvciBiaW5hcnkgV2ViU29ja2V0IGZyYW1lcy5cbiAqXG4gKiBEZWZhdWx0cyB0byBcImFycmF5YnVmZmVyXCJcbiAqXG4gKiBAcGFyYW0ge3Zzbn0gW29wdHMudnNuXSAtIFRoZSBzZXJpYWxpemVyJ3MgcHJvdG9jb2wgdmVyc2lvbiB0byBzZW5kIG9uIGNvbm5lY3QuXG4gKlxuICogRGVmYXVsdHMgdG8gREVGQVVMVF9WU04uXG4gKlxuICogQHBhcmFtIHtPYmplY3R9IFtvcHRzLnNlc3Npb25TdG9yYWdlXSAtIEFuIG9wdGlvbmFsIFN0b3JhZ2UgY29tcGF0aWJsZSBvYmplY3RcbiAqIFBob2VuaXggdXNlcyBzZXNzaW9uU3RvcmFnZSBmb3IgbG9uZ3BvbGwgZmFsbGJhY2sgaGlzdG9yeS4gT3ZlcnJpZGluZyB0aGUgc3RvcmUgaXNcbiAqIHVzZWZ1bCB3aGVuIFBob2VuaXggd29uJ3QgaGF2ZSBhY2Nlc3MgdG8gYHNlc3Npb25TdG9yYWdlYC4gRm9yIGV4YW1wbGUsIFRoaXMgY291bGRcbiAqIGhhcHBlbiBpZiBhIHNpdGUgbG9hZHMgYSBjcm9zcy1kb21haW4gY2hhbm5lbCBpbiBhbiBpZnJhbWUuIEV4YW1wbGUgdXNhZ2U6XG4gKlxuICogICAgIGNsYXNzIEluTWVtb3J5U3RvcmFnZSB7XG4gKiAgICAgICBjb25zdHJ1Y3RvcigpIHsgdGhpcy5zdG9yYWdlID0ge30gfVxuICogICAgICAgZ2V0SXRlbShrZXlOYW1lKSB7IHJldHVybiB0aGlzLnN0b3JhZ2Vba2V5TmFtZV0gfHwgbnVsbCB9XG4gKiAgICAgICByZW1vdmVJdGVtKGtleU5hbWUpIHsgZGVsZXRlIHRoaXMuc3RvcmFnZVtrZXlOYW1lXSB9XG4gKiAgICAgICBzZXRJdGVtKGtleU5hbWUsIGtleVZhbHVlKSB7IHRoaXMuc3RvcmFnZVtrZXlOYW1lXSA9IGtleVZhbHVlIH1cbiAqICAgICB9XG4gKlxuKi9cbmV4cG9ydCBkZWZhdWx0IGNsYXNzIFNvY2tldCB7XG4gIGNvbnN0cnVjdG9yKGVuZFBvaW50LCBvcHRzID0ge30pe1xuICAgIHRoaXMuc3RhdGVDaGFuZ2VDYWxsYmFja3MgPSB7b3BlbjogW10sIGNsb3NlOiBbXSwgZXJyb3I6IFtdLCBtZXNzYWdlOiBbXX1cbiAgICB0aGlzLmNoYW5uZWxzID0gW11cbiAgICB0aGlzLnNlbmRCdWZmZXIgPSBbXVxuICAgIHRoaXMucmVmID0gMFxuICAgIHRoaXMudGltZW91dCA9IG9wdHMudGltZW91dCB8fCBERUZBVUxUX1RJTUVPVVRcbiAgICB0aGlzLnRyYW5zcG9ydCA9IG9wdHMudHJhbnNwb3J0IHx8IGdsb2JhbC5XZWJTb2NrZXQgfHwgTG9uZ1BvbGxcbiAgICB0aGlzLnByaW1hcnlQYXNzZWRIZWFsdGhDaGVjayA9IGZhbHNlXG4gICAgdGhpcy5sb25nUG9sbEZhbGxiYWNrTXMgPSBvcHRzLmxvbmdQb2xsRmFsbGJhY2tNc1xuICAgIHRoaXMuZmFsbGJhY2tUaW1lciA9IG51bGxcbiAgICB0aGlzLnNlc3Npb25TdG9yZSA9IG9wdHMuc2Vzc2lvblN0b3JhZ2UgfHwgKGdsb2JhbCAmJiBnbG9iYWwuc2Vzc2lvblN0b3JhZ2UpXG4gICAgdGhpcy5lc3RhYmxpc2hlZENvbm5lY3Rpb25zID0gMFxuICAgIHRoaXMuZGVmYXVsdEVuY29kZXIgPSBTZXJpYWxpemVyLmVuY29kZS5iaW5kKFNlcmlhbGl6ZXIpXG4gICAgdGhpcy5kZWZhdWx0RGVjb2RlciA9IFNlcmlhbGl6ZXIuZGVjb2RlLmJpbmQoU2VyaWFsaXplcilcbiAgICB0aGlzLmNsb3NlV2FzQ2xlYW4gPSBmYWxzZVxuICAgIHRoaXMuZGlzY29ubmVjdGluZyA9IGZhbHNlXG4gICAgdGhpcy5iaW5hcnlUeXBlID0gb3B0cy5iaW5hcnlUeXBlIHx8IFwiYXJyYXlidWZmZXJcIlxuICAgIHRoaXMuY29ubmVjdENsb2NrID0gMVxuICAgIGlmKHRoaXMudHJhbnNwb3J0ICE9PSBMb25nUG9sbCl7XG4gICAgICB0aGlzLmVuY29kZSA9IG9wdHMuZW5jb2RlIHx8IHRoaXMuZGVmYXVsdEVuY29kZXJcbiAgICAgIHRoaXMuZGVjb2RlID0gb3B0cy5kZWNvZGUgfHwgdGhpcy5kZWZhdWx0RGVjb2RlclxuICAgIH0gZWxzZSB7XG4gICAgICB0aGlzLmVuY29kZSA9IHRoaXMuZGVmYXVsdEVuY29kZXJcbiAgICAgIHRoaXMuZGVjb2RlID0gdGhpcy5kZWZhdWx0RGVjb2RlclxuICAgIH1cbiAgICBsZXQgYXdhaXRpbmdDb25uZWN0aW9uT25QYWdlU2hvdyA9IG51bGxcbiAgICBpZihwaHhXaW5kb3cgJiYgcGh4V2luZG93LmFkZEV2ZW50TGlzdGVuZXIpe1xuICAgICAgcGh4V2luZG93LmFkZEV2ZW50TGlzdGVuZXIoXCJwYWdlaGlkZVwiLCBfZSA9PiB7XG4gICAgICAgIGlmKHRoaXMuY29ubil7XG4gICAgICAgICAgdGhpcy5kaXNjb25uZWN0KClcbiAgICAgICAgICBhd2FpdGluZ0Nvbm5lY3Rpb25PblBhZ2VTaG93ID0gdGhpcy5jb25uZWN0Q2xvY2tcbiAgICAgICAgfVxuICAgICAgfSlcbiAgICAgIHBoeFdpbmRvdy5hZGRFdmVudExpc3RlbmVyKFwicGFnZXNob3dcIiwgX2UgPT4ge1xuICAgICAgICBpZihhd2FpdGluZ0Nvbm5lY3Rpb25PblBhZ2VTaG93ID09PSB0aGlzLmNvbm5lY3RDbG9jayl7XG4gICAgICAgICAgYXdhaXRpbmdDb25uZWN0aW9uT25QYWdlU2hvdyA9IG51bGxcbiAgICAgICAgICB0aGlzLmNvbm5lY3QoKVxuICAgICAgICB9XG4gICAgICB9KVxuICAgIH1cbiAgICB0aGlzLmhlYXJ0YmVhdEludGVydmFsTXMgPSBvcHRzLmhlYXJ0YmVhdEludGVydmFsTXMgfHwgMzAwMDBcbiAgICB0aGlzLnJlam9pbkFmdGVyTXMgPSAodHJpZXMpID0+IHtcbiAgICAgIGlmKG9wdHMucmVqb2luQWZ0ZXJNcyl7XG4gICAgICAgIHJldHVybiBvcHRzLnJlam9pbkFmdGVyTXModHJpZXMpXG4gICAgICB9IGVsc2Uge1xuICAgICAgICByZXR1cm4gWzEwMDAsIDIwMDAsIDUwMDBdW3RyaWVzIC0gMV0gfHwgMTAwMDBcbiAgICAgIH1cbiAgICB9XG4gICAgdGhpcy5yZWNvbm5lY3RBZnRlck1zID0gKHRyaWVzKSA9PiB7XG4gICAgICBpZihvcHRzLnJlY29ubmVjdEFmdGVyTXMpe1xuICAgICAgICByZXR1cm4gb3B0cy5yZWNvbm5lY3RBZnRlck1zKHRyaWVzKVxuICAgICAgfSBlbHNlIHtcbiAgICAgICAgcmV0dXJuIFsxMCwgNTAsIDEwMCwgMTUwLCAyMDAsIDI1MCwgNTAwLCAxMDAwLCAyMDAwXVt0cmllcyAtIDFdIHx8IDUwMDBcbiAgICAgIH1cbiAgICB9XG4gICAgdGhpcy5sb2dnZXIgPSBvcHRzLmxvZ2dlciB8fCBudWxsXG4gICAgaWYoIXRoaXMubG9nZ2VyICYmIG9wdHMuZGVidWcpe1xuICAgICAgdGhpcy5sb2dnZXIgPSAoa2luZCwgbXNnLCBkYXRhKSA9PiB7IGNvbnNvbGUubG9nKGAke2tpbmR9OiAke21zZ31gLCBkYXRhKSB9XG4gICAgfVxuICAgIHRoaXMubG9uZ3BvbGxlclRpbWVvdXQgPSBvcHRzLmxvbmdwb2xsZXJUaW1lb3V0IHx8IDIwMDAwXG4gICAgdGhpcy5wYXJhbXMgPSBjbG9zdXJlKG9wdHMucGFyYW1zIHx8IHt9KVxuICAgIHRoaXMuZW5kUG9pbnQgPSBgJHtlbmRQb2ludH0vJHtUUkFOU1BPUlRTLndlYnNvY2tldH1gXG4gICAgdGhpcy52c24gPSBvcHRzLnZzbiB8fCBERUZBVUxUX1ZTTlxuICAgIHRoaXMuaGVhcnRiZWF0VGltZW91dFRpbWVyID0gbnVsbFxuICAgIHRoaXMuaGVhcnRiZWF0VGltZXIgPSBudWxsXG4gICAgdGhpcy5wZW5kaW5nSGVhcnRiZWF0UmVmID0gbnVsbFxuICAgIHRoaXMucmVjb25uZWN0VGltZXIgPSBuZXcgVGltZXIoKCkgPT4ge1xuICAgICAgdGhpcy50ZWFyZG93bigoKSA9PiB0aGlzLmNvbm5lY3QoKSlcbiAgICB9LCB0aGlzLnJlY29ubmVjdEFmdGVyTXMpXG4gIH1cblxuICAvKipcbiAgICogUmV0dXJucyB0aGUgTG9uZ1BvbGwgdHJhbnNwb3J0IHJlZmVyZW5jZVxuICAgKi9cbiAgZ2V0TG9uZ1BvbGxUcmFuc3BvcnQoKXsgcmV0dXJuIExvbmdQb2xsIH1cblxuICAvKipcbiAgICogRGlzY29ubmVjdHMgYW5kIHJlcGxhY2VzIHRoZSBhY3RpdmUgdHJhbnNwb3J0XG4gICAqXG4gICAqIEBwYXJhbSB7RnVuY3Rpb259IG5ld1RyYW5zcG9ydCAtIFRoZSBuZXcgdHJhbnNwb3J0IGNsYXNzIHRvIGluc3RhbnRpYXRlXG4gICAqXG4gICAqL1xuICByZXBsYWNlVHJhbnNwb3J0KG5ld1RyYW5zcG9ydCl7XG4gICAgdGhpcy5jb25uZWN0Q2xvY2srK1xuICAgIHRoaXMuY2xvc2VXYXNDbGVhbiA9IHRydWVcbiAgICBjbGVhclRpbWVvdXQodGhpcy5mYWxsYmFja1RpbWVyKVxuICAgIHRoaXMucmVjb25uZWN0VGltZXIucmVzZXQoKVxuICAgIGlmKHRoaXMuY29ubil7XG4gICAgICB0aGlzLmNvbm4uY2xvc2UoKVxuICAgICAgdGhpcy5jb25uID0gbnVsbFxuICAgIH1cbiAgICB0aGlzLnRyYW5zcG9ydCA9IG5ld1RyYW5zcG9ydFxuICB9XG5cbiAgLyoqXG4gICAqIFJldHVybnMgdGhlIHNvY2tldCBwcm90b2NvbFxuICAgKlxuICAgKiBAcmV0dXJucyB7c3RyaW5nfVxuICAgKi9cbiAgcHJvdG9jb2woKXsgcmV0dXJuIGxvY2F0aW9uLnByb3RvY29sLm1hdGNoKC9eaHR0cHMvKSA/IFwid3NzXCIgOiBcIndzXCIgfVxuXG4gIC8qKlxuICAgKiBUaGUgZnVsbHkgcXVhbGlmaWVkIHNvY2tldCB1cmxcbiAgICpcbiAgICogQHJldHVybnMge3N0cmluZ31cbiAgICovXG4gIGVuZFBvaW50VVJMKCl7XG4gICAgbGV0IHVyaSA9IEFqYXguYXBwZW5kUGFyYW1zKFxuICAgICAgQWpheC5hcHBlbmRQYXJhbXModGhpcy5lbmRQb2ludCwgdGhpcy5wYXJhbXMoKSksIHt2c246IHRoaXMudnNufSlcbiAgICBpZih1cmkuY2hhckF0KDApICE9PSBcIi9cIil7IHJldHVybiB1cmkgfVxuICAgIGlmKHVyaS5jaGFyQXQoMSkgPT09IFwiL1wiKXsgcmV0dXJuIGAke3RoaXMucHJvdG9jb2woKX06JHt1cml9YCB9XG5cbiAgICByZXR1cm4gYCR7dGhpcy5wcm90b2NvbCgpfTovLyR7bG9jYXRpb24uaG9zdH0ke3VyaX1gXG4gIH1cblxuICAvKipcbiAgICogRGlzY29ubmVjdHMgdGhlIHNvY2tldFxuICAgKlxuICAgKiBTZWUgaHR0cHM6Ly9kZXZlbG9wZXIubW96aWxsYS5vcmcvZW4tVVMvZG9jcy9XZWIvQVBJL0Nsb3NlRXZlbnQjU3RhdHVzX2NvZGVzIGZvciB2YWxpZCBzdGF0dXMgY29kZXMuXG4gICAqXG4gICAqIEBwYXJhbSB7RnVuY3Rpb259IGNhbGxiYWNrIC0gT3B0aW9uYWwgY2FsbGJhY2sgd2hpY2ggaXMgY2FsbGVkIGFmdGVyIHNvY2tldCBpcyBkaXNjb25uZWN0ZWQuXG4gICAqIEBwYXJhbSB7aW50ZWdlcn0gY29kZSAtIEEgc3RhdHVzIGNvZGUgZm9yIGRpc2Nvbm5lY3Rpb24gKE9wdGlvbmFsKS5cbiAgICogQHBhcmFtIHtzdHJpbmd9IHJlYXNvbiAtIEEgdGV4dHVhbCBkZXNjcmlwdGlvbiBvZiB0aGUgcmVhc29uIHRvIGRpc2Nvbm5lY3QuIChPcHRpb25hbClcbiAgICovXG4gIGRpc2Nvbm5lY3QoY2FsbGJhY2ssIGNvZGUsIHJlYXNvbil7XG4gICAgdGhpcy5jb25uZWN0Q2xvY2srK1xuICAgIHRoaXMuZGlzY29ubmVjdGluZyA9IHRydWVcbiAgICB0aGlzLmNsb3NlV2FzQ2xlYW4gPSB0cnVlXG4gICAgY2xlYXJUaW1lb3V0KHRoaXMuZmFsbGJhY2tUaW1lcilcbiAgICB0aGlzLnJlY29ubmVjdFRpbWVyLnJlc2V0KClcbiAgICB0aGlzLnRlYXJkb3duKCgpID0+IHtcbiAgICAgIHRoaXMuZGlzY29ubmVjdGluZyA9IGZhbHNlXG4gICAgICBjYWxsYmFjayAmJiBjYWxsYmFjaygpXG4gICAgfSwgY29kZSwgcmVhc29uKVxuICB9XG5cbiAgLyoqXG4gICAqXG4gICAqIEBwYXJhbSB7T2JqZWN0fSBwYXJhbXMgLSBUaGUgcGFyYW1zIHRvIHNlbmQgd2hlbiBjb25uZWN0aW5nLCBmb3IgZXhhbXBsZSBge3VzZXJfaWQ6IHVzZXJUb2tlbn1gXG4gICAqXG4gICAqIFBhc3NpbmcgcGFyYW1zIHRvIGNvbm5lY3QgaXMgZGVwcmVjYXRlZDsgcGFzcyB0aGVtIGluIHRoZSBTb2NrZXQgY29uc3RydWN0b3IgaW5zdGVhZDpcbiAgICogYG5ldyBTb2NrZXQoXCIvc29ja2V0XCIsIHtwYXJhbXM6IHt1c2VyX2lkOiB1c2VyVG9rZW59fSlgLlxuICAgKi9cbiAgY29ubmVjdChwYXJhbXMpe1xuICAgIGlmKHBhcmFtcyl7XG4gICAgICBjb25zb2xlICYmIGNvbnNvbGUubG9nKFwicGFzc2luZyBwYXJhbXMgdG8gY29ubmVjdCBpcyBkZXByZWNhdGVkLiBJbnN0ZWFkIHBhc3MgOnBhcmFtcyB0byB0aGUgU29ja2V0IGNvbnN0cnVjdG9yXCIpXG4gICAgICB0aGlzLnBhcmFtcyA9IGNsb3N1cmUocGFyYW1zKVxuICAgIH1cbiAgICBpZih0aGlzLmNvbm4gJiYgIXRoaXMuZGlzY29ubmVjdGluZyl7IHJldHVybiB9XG4gICAgaWYodGhpcy5sb25nUG9sbEZhbGxiYWNrTXMgJiYgdGhpcy50cmFuc3BvcnQgIT09IExvbmdQb2xsKXtcbiAgICAgIHRoaXMuY29ubmVjdFdpdGhGYWxsYmFjayhMb25nUG9sbCwgdGhpcy5sb25nUG9sbEZhbGxiYWNrTXMpXG4gICAgfSBlbHNlIHtcbiAgICAgIHRoaXMudHJhbnNwb3J0Q29ubmVjdCgpXG4gICAgfVxuICB9XG5cbiAgLyoqXG4gICAqIExvZ3MgdGhlIG1lc3NhZ2UuIE92ZXJyaWRlIGB0aGlzLmxvZ2dlcmAgZm9yIHNwZWNpYWxpemVkIGxvZ2dpbmcuIG5vb3BzIGJ5IGRlZmF1bHRcbiAgICogQHBhcmFtIHtzdHJpbmd9IGtpbmRcbiAgICogQHBhcmFtIHtzdHJpbmd9IG1zZ1xuICAgKiBAcGFyYW0ge09iamVjdH0gZGF0YVxuICAgKi9cbiAgbG9nKGtpbmQsIG1zZywgZGF0YSl7IHRoaXMubG9nZ2VyICYmIHRoaXMubG9nZ2VyKGtpbmQsIG1zZywgZGF0YSkgfVxuXG4gIC8qKlxuICAgKiBSZXR1cm5zIHRydWUgaWYgYSBsb2dnZXIgaGFzIGJlZW4gc2V0IG9uIHRoaXMgc29ja2V0LlxuICAgKi9cbiAgaGFzTG9nZ2VyKCl7IHJldHVybiB0aGlzLmxvZ2dlciAhPT0gbnVsbCB9XG5cbiAgLyoqXG4gICAqIFJlZ2lzdGVycyBjYWxsYmFja3MgZm9yIGNvbm5lY3Rpb24gb3BlbiBldmVudHNcbiAgICpcbiAgICogQGV4YW1wbGUgc29ja2V0Lm9uT3BlbihmdW5jdGlvbigpeyBjb25zb2xlLmluZm8oXCJ0aGUgc29ja2V0IHdhcyBvcGVuZWRcIikgfSlcbiAgICpcbiAgICogQHBhcmFtIHtGdW5jdGlvbn0gY2FsbGJhY2tcbiAgICovXG4gIG9uT3BlbihjYWxsYmFjayl7XG4gICAgbGV0IHJlZiA9IHRoaXMubWFrZVJlZigpXG4gICAgdGhpcy5zdGF0ZUNoYW5nZUNhbGxiYWNrcy5vcGVuLnB1c2goW3JlZiwgY2FsbGJhY2tdKVxuICAgIHJldHVybiByZWZcbiAgfVxuXG4gIC8qKlxuICAgKiBSZWdpc3RlcnMgY2FsbGJhY2tzIGZvciBjb25uZWN0aW9uIGNsb3NlIGV2ZW50c1xuICAgKiBAcGFyYW0ge0Z1bmN0aW9ufSBjYWxsYmFja1xuICAgKi9cbiAgb25DbG9zZShjYWxsYmFjayl7XG4gICAgbGV0IHJlZiA9IHRoaXMubWFrZVJlZigpXG4gICAgdGhpcy5zdGF0ZUNoYW5nZUNhbGxiYWNrcy5jbG9zZS5wdXNoKFtyZWYsIGNhbGxiYWNrXSlcbiAgICByZXR1cm4gcmVmXG4gIH1cblxuICAvKipcbiAgICogUmVnaXN0ZXJzIGNhbGxiYWNrcyBmb3IgY29ubmVjdGlvbiBlcnJvciBldmVudHNcbiAgICpcbiAgICogQGV4YW1wbGUgc29ja2V0Lm9uRXJyb3IoZnVuY3Rpb24oZXJyb3IpeyBhbGVydChcIkFuIGVycm9yIG9jY3VycmVkXCIpIH0pXG4gICAqXG4gICAqIEBwYXJhbSB7RnVuY3Rpb259IGNhbGxiYWNrXG4gICAqL1xuICBvbkVycm9yKGNhbGxiYWNrKXtcbiAgICBsZXQgcmVmID0gdGhpcy5tYWtlUmVmKClcbiAgICB0aGlzLnN0YXRlQ2hhbmdlQ2FsbGJhY2tzLmVycm9yLnB1c2goW3JlZiwgY2FsbGJhY2tdKVxuICAgIHJldHVybiByZWZcbiAgfVxuXG4gIC8qKlxuICAgKiBSZWdpc3RlcnMgY2FsbGJhY2tzIGZvciBjb25uZWN0aW9uIG1lc3NhZ2UgZXZlbnRzXG4gICAqIEBwYXJhbSB7RnVuY3Rpb259IGNhbGxiYWNrXG4gICAqL1xuICBvbk1lc3NhZ2UoY2FsbGJhY2spe1xuICAgIGxldCByZWYgPSB0aGlzLm1ha2VSZWYoKVxuICAgIHRoaXMuc3RhdGVDaGFuZ2VDYWxsYmFja3MubWVzc2FnZS5wdXNoKFtyZWYsIGNhbGxiYWNrXSlcbiAgICByZXR1cm4gcmVmXG4gIH1cblxuICAvKipcbiAgICogUGluZ3MgdGhlIHNlcnZlciBhbmQgaW52b2tlcyB0aGUgY2FsbGJhY2sgd2l0aCB0aGUgUlRUIGluIG1pbGxpc2Vjb25kc1xuICAgKiBAcGFyYW0ge0Z1bmN0aW9ufSBjYWxsYmFja1xuICAgKlxuICAgKiBSZXR1cm5zIHRydWUgaWYgdGhlIHBpbmcgd2FzIHB1c2hlZCBvciBmYWxzZSBpZiB1bmFibGUgdG8gYmUgcHVzaGVkLlxuICAgKi9cbiAgcGluZyhjYWxsYmFjayl7XG4gICAgaWYoIXRoaXMuaXNDb25uZWN0ZWQoKSl7IHJldHVybiBmYWxzZSB9XG4gICAgbGV0IHJlZiA9IHRoaXMubWFrZVJlZigpXG4gICAgbGV0IHN0YXJ0VGltZSA9IERhdGUubm93KClcbiAgICB0aGlzLnB1c2goe3RvcGljOiBcInBob2VuaXhcIiwgZXZlbnQ6IFwiaGVhcnRiZWF0XCIsIHBheWxvYWQ6IHt9LCByZWY6IHJlZn0pXG4gICAgbGV0IG9uTXNnUmVmID0gdGhpcy5vbk1lc3NhZ2UobXNnID0+IHtcbiAgICAgIGlmKG1zZy5yZWYgPT09IHJlZil7XG4gICAgICAgIHRoaXMub2ZmKFtvbk1zZ1JlZl0pXG4gICAgICAgIGNhbGxiYWNrKERhdGUubm93KCkgLSBzdGFydFRpbWUpXG4gICAgICB9XG4gICAgfSlcbiAgICByZXR1cm4gdHJ1ZVxuICB9XG5cbiAgLyoqXG4gICAqIEBwcml2YXRlXG4gICAqL1xuXG4gIHRyYW5zcG9ydENvbm5lY3QoKXtcbiAgICB0aGlzLmNvbm5lY3RDbG9jaysrXG4gICAgdGhpcy5jbG9zZVdhc0NsZWFuID0gZmFsc2VcbiAgICB0aGlzLmNvbm4gPSBuZXcgdGhpcy50cmFuc3BvcnQodGhpcy5lbmRQb2ludFVSTCgpKVxuICAgIHRoaXMuY29ubi5iaW5hcnlUeXBlID0gdGhpcy5iaW5hcnlUeXBlXG4gICAgdGhpcy5jb25uLnRpbWVvdXQgPSB0aGlzLmxvbmdwb2xsZXJUaW1lb3V0XG4gICAgdGhpcy5jb25uLm9ub3BlbiA9ICgpID0+IHRoaXMub25Db25uT3BlbigpXG4gICAgdGhpcy5jb25uLm9uZXJyb3IgPSBlcnJvciA9PiB0aGlzLm9uQ29ubkVycm9yKGVycm9yKVxuICAgIHRoaXMuY29ubi5vbm1lc3NhZ2UgPSBldmVudCA9PiB0aGlzLm9uQ29ubk1lc3NhZ2UoZXZlbnQpXG4gICAgdGhpcy5jb25uLm9uY2xvc2UgPSBldmVudCA9PiB0aGlzLm9uQ29ubkNsb3NlKGV2ZW50KVxuICB9XG5cbiAgZ2V0U2Vzc2lvbihrZXkpeyByZXR1cm4gdGhpcy5zZXNzaW9uU3RvcmUgJiYgdGhpcy5zZXNzaW9uU3RvcmUuZ2V0SXRlbShrZXkpIH1cblxuICBzdG9yZVNlc3Npb24oa2V5LCB2YWwpeyB0aGlzLnNlc3Npb25TdG9yZSAmJiB0aGlzLnNlc3Npb25TdG9yZS5zZXRJdGVtKGtleSwgdmFsKSB9XG5cbiAgY29ubmVjdFdpdGhGYWxsYmFjayhmYWxsYmFja1RyYW5zcG9ydCwgZmFsbGJhY2tUaHJlc2hvbGQgPSAyNTAwKXtcbiAgICBjbGVhclRpbWVvdXQodGhpcy5mYWxsYmFja1RpbWVyKVxuICAgIGxldCBlc3RhYmxpc2hlZCA9IGZhbHNlXG4gICAgbGV0IHByaW1hcnlUcmFuc3BvcnQgPSB0cnVlXG4gICAgbGV0IG9wZW5SZWYsIGVycm9yUmVmXG4gICAgbGV0IGZhbGxiYWNrID0gKHJlYXNvbikgPT4ge1xuICAgICAgdGhpcy5sb2coXCJ0cmFuc3BvcnRcIiwgYGZhbGxpbmcgYmFjayB0byAke2ZhbGxiYWNrVHJhbnNwb3J0Lm5hbWV9Li4uYCwgcmVhc29uKVxuICAgICAgdGhpcy5vZmYoW29wZW5SZWYsIGVycm9yUmVmXSlcbiAgICAgIHByaW1hcnlUcmFuc3BvcnQgPSBmYWxzZVxuICAgICAgdGhpcy5yZXBsYWNlVHJhbnNwb3J0KGZhbGxiYWNrVHJhbnNwb3J0KVxuICAgICAgdGhpcy50cmFuc3BvcnRDb25uZWN0KClcbiAgICB9XG4gICAgaWYodGhpcy5nZXRTZXNzaW9uKGBwaHg6ZmFsbGJhY2s6JHtmYWxsYmFja1RyYW5zcG9ydC5uYW1lfWApKXsgcmV0dXJuIGZhbGxiYWNrKFwibWVtb3JpemVkXCIpIH1cblxuICAgIHRoaXMuZmFsbGJhY2tUaW1lciA9IHNldFRpbWVvdXQoZmFsbGJhY2ssIGZhbGxiYWNrVGhyZXNob2xkKVxuXG4gICAgZXJyb3JSZWYgPSB0aGlzLm9uRXJyb3IocmVhc29uID0+IHtcbiAgICAgIHRoaXMubG9nKFwidHJhbnNwb3J0XCIsIFwiZXJyb3JcIiwgcmVhc29uKVxuICAgICAgaWYocHJpbWFyeVRyYW5zcG9ydCAmJiAhZXN0YWJsaXNoZWQpe1xuICAgICAgICBjbGVhclRpbWVvdXQodGhpcy5mYWxsYmFja1RpbWVyKVxuICAgICAgICBmYWxsYmFjayhyZWFzb24pXG4gICAgICB9XG4gICAgfSlcbiAgICB0aGlzLm9uT3BlbigoKSA9PiB7XG4gICAgICBlc3RhYmxpc2hlZCA9IHRydWVcbiAgICAgIGlmKCFwcmltYXJ5VHJhbnNwb3J0KXtcbiAgICAgICAgLy8gb25seSBtZW1vcml6ZSBMUCBpZiB3ZSBuZXZlciBjb25uZWN0ZWQgdG8gcHJpbWFyeVxuICAgICAgICBpZighdGhpcy5wcmltYXJ5UGFzc2VkSGVhbHRoQ2hlY2speyB0aGlzLnN0b3JlU2Vzc2lvbihgcGh4OmZhbGxiYWNrOiR7ZmFsbGJhY2tUcmFuc3BvcnQubmFtZX1gLCBcInRydWVcIikgfVxuICAgICAgICByZXR1cm4gdGhpcy5sb2coXCJ0cmFuc3BvcnRcIiwgYGVzdGFibGlzaGVkICR7ZmFsbGJhY2tUcmFuc3BvcnQubmFtZX0gZmFsbGJhY2tgKVxuICAgICAgfVxuICAgICAgLy8gaWYgd2UndmUgZXN0YWJsaXNoZWQgcHJpbWFyeSwgZ2l2ZSB0aGUgZmFsbGJhY2sgYSBuZXcgcGVyaW9kIHRvIGF0dGVtcHQgcGluZ1xuICAgICAgY2xlYXJUaW1lb3V0KHRoaXMuZmFsbGJhY2tUaW1lcilcbiAgICAgIHRoaXMuZmFsbGJhY2tUaW1lciA9IHNldFRpbWVvdXQoZmFsbGJhY2ssIGZhbGxiYWNrVGhyZXNob2xkKVxuICAgICAgdGhpcy5waW5nKHJ0dCA9PiB7XG4gICAgICAgIHRoaXMubG9nKFwidHJhbnNwb3J0XCIsIFwiY29ubmVjdGVkIHRvIHByaW1hcnkgYWZ0ZXJcIiwgcnR0KVxuICAgICAgICB0aGlzLnByaW1hcnlQYXNzZWRIZWFsdGhDaGVjayA9IHRydWVcbiAgICAgICAgY2xlYXJUaW1lb3V0KHRoaXMuZmFsbGJhY2tUaW1lcilcbiAgICAgIH0pXG4gICAgfSlcbiAgICB0aGlzLnRyYW5zcG9ydENvbm5lY3QoKVxuICB9XG5cbiAgY2xlYXJIZWFydGJlYXRzKCl7XG4gICAgY2xlYXJUaW1lb3V0KHRoaXMuaGVhcnRiZWF0VGltZXIpXG4gICAgY2xlYXJUaW1lb3V0KHRoaXMuaGVhcnRiZWF0VGltZW91dFRpbWVyKVxuICB9XG5cbiAgb25Db25uT3Blbigpe1xuICAgIGlmKHRoaXMuaGFzTG9nZ2VyKCkpIHRoaXMubG9nKFwidHJhbnNwb3J0XCIsIGAke3RoaXMudHJhbnNwb3J0Lm5hbWV9IGNvbm5lY3RlZCB0byAke3RoaXMuZW5kUG9pbnRVUkwoKX1gKVxuICAgIHRoaXMuY2xvc2VXYXNDbGVhbiA9IGZhbHNlXG4gICAgdGhpcy5kaXNjb25uZWN0aW5nID0gZmFsc2VcbiAgICB0aGlzLmVzdGFibGlzaGVkQ29ubmVjdGlvbnMrK1xuICAgIHRoaXMuZmx1c2hTZW5kQnVmZmVyKClcbiAgICB0aGlzLnJlY29ubmVjdFRpbWVyLnJlc2V0KClcbiAgICB0aGlzLnJlc2V0SGVhcnRiZWF0KClcbiAgICB0aGlzLnN0YXRlQ2hhbmdlQ2FsbGJhY2tzLm9wZW4uZm9yRWFjaCgoWywgY2FsbGJhY2tdKSA9PiBjYWxsYmFjaygpKVxuICB9XG5cbiAgLyoqXG4gICAqIEBwcml2YXRlXG4gICAqL1xuXG4gIGhlYXJ0YmVhdFRpbWVvdXQoKXtcbiAgICBpZih0aGlzLnBlbmRpbmdIZWFydGJlYXRSZWYpe1xuICAgICAgdGhpcy5wZW5kaW5nSGVhcnRiZWF0UmVmID0gbnVsbFxuICAgICAgaWYodGhpcy5oYXNMb2dnZXIoKSl7IHRoaXMubG9nKFwidHJhbnNwb3J0XCIsIFwiaGVhcnRiZWF0IHRpbWVvdXQuIEF0dGVtcHRpbmcgdG8gcmUtZXN0YWJsaXNoIGNvbm5lY3Rpb25cIikgfVxuICAgICAgdGhpcy50cmlnZ2VyQ2hhbkVycm9yKClcbiAgICAgIHRoaXMuY2xvc2VXYXNDbGVhbiA9IGZhbHNlXG4gICAgICB0aGlzLnRlYXJkb3duKCgpID0+IHRoaXMucmVjb25uZWN0VGltZXIuc2NoZWR1bGVUaW1lb3V0KCksIFdTX0NMT1NFX05PUk1BTCwgXCJoZWFydGJlYXQgdGltZW91dFwiKVxuICAgIH1cbiAgfVxuXG4gIHJlc2V0SGVhcnRiZWF0KCl7XG4gICAgaWYodGhpcy5jb25uICYmIHRoaXMuY29ubi5za2lwSGVhcnRiZWF0KXsgcmV0dXJuIH1cbiAgICB0aGlzLnBlbmRpbmdIZWFydGJlYXRSZWYgPSBudWxsXG4gICAgdGhpcy5jbGVhckhlYXJ0YmVhdHMoKVxuICAgIHRoaXMuaGVhcnRiZWF0VGltZXIgPSBzZXRUaW1lb3V0KCgpID0+IHRoaXMuc2VuZEhlYXJ0YmVhdCgpLCB0aGlzLmhlYXJ0YmVhdEludGVydmFsTXMpXG4gIH1cblxuICB0ZWFyZG93bihjYWxsYmFjaywgY29kZSwgcmVhc29uKXtcbiAgICBpZighdGhpcy5jb25uKXtcbiAgICAgIHJldHVybiBjYWxsYmFjayAmJiBjYWxsYmFjaygpXG4gICAgfVxuICAgIGxldCBjb25uZWN0Q2xvY2sgPSB0aGlzLmNvbm5lY3RDbG9ja1xuXG4gICAgdGhpcy53YWl0Rm9yQnVmZmVyRG9uZSgoKSA9PiB7XG4gICAgICBpZihjb25uZWN0Q2xvY2sgIT09IHRoaXMuY29ubmVjdENsb2NrKXsgcmV0dXJuIH1cbiAgICAgIGlmKHRoaXMuY29ubil7XG4gICAgICAgIGlmKGNvZGUpeyB0aGlzLmNvbm4uY2xvc2UoY29kZSwgcmVhc29uIHx8IFwiXCIpIH0gZWxzZSB7IHRoaXMuY29ubi5jbG9zZSgpIH1cbiAgICAgIH1cblxuICAgICAgdGhpcy53YWl0Rm9yU29ja2V0Q2xvc2VkKCgpID0+IHtcbiAgICAgICAgaWYoY29ubmVjdENsb2NrICE9PSB0aGlzLmNvbm5lY3RDbG9jayl7IHJldHVybiB9XG4gICAgICAgIGlmKHRoaXMuY29ubil7XG4gICAgICAgICAgdGhpcy5jb25uLm9ub3BlbiA9IGZ1bmN0aW9uICgpeyB9IC8vIG5vb3BcbiAgICAgICAgICB0aGlzLmNvbm4ub25lcnJvciA9IGZ1bmN0aW9uICgpeyB9IC8vIG5vb3BcbiAgICAgICAgICB0aGlzLmNvbm4ub25tZXNzYWdlID0gZnVuY3Rpb24gKCl7IH0gLy8gbm9vcFxuICAgICAgICAgIHRoaXMuY29ubi5vbmNsb3NlID0gZnVuY3Rpb24gKCl7IH0gLy8gbm9vcFxuICAgICAgICAgIHRoaXMuY29ubiA9IG51bGxcbiAgICAgICAgfVxuXG4gICAgICAgIGNhbGxiYWNrICYmIGNhbGxiYWNrKClcbiAgICAgIH0pXG4gICAgfSlcbiAgfVxuXG4gIHdhaXRGb3JCdWZmZXJEb25lKGNhbGxiYWNrLCB0cmllcyA9IDEpe1xuICAgIGlmKHRyaWVzID09PSA1IHx8ICF0aGlzLmNvbm4gfHwgIXRoaXMuY29ubi5idWZmZXJlZEFtb3VudCl7XG4gICAgICBjYWxsYmFjaygpXG4gICAgICByZXR1cm5cbiAgICB9XG5cbiAgICBzZXRUaW1lb3V0KCgpID0+IHtcbiAgICAgIHRoaXMud2FpdEZvckJ1ZmZlckRvbmUoY2FsbGJhY2ssIHRyaWVzICsgMSlcbiAgICB9LCAxNTAgKiB0cmllcylcbiAgfVxuXG4gIHdhaXRGb3JTb2NrZXRDbG9zZWQoY2FsbGJhY2ssIHRyaWVzID0gMSl7XG4gICAgaWYodHJpZXMgPT09IDUgfHwgIXRoaXMuY29ubiB8fCB0aGlzLmNvbm4ucmVhZHlTdGF0ZSA9PT0gU09DS0VUX1NUQVRFUy5jbG9zZWQpe1xuICAgICAgY2FsbGJhY2soKVxuICAgICAgcmV0dXJuXG4gICAgfVxuXG4gICAgc2V0VGltZW91dCgoKSA9PiB7XG4gICAgICB0aGlzLndhaXRGb3JTb2NrZXRDbG9zZWQoY2FsbGJhY2ssIHRyaWVzICsgMSlcbiAgICB9LCAxNTAgKiB0cmllcylcbiAgfVxuXG4gIG9uQ29ubkNsb3NlKGV2ZW50KXtcbiAgICBsZXQgY2xvc2VDb2RlID0gZXZlbnQgJiYgZXZlbnQuY29kZVxuICAgIGlmKHRoaXMuaGFzTG9nZ2VyKCkpIHRoaXMubG9nKFwidHJhbnNwb3J0XCIsIFwiY2xvc2VcIiwgZXZlbnQpXG4gICAgdGhpcy50cmlnZ2VyQ2hhbkVycm9yKClcbiAgICB0aGlzLmNsZWFySGVhcnRiZWF0cygpXG4gICAgaWYoIXRoaXMuY2xvc2VXYXNDbGVhbiAmJiBjbG9zZUNvZGUgIT09IDEwMDApe1xuICAgICAgdGhpcy5yZWNvbm5lY3RUaW1lci5zY2hlZHVsZVRpbWVvdXQoKVxuICAgIH1cbiAgICB0aGlzLnN0YXRlQ2hhbmdlQ2FsbGJhY2tzLmNsb3NlLmZvckVhY2goKFssIGNhbGxiYWNrXSkgPT4gY2FsbGJhY2soZXZlbnQpKVxuICB9XG5cbiAgLyoqXG4gICAqIEBwcml2YXRlXG4gICAqL1xuICBvbkNvbm5FcnJvcihlcnJvcil7XG4gICAgaWYodGhpcy5oYXNMb2dnZXIoKSkgdGhpcy5sb2coXCJ0cmFuc3BvcnRcIiwgZXJyb3IpXG4gICAgbGV0IHRyYW5zcG9ydEJlZm9yZSA9IHRoaXMudHJhbnNwb3J0XG4gICAgbGV0IGVzdGFibGlzaGVkQmVmb3JlID0gdGhpcy5lc3RhYmxpc2hlZENvbm5lY3Rpb25zXG4gICAgdGhpcy5zdGF0ZUNoYW5nZUNhbGxiYWNrcy5lcnJvci5mb3JFYWNoKChbLCBjYWxsYmFja10pID0+IHtcbiAgICAgIGNhbGxiYWNrKGVycm9yLCB0cmFuc3BvcnRCZWZvcmUsIGVzdGFibGlzaGVkQmVmb3JlKVxuICAgIH0pXG4gICAgaWYodHJhbnNwb3J0QmVmb3JlID09PSB0aGlzLnRyYW5zcG9ydCB8fCBlc3RhYmxpc2hlZEJlZm9yZSA+IDApe1xuICAgICAgdGhpcy50cmlnZ2VyQ2hhbkVycm9yKClcbiAgICB9XG4gIH1cblxuICAvKipcbiAgICogQHByaXZhdGVcbiAgICovXG4gIHRyaWdnZXJDaGFuRXJyb3IoKXtcbiAgICB0aGlzLmNoYW5uZWxzLmZvckVhY2goY2hhbm5lbCA9PiB7XG4gICAgICBpZighKGNoYW5uZWwuaXNFcnJvcmVkKCkgfHwgY2hhbm5lbC5pc0xlYXZpbmcoKSB8fCBjaGFubmVsLmlzQ2xvc2VkKCkpKXtcbiAgICAgICAgY2hhbm5lbC50cmlnZ2VyKENIQU5ORUxfRVZFTlRTLmVycm9yKVxuICAgICAgfVxuICAgIH0pXG4gIH1cblxuICAvKipcbiAgICogQHJldHVybnMge3N0cmluZ31cbiAgICovXG4gIGNvbm5lY3Rpb25TdGF0ZSgpe1xuICAgIHN3aXRjaCh0aGlzLmNvbm4gJiYgdGhpcy5jb25uLnJlYWR5U3RhdGUpe1xuICAgICAgY2FzZSBTT0NLRVRfU1RBVEVTLmNvbm5lY3Rpbmc6IHJldHVybiBcImNvbm5lY3RpbmdcIlxuICAgICAgY2FzZSBTT0NLRVRfU1RBVEVTLm9wZW46IHJldHVybiBcIm9wZW5cIlxuICAgICAgY2FzZSBTT0NLRVRfU1RBVEVTLmNsb3Npbmc6IHJldHVybiBcImNsb3NpbmdcIlxuICAgICAgZGVmYXVsdDogcmV0dXJuIFwiY2xvc2VkXCJcbiAgICB9XG4gIH1cblxuICAvKipcbiAgICogQHJldHVybnMge2Jvb2xlYW59XG4gICAqL1xuICBpc0Nvbm5lY3RlZCgpeyByZXR1cm4gdGhpcy5jb25uZWN0aW9uU3RhdGUoKSA9PT0gXCJvcGVuXCIgfVxuXG4gIC8qKlxuICAgKiBAcHJpdmF0ZVxuICAgKlxuICAgKiBAcGFyYW0ge0NoYW5uZWx9XG4gICAqL1xuICByZW1vdmUoY2hhbm5lbCl7XG4gICAgdGhpcy5vZmYoY2hhbm5lbC5zdGF0ZUNoYW5nZVJlZnMpXG4gICAgdGhpcy5jaGFubmVscyA9IHRoaXMuY2hhbm5lbHMuZmlsdGVyKGMgPT4gYyAhPT0gY2hhbm5lbClcbiAgfVxuXG4gIC8qKlxuICAgKiBSZW1vdmVzIGBvbk9wZW5gLCBgb25DbG9zZWAsIGBvbkVycm9yLGAgYW5kIGBvbk1lc3NhZ2VgIHJlZ2lzdHJhdGlvbnMuXG4gICAqXG4gICAqIEBwYXJhbSB7cmVmc30gLSBsaXN0IG9mIHJlZnMgcmV0dXJuZWQgYnkgY2FsbHMgdG9cbiAgICogICAgICAgICAgICAgICAgIGBvbk9wZW5gLCBgb25DbG9zZWAsIGBvbkVycm9yLGAgYW5kIGBvbk1lc3NhZ2VgXG4gICAqL1xuICBvZmYocmVmcyl7XG4gICAgZm9yKGxldCBrZXkgaW4gdGhpcy5zdGF0ZUNoYW5nZUNhbGxiYWNrcyl7XG4gICAgICB0aGlzLnN0YXRlQ2hhbmdlQ2FsbGJhY2tzW2tleV0gPSB0aGlzLnN0YXRlQ2hhbmdlQ2FsbGJhY2tzW2tleV0uZmlsdGVyKChbcmVmXSkgPT4ge1xuICAgICAgICByZXR1cm4gcmVmcy5pbmRleE9mKHJlZikgPT09IC0xXG4gICAgICB9KVxuICAgIH1cbiAgfVxuXG4gIC8qKlxuICAgKiBJbml0aWF0ZXMgYSBuZXcgY2hhbm5lbCBmb3IgdGhlIGdpdmVuIHRvcGljXG4gICAqXG4gICAqIEBwYXJhbSB7c3RyaW5nfSB0b3BpY1xuICAgKiBAcGFyYW0ge09iamVjdH0gY2hhblBhcmFtcyAtIFBhcmFtZXRlcnMgZm9yIHRoZSBjaGFubmVsXG4gICAqIEByZXR1cm5zIHtDaGFubmVsfVxuICAgKi9cbiAgY2hhbm5lbCh0b3BpYywgY2hhblBhcmFtcyA9IHt9KXtcbiAgICBsZXQgY2hhbiA9IG5ldyBDaGFubmVsKHRvcGljLCBjaGFuUGFyYW1zLCB0aGlzKVxuICAgIHRoaXMuY2hhbm5lbHMucHVzaChjaGFuKVxuICAgIHJldHVybiBjaGFuXG4gIH1cblxuICAvKipcbiAgICogQHBhcmFtIHtPYmplY3R9IGRhdGFcbiAgICovXG4gIHB1c2goZGF0YSl7XG4gICAgaWYodGhpcy5oYXNMb2dnZXIoKSl7XG4gICAgICBsZXQge3RvcGljLCBldmVudCwgcGF5bG9hZCwgcmVmLCBqb2luX3JlZn0gPSBkYXRhXG4gICAgICB0aGlzLmxvZyhcInB1c2hcIiwgYCR7dG9waWN9ICR7ZXZlbnR9ICgke2pvaW5fcmVmfSwgJHtyZWZ9KWAsIHBheWxvYWQpXG4gICAgfVxuXG4gICAgaWYodGhpcy5pc0Nvbm5lY3RlZCgpKXtcbiAgICAgIHRoaXMuZW5jb2RlKGRhdGEsIHJlc3VsdCA9PiB0aGlzLmNvbm4uc2VuZChyZXN1bHQpKVxuICAgIH0gZWxzZSB7XG4gICAgICB0aGlzLnNlbmRCdWZmZXIucHVzaCgoKSA9PiB0aGlzLmVuY29kZShkYXRhLCByZXN1bHQgPT4gdGhpcy5jb25uLnNlbmQocmVzdWx0KSkpXG4gICAgfVxuICB9XG5cbiAgLyoqXG4gICAqIFJldHVybiB0aGUgbmV4dCBtZXNzYWdlIHJlZiwgYWNjb3VudGluZyBmb3Igb3ZlcmZsb3dzXG4gICAqIEByZXR1cm5zIHtzdHJpbmd9XG4gICAqL1xuICBtYWtlUmVmKCl7XG4gICAgbGV0IG5ld1JlZiA9IHRoaXMucmVmICsgMVxuICAgIGlmKG5ld1JlZiA9PT0gdGhpcy5yZWYpeyB0aGlzLnJlZiA9IDAgfSBlbHNlIHsgdGhpcy5yZWYgPSBuZXdSZWYgfVxuXG4gICAgcmV0dXJuIHRoaXMucmVmLnRvU3RyaW5nKClcbiAgfVxuXG4gIHNlbmRIZWFydGJlYXQoKXtcbiAgICBpZih0aGlzLnBlbmRpbmdIZWFydGJlYXRSZWYgJiYgIXRoaXMuaXNDb25uZWN0ZWQoKSl7IHJldHVybiB9XG4gICAgdGhpcy5wZW5kaW5nSGVhcnRiZWF0UmVmID0gdGhpcy5tYWtlUmVmKClcbiAgICB0aGlzLnB1c2goe3RvcGljOiBcInBob2VuaXhcIiwgZXZlbnQ6IFwiaGVhcnRiZWF0XCIsIHBheWxvYWQ6IHt9LCByZWY6IHRoaXMucGVuZGluZ0hlYXJ0YmVhdFJlZn0pXG4gICAgdGhpcy5oZWFydGJlYXRUaW1lb3V0VGltZXIgPSBzZXRUaW1lb3V0KCgpID0+IHRoaXMuaGVhcnRiZWF0VGltZW91dCgpLCB0aGlzLmhlYXJ0YmVhdEludGVydmFsTXMpXG4gIH1cblxuICBmbHVzaFNlbmRCdWZmZXIoKXtcbiAgICBpZih0aGlzLmlzQ29ubmVjdGVkKCkgJiYgdGhpcy5zZW5kQnVmZmVyLmxlbmd0aCA+IDApe1xuICAgICAgdGhpcy5zZW5kQnVmZmVyLmZvckVhY2goY2FsbGJhY2sgPT4gY2FsbGJhY2soKSlcbiAgICAgIHRoaXMuc2VuZEJ1ZmZlciA9IFtdXG4gICAgfVxuICB9XG5cbiAgb25Db25uTWVzc2FnZShyYXdNZXNzYWdlKXtcbiAgICB0aGlzLmRlY29kZShyYXdNZXNzYWdlLmRhdGEsIG1zZyA9PiB7XG4gICAgICBsZXQge3RvcGljLCBldmVudCwgcGF5bG9hZCwgcmVmLCBqb2luX3JlZn0gPSBtc2dcbiAgICAgIGlmKHJlZiAmJiByZWYgPT09IHRoaXMucGVuZGluZ0hlYXJ0YmVhdFJlZil7XG4gICAgICAgIHRoaXMuY2xlYXJIZWFydGJlYXRzKClcbiAgICAgICAgdGhpcy5wZW5kaW5nSGVhcnRiZWF0UmVmID0gbnVsbFxuICAgICAgICB0aGlzLmhlYXJ0YmVhdFRpbWVyID0gc2V0VGltZW91dCgoKSA9PiB0aGlzLnNlbmRIZWFydGJlYXQoKSwgdGhpcy5oZWFydGJlYXRJbnRlcnZhbE1zKVxuICAgICAgfVxuXG4gICAgICBpZih0aGlzLmhhc0xvZ2dlcigpKSB0aGlzLmxvZyhcInJlY2VpdmVcIiwgYCR7cGF5bG9hZC5zdGF0dXMgfHwgXCJcIn0gJHt0b3BpY30gJHtldmVudH0gJHtyZWYgJiYgXCIoXCIgKyByZWYgKyBcIilcIiB8fCBcIlwifWAsIHBheWxvYWQpXG5cbiAgICAgIGZvcihsZXQgaSA9IDA7IGkgPCB0aGlzLmNoYW5uZWxzLmxlbmd0aDsgaSsrKXtcbiAgICAgICAgY29uc3QgY2hhbm5lbCA9IHRoaXMuY2hhbm5lbHNbaV1cbiAgICAgICAgaWYoIWNoYW5uZWwuaXNNZW1iZXIodG9waWMsIGV2ZW50LCBwYXlsb2FkLCBqb2luX3JlZikpeyBjb250aW51ZSB9XG4gICAgICAgIGNoYW5uZWwudHJpZ2dlcihldmVudCwgcGF5bG9hZCwgcmVmLCBqb2luX3JlZilcbiAgICAgIH1cblxuICAgICAgZm9yKGxldCBpID0gMDsgaSA8IHRoaXMuc3RhdGVDaGFuZ2VDYWxsYmFja3MubWVzc2FnZS5sZW5ndGg7IGkrKyl7XG4gICAgICAgIGxldCBbLCBjYWxsYmFja10gPSB0aGlzLnN0YXRlQ2hhbmdlQ2FsbGJhY2tzLm1lc3NhZ2VbaV1cbiAgICAgICAgY2FsbGJhY2sobXNnKVxuICAgICAgfVxuICAgIH0pXG4gIH1cblxuICBsZWF2ZU9wZW5Ub3BpYyh0b3BpYyl7XG4gICAgbGV0IGR1cENoYW5uZWwgPSB0aGlzLmNoYW5uZWxzLmZpbmQoYyA9PiBjLnRvcGljID09PSB0b3BpYyAmJiAoYy5pc0pvaW5lZCgpIHx8IGMuaXNKb2luaW5nKCkpKVxuICAgIGlmKGR1cENoYW5uZWwpe1xuICAgICAgaWYodGhpcy5oYXNMb2dnZXIoKSkgdGhpcy5sb2coXCJ0cmFuc3BvcnRcIiwgYGxlYXZpbmcgZHVwbGljYXRlIHRvcGljIFwiJHt0b3BpY31cImApXG4gICAgICBkdXBDaGFubmVsLmxlYXZlKClcbiAgICB9XG4gIH1cbn1cbiIsICIvLyBEYXJrIFBvb2wgXHUyMDE0IHJlYWx0aW1lIGNsaWVudFxuaW1wb3J0IHsgU29ja2V0IH0gZnJvbSBcInBob2VuaXhcIjtcblxuLy8gXHUyNTAwXHUyNTAwXHUyNTAwIEJvb3QgXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXG5jb25zdCBjb25maWcgPSB3aW5kb3cuRGFya1Bvb2wgfHwge307XG5cbmZ1bmN0aW9uIGJvb3QoY2ZnKSB7XG4gIGlmICghY2ZnLnNvY2tldFRva2VuKSB7XG4gICAgY29uc29sZS5kZWJ1ZyhcIltEYXJrUG9vbF0gTm8gc29ja2V0IHRva2VuIGZvdW5kIFx1MjAxNCBXZWJTb2NrZXQgbm90IHN0YXJ0ZWQuXCIpO1xuICAgIHJldHVybjtcbiAgfVxuXG4gIGNvbnN0IHNvY2tldCA9IG5ldyBTb2NrZXQoXCIvc29ja2V0XCIsIHtcbiAgICBwYXJhbXM6IHsgdG9rZW46IGNmZy5zb2NrZXRUb2tlbiB9LFxuICB9KTtcblxuICBzb2NrZXQuY29ubmVjdCgpO1xuXG4gIGNvbnN0IGxvYmJ5TWdyID0gbmV3IExvYmJ5TWFuYWdlcihzb2NrZXQsIGNmZyk7XG4gIGxvYmJ5TWdyLmluaXQoKTtcblxuICBpZiAoY2ZnLm1hdGNoSWQpIHtcbiAgICBjb25zdCBtYXRjaE1nciA9IG5ldyBNYXRjaE1hbmFnZXIoc29ja2V0LCBjZmcsIGxvYmJ5TWdyKTtcbiAgICBtYXRjaE1nci5pbml0KCk7XG4gIH1cbn1cblxuLy8gXHUyNTAwXHUyNTAwXHUyNTAwIFRvYXN0IFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFxuY29uc3QgVG9hc3QgPSB7XG4gIHNob3cobXNnLCB0eXBlID0gXCJpbmZvXCIsIGR1cmF0aW9uID0gNDAwMCkge1xuICAgIGNvbnN0IGljb25zID0ge1xuICAgICAgaW5mbzogXCJcdTIxMzlcIixcbiAgICAgIHN1Y2Nlc3M6IFwiXHUyNzEzXCIsXG4gICAgICB3YXJuaW5nOiBcIlx1MjZBMFwiLFxuICAgICAgZXJyb3I6IFwiXHUyNzE1XCIsXG4gICAgfTtcblxuICAgIGxldCBjb250YWluZXIgPSBkb2N1bWVudC5nZXRFbGVtZW50QnlJZChcInRvYXN0LWNvbnRhaW5lclwiKTtcbiAgICBpZiAoIWNvbnRhaW5lcikge1xuICAgICAgY29udGFpbmVyID0gZG9jdW1lbnQuY3JlYXRlRWxlbWVudChcImRpdlwiKTtcbiAgICAgIGNvbnRhaW5lci5pZCA9IFwidG9hc3QtY29udGFpbmVyXCI7XG4gICAgICBjb250YWluZXIuc3R5bGUucG9zaXRpb24gPSBcImZpeGVkXCI7XG4gICAgICBjb250YWluZXIuc3R5bGUudG9wID0gXCIxNnB4XCI7XG4gICAgICBjb250YWluZXIuc3R5bGUucmlnaHQgPSBcIjE2cHhcIjtcbiAgICAgIGNvbnRhaW5lci5zdHlsZS56SW5kZXggPSBcIjk5OTlcIjtcbiAgICAgIGNvbnRhaW5lci5zdHlsZS5kaXNwbGF5ID0gXCJmbGV4XCI7XG4gICAgICBjb250YWluZXIuc3R5bGUuZmxleERpcmVjdGlvbiA9IFwiY29sdW1uXCI7XG4gICAgICBjb250YWluZXIuc3R5bGUuZ2FwID0gXCI4cHhcIjtcbiAgICAgIGRvY3VtZW50LmJvZHkuYXBwZW5kQ2hpbGQoY29udGFpbmVyKTtcbiAgICB9XG5cbiAgICBjb25zdCBlbCA9IGRvY3VtZW50LmNyZWF0ZUVsZW1lbnQoXCJkaXZcIik7XG4gICAgZWwuY2xhc3NOYW1lID0gYHRvYXN0ICR7dHlwZX1gO1xuICAgIGVsLnN0eWxlLnBhZGRpbmcgPSBcIjEwcHggMTJweFwiO1xuICAgIGVsLnN0eWxlLmJvcmRlclJhZGl1cyA9IFwiMTBweFwiO1xuICAgIGVsLnN0eWxlLmJvcmRlciA9IFwiMXB4IHNvbGlkIHJnYmEoMjU1LDI1NSwyNTUsLjEyKVwiO1xuICAgIGVsLnN0eWxlLmJhY2tncm91bmQgPSBcIiMxMTE4MjdcIjtcbiAgICBlbC5zdHlsZS5jb2xvciA9IFwiI2U1ZTdlYlwiO1xuICAgIGVsLnN0eWxlLmZvbnRTaXplID0gXCIxM3B4XCI7XG4gICAgZWwuc3R5bGUuZm9udEZhbWlseSA9IFwidWktbW9ub3NwYWNlLCBTRk1vbm8tUmVndWxhciwgTWVubG8sIG1vbm9zcGFjZVwiO1xuICAgIGVsLnN0eWxlLmRpc3BsYXkgPSBcImZsZXhcIjtcbiAgICBlbC5zdHlsZS5hbGlnbkl0ZW1zID0gXCJjZW50ZXJcIjtcbiAgICBlbC5zdHlsZS5nYXAgPSBcIjhweFwiO1xuICAgIGVsLnN0eWxlLmN1cnNvciA9IFwicG9pbnRlclwiO1xuICAgIGVsLmlubmVySFRNTCA9IGBcbiAgICAgIDxzcGFuIHN0eWxlPVwiZmxleC1zaHJpbms6MFwiPiR7aWNvbnNbdHlwZV0gfHwgXCJcdTIwMjJcIn08L3NwYW4+XG4gICAgICA8c3Bhbj4ke2VzY2FwZUh0bWwobXNnKX08L3NwYW4+XG4gICAgYDtcblxuICAgIGNvbnRhaW5lci5hcHBlbmRDaGlsZChlbCk7XG5cbiAgICBjb25zdCB0aW1lciA9IHNldFRpbWVvdXQoKCkgPT4gZWwucmVtb3ZlKCksIGR1cmF0aW9uKTtcbiAgICBlbC5hZGRFdmVudExpc3RlbmVyKFwiY2xpY2tcIiwgKCkgPT4ge1xuICAgICAgY2xlYXJUaW1lb3V0KHRpbWVyKTtcbiAgICAgIGVsLnJlbW92ZSgpO1xuICAgIH0pO1xuICB9LFxufTtcblxuLy8gXHUyNTAwXHUyNTAwXHUyNTAwIFNwYXJrbGluZSBDaGFydCBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcbmNsYXNzIFNwYXJrbGluZUNoYXJ0IHtcbiAgY29uc3RydWN0b3IoY29udGFpbmVySWQsIG9wdGlvbnMgPSB7fSkge1xuICAgIHRoaXMuY29udGFpbmVyID0gZG9jdW1lbnQuZ2V0RWxlbWVudEJ5SWQoY29udGFpbmVySWQpO1xuICAgIHRoaXMuaGlzdG9yeSA9IFtdO1xuICAgIHRoaXMubWF4UG9pbnRzID0gb3B0aW9ucy5tYXhQb2ludHMgfHwgMjA7XG4gICAgdGhpcy53ID0gb3B0aW9ucy53aWR0aCB8fCA4MDtcbiAgICB0aGlzLmggPSBvcHRpb25zLmhlaWdodCB8fCAyODtcbiAgICBpZiAodGhpcy5jb250YWluZXIpIHRoaXMuX2NyZWF0ZVN2ZygpO1xuICB9XG5cbiAgX2NyZWF0ZVN2ZygpIHtcbiAgICB0aGlzLnN2ZyA9IGRvY3VtZW50LmNyZWF0ZUVsZW1lbnROUyhcImh0dHA6Ly93d3cudzMub3JnLzIwMDAvc3ZnXCIsIFwic3ZnXCIpO1xuICAgIHRoaXMuc3ZnLnNldEF0dHJpYnV0ZShcInZpZXdCb3hcIiwgYDAgMCAke3RoaXMud30gJHt0aGlzLmh9YCk7XG4gICAgdGhpcy5zdmcuc2V0QXR0cmlidXRlKFwid2lkdGhcIiwgdGhpcy53KTtcbiAgICB0aGlzLnN2Zy5zZXRBdHRyaWJ1dGUoXCJoZWlnaHRcIiwgdGhpcy5oKTtcblxuICAgIGNvbnN0IGdyYWRJZCA9IGBzZy0ke01hdGgucmFuZG9tKCkudG9TdHJpbmcoMzYpLnNsaWNlKDIpfWA7XG4gICAgdGhpcy5zdmcuaW5uZXJIVE1MID0gYFxuICAgICAgPGRlZnM+XG4gICAgICAgIDxsaW5lYXJHcmFkaWVudCBpZD1cIiR7Z3JhZElkfVwiIHgxPVwiMFwiIHkxPVwiMFwiIHgyPVwiMFwiIHkyPVwiMVwiPlxuICAgICAgICAgIDxzdG9wIG9mZnNldD1cIjAlXCIgc3RvcC1jb2xvcj1cInZhcigtLXNwYXJrLWNvbG9yLCMxMGI5ODEpXCIgc3RvcC1vcGFjaXR5PVwiMC4zNVwiLz5cbiAgICAgICAgICA8c3RvcCBvZmZzZXQ9XCIxMDAlXCIgc3RvcC1jb2xvcj1cInZhcigtLXNwYXJrLWNvbG9yLCMxMGI5ODEpXCIgc3RvcC1vcGFjaXR5PVwiMFwiLz5cbiAgICAgICAgPC9saW5lYXJHcmFkaWVudD5cbiAgICAgIDwvZGVmcz5cbiAgICAgIDxwYXRoIGNsYXNzPVwiYXJlYVwiIGZpbGw9XCJ1cmwoIyR7Z3JhZElkfSlcIj48L3BhdGg+XG4gICAgICA8cG9seWxpbmUgY2xhc3M9XCJsaW5lXCIgZmlsbD1cIm5vbmVcIiBzdHJva2U9XCJ2YXIoLS1zcGFyay1jb2xvciwjMTBiOTgxKVwiIHN0cm9rZS13aWR0aD1cIjJcIj48L3BvbHlsaW5lPlxuICAgIGA7XG4gICAgdGhpcy5ncmFkSWQgPSBncmFkSWQ7XG4gICAgdGhpcy5jb250YWluZXIuaW5uZXJIVE1MID0gXCJcIjtcbiAgICB0aGlzLmNvbnRhaW5lci5hcHBlbmRDaGlsZCh0aGlzLnN2Zyk7XG4gIH1cblxuICByZXNldCh2YWx1ZXMgPSBbXSkge1xuICAgIHRoaXMuaGlzdG9yeSA9IFtdO1xuICAgIHZhbHVlcy5mb3JFYWNoKCh2KSA9PiB0aGlzLnB1c2godikpO1xuICB9XG5cbiAgcHVzaChwcmljZSkge1xuICAgIGNvbnN0IHAgPSBwYXJzZUZsb2F0KHByaWNlKTtcbiAgICBpZiAoTnVtYmVyLmlzTmFOKHApKSByZXR1cm47XG4gICAgdGhpcy5oaXN0b3J5LnB1c2gocCk7XG4gICAgaWYgKHRoaXMuaGlzdG9yeS5sZW5ndGggPiB0aGlzLm1heFBvaW50cykgdGhpcy5oaXN0b3J5LnNoaWZ0KCk7XG4gICAgdGhpcy5fcmVuZGVyKCk7XG4gIH1cblxuICBzZXRDb2xvcihpc1VwKSB7XG4gICAgaWYgKCF0aGlzLnN2ZykgcmV0dXJuO1xuICAgIGNvbnN0IGMgPSBpc1VwID8gXCIjMTBiOTgxXCIgOiBcIiNlZjQ0NDRcIjtcbiAgICB0aGlzLnN2Zy5zdHlsZS5zZXRQcm9wZXJ0eShcIi0tc3BhcmstY29sb3JcIiwgYyk7XG4gICAgY29uc3QgZmlyc3RTdG9wID0gdGhpcy5zdmcucXVlcnlTZWxlY3RvcihcInN0b3BcIik7XG4gICAgaWYgKGZpcnN0U3RvcCkgZmlyc3RTdG9wLnNldEF0dHJpYnV0ZShcInN0b3AtY29sb3JcIiwgYyk7XG4gIH1cblxuICBfcmVuZGVyKCkge1xuICAgIGlmICghdGhpcy5zdmcgfHwgdGhpcy5oaXN0b3J5Lmxlbmd0aCA9PT0gMCkgcmV0dXJuO1xuXG4gICAgY29uc3QgbWluID0gTWF0aC5taW4oLi4udGhpcy5oaXN0b3J5KTtcbiAgICBjb25zdCBtYXggPSBNYXRoLm1heCguLi50aGlzLmhpc3RvcnkpO1xuICAgIGNvbnN0IHJhbmdlID0gbWF4IC0gbWluIHx8IDE7XG4gICAgY29uc3QgcGFkID0gMjtcbiAgICBjb25zdCBzdGVwID1cbiAgICAgIHRoaXMuaGlzdG9yeS5sZW5ndGggPiAxXG4gICAgICAgID8gKHRoaXMudyAtIHBhZCAqIDIpIC8gKHRoaXMuaGlzdG9yeS5sZW5ndGggLSAxKVxuICAgICAgICA6IDA7XG5cbiAgICBjb25zdCBwb2ludHMgPSB0aGlzLmhpc3RvcnkubWFwKCh2LCBpKSA9PiB7XG4gICAgICBjb25zdCB4ID0gcGFkICsgaSAqIHN0ZXA7XG4gICAgICBjb25zdCB5ID0gcGFkICsgKDEgLSAodiAtIG1pbikgLyByYW5nZSkgKiAodGhpcy5oIC0gcGFkICogMik7XG4gICAgICByZXR1cm4gYCR7eC50b0ZpeGVkKDIpfSwke3kudG9GaXhlZCgyKX1gO1xuICAgIH0pO1xuXG4gICAgY29uc3QgbGluZSA9IHRoaXMuc3ZnLnF1ZXJ5U2VsZWN0b3IoXCIubGluZVwiKTtcbiAgICBjb25zdCBhcmVhID0gdGhpcy5zdmcucXVlcnlTZWxlY3RvcihcIi5hcmVhXCIpO1xuXG4gICAgaWYgKGxpbmUpIGxpbmUuc2V0QXR0cmlidXRlKFwicG9pbnRzXCIsIHBvaW50cy5qb2luKFwiIFwiKSk7XG4gICAgaWYgKGFyZWEgJiYgcG9pbnRzLmxlbmd0aCA+IDApIHtcbiAgICAgIGNvbnN0IGZpcnN0ID0gcG9pbnRzWzBdLnNwbGl0KFwiLFwiKTtcbiAgICAgIGNvbnN0IGxhc3QgPSBwb2ludHNbcG9pbnRzLmxlbmd0aCAtIDFdLnNwbGl0KFwiLFwiKTtcbiAgICAgIGFyZWEuc2V0QXR0cmlidXRlKFxuICAgICAgICBcImRcIixcbiAgICAgICAgYE0ke2ZpcnN0WzBdfSwke3RoaXMuaCAtIHBhZH0gTCR7cG9pbnRzLmpvaW4oXCIgTFwiKX0gTCR7bGFzdFswXX0sJHt0aGlzLmggLSBwYWR9IFpgLFxuICAgICAgKTtcbiAgICB9XG4gIH1cbn1cblxuLy8gXHUyNTAwXHUyNTAwXHUyNTAwIExhcmdlIERldGFpbCBDaGFydCBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcbmNsYXNzIERldGFpbENoYXJ0IHtcbiAgY29uc3RydWN0b3IoY2FudmFzSWQpIHtcbiAgICB0aGlzLmNhbnZhcyA9IGRvY3VtZW50LmdldEVsZW1lbnRCeUlkKGNhbnZhc0lkKTtcbiAgICB0aGlzLmN0eCA9IHRoaXMuY2FudmFzPy5nZXRDb250ZXh0KFwiMmRcIik7XG4gIH1cblxuICByZW5kZXIocG9pbnRzKSB7XG4gICAgaWYgKCF0aGlzLmNhbnZhcyB8fCAhdGhpcy5jdHgpIHJldHVybjtcblxuICAgIGNvbnN0IHJlY3QgPSB0aGlzLmNhbnZhcy5nZXRCb3VuZGluZ0NsaWVudFJlY3QoKTtcbiAgICBjb25zdCB3ID0gTWF0aC5tYXgoNDIwLCBNYXRoLmZsb29yKHJlY3Qud2lkdGggfHwgNDIwKSk7XG4gICAgY29uc3QgaCA9IE1hdGgubWF4KDI2MCwgTWF0aC5mbG9vcihyZWN0LmhlaWdodCB8fCAyNjApKTtcbiAgICB0aGlzLmNhbnZhcy53aWR0aCA9IHc7XG4gICAgdGhpcy5jYW52YXMuaGVpZ2h0ID0gaDtcblxuICAgIGNvbnN0IGN0eCA9IHRoaXMuY3R4O1xuICAgIGN0eC5jbGVhclJlY3QoMCwgMCwgdywgaCk7XG5cbiAgICBpZiAoIXBvaW50cyB8fCBwb2ludHMubGVuZ3RoID09PSAwKSByZXR1cm47XG5cbiAgICBjb25zdCB2YWx1ZXMgPSBwb2ludHMubWFwKChwKSA9PiBwLnByaWNlKTtcbiAgICBjb25zdCBtaW4gPSBNYXRoLm1pbiguLi52YWx1ZXMpO1xuICAgIGNvbnN0IG1heCA9IE1hdGgubWF4KC4uLnZhbHVlcyk7XG4gICAgY29uc3QgcmFuZ2UgPSBtYXggLSBtaW4gfHwgMTtcbiAgICBjb25zdCBwYWRMID0gNDQ7XG4gICAgY29uc3QgcGFkUiA9IDE4O1xuICAgIGNvbnN0IHBhZFQgPSAxODtcbiAgICBjb25zdCBwYWRCID0gMzA7XG5cbiAgICBjdHguc3Ryb2tlU3R5bGUgPSBcInJnYmEoMjU1LDI1NSwyNTUsMC4wOClcIjtcbiAgICBjdHgubGluZVdpZHRoID0gMTtcblxuICAgIGZvciAobGV0IGkgPSAwOyBpIDwgNDsgaSsrKSB7XG4gICAgICBjb25zdCB5ID0gcGFkVCArICgoaCAtIHBhZFQgLSBwYWRCKSAvIDMpICogaTtcbiAgICAgIGN0eC5iZWdpblBhdGgoKTtcbiAgICAgIGN0eC5tb3ZlVG8ocGFkTCwgeSk7XG4gICAgICBjdHgubGluZVRvKHcgLSBwYWRSLCB5KTtcbiAgICAgIGN0eC5zdHJva2UoKTtcbiAgICB9XG5cbiAgICBjdHguYmVnaW5QYXRoKCk7XG4gICAgcG9pbnRzLmZvckVhY2goKHAsIGkpID0+IHtcbiAgICAgIGNvbnN0IHggPSBwYWRMICsgKGkgLyBNYXRoLm1heCgxLCBwb2ludHMubGVuZ3RoIC0gMSkpICogKHcgLSBwYWRMIC0gcGFkUik7XG4gICAgICBjb25zdCB5ID0gaCAtIHBhZEIgLSAoKHAucHJpY2UgLSBtaW4pIC8gcmFuZ2UpICogKGggLSBwYWRUIC0gcGFkQik7XG4gICAgICBpZiAoaSA9PT0gMCkgY3R4Lm1vdmVUbyh4LCB5KTtcbiAgICAgIGVsc2UgY3R4LmxpbmVUbyh4LCB5KTtcbiAgICB9KTtcbiAgICBjdHguc3Ryb2tlU3R5bGUgPSBcIiMxMGI5ODFcIjtcbiAgICBjdHgubGluZVdpZHRoID0gMztcbiAgICBjdHguc3Ryb2tlKCk7XG5cbiAgICBwb2ludHMuZm9yRWFjaCgocCwgaSkgPT4ge1xuICAgICAgY29uc3QgeCA9IHBhZEwgKyAoaSAvIE1hdGgubWF4KDEsIHBvaW50cy5sZW5ndGggLSAxKSkgKiAodyAtIHBhZEwgLSBwYWRSKTtcbiAgICAgIGNvbnN0IHkgPSBoIC0gcGFkQiAtICgocC5wcmljZSAtIG1pbikgLyByYW5nZSkgKiAoaCAtIHBhZFQgLSBwYWRCKTtcbiAgICAgIGN0eC5iZWdpblBhdGgoKTtcbiAgICAgIGN0eC5hcmMoeCwgeSwgMywgMCwgTWF0aC5QSSAqIDIpO1xuICAgICAgY3R4LmZpbGxTdHlsZSA9IFwiIzEwYjk4MVwiO1xuICAgICAgY3R4LmZpbGwoKTtcbiAgICB9KTtcblxuICAgIGN0eC5maWxsU3R5bGUgPSBcIiM5Y2EzYWZcIjtcbiAgICBjdHguZm9udCA9IFwiMTJweCB1aS1tb25vc3BhY2UsIG1vbm9zcGFjZVwiO1xuICAgIGN0eC5maWxsVGV4dChgJCR7bWF4LnRvRml4ZWQoMil9YCwgNiwgMTQpO1xuICAgIGN0eC5maWxsVGV4dChgJCR7bWluLnRvRml4ZWQoMil9YCwgNiwgaCAtIDEwKTtcblxuICAgIHBvaW50cy5mb3JFYWNoKChwLCBpKSA9PiB7XG4gICAgICBjb25zdCB4ID0gcGFkTCArIChpIC8gTWF0aC5tYXgoMSwgcG9pbnRzLmxlbmd0aCAtIDEpKSAqICh3IC0gcGFkTCAtIHBhZFIpO1xuICAgICAgY3R4LmZpbGxUZXh0KFN0cmluZyhwLnJvdW5kKSwgeCAtIDMsIGggLSA4KTtcbiAgICB9KTtcbiAgfVxufVxuXG4vLyBcdTI1MDBcdTI1MDBcdTI1MDAgTG9iYnlNYW5hZ2VyIFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFxuY2xhc3MgTG9iYnlNYW5hZ2VyIHtcbiAgY29uc3RydWN0b3Ioc29ja2V0LCBjZmcpIHtcbiAgICB0aGlzLnNvY2tldCA9IHNvY2tldDtcbiAgICB0aGlzLmNmZyA9IGNmZztcbiAgICB0aGlzLmNoYW5uZWwgPSBudWxsO1xuICAgIHRoaXMuam9pbmVkID0gZmFsc2U7XG4gIH1cblxuICBpbml0KCkge1xuICAgIHRoaXMuY2hhbm5lbCA9IHRoaXMuc29ja2V0LmNoYW5uZWwoXCJsb2JieTpnZW5lcmFsXCIsIHt9KTtcblxuICAgIHRoaXMuY2hhbm5lbC5vbihcIm9wZW5fbWF0Y2hlc1wiLCAocCkgPT5cbiAgICAgIHRoaXMuX3VwZGF0ZU1hdGNoTGlzdChwLm1hdGNoZXMgfHwgW10pLFxuICAgICk7XG4gICAgdGhpcy5jaGFubmVsLm9uKFwibWF0Y2hfY3JlYXRlZFwiLCAobWF0Y2gpID0+IHRoaXMuX2FkZE9yVXBkYXRlTWF0Y2gobWF0Y2gpKTtcbiAgICB0aGlzLmNoYW5uZWwub24oXCJtYXRjaF91cGRhdGVkXCIsIChtYXRjaCkgPT4gdGhpcy5fYWRkT3JVcGRhdGVNYXRjaChtYXRjaCkpO1xuICAgIHRoaXMuY2hhbm5lbC5vbihcIm1hdGNoX3N0YXJ0ZWRcIiwgKHApID0+IHRoaXMuX29uTWF0Y2hTdGFydGVkKHAubWF0Y2hfaWQpKTtcbiAgICB0aGlzLmNoYW5uZWwub24oXCJwcmVzZW5jZV9zdGF0ZVwiLCAoc3RhdGUpID0+IHRoaXMuX29uUHJlc2VuY2VTdGF0ZShzdGF0ZSkpO1xuICAgIHRoaXMuY2hhbm5lbC5vbihcInByZXNlbmNlX2RpZmZcIiwgKGRpZmYpID0+IHRoaXMuX29uUHJlc2VuY2VEaWZmKGRpZmYpKTtcblxuICAgIHRoaXMuY2hhbm5lbFxuICAgICAgLmpvaW4oKVxuICAgICAgLnJlY2VpdmUoXCJva1wiLCAoKSA9PiB7XG4gICAgICAgIHRoaXMuam9pbmVkID0gdHJ1ZTtcbiAgICAgICAgdGhpcy5fd2lyZUpvaW5CdXR0b25zKGRvY3VtZW50KTtcbiAgICAgIH0pXG4gICAgICAucmVjZWl2ZShcImVycm9yXCIsICgpID0+IHtcbiAgICAgICAgdGhpcy5fd2lyZUpvaW5CdXR0b25zKGRvY3VtZW50KTtcbiAgICAgIH0pO1xuICB9XG5cbiAgcHVzaChldmVudCwgcGF5bG9hZCkge1xuICAgIGlmICh0aGlzLmNoYW5uZWwpIHJldHVybiB0aGlzLmNoYW5uZWwucHVzaChldmVudCwgcGF5bG9hZCk7XG4gIH1cblxuICBfd2lyZUpvaW5CdXR0b25zKHJvb3QgPSBkb2N1bWVudCkge1xuICAgIHJvb3RcbiAgICAgIC5xdWVyeVNlbGVjdG9yQWxsKFwiLmpvaW4tbWF0Y2gtYnRuOm5vdChbZGF0YS13aXJlZF0pXCIpXG4gICAgICAuZm9yRWFjaCgoYnRuKSA9PiB7XG4gICAgICAgIGJ0bi5kYXRhc2V0LndpcmVkID0gXCIxXCI7XG4gICAgICAgIGJ0bi5hZGRFdmVudExpc3RlbmVyKFwiY2xpY2tcIiwgKGUpID0+IHtcbiAgICAgICAgICBlLnByZXZlbnREZWZhdWx0KCk7XG4gICAgICAgICAgY29uc3QgbWF0Y2hJZCA9XG4gICAgICAgICAgICBidG4uZGF0YXNldC5tYXRjaElkIHx8XG4gICAgICAgICAgICBidG4uY2xvc2VzdChcIltkYXRhLW1hdGNoLWlkXVwiKT8uZGF0YXNldC5tYXRjaElkO1xuICAgICAgICAgIGlmICghbWF0Y2hJZCkgcmV0dXJuO1xuICAgICAgICAgIHRoaXMuX2pvaW5NYXRjaChtYXRjaElkLCBidG4pO1xuICAgICAgICB9KTtcbiAgICAgIH0pO1xuICB9XG5cbiAgX2pvaW5NYXRjaChtYXRjaElkLCBidG4pIHtcbiAgICBjb25zdCBvcmlnaW5hbCA9IGJ0bi50ZXh0Q29udGVudDtcbiAgICBidG4uZGlzYWJsZWQgPSB0cnVlO1xuICAgIGJ0bi50ZXh0Q29udGVudCA9IFwiSm9pbmluZy4uLlwiO1xuXG4gICAgdGhpcy5jaGFubmVsXG4gICAgICAucHVzaChcImpvaW5fbWF0Y2hcIiwgeyBtYXRjaF9pZDogbWF0Y2hJZCB9KVxuICAgICAgLnJlY2VpdmUoXCJva1wiLCAoKSA9PiB7XG4gICAgICAgIHdpbmRvdy5sb2NhdGlvbi5ocmVmID0gYC9tYXRjaGVzLyR7bWF0Y2hJZH1gO1xuICAgICAgfSlcbiAgICAgIC5yZWNlaXZlKFwiZXJyb3JcIiwgKGVycikgPT4ge1xuICAgICAgICBidG4uZGlzYWJsZWQgPSBmYWxzZTtcbiAgICAgICAgYnRuLnRleHRDb250ZW50ID0gb3JpZ2luYWw7XG4gICAgICAgIFRvYXN0LnNob3coXG4gICAgICAgICAgYENvdWxkIG5vdCBqb2luOiAke2Vycj8ucmVhc29uIHx8IFwidW5rbm93biBlcnJvclwifWAsXG4gICAgICAgICAgXCJlcnJvclwiLFxuICAgICAgICApO1xuICAgICAgfSlcbiAgICAgIC5yZWNlaXZlKFwidGltZW91dFwiLCAoKSA9PiB7XG4gICAgICAgIGJ0bi5kaXNhYmxlZCA9IGZhbHNlO1xuICAgICAgICBidG4udGV4dENvbnRlbnQgPSBvcmlnaW5hbDtcbiAgICAgICAgVG9hc3Quc2hvdyhcIkpvaW4gdGltZWQgb3V0LiBQbGVhc2UgdHJ5IGFnYWluLlwiLCBcIndhcm5pbmdcIik7XG4gICAgICB9KTtcbiAgfVxuXG4gIF9hZGRPclVwZGF0ZU1hdGNoKG1hdGNoKSB7XG4gICAgY29uc3QgbGlzdCA9IGRvY3VtZW50LmdldEVsZW1lbnRCeUlkKFwib3Blbi1tYXRjaGVzLWxpc3RcIik7XG4gICAgaWYgKCFsaXN0IHx8ICFtYXRjaCkgcmV0dXJuO1xuXG4gICAgZG9jdW1lbnQuZ2V0RWxlbWVudEJ5SWQoXCJuby1tYXRjaGVzLXBsYWNlaG9sZGVyXCIpPy5yZW1vdmUoKTtcblxuICAgIGNvbnN0IGV4aXN0aW5nID0gbGlzdC5xdWVyeVNlbGVjdG9yKGBbZGF0YS1tYXRjaC1pZD1cIiR7bWF0Y2guaWR9XCJdYCk7XG4gICAgY29uc3QgY2FyZCA9IHRoaXMuX2J1aWxkTWF0Y2hDYXJkKG1hdGNoKTtcblxuICAgIGlmIChleGlzdGluZykgZXhpc3RpbmcucmVwbGFjZVdpdGgoY2FyZCk7XG4gICAgZWxzZSBsaXN0Lmluc2VydEFkamFjZW50RWxlbWVudChcImFmdGVyYmVnaW5cIiwgY2FyZCk7XG5cbiAgICB0aGlzLl93aXJlSm9pbkJ1dHRvbnMobGlzdCk7XG4gIH1cblxuICBfYnVpbGRNYXRjaENhcmQobWF0Y2gpIHtcbiAgICBjb25zdCBkaXYgPSBkb2N1bWVudC5jcmVhdGVFbGVtZW50KFwiZGl2XCIpO1xuICAgIGRpdi5jbGFzc05hbWUgPSBcImNhcmQgY2FyZC1ob3ZlciBwLTQgbWF0Y2gtY2FyZCBmYWRlLWluLXVwXCI7XG4gICAgZGl2LmRhdGFzZXQubWF0Y2hJZCA9IG1hdGNoLmlkO1xuXG4gICAgY29uc3QgaXNGdWxsID0gKG1hdGNoLnBsYXllcl9jb3VudCB8fCAwKSA+PSBtYXRjaC5tYXhfcGxheWVycztcblxuICAgIGRpdi5pbm5lckhUTUwgPSBgXG4gICAgICA8ZGl2IGNsYXNzPVwiZmxleCBpdGVtcy1jZW50ZXIganVzdGlmeS1iZXR3ZWVuXCI+XG4gICAgICAgIDxkaXYgY2xhc3M9XCJmbGV4IGl0ZW1zLWNlbnRlciBnYXAtM1wiPlxuICAgICAgICAgIDxkaXYgY2xhc3M9XCJ3LTkgaC05IHJvdW5kZWQtbGcgYmctZW1lcmFsZC01MDAvMTAgYm9yZGVyIGJvcmRlci1lbWVyYWxkLTUwMC8yMCBmbGV4IGl0ZW1zLWNlbnRlciBqdXN0aWZ5LWNlbnRlciBzaHJpbmstMFwiPlxuICAgICAgICAgICAgPHN2ZyBjbGFzcz1cInctNCBoLTQgdGV4dC1lbWVyYWxkLTQwMFwiIGZpbGw9XCJub25lXCIgdmlld0JveD1cIjAgMCAyNCAyNFwiIHN0cm9rZT1cImN1cnJlbnRDb2xvclwiIHN0cm9rZS13aWR0aD1cIjJcIj5cbiAgICAgICAgICAgICAgPHBvbHlsaW5lIHBvaW50cz1cIjIyIDcgMTMuNSAxNS41IDguNSAxMC41IDIgMTdcIj48L3BvbHlsaW5lPlxuICAgICAgICAgICAgPC9zdmc+XG4gICAgICAgICAgPC9kaXY+XG4gICAgICAgICAgPGRpdj5cbiAgICAgICAgICAgIDxkaXYgY2xhc3M9XCJmb250LXNlbWlib2xkIHRleHQtc20gdGV4dC13aGl0ZSBmb250LW1vbm9cIj4ke2VzY2FwZUh0bWwobWF0Y2gubmFtZSl9PC9kaXY+XG4gICAgICAgICAgICA8ZGl2IGNsYXNzPVwidGV4dC14cyB0ZXh0LWdyYXktNTAwIG10LTAuNVwiPlxuICAgICAgICAgICAgICBob3N0ZWQgYnkgPHNwYW4gY2xhc3M9XCJ0ZXh0LWdyYXktNDAwXCI+JHtlc2NhcGVIdG1sKG1hdGNoLmhvc3RfdXNlcm5hbWUgfHwgXCJcdTIwMTRcIil9PC9zcGFuPlxuICAgICAgICAgICAgPC9kaXY+XG4gICAgICAgICAgPC9kaXY+XG4gICAgICAgIDwvZGl2PlxuICAgICAgICA8ZGl2IGNsYXNzPVwiZmxleCBpdGVtcy1jZW50ZXIgZ2FwLTNcIj5cbiAgICAgICAgICA8ZGl2IGNsYXNzPVwidGV4dC1yaWdodFwiPlxuICAgICAgICAgICAgPGRpdiBjbGFzcz1cInRleHQteHMgZm9udC1tb25vIHRleHQtZ3JheS00MDBcIj5cbiAgICAgICAgICAgICAgPHNwYW4gY2xhc3M9XCJ0ZXh0LXdoaXRlIGZvbnQtc2VtaWJvbGRcIj4ke21hdGNoLnBsYXllcl9jb3VudCB8fCAwfTwvc3Bhbj4vJHttYXRjaC5tYXhfcGxheWVycyB8fCA0fVxuICAgICAgICAgICAgPC9kaXY+XG4gICAgICAgICAgPC9kaXY+XG4gICAgICAgICAgJHtcbiAgICAgICAgICAgIGlzRnVsbFxuICAgICAgICAgICAgICA/IGA8c3BhbiBjbGFzcz1cImJ0bi1naG9zdCB0ZXh0LXhzIHB5LTEuNSBweC0zIG9wYWNpdHktNDAgcG9pbnRlci1ldmVudHMtbm9uZVwiPkZ1bGw8L3NwYW4+YFxuICAgICAgICAgICAgICA6IGA8YnV0dG9uIHR5cGU9XCJidXR0b25cIiBkYXRhLW1hdGNoLWlkPVwiJHtlc2NhcGVIdG1sKG1hdGNoLmlkKX1cIiBjbGFzcz1cImpvaW4tbWF0Y2gtYnRuIGJ0bi1wcmltYXJ5IHRleHQteHMgcHktMS41IHB4LTRcIj5Kb2luPC9idXR0b24+YFxuICAgICAgICAgIH1cbiAgICAgICAgPC9kaXY+XG4gICAgICA8L2Rpdj5cbiAgICBgO1xuXG4gICAgcmV0dXJuIGRpdjtcbiAgfVxuXG4gIF91cGRhdGVNYXRjaExpc3QobWF0Y2hlcykge1xuICAgIGNvbnN0IGxpc3QgPSBkb2N1bWVudC5nZXRFbGVtZW50QnlJZChcIm9wZW4tbWF0Y2hlcy1saXN0XCIpO1xuICAgIGlmICghbGlzdCkgcmV0dXJuO1xuXG4gICAgaWYgKCFtYXRjaGVzLmxlbmd0aCkgcmV0dXJuO1xuXG4gICAgbGlzdC5pbm5lckhUTUwgPSBcIlwiO1xuICAgIG1hdGNoZXMuZm9yRWFjaCgobSkgPT4gbGlzdC5hcHBlbmRDaGlsZCh0aGlzLl9idWlsZE1hdGNoQ2FyZChtKSkpO1xuICAgIHRoaXMuX3dpcmVKb2luQnV0dG9ucyhsaXN0KTtcbiAgfVxuXG4gIF9vbk1hdGNoU3RhcnRlZChtYXRjaElkKSB7XG4gICAgY29uc3QgY2FyZCA9IGRvY3VtZW50LnF1ZXJ5U2VsZWN0b3IoXG4gICAgICBgI29wZW4tbWF0Y2hlcy1saXN0IFtkYXRhLW1hdGNoLWlkPVwiJHttYXRjaElkfVwiXWAsXG4gICAgKTtcbiAgICBpZiAoY2FyZCkgY2FyZC5yZW1vdmUoKTtcbiAgfVxuXG4gIF9vblByZXNlbmNlU3RhdGUoc3RhdGUpIHtcbiAgICBjb25zdCBsaXN0ID0gZG9jdW1lbnQuZ2V0RWxlbWVudEJ5SWQoXCJsb2JieS1wcmVzZW5jZS1saXN0XCIpO1xuICAgIGNvbnN0IGNvdW50ZXIgPSBkb2N1bWVudC5nZXRFbGVtZW50QnlJZChcImxvYmJ5LW9ubGluZS1jb3VudFwiKTtcbiAgICBpZiAoIWxpc3QpIHJldHVybjtcblxuICAgIGNvbnN0IHVzZXJzID0gW107XG4gICAgT2JqZWN0LnZhbHVlcyhzdGF0ZSB8fCB7fSkuZm9yRWFjaCgoZW50cnkpID0+IHtcbiAgICAgIGNvbnN0IG1ldGEgPSBlbnRyeS5tZXRhcz8uWzBdO1xuICAgICAgaWYgKG1ldGE/LnVzZXJuYW1lKSB1c2Vycy5wdXNoKG1ldGEudXNlcm5hbWUpO1xuICAgIH0pO1xuXG4gICAgaWYgKGNvdW50ZXIpIGNvdW50ZXIudGV4dENvbnRlbnQgPSBTdHJpbmcodXNlcnMubGVuZ3RoKTtcbiAgICB0aGlzLl9yZW5kZXJQcmVzZW5jZUxpc3QobGlzdCwgdXNlcnMpO1xuICB9XG5cbiAgX29uUHJlc2VuY2VEaWZmKGRpZmYpIHtcbiAgICBjb25zdCBsaXN0ID0gZG9jdW1lbnQuZ2V0RWxlbWVudEJ5SWQoXCJsb2JieS1wcmVzZW5jZS1saXN0XCIpO1xuICAgIGlmICghbGlzdCkgcmV0dXJuO1xuXG4gICAgY29uc3QgY3VycmVudCA9IG5ldyBNYXAoKTtcbiAgICBsaXN0LnF1ZXJ5U2VsZWN0b3JBbGwoXCJbZGF0YS1wcmVzZW5jZS11c2VyXVwiKS5mb3JFYWNoKChlbCkgPT4ge1xuICAgICAgY3VycmVudC5zZXQoZWwuZGF0YXNldC5wcmVzZW5jZVVzZXIsIGVsKTtcbiAgICB9KTtcblxuICAgIE9iamVjdC52YWx1ZXMoZGlmZj8uam9pbnMgfHwge30pLmZvckVhY2goKGVudHJ5KSA9PiB7XG4gICAgICBjb25zdCBtZXRhID0gZW50cnkubWV0YXM/LlswXTtcbiAgICAgIGlmICghbWV0YT8udXNlcm5hbWUgfHwgY3VycmVudC5oYXMobWV0YS51c2VybmFtZSkpIHJldHVybjtcbiAgICAgIGNvbnN0IGVsID0gdGhpcy5fYnVpbGRQcmVzZW5jZVJvdyhtZXRhLnVzZXJuYW1lKTtcbiAgICAgIGxpc3QuYXBwZW5kQ2hpbGQoZWwpO1xuICAgICAgY3VycmVudC5zZXQobWV0YS51c2VybmFtZSwgZWwpO1xuICAgIH0pO1xuXG4gICAgT2JqZWN0LnZhbHVlcyhkaWZmPy5sZWF2ZXMgfHwge30pLmZvckVhY2goKGVudHJ5KSA9PiB7XG4gICAgICBjb25zdCBtZXRhID0gZW50cnkubWV0YXM/LlswXTtcbiAgICAgIGlmICghbWV0YT8udXNlcm5hbWUpIHJldHVybjtcbiAgICAgIGNvbnN0IGVsID0gY3VycmVudC5nZXQobWV0YS51c2VybmFtZSk7XG4gICAgICBpZiAoZWwpIHtcbiAgICAgICAgZWwucmVtb3ZlKCk7XG4gICAgICAgIGN1cnJlbnQuZGVsZXRlKG1ldGEudXNlcm5hbWUpO1xuICAgICAgfVxuICAgIH0pO1xuXG4gICAgY29uc3QgY291bnRlciA9IGRvY3VtZW50LmdldEVsZW1lbnRCeUlkKFwibG9iYnktb25saW5lLWNvdW50XCIpO1xuICAgIGlmIChjb3VudGVyKSBjb3VudGVyLnRleHRDb250ZW50ID0gU3RyaW5nKGN1cnJlbnQuc2l6ZSk7XG5cbiAgICBpZiAoY3VycmVudC5zaXplID09PSAwKSB7XG4gICAgICBsaXN0LmlubmVySFRNTCA9IGA8cCBjbGFzcz1cInRleHQteHMgdGV4dC1ncmF5LTYwMCBmb250LW1vbm9cIj5Ob2JvZHkgZWxzZSBvbmxpbmUuPC9wPmA7XG4gICAgfVxuICB9XG5cbiAgX3JlbmRlclByZXNlbmNlTGlzdChsaXN0LCB1c2Vycykge1xuICAgIGlmICghdXNlcnMubGVuZ3RoKSB7XG4gICAgICBsaXN0LmlubmVySFRNTCA9IGA8cCBjbGFzcz1cInRleHQteHMgdGV4dC1ncmF5LTYwMCBmb250LW1vbm9cIj5Ob2JvZHkgZWxzZSBvbmxpbmUuPC9wPmA7XG4gICAgICByZXR1cm47XG4gICAgfVxuXG4gICAgbGlzdC5pbm5lckhUTUwgPSBcIlwiO1xuICAgIHVzZXJzLmZvckVhY2goKHUpID0+IGxpc3QuYXBwZW5kQ2hpbGQodGhpcy5fYnVpbGRQcmVzZW5jZVJvdyh1KSkpO1xuICB9XG5cbiAgX2J1aWxkUHJlc2VuY2VSb3codXNlcm5hbWUpIHtcbiAgICBjb25zdCBlbCA9IGRvY3VtZW50LmNyZWF0ZUVsZW1lbnQoXCJkaXZcIik7XG4gICAgZWwuY2xhc3NOYW1lID0gXCJmbGV4IGl0ZW1zLWNlbnRlciBnYXAtMiBmYWRlLWluLXVwXCI7XG4gICAgZWwuZGF0YXNldC5wcmVzZW5jZVVzZXIgPSB1c2VybmFtZTtcbiAgICBlbC5pbm5lckhUTUwgPSBgXG4gICAgICA8ZGl2IGNsYXNzPVwicHJlc2VuY2UtZG90XCI+PC9kaXY+XG4gICAgICA8c3BhbiBjbGFzcz1cInRleHQteHMgZm9udC1tb25vIHRleHQtZ3JheS0zMDBcIj4ke2VzY2FwZUh0bWwodXNlcm5hbWUpfTwvc3Bhbj5cbiAgICBgO1xuICAgIHJldHVybiBlbDtcbiAgfVxufVxuXG4vLyBcdTI1MDBcdTI1MDBcdTI1MDAgTWF0Y2hNYW5hZ2VyIFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFxuY2xhc3MgTWF0Y2hNYW5hZ2VyIHtcbiAgY29uc3RydWN0b3Ioc29ja2V0LCBjZmcsIGxvYmJ5TWdyKSB7XG4gICAgdGhpcy5zb2NrZXQgPSBzb2NrZXQ7XG4gICAgdGhpcy5jZmcgPSBjZmc7XG4gICAgdGhpcy5sb2JieU1nciA9IGxvYmJ5TWdyO1xuICAgIHRoaXMuY2hhbm5lbCA9IG51bGw7XG4gICAgdGhpcy5wbGF5ZXJzID0gW107XG4gICAgdGhpcy5jb21wYW5pZXMgPSBbXTtcbiAgICB0aGlzLm15U3RhdGUgPSBudWxsO1xuICAgIHRoaXMucGhhc2UgPSBudWxsO1xuICAgIHRoaXMuY291bnRkb3duID0gbnVsbDtcbiAgICB0aGlzLnBoYXNlT3ZlcmxheVRpbWVyID0gbnVsbDtcblxuICAgIHRoaXMucHJldlByaWNlcyA9IHt9O1xuICAgIHRoaXMuc3BhcmtsaW5lQ2hhcnRzID0ge307XG4gICAgdGhpcy5wcmljZUhpc3RvcnkgPSB7fTtcbiAgICB0aGlzLnNlbGVjdGVkVGlja2VyID0gbnVsbDtcbiAgICB0aGlzLmRldGFpbENoYXJ0ID0gbmV3IERldGFpbENoYXJ0KFwic3RvY2stZGV0YWlsLWNoYXJ0XCIpO1xuICB9XG5cbiAgaW5pdCgpIHtcbiAgICB0aGlzLmNoYW5uZWwgPSB0aGlzLnNvY2tldC5jaGFubmVsKGBtYXRjaDoke3RoaXMuY2ZnLm1hdGNoSWR9YCwge30pO1xuXG4gICAgdGhpcy5jaGFubmVsLm9uKFwic3RhdGVfdXBkYXRlZFwiLCAocCkgPT4gdGhpcy5fb25TdGF0ZVVwZGF0ZWQocCkpO1xuICAgIHRoaXMuY2hhbm5lbC5vbihcInBoYXNlX2NoYW5nZWRcIiwgKHApID0+IHRoaXMuX29uUGhhc2VDaGFuZ2VkKHApKTtcbiAgICB0aGlzLmNoYW5uZWwub24oXCJwcml2YXRlX3N0YXRlXCIsIChwKSA9PiB0aGlzLl9vblByaXZhdGVTdGF0ZShwKSk7XG4gICAgdGhpcy5jaGFubmVsLm9uKFwicHJpdmF0ZV9ldmVudHNcIiwgKHApID0+IHRoaXMuX29uUHJpdmF0ZUV2ZW50cyhwKSk7XG4gICAgdGhpcy5jaGFubmVsLm9uKFwibmV3X21lc3NhZ2VcIiwgKG0pID0+IHRoaXMuX2FwcGVuZENoYXQobSkpO1xuICAgIHRoaXMuY2hhbm5lbC5vbihcIm1hdGNoX2ZpbmlzaGVkXCIsIChwKSA9PiB0aGlzLl9vbk1hdGNoRmluaXNoZWQocCkpO1xuICAgIHRoaXMuY2hhbm5lbC5vbihcInByZXNlbmNlX3N0YXRlXCIsIChzKSA9PiB0aGlzLl9vblByZXNlbmNlU3RhdGUocykpO1xuICAgIHRoaXMuY2hhbm5lbC5vbihcInByZXNlbmNlX2RpZmZcIiwgKGQpID0+IHRoaXMuX29uUHJlc2VuY2VEaWZmKGQpKTtcbiAgICB0aGlzLmNoYW5uZWwub24oXCJ3YWl0aW5nX3Jvb21fdXBkYXRlZFwiLCAocCkgPT5cbiAgICAgIHRoaXMuX29uV2FpdGluZ1Jvb21VcGRhdGVkKHApLFxuICAgICk7XG5cbiAgICB0aGlzLmNoYW5uZWxcbiAgICAgIC5qb2luKClcbiAgICAgIC5yZWNlaXZlKFwib2tcIiwgKCkgPT4ge30pXG4gICAgICAucmVjZWl2ZShcImVycm9yXCIsICh7IHJlYXNvbiB9KSA9PiB7XG4gICAgICAgIFRvYXN0LnNob3coYENvdWxkIG5vdCBqb2luIG1hdGNoOiAke3JlYXNvbn1gLCBcImVycm9yXCIpO1xuICAgICAgfSk7XG5cbiAgICB0aGlzLl93aXJlQ29udHJvbHMoKTtcbiAgfVxuXG4gIF93aXJlQ29udHJvbHMoKSB7XG4gICAgY29uc3Qgc3RhcnRCdG4gPSBkb2N1bWVudC5nZXRFbGVtZW50QnlJZChcInN0YXJ0LW1hdGNoLWJ0blwiKTtcbiAgICBpZiAoc3RhcnRCdG4pIHtcbiAgICAgIHN0YXJ0QnRuLmFkZEV2ZW50TGlzdGVuZXIoXCJjbGlja1wiLCAoZSkgPT4ge1xuICAgICAgICBlLnByZXZlbnREZWZhdWx0KCk7XG4gICAgICAgIHN0YXJ0QnRuLmRpc2FibGVkID0gdHJ1ZTtcbiAgICAgICAgc3RhcnRCdG4udGV4dENvbnRlbnQgPSBcIlN0YXJ0aW5nLi4uXCI7XG5cbiAgICAgICAgdGhpcy5sb2JieU1nclxuICAgICAgICAgIC5wdXNoKFwic3RhcnRfbWF0Y2hcIiwgeyBtYXRjaF9pZDogdGhpcy5jZmcubWF0Y2hJZCB9KVxuICAgICAgICAgID8ucmVjZWl2ZShcIm9rXCIsICgpID0+IHtcbiAgICAgICAgICAgIHN0YXJ0QnRuLnJlbW92ZSgpO1xuICAgICAgICAgIH0pXG4gICAgICAgICAgPy5yZWNlaXZlKFwiZXJyb3JcIiwgKGVycikgPT4ge1xuICAgICAgICAgICAgc3RhcnRCdG4uZGlzYWJsZWQgPSBmYWxzZTtcbiAgICAgICAgICAgIHN0YXJ0QnRuLnRleHRDb250ZW50ID0gXCJTdGFydCBNYXRjaFwiO1xuICAgICAgICAgICAgVG9hc3Quc2hvdyhcbiAgICAgICAgICAgICAgYENvdWxkIG5vdCBzdGFydDogJHtlcnI/LnJlYXNvbiB8fCBcInVua25vd24gZXJyb3JcIn1gLFxuICAgICAgICAgICAgICBcImVycm9yXCIsXG4gICAgICAgICAgICApO1xuICAgICAgICAgIH0pO1xuICAgICAgfSk7XG4gICAgfVxuXG4gICAgZG9jdW1lbnQucXVlcnlTZWxlY3RvckFsbChcIi5hY3Rpb24tYnRuXCIpLmZvckVhY2goKGJ0bikgPT4ge1xuICAgICAgYnRuLmFkZEV2ZW50TGlzdGVuZXIoXCJjbGlja1wiLCAoZSkgPT4ge1xuICAgICAgICBlLnByZXZlbnREZWZhdWx0KCk7XG4gICAgICAgIGlmIChidG4uZGlzYWJsZWQpIHJldHVybjtcbiAgICAgICAgdGhpcy5fb3BlbkFjdGlvbk1vZGFsKGJ0bi5kYXRhc2V0KTtcbiAgICAgIH0pO1xuICAgIH0pO1xuXG4gICAgZG9jdW1lbnRcbiAgICAgIC5nZXRFbGVtZW50QnlJZChcImNvcHktaW52aXRlLWJ0blwiKVxuICAgICAgPy5hZGRFdmVudExpc3RlbmVyKFwiY2xpY2tcIiwgKGUpID0+IHtcbiAgICAgICAgZS5wcmV2ZW50RGVmYXVsdCgpO1xuICAgICAgICBjb25zdCB1cmwgPSB3aW5kb3cubG9jYXRpb24uaHJlZjtcbiAgICAgICAgbmF2aWdhdG9yLmNsaXBib2FyZFxuICAgICAgICAgID8ud3JpdGVUZXh0KHVybClcbiAgICAgICAgICAudGhlbigoKSA9PiBUb2FzdC5zaG93KFwiSW52aXRlIGxpbmsgY29waWVkXCIsIFwic3VjY2Vzc1wiKSlcbiAgICAgICAgICAuY2F0Y2goKCkgPT4gd2luZG93LnByb21wdChcIkNvcHkgdGhpcyBpbnZpdGUgbGluazpcIiwgdXJsKSk7XG4gICAgICB9KTtcblxuICAgIGRvY3VtZW50LmdldEVsZW1lbnRCeUlkKFwibW9kYWwtY2xvc2VcIik/LmFkZEV2ZW50TGlzdGVuZXIoXCJjbGlja1wiLCAoZSkgPT4ge1xuICAgICAgZS5wcmV2ZW50RGVmYXVsdCgpO1xuICAgICAgdGhpcy5fY2xvc2VNb2RhbCgpO1xuICAgIH0pO1xuXG4gICAgZG9jdW1lbnQuZ2V0RWxlbWVudEJ5SWQoXCJhY3Rpb24tbW9kYWxcIik/LmFkZEV2ZW50TGlzdGVuZXIoXCJjbGlja1wiLCAoZSkgPT4ge1xuICAgICAgaWYgKGUudGFyZ2V0ID09PSBlLmN1cnJlbnRUYXJnZXQpIHRoaXMuX2Nsb3NlTW9kYWwoKTtcbiAgICB9KTtcblxuICAgIGRvY3VtZW50LmdldEVsZW1lbnRCeUlkKFwibW9kYWwtc3VibWl0XCIpPy5hZGRFdmVudExpc3RlbmVyKFwiY2xpY2tcIiwgKGUpID0+IHtcbiAgICAgIGUucHJldmVudERlZmF1bHQoKTtcbiAgICAgIHRoaXMuX3N1Ym1pdEFjdGlvbigpO1xuICAgIH0pO1xuXG4gICAgZG9jdW1lbnRcbiAgICAgIC5nZXRFbGVtZW50QnlJZChcImlucHV0LXF1YW50aXR5XCIpXG4gICAgICA/LmFkZEV2ZW50TGlzdGVuZXIoXCJrZXlkb3duXCIsIChlKSA9PiB7XG4gICAgICAgIGlmIChlLmtleSA9PT0gXCJFbnRlclwiKSB7XG4gICAgICAgICAgZS5wcmV2ZW50RGVmYXVsdCgpO1xuICAgICAgICAgIHRoaXMuX3N1Ym1pdEFjdGlvbigpO1xuICAgICAgICB9XG4gICAgICB9KTtcblxuICAgIGNvbnN0IHNlbmRCdG4gPSBkb2N1bWVudC5nZXRFbGVtZW50QnlJZChcImNoYXQtc2VuZFwiKTtcbiAgICBjb25zdCBjaGF0SW5wdXQgPSBkb2N1bWVudC5nZXRFbGVtZW50QnlJZChcImNoYXQtaW5wdXRcIik7XG5cbiAgICBpZiAoc2VuZEJ0biAmJiBjaGF0SW5wdXQpIHtcbiAgICAgIHNlbmRCdG4uYWRkRXZlbnRMaXN0ZW5lcihcImNsaWNrXCIsIChlKSA9PiB7XG4gICAgICAgIGUucHJldmVudERlZmF1bHQoKTtcbiAgICAgICAgdGhpcy5fc2VuZENoYXRNZXNzYWdlKCk7XG4gICAgICB9KTtcblxuICAgICAgY2hhdElucHV0LmFkZEV2ZW50TGlzdGVuZXIoXCJrZXlkb3duXCIsIChlKSA9PiB7XG4gICAgICAgIGlmIChlLmtleSA9PT0gXCJFbnRlclwiICYmICFlLnNoaWZ0S2V5KSB7XG4gICAgICAgICAgZS5wcmV2ZW50RGVmYXVsdCgpO1xuICAgICAgICAgIHRoaXMuX3NlbmRDaGF0TWVzc2FnZSgpO1xuICAgICAgICB9XG4gICAgICB9KTtcbiAgICB9XG4gIH1cblxuICAvLyBcdTI1MDBcdTI1MDAgc3RhdGUgdXBkYXRlcyBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcblxuICBfb25TdGF0ZVVwZGF0ZWQocGF5bG9hZCkge1xuICAgIHRoaXMucGxheWVycyA9IHBheWxvYWQucGxheWVycyB8fCBbXTtcbiAgICB0aGlzLmNvbXBhbmllcyA9IHBheWxvYWQuY29tcGFuaWVzIHx8IFtdO1xuICAgIHRoaXMucGhhc2UgPSBwYXlsb2FkLnBoYXNlO1xuXG4gICAgdGhpcy5fcmVuZGVyUm91bmQocGF5bG9hZC5yb3VuZCwgcGF5bG9hZC50b3RhbF9yb3VuZHMpO1xuICAgIHRoaXMuX3JlbmRlclBoYXNlKHBheWxvYWQucGhhc2UpO1xuICAgIHRoaXMuX3JlbmRlclBsYXllcnMocGF5bG9hZC5wbGF5ZXJzIHx8IFtdKTtcbiAgICB0aGlzLl9yZW5kZXJNYXJrZXQocGF5bG9hZC5jb21wYW5pZXMgfHwgW10sIHBheWxvYWQucm91bmQpO1xuICAgIHRoaXMuX3JlbmRlckFjdGlvblBhbmVsKHBheWxvYWQucGhhc2UpO1xuICAgIHRoaXMuX3JlbmRlckV2ZW50cyhwYXlsb2FkLnB1YmxpY19ldmVudHMgfHwgW10pO1xuICAgIHRoaXMuX3JlbmRlck5ld3MocGF5bG9hZC5wdWJsaWNfZXZlbnRzIHx8IFtdKTtcbiAgICB0aGlzLl91cGRhdGVDaGF0SW5kaWNhdG9yKHBheWxvYWQucGhhc2UpO1xuXG4gICAgaWYgKHBheWxvYWQucGhhc2UgPT09IFwibmVnb3RpYXRpb25cIiAmJiBwYXlsb2FkLm5lZ290aWF0aW9uX2RlYWRsaW5lKSB7XG4gICAgICB0aGlzLl9zdGFydENvdW50ZG93bihuZXcgRGF0ZShwYXlsb2FkLm5lZ290aWF0aW9uX2RlYWRsaW5lKSk7XG4gICAgfSBlbHNlIGlmIChwYXlsb2FkLnBoYXNlICE9PSBcIm5lZ290aWF0aW9uXCIpIHtcbiAgICAgIHRoaXMuX2NsZWFyQ291bnRkb3duKCk7XG4gICAgfVxuICB9XG5cbiAgX29uUGhhc2VDaGFuZ2VkKHBheWxvYWQpIHtcbiAgICB0aGlzLnBoYXNlID0gcGF5bG9hZC5waGFzZTtcblxuICAgIHRoaXMuX3JlbmRlclJvdW5kKHBheWxvYWQucm91bmQsIHBheWxvYWQudG90YWxfcm91bmRzKTtcbiAgICB0aGlzLl9yZW5kZXJQaGFzZShwYXlsb2FkLnBoYXNlKTtcbiAgICB0aGlzLl9yZW5kZXJBY3Rpb25QYW5lbChwYXlsb2FkLnBoYXNlKTtcbiAgICB0aGlzLl91cGRhdGVDaGF0SW5kaWNhdG9yKHBheWxvYWQucGhhc2UpO1xuICAgIHRoaXMuX3Nob3dQaGFzZU92ZXJsYXkocGF5bG9hZC5waGFzZSk7XG5cbiAgICBpZiAocGF5bG9hZC5waGFzZSA9PT0gXCJhY3Rpb25fc3VibWlzc2lvblwiKSB7XG4gICAgICB0aGlzLl9jbGVhckNvdW50ZG93bigpO1xuICAgICAgdGhpcy5faGlkZShkb2N1bWVudC5nZXRFbGVtZW50QnlJZChcImFjdGlvbi1zdWJtaXR0ZWQtYmFkZ2VcIikpO1xuICAgICAgZG9jdW1lbnQucXVlcnlTZWxlY3RvckFsbChcIi5hY3Rpb24tYnRuXCIpLmZvckVhY2goKGIpID0+IHtcbiAgICAgICAgYi5kaXNhYmxlZCA9IGZhbHNlO1xuICAgICAgICBiLmNsYXNzTGlzdC5yZW1vdmUoXCJzdWJtaXR0ZWRcIik7XG4gICAgICB9KTtcbiAgICB9XG5cbiAgICBpZiAocGF5bG9hZC5waGFzZSA9PT0gXCJuZWdvdGlhdGlvblwiICYmIHBheWxvYWQubmVnb3RpYXRpb25fZGVhZGxpbmUpIHtcbiAgICAgIHRoaXMuX3N0YXJ0Q291bnRkb3duKG5ldyBEYXRlKHBheWxvYWQubmVnb3RpYXRpb25fZGVhZGxpbmUpKTtcbiAgICB9IGVsc2UgaWYgKHBheWxvYWQucGhhc2UgIT09IFwibmVnb3RpYXRpb25cIikge1xuICAgICAgdGhpcy5fY2xlYXJDb3VudGRvd24oKTtcbiAgICB9XG4gIH1cblxuICBfb25Qcml2YXRlU3RhdGUoc3RhdGUpIHtcbiAgICB0aGlzLm15U3RhdGUgPSBzdGF0ZTtcbiAgICB0aGlzLl9yZW5kZXJNeVN0YXRzKHN0YXRlKTtcbiAgfVxuXG4gIF9vblByaXZhdGVFdmVudHMocGF5bG9hZCkge1xuICAgIChwYXlsb2FkLmV2ZW50cyB8fCBbXSkuZm9yRWFjaCgoZXYpID0+IHtcbiAgICAgIGNvbnN0IG1zZyA9IGV2Lm1lc3NhZ2UgfHwgZXYuaGVhZGxpbmUgfHwgZXYudHlwZSB8fCBcIkV2ZW50XCI7XG4gICAgICBUb2FzdC5zaG93KG1zZywgZXYucG9zaXRpdmUgPyBcInN1Y2Nlc3NcIiA6IFwiaW5mb1wiKTtcbiAgICB9KTtcbiAgfVxuXG4gIF9vbk1hdGNoRmluaXNoZWQocGF5bG9hZCkge1xuICAgIHRoaXMuX2NsZWFyQ291bnRkb3duKCk7XG4gICAgdGhpcy5fcmVuZGVyTGVhZGVyYm9hcmQocGF5bG9hZC5sZWFkZXJib2FyZCB8fCBbXSk7XG4gICAgdGhpcy5fc2hvd1BoYXNlT3ZlcmxheShcImZpbmlzaGVkXCIpO1xuICAgIHRoaXMuX2hpZGUoZG9jdW1lbnQuZ2V0RWxlbWVudEJ5SWQoXCJhY3Rpb24tcGFuZWxcIikpO1xuICAgIHRoaXMuX3Nob3coZG9jdW1lbnQuZ2V0RWxlbWVudEJ5SWQoXCJsZWFkZXJib2FyZC1wYW5lbFwiKSwgXCJibG9ja1wiKTtcbiAgICBUb2FzdC5zaG93KFwiTWF0Y2ggb3ZlciEgRmluYWwgcmFua2luZ3MgYXJlIGluLlwiLCBcImluZm9cIiwgODAwMCk7XG4gIH1cblxuICAvLyBcdTI1MDBcdTI1MDAgcHJlc2VuY2UgXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXG5cbiAgX29uUHJlc2VuY2VTdGF0ZShzdGF0ZSkge1xuICAgIE9iamVjdC5lbnRyaWVzKHN0YXRlIHx8IHt9KS5mb3JFYWNoKChbdXNlcklkXSkgPT4ge1xuICAgICAgdGhpcy5fc2V0UGxheWVyT25saW5lKHVzZXJJZCwgdHJ1ZSk7XG4gICAgfSk7XG4gIH1cblxuICBfb25QcmVzZW5jZURpZmYoZGlmZikge1xuICAgIE9iamVjdC5lbnRyaWVzKGRpZmY/LmpvaW5zIHx8IHt9KS5mb3JFYWNoKChbdXNlcklkXSkgPT5cbiAgICAgIHRoaXMuX3NldFBsYXllck9ubGluZSh1c2VySWQsIHRydWUpLFxuICAgICk7XG4gICAgT2JqZWN0LmVudHJpZXMoZGlmZj8ubGVhdmVzIHx8IHt9KS5mb3JFYWNoKChbdXNlcklkXSkgPT5cbiAgICAgIHRoaXMuX3NldFBsYXllck9ubGluZSh1c2VySWQsIGZhbHNlKSxcbiAgICApO1xuICB9XG5cbiAgX29uV2FpdGluZ1Jvb21VcGRhdGVkKHBheWxvYWQpIHtcbiAgICBpZiAocGF5bG9hZC5wbGF5ZXJzKSB7XG4gICAgICB0aGlzLnBsYXllcnMgPSBwYXlsb2FkLnBsYXllcnM7XG4gICAgICB0aGlzLl9yZW5kZXJQbGF5ZXJzKHBheWxvYWQucGxheWVycyk7XG4gICAgfVxuICB9XG5cbiAgX3NldFBsYXllck9ubGluZSh1c2VySWQsIG9ubGluZSkge1xuICAgIGNvbnN0IHJvdyA9IGRvY3VtZW50LnF1ZXJ5U2VsZWN0b3IoYFtkYXRhLXBsYXllci1pZD1cIiR7dXNlcklkfVwiXWApO1xuICAgIGNvbnN0IGRvdCA9IHJvdz8ucXVlcnlTZWxlY3RvcihcIi5wcmVzZW5jZS1kb3RcIik7XG4gICAgaWYgKGRvdCkgZG90LmNsYXNzTGlzdC50b2dnbGUoXCJvZmZsaW5lXCIsICFvbmxpbmUpO1xuICB9XG5cbiAgLy8gXHUyNTAwXHUyNTAwIHJlbmRlcmluZyBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcblxuICBfcmVuZGVyUm91bmQocm91bmQsIHRvdGFsKSB7XG4gICAgY29uc3QgZWwgPSBkb2N1bWVudC5nZXRFbGVtZW50QnlJZChcImN1cnJlbnQtcm91bmRcIik7XG4gICAgaWYgKGVsKSBlbC50ZXh0Q29udGVudCA9IHJvdW5kIHx8IFwiXHUyMDE0XCI7XG5cbiAgICBjb25zdCBsYXN0Um91bmQgPSBkb2N1bWVudC5nZXRFbGVtZW50QnlJZChcImRldGFpbC1sYXN0LXJvdW5kXCIpO1xuICAgIGlmIChsYXN0Um91bmQpIGxhc3RSb3VuZC50ZXh0Q29udGVudCA9IHJvdW5kIHx8IFwiMVwiO1xuICB9XG5cbiAgX3JlbmRlclBoYXNlKHBoYXNlKSB7XG4gICAgY29uc3QgY29udGFpbmVyID0gZG9jdW1lbnQuZ2V0RWxlbWVudEJ5SWQoXCJwaGFzZS1iYWRnZS1jb250YWluZXJcIik7XG4gICAgaWYgKCFjb250YWluZXIpIHJldHVybjtcbiAgICBjb250YWluZXIuaW5uZXJIVE1MID0gYDxzcGFuIGNsYXNzPVwicGhhc2UtYmFkZ2UgcGhhc2UtJHtwaGFzZSB8fCBcIndhaXRpbmdcIn1cIj4ke3BoYXNlTGFiZWwocGhhc2UpfTwvc3Bhbj5gO1xuICB9XG5cbiAgX3JlbmRlckFjdGlvblBhbmVsKHBoYXNlKSB7XG4gICAgY29uc3QgcGFuZWwgPSBkb2N1bWVudC5nZXRFbGVtZW50QnlJZChcImFjdGlvbi1wYW5lbFwiKTtcbiAgICBpZiAoIXBhbmVsKSByZXR1cm47XG5cbiAgICBpZiAocGhhc2UgPT09IFwiYWN0aW9uX3N1Ym1pc3Npb25cIikgdGhpcy5fc2hvdyhwYW5lbCwgXCJibG9ja1wiKTtcbiAgICBlbHNlIHRoaXMuX2hpZGUocGFuZWwpO1xuICB9XG5cbiAgX3JlbmRlclBsYXllcnMocGxheWVycykge1xuICAgIGNvbnN0IGxpc3QgPSBkb2N1bWVudC5nZXRFbGVtZW50QnlJZChcInBsYXllcnMtbGlzdFwiKTtcbiAgICBpZiAoIWxpc3QgfHwgIXBsYXllcnMpIHJldHVybjtcblxuICAgIGxpc3QuaW5uZXJIVE1MID0gXCJcIjtcblxuICAgIHBsYXllcnNcbiAgICAgIC5zbGljZSgpXG4gICAgICAuc29ydCgoYSwgYikgPT4gKGEuc2VhdF9udW1iZXIgfHwgMCkgLSAoYi5zZWF0X251bWJlciB8fCAwKSlcbiAgICAgIC5mb3JFYWNoKChwKSA9PiB7XG4gICAgICAgIGNvbnN0IHJvdyA9IGRvY3VtZW50LmNyZWF0ZUVsZW1lbnQoXCJkaXZcIik7XG4gICAgICAgIHJvdy5jbGFzc05hbWUgPSBcImZsZXggaXRlbXMtY2VudGVyIGdhcC0yLjVcIjtcbiAgICAgICAgcm93LmRhdGFzZXQucGxheWVySWQgPSBwLnVzZXJfaWQ7XG5cbiAgICAgICAgcm93LmlubmVySFRNTCA9IGBcbiAgICAgICAgICA8ZGl2IGNsYXNzPVwicHJlc2VuY2UtZG90IG9mZmxpbmVcIj48L2Rpdj5cbiAgICAgICAgICA8ZGl2IGNsYXNzPVwiZmxleC0xIG1pbi13LTBcIj5cbiAgICAgICAgICAgIDxzcGFuIGNsYXNzPVwidGV4dC1zbSB0ZXh0LWdyYXktMzAwIGZvbnQtbW9ubyB0cnVuY2F0ZVwiPiR7ZXNjYXBlSHRtbChwLnVzZXJuYW1lKX08L3NwYW4+XG4gICAgICAgICAgPC9kaXY+XG4gICAgICAgICAgPHNwYW4gY2xhc3M9XCJ0ZXh0LXhzIGZvbnQtbW9ubyB0ZXh0LWFtYmVyLTQwMFwiPiR7KHAucmVndWxhdG9yeV9oZWF0IHx8IDApID4gMCA/IGBcdTI2OTYke3AucmVndWxhdG9yeV9oZWF0fWAgOiBcIlwifTwvc3Bhbj5cbiAgICAgICAgICA8c3BhbiBjbGFzcz1cInRleHQteHMgZm9udC1tb25vIHRleHQtYmx1ZS00MDBcIj4ke3AubGlxdWlkaXR5X2Zyb3plbiA/IFwiXHUyNzQ0XCIgOiBcIlwifTwvc3Bhbj5cbiAgICAgICAgICA8c3BhbiBjbGFzcz1cInRleHQteHMgZm9udC1tb25vIHRleHQtZW1lcmFsZC00MDBcIj4ke3AuaGFzX3N1Ym1pdHRlZCA/IFwiXHUyNzEzXCIgOiBcIlwifTwvc3Bhbj5cbiAgICAgICAgYDtcblxuICAgICAgICBsaXN0LmFwcGVuZENoaWxkKHJvdyk7XG4gICAgICB9KTtcbiAgfVxuXG4gIF9yZW5kZXJNeVN0YXRzKHN0YXRlKSB7XG4gICAgdGhpcy5fc2hvdyhkb2N1bWVudC5nZXRFbGVtZW50QnlJZChcIm15LXN0YXRzXCIpLCBcImJsb2NrXCIpO1xuXG4gICAgY29uc3QgY2FzaCA9IHBhcnNlRmxvYXQoc3RhdGUuY2FzaCB8fCAwKTtcbiAgICBjb25zdCBuZXRXb3J0aCA9IHBhcnNlRmxvYXQoc3RhdGUubmV0X3dvcnRoIHx8IDApO1xuXG4gICAgY29uc3QgY2FzaEVsID0gZG9jdW1lbnQuZ2V0RWxlbWVudEJ5SWQoXCJteS1jYXNoXCIpO1xuICAgIGNvbnN0IG53RWwgPSBkb2N1bWVudC5nZXRFbGVtZW50QnlJZChcIm15LW5ldHdvcnRoXCIpO1xuICAgIGNvbnN0IGhlYXRFbCA9IGRvY3VtZW50LmdldEVsZW1lbnRCeUlkKFwibXktaGVhdFwiKTtcblxuICAgIGlmIChjYXNoRWwpIGNhc2hFbC50ZXh0Q29udGVudCA9IGAkJHtjYXNoLnRvRml4ZWQoMil9YDtcbiAgICBpZiAobndFbCkgbndFbC50ZXh0Q29udGVudCA9IGAkJHtuZXRXb3J0aC50b0ZpeGVkKDIpfWA7XG4gICAgaWYgKGhlYXRFbCkgaGVhdEVsLnRleHRDb250ZW50ID0gc3RhdGUucmVndWxhdG9yeV9oZWF0IHx8IDA7XG5cbiAgICBpZiAoc3RhdGUubGlxdWlkaXR5X2Zyb3plbilcbiAgICAgIHRoaXMuX3Nob3coZG9jdW1lbnQuZ2V0RWxlbWVudEJ5SWQoXCJteS1mcm96ZW4tYmFkZ2VcIiksIFwiYmxvY2tcIik7XG4gICAgZWxzZSB0aGlzLl9oaWRlKGRvY3VtZW50LmdldEVsZW1lbnRCeUlkKFwibXktZnJvemVuLWJhZGdlXCIpKTtcblxuICAgIGNvbnN0IGhvbGRpbmdzRWwgPSBkb2N1bWVudC5nZXRFbGVtZW50QnlJZChcIm15LWhvbGRpbmdzXCIpO1xuICAgIGlmIChob2xkaW5nc0VsKSB7XG4gICAgICBob2xkaW5nc0VsLmlubmVySFRNTCA9IFwiXCI7XG4gICAgICBPYmplY3QuZW50cmllcyhzdGF0ZS5wb3J0Zm9saW8gfHwge30pLmZvckVhY2goKFt0aWNrZXIsIHF0eV0pID0+IHtcbiAgICAgICAgY29uc3Qgcm93ID0gZG9jdW1lbnQuY3JlYXRlRWxlbWVudChcImRpdlwiKTtcbiAgICAgICAgcm93LmNsYXNzTmFtZSA9IFwiZmxleCBqdXN0aWZ5LWJldHdlZW4gdGV4dC14cyBmb250LW1vbm9cIjtcbiAgICAgICAgcm93LmlubmVySFRNTCA9IGA8c3BhbiBjbGFzcz1cInRleHQtZ3JheS01MDBcIj4ke2VzY2FwZUh0bWwodGlja2VyKX08L3NwYW4+PHNwYW4gY2xhc3M9XCJ0ZXh0LWdyYXktMzAwXCI+JHtxdHl9IHNoPC9zcGFuPmA7XG4gICAgICAgIGhvbGRpbmdzRWwuYXBwZW5kQ2hpbGQocm93KTtcbiAgICAgIH0pO1xuICAgIH1cbiAgfVxuXG4gIF9yZW5kZXJNYXJrZXQoY29tcGFuaWVzLCByb3VuZCkge1xuICAgIGNvbnN0IGxpc3QgPSBkb2N1bWVudC5nZXRFbGVtZW50QnlJZChcImNvbXBhbmllcy1saXN0XCIpO1xuICAgIGlmICghbGlzdCkgcmV0dXJuO1xuXG4gICAgbGlzdC5pbm5lckhUTUwgPSBcIlwiO1xuXG4gICAgY29tcGFuaWVzLmZvckVhY2goKGMpID0+IHtcbiAgICAgIGNvbnN0IHByaWNlID0gcGFyc2VGbG9hdChjLnByaWNlIHx8IDApO1xuICAgICAgY29uc3QgcHJldiA9IHRoaXMucHJldlByaWNlc1tjLnRpY2tlcl07XG4gICAgICBjb25zdCBpc1VwID0gcHJldiA9PT0gdW5kZWZpbmVkID8gdHJ1ZSA6IHByaWNlID49IHByZXY7XG4gICAgICB0aGlzLnByZXZQcmljZXNbYy50aWNrZXJdID0gcHJpY2U7XG5cbiAgICAgIGlmICghdGhpcy5wcmljZUhpc3RvcnlbYy50aWNrZXJdKSB0aGlzLnByaWNlSGlzdG9yeVtjLnRpY2tlcl0gPSBbXTtcbiAgICAgIGNvbnN0IGhpc3RvcnkgPSB0aGlzLnByaWNlSGlzdG9yeVtjLnRpY2tlcl07XG4gICAgICBjb25zdCBsYXN0ID0gaGlzdG9yeVtoaXN0b3J5Lmxlbmd0aCAtIDFdO1xuICAgICAgaWYgKCFsYXN0IHx8IGxhc3Qucm91bmQgIT09IHJvdW5kIHx8IGxhc3QucHJpY2UgIT09IHByaWNlKSB7XG4gICAgICAgIGhpc3RvcnkucHVzaCh7XG4gICAgICAgICAgcm91bmQ6IHJvdW5kIHx8IDEsXG4gICAgICAgICAgcHJpY2UsXG4gICAgICAgICAgcmVndWxhdG9yeV9oZWF0OiBjLnJlZ3VsYXRvcnlfaGVhdCB8fCAwLFxuICAgICAgICB9KTtcbiAgICAgIH1cblxuICAgICAgY29uc3Qgcm93ID0gZG9jdW1lbnQuY3JlYXRlRWxlbWVudChcImRpdlwiKTtcbiAgICAgIHJvdy5jbGFzc05hbWUgPSBgdGlja2VyLXJvdyAke3RoaXMuc2VsZWN0ZWRUaWNrZXIgPT09IGMudGlja2VyID8gXCJzZWxlY3RlZFwiIDogXCJcIn1gO1xuICAgICAgcm93LmRhdGFzZXQudGlja2VyID0gYy50aWNrZXI7XG5cbiAgICAgIGNvbnN0IGNoYXJ0SWQgPSBgY2hhcnQtJHtjLnRpY2tlcn1gO1xuICAgICAgY29uc3QgcGN0ID1cbiAgICAgICAgcHJldiA9PT0gdW5kZWZpbmVkIHx8IHByZXYgPT09IDBcbiAgICAgICAgICA/IFwiXCJcbiAgICAgICAgICA6IGAke3ByaWNlID49IHByZXYgPyBcIitcIiA6IFwiXCJ9JHsoKChwcmljZSAtIHByZXYpIC8gcHJldikgKiAxMDApLnRvRml4ZWQoMil9JWA7XG5cbiAgICAgIHJvdy5pbm5lckhUTUwgPSBgXG4gICAgICAgIDxzcGFuIGNsYXNzPVwiZm9udC1tb25vIGZvbnQtYm9sZCB0ZXh0LXNtIHRpY2tlci1zeW1ib2xcIj4ke2VzY2FwZUh0bWwoYy50aWNrZXIpfTwvc3Bhbj5cbiAgICAgICAgPHNwYW4gY2xhc3M9XCJ0ZXh0LXhzIHRleHQtZ3JheS01MDAgdHJ1bmNhdGUgdGlja2VyLW5hbWVcIj4ke2VzY2FwZUh0bWwoYy5uYW1lIHx8IFwiXCIpfTwvc3Bhbj5cbiAgICAgICAgPGRpdiBjbGFzcz1cInRleHQtcmlnaHRcIj5cbiAgICAgICAgICA8ZGl2IGNsYXNzPVwiZm9udC1tb25vIGZvbnQtc2VtaWJvbGQgdGV4dC1zbSB0aWNrZXItcHJpY2VcIiBzdHlsZT1cImNvbG9yOiR7aXNVcCA/IFwiIzEwYjk4MVwiIDogXCIjZWY0NDQ0XCJ9XCI+JCR7cHJpY2UudG9GaXhlZCgyKX08L2Rpdj5cbiAgICAgICAgICA8ZGl2IGNsYXNzPVwidGV4dC14cyBmb250LW1vbm8gdGlja2VyLWNoYW5nZSBtdC0wLjVcIiBzdHlsZT1cImNvbG9yOiR7aXNVcCA/IFwiIzEwYjk4MVwiIDogXCIjZWY0NDQ0XCJ9XCI+JHtwY3R9PC9kaXY+XG4gICAgICAgIDwvZGl2PlxuICAgICAgICA8ZGl2IGlkPVwiJHtjaGFydElkfVwiIGNsYXNzPVwiZmxleCBpdGVtcy1jZW50ZXIganVzdGlmeS1lbmRcIiBzdHlsZT1cImhlaWdodDoyOHB4O3dpZHRoOjgwcHhcIj48L2Rpdj5cbiAgICAgICAgPGRpdj48L2Rpdj5cbiAgICAgIGA7XG5cbiAgICAgIHJvdy5hZGRFdmVudExpc3RlbmVyKFwiY2xpY2tcIiwgKCkgPT4ge1xuICAgICAgICB0aGlzLnNlbGVjdGVkVGlja2VyID0gYy50aWNrZXI7XG4gICAgICAgIHRoaXMuX3JlbmRlck1hcmtldCh0aGlzLmNvbXBhbmllcywgcm91bmQpO1xuICAgICAgICB0aGlzLl9yZW5kZXJTdG9ja0RldGFpbChjLnRpY2tlcik7XG4gICAgICB9KTtcblxuICAgICAgbGlzdC5hcHBlbmRDaGlsZChyb3cpO1xuXG4gICAgICByZXF1ZXN0QW5pbWF0aW9uRnJhbWUoKCkgPT4ge1xuICAgICAgICBpZiAoIXRoaXMuc3BhcmtsaW5lQ2hhcnRzW2MudGlja2VyXSkge1xuICAgICAgICAgIHRoaXMuc3BhcmtsaW5lQ2hhcnRzW2MudGlja2VyXSA9IG5ldyBTcGFya2xpbmVDaGFydChjaGFydElkLCB7XG4gICAgICAgICAgICB3aWR0aDogODAsXG4gICAgICAgICAgICBoZWlnaHQ6IDI4LFxuICAgICAgICAgIH0pO1xuICAgICAgICB9XG4gICAgICAgIHRoaXMuc3BhcmtsaW5lQ2hhcnRzW2MudGlja2VyXS5yZXNldChcbiAgICAgICAgICB0aGlzLnByaWNlSGlzdG9yeVtjLnRpY2tlcl0ubWFwKChwKSA9PiBwLnByaWNlKSxcbiAgICAgICAgKTtcbiAgICAgICAgdGhpcy5zcGFya2xpbmVDaGFydHNbYy50aWNrZXJdLnNldENvbG9yKGlzVXApO1xuICAgICAgfSk7XG4gICAgfSk7XG5cbiAgICBpZiAoIXRoaXMuc2VsZWN0ZWRUaWNrZXIgJiYgY29tcGFuaWVzWzBdKSB7XG4gICAgICB0aGlzLnNlbGVjdGVkVGlja2VyID0gY29tcGFuaWVzWzBdLnRpY2tlcjtcbiAgICB9XG5cbiAgICBpZiAodGhpcy5zZWxlY3RlZFRpY2tlcikge1xuICAgICAgdGhpcy5fcmVuZGVyU3RvY2tEZXRhaWwodGhpcy5zZWxlY3RlZFRpY2tlcik7XG4gICAgfVxuICB9XG5cbiAgX3JlbmRlclN0b2NrRGV0YWlsKHRpY2tlcikge1xuICAgIGNvbnN0IGNvbXBhbnkgPSB0aGlzLmNvbXBhbmllcy5maW5kKChjKSA9PiBjLnRpY2tlciA9PT0gdGlja2VyKTtcbiAgICBjb25zdCBoaXN0b3J5ID0gdGhpcy5wcmljZUhpc3RvcnlbdGlja2VyXSB8fCBbXTtcbiAgICBpZiAoIWNvbXBhbnkpIHJldHVybjtcblxuICAgIHRoaXMuX2hpZGUoZG9jdW1lbnQuZ2V0RWxlbWVudEJ5SWQoXCJzdG9jay1kZXRhaWwtZW1wdHlcIikpO1xuICAgIHRoaXMuX3Nob3coZG9jdW1lbnQuZ2V0RWxlbWVudEJ5SWQoXCJzdG9jay1kZXRhaWwtcGFuZWxcIiksIFwiYmxvY2tcIik7XG5cbiAgICBjb25zdCBjdXJyZW50UHJpY2UgPSBwYXJzZUZsb2F0KGNvbXBhbnkucHJpY2UgfHwgMCk7XG4gICAgY29uc3QgZmlyc3RQcmljZSA9IGhpc3RvcnlbMF0/LnByaWNlID8/IGN1cnJlbnRQcmljZTtcbiAgICBjb25zdCBsYXN0UHJpY2UgPSBoaXN0b3J5W2hpc3RvcnkubGVuZ3RoIC0gMV0/LnByaWNlID8/IGN1cnJlbnRQcmljZTtcbiAgICBjb25zdCBwY3QgPSAoKGxhc3RQcmljZSAtIGZpcnN0UHJpY2UpIC8gTWF0aC5tYXgoZmlyc3RQcmljZSwgMC4wMDAxKSkgKiAxMDA7XG5cbiAgICBjb25zdCB0aXRsZSA9IGRvY3VtZW50LmdldEVsZW1lbnRCeUlkKFwic2VsZWN0ZWQtc3RvY2stdGl0bGVcIik7XG4gICAgY29uc3QgbWV0YSA9IGRvY3VtZW50LmdldEVsZW1lbnRCeUlkKFwic2VsZWN0ZWQtc3RvY2stbWV0YVwiKTtcbiAgICBjb25zdCBjdXJyZW50ID0gZG9jdW1lbnQuZ2V0RWxlbWVudEJ5SWQoXCJkZXRhaWwtY3VycmVudC1wcmljZVwiKTtcbiAgICBjb25zdCBjaGFuZ2UgPSBkb2N1bWVudC5nZXRFbGVtZW50QnlJZChcImRldGFpbC1yb3VuZC1jaGFuZ2VcIik7XG4gICAgY29uc3QgaGVhdCA9IGRvY3VtZW50LmdldEVsZW1lbnRCeUlkKFwiZGV0YWlsLXJlZy1oZWF0XCIpO1xuICAgIGNvbnN0IGNvdW50ID0gZG9jdW1lbnQuZ2V0RWxlbWVudEJ5SWQoXCJkZXRhaWwtaGlzdG9yeS1jb3VudFwiKTtcbiAgICBjb25zdCBsYXN0Um91bmQgPSBkb2N1bWVudC5nZXRFbGVtZW50QnlJZChcImRldGFpbC1sYXN0LXJvdW5kXCIpO1xuICAgIGNvbnN0IHN1bW1hcnkgPSBkb2N1bWVudC5nZXRFbGVtZW50QnlJZChcImRldGFpbC1oaXN0b3J5LXN1bW1hcnlcIik7XG5cbiAgICBpZiAodGl0bGUpIHRpdGxlLnRleHRDb250ZW50ID0gYCR7dGlja2VyfSBcdTAwQjcgJHtjb21wYW55Lm5hbWV9YDtcbiAgICBpZiAobWV0YSkgbWV0YS50ZXh0Q29udGVudCA9IGBSb3VuZC1ieS1yb3VuZCBtb3ZlbWVudCBmb3IgJHt0aWNrZXJ9YDtcbiAgICBpZiAoY3VycmVudCkgY3VycmVudC50ZXh0Q29udGVudCA9IGAkJHtjdXJyZW50UHJpY2UudG9GaXhlZCgyKX1gO1xuICAgIGlmIChjaGFuZ2UpIHtcbiAgICAgIGNoYW5nZS50ZXh0Q29udGVudCA9IGAke3BjdCA+PSAwID8gXCIrXCIgOiBcIlwifSR7cGN0LnRvRml4ZWQoMil9JWA7XG4gICAgICBjaGFuZ2Uuc3R5bGUuY29sb3IgPSBwY3QgPj0gMCA/IFwiIzEwYjk4MVwiIDogXCIjZWY0NDQ0XCI7XG4gICAgfVxuICAgIGlmIChoZWF0KSBoZWF0LnRleHRDb250ZW50ID0gU3RyaW5nKGNvbXBhbnkucmVndWxhdG9yeV9oZWF0IHx8IDApO1xuICAgIGlmIChjb3VudCkgY291bnQudGV4dENvbnRlbnQgPSBTdHJpbmcoaGlzdG9yeS5sZW5ndGgpO1xuICAgIGlmIChsYXN0Um91bmQpXG4gICAgICBsYXN0Um91bmQudGV4dENvbnRlbnQgPSBTdHJpbmcoaGlzdG9yeVtoaXN0b3J5Lmxlbmd0aCAtIDFdPy5yb3VuZCB8fCAxKTtcbiAgICBpZiAoc3VtbWFyeSkgc3VtbWFyeS50ZXh0Q29udGVudCA9IGAke2hpc3RvcnkubGVuZ3RofSBwcmljZSBwb2ludHMgdHJhY2tlZGA7XG5cbiAgICBjb25zdCBoaXN0b3J5TGlzdCA9IGRvY3VtZW50LmdldEVsZW1lbnRCeUlkKFwic3RvY2staGlzdG9yeS1saXN0XCIpO1xuICAgIGlmIChoaXN0b3J5TGlzdCkge1xuICAgICAgaGlzdG9yeUxpc3QuaW5uZXJIVE1MID0gXCJcIjtcblxuICAgICAgaWYgKCFoaXN0b3J5Lmxlbmd0aCkge1xuICAgICAgICBoaXN0b3J5TGlzdC5pbm5lckhUTUwgPSBgPHAgY2xhc3M9XCJ0ZXh0LXhzIHRleHQtZ3JheS02MDAgZm9udC1tb25vIHRleHQtY2VudGVyIHB5LTRcIj5XYWl0aW5nIGZvciBwcmljZSBoaXN0b3J5XHUyMDI2PC9wPmA7XG4gICAgICB9IGVsc2Uge1xuICAgICAgICBoaXN0b3J5LmZvckVhY2goKGl0ZW0sIGlkeCkgPT4ge1xuICAgICAgICAgIGNvbnN0IHByZXYgPSBpZHggPiAwID8gaGlzdG9yeVtpZHggLSAxXS5wcmljZSA6IGl0ZW0ucHJpY2U7XG4gICAgICAgICAgY29uc3QgZGlyID1cbiAgICAgICAgICAgIGl0ZW0ucHJpY2UgPiBwcmV2ID8gXCJ1cFwiIDogaXRlbS5wcmljZSA8IHByZXYgPyBcImRvd25cIiA6IFwiZmxhdFwiO1xuICAgICAgICAgIGNvbnN0IHJvdyA9IGRvY3VtZW50LmNyZWF0ZUVsZW1lbnQoXCJkaXZcIik7XG4gICAgICAgICAgcm93LmNsYXNzTmFtZSA9IGBoaXN0b3J5LWl0ZW0gJHtkaXJ9YDtcbiAgICAgICAgICByb3cuaW5uZXJIVE1MID0gYFxuICAgICAgICAgICAgPGRpdiBjbGFzcz1cImZsZXggaXRlbXMtY2VudGVyIGp1c3RpZnktYmV0d2VlblwiPlxuICAgICAgICAgICAgICA8c3BhbiBjbGFzcz1cInRleHQteHMgZm9udC1tb25vIHRleHQtZ3JheS00MDBcIj5Sb3VuZCAke2l0ZW0ucm91bmR9PC9zcGFuPlxuICAgICAgICAgICAgICA8c3BhbiBjbGFzcz1cInRleHQteHMgZm9udC1tb25vICR7XG4gICAgICAgICAgICAgICAgZGlyID09PSBcInVwXCJcbiAgICAgICAgICAgICAgICAgID8gXCJ0ZXh0LWVtZXJhbGQtNDAwXCJcbiAgICAgICAgICAgICAgICAgIDogZGlyID09PSBcImRvd25cIlxuICAgICAgICAgICAgICAgICAgICA/IFwidGV4dC1yZWQtNDAwXCJcbiAgICAgICAgICAgICAgICAgICAgOiBcInRleHQtZ3JheS00MDBcIlxuICAgICAgICAgICAgICB9XCI+JCR7aXRlbS5wcmljZS50b0ZpeGVkKDIpfTwvc3Bhbj5cbiAgICAgICAgICAgIDwvZGl2PlxuICAgICAgICAgIGA7XG4gICAgICAgICAgaGlzdG9yeUxpc3QuYXBwZW5kQ2hpbGQocm93KTtcbiAgICAgICAgfSk7XG4gICAgICB9XG4gICAgfVxuXG4gICAgdGhpcy5kZXRhaWxDaGFydC5yZW5kZXIoaGlzdG9yeSk7XG4gIH1cblxuICBfcmVuZGVyRXZlbnRzKGV2ZW50cykge1xuICAgIGNvbnN0IGJveCA9IGRvY3VtZW50LmdldEVsZW1lbnRCeUlkKFwiZXZlbnRzLWZlZWRcIik7XG4gICAgaWYgKCFib3gpIHJldHVybjtcblxuICAgIGJveC5pbm5lckhUTUwgPSBcIlwiO1xuXG4gICAgaWYgKCFldmVudHMubGVuZ3RoKSB7XG4gICAgICBib3guaW5uZXJIVE1MID0gYDxwIGNsYXNzPVwidGV4dC14cyB0ZXh0LWdyYXktNjAwIGZvbnQtbW9ubyB0ZXh0LWNlbnRlciBweS0yXCI+Tm8gZXZlbnRzIHlldC48L3A+YDtcbiAgICAgIHJldHVybjtcbiAgICB9XG5cbiAgICBldmVudHMuZm9yRWFjaCgoZXYpID0+IHtcbiAgICAgIGNvbnN0IGl0ZW0gPSBkb2N1bWVudC5jcmVhdGVFbGVtZW50KFwiZGl2XCIpO1xuICAgICAgaXRlbS5jbGFzc05hbWUgPSBcImhpc3RvcnktaXRlbVwiO1xuXG4gICAgICBpZiAoZXYudHlwZSA9PT0gXCJuZXdzX2V2ZW50XCIpIHtcbiAgICAgICAgaXRlbS5pbm5lckhUTUwgPSBgXG4gICAgICAgICAgPGRpdiBjbGFzcz1cInRleHQteHMgZm9udC1tb25vIHRleHQtZW1lcmFsZC00MDAgbWItMVwiPk5FV1M8L2Rpdj5cbiAgICAgICAgICA8ZGl2IGNsYXNzPVwidGV4dC14cyB0ZXh0LWdyYXktMzAwXCI+JHtlc2NhcGVIdG1sKGV2LmhlYWRsaW5lIHx8IFwiXCIpfTwvZGl2PlxuICAgICAgICBgO1xuICAgICAgfSBlbHNlIGlmIChldi50eXBlID09PSBcInByaWNlX3VwZGF0ZWRcIikge1xuICAgICAgICBpdGVtLmlubmVySFRNTCA9IGBcbiAgICAgICAgICA8ZGl2IGNsYXNzPVwidGV4dC14cyBmb250LW1vbm8gdGV4dC1jeWFuLTQwMCBtYi0xXCI+JHtlc2NhcGVIdG1sKGV2LnRpY2tlciB8fCBcIlwiKX08L2Rpdj5cbiAgICAgICAgICA8ZGl2IGNsYXNzPVwidGV4dC14cyB0ZXh0LWdyYXktMzAwXCI+UHJpY2Ugbm93ICQke3BhcnNlRmxvYXQoZXYucHJpY2UgfHwgMCkudG9GaXhlZCgyKX08L2Rpdj5cbiAgICAgICAgYDtcbiAgICAgIH0gZWxzZSB7XG4gICAgICAgIGl0ZW0uaW5uZXJIVE1MID0gYDxkaXYgY2xhc3M9XCJ0ZXh0LXhzIHRleHQtZ3JheS0zMDBcIj4ke2VzY2FwZUh0bWwoZXYuaGVhZGxpbmUgfHwgSlNPTi5zdHJpbmdpZnkoZXYpKX08L2Rpdj5gO1xuICAgICAgfVxuXG4gICAgICBib3guYXBwZW5kQ2hpbGQoaXRlbSk7XG4gICAgfSk7XG4gIH1cblxuICBfcmVuZGVyTmV3cyhldmVudHMpIHtcbiAgICBjb25zdCBib3ggPSBkb2N1bWVudC5nZXRFbGVtZW50QnlJZChcIm5ld3MtZmVlZFwiKTtcbiAgICBpZiAoIWJveCkgcmV0dXJuO1xuXG4gICAgY29uc3QgbmV3cyA9IChldmVudHMgfHwgW10pLmZpbHRlcihcbiAgICAgIChldikgPT4gZXYudHlwZSA9PT0gXCJuZXdzX2V2ZW50XCIgfHwgZXYudHlwZSA9PT0gXCJwcmljZV91cGRhdGVkXCIsXG4gICAgKTtcbiAgICBib3guaW5uZXJIVE1MID0gXCJcIjtcblxuICAgIGlmICghbmV3cy5sZW5ndGgpIHtcbiAgICAgIGJveC5pbm5lckhUTUwgPSBgPHAgY2xhc3M9XCJ0ZXh0LXhzIHRleHQtZ3JheS02MDAgZm9udC1tb25vIHRleHQtY2VudGVyIHB5LTRcIj5ObyBuZXdzIHlldC48L3A+YDtcbiAgICAgIHJldHVybjtcbiAgICB9XG5cbiAgICBuZXdzLmZvckVhY2goKGV2KSA9PiB7XG4gICAgICBjb25zdCBpdGVtID0gZG9jdW1lbnQuY3JlYXRlRWxlbWVudChcImRpdlwiKTtcbiAgICAgIGl0ZW0uY2xhc3NOYW1lID0gYG5ld3MtaXRlbSAke1xuICAgICAgICBldi50eXBlID09PSBcIm5ld3NfZXZlbnRcIlxuICAgICAgICAgID8gKGV2LmltcGFjdCB8fCAwKSA+PSAwXG4gICAgICAgICAgICA/IFwicG9zaXRpdmVcIlxuICAgICAgICAgICAgOiBcIm5lZ2F0aXZlXCJcbiAgICAgICAgICA6IFwibmV1dHJhbFwiXG4gICAgICB9YDtcblxuICAgICAgaWYgKGV2LnR5cGUgPT09IFwibmV3c19ldmVudFwiKSB7XG4gICAgICAgIGl0ZW0uaW5uZXJIVE1MID0gYFxuICAgICAgICAgIDxkaXYgY2xhc3M9XCJmbGV4IGl0ZW1zLWNlbnRlciBqdXN0aWZ5LWJldHdlZW4gbWItMVwiPlxuICAgICAgICAgICAgPHNwYW4gY2xhc3M9XCJ0ZXh0LVsxMHB4XSBmb250LW1vbm8gdXBwZXJjYXNlIHRyYWNraW5nLXdpZGVzdCB0ZXh0LWVtZXJhbGQtNDAwXCI+Um91bmQgJHtldi5yb3VuZCB8fCBcIlx1MjAxNFwifTwvc3Bhbj5cbiAgICAgICAgICAgIDxzcGFuIGNsYXNzPVwidGV4dC1bMTBweF0gZm9udC1tb25vIHRleHQtZ3JheS01MDBcIj4ke2VzY2FwZUh0bWwoKGV2LnRhcmdldHMgfHwgW10pLmpvaW4oXCIsIFwiKSl9PC9zcGFuPlxuICAgICAgICAgIDwvZGl2PlxuICAgICAgICAgIDxkaXYgY2xhc3M9XCJ0ZXh0LXNtIHRleHQtZ3JheS0yMDBcIj4ke2VzY2FwZUh0bWwoZXYuaGVhZGxpbmUgfHwgXCJcIil9PC9kaXY+XG4gICAgICAgIGA7XG4gICAgICB9IGVsc2Uge1xuICAgICAgICBpdGVtLmlubmVySFRNTCA9IGBcbiAgICAgICAgICA8ZGl2IGNsYXNzPVwiZmxleCBpdGVtcy1jZW50ZXIganVzdGlmeS1iZXR3ZWVuXCI+XG4gICAgICAgICAgICA8c3BhbiBjbGFzcz1cInRleHQteHMgZm9udC1tb25vIHRleHQtY3lhbi00MDBcIj4ke2VzY2FwZUh0bWwoZXYudGlja2VyIHx8IFwiXCIpfTwvc3Bhbj5cbiAgICAgICAgICAgIDxzcGFuIGNsYXNzPVwidGV4dC14cyBmb250LW1vbm8gdGV4dC13aGl0ZVwiPiQke3BhcnNlRmxvYXQoZXYucHJpY2UgfHwgMCkudG9GaXhlZCgyKX08L3NwYW4+XG4gICAgICAgICAgPC9kaXY+XG4gICAgICAgIGA7XG4gICAgICB9XG5cbiAgICAgIGJveC5hcHBlbmRDaGlsZChpdGVtKTtcbiAgICB9KTtcbiAgfVxuXG4gIF91cGRhdGVDaGF0SW5kaWNhdG9yKHBoYXNlKSB7XG4gICAgY29uc3QgZWwgPSBkb2N1bWVudC5nZXRFbGVtZW50QnlJZChcImNoYXQtcGhhc2UtaW5kaWNhdG9yXCIpO1xuICAgIGlmICghZWwpIHJldHVybjtcblxuICAgIGNvbnN0IG9wZW4gPSBbXCJuZWdvdGlhdGlvblwiLCBcImRpc2Nsb3N1cmVcIiwgXCJmaW5pc2hlZFwiXS5pbmNsdWRlcyhwaGFzZSk7XG4gICAgZWwudGV4dENvbnRlbnQgPSBvcGVuID8gXCJPcGVuXCIgOiBcIkNsb3NlZFwiO1xuICAgIGVsLnN0eWxlLmNvbG9yID0gb3BlbiA/IFwiIzEwYjk4MVwiIDogXCIjNmI3MjgwXCI7XG4gIH1cblxuICBfc2hvd1BoYXNlT3ZlcmxheShwaGFzZSkge1xuICAgIGNvbnN0IG92ZXJsYXkgPSBkb2N1bWVudC5nZXRFbGVtZW50QnlJZChcInBoYXNlLW92ZXJsYXlcIik7XG4gICAgY29uc3QgYmFkZ2UgPSBkb2N1bWVudC5nZXRFbGVtZW50QnlJZChcInBoYXNlLW92ZXJsYXktYmFkZ2VcIik7XG4gICAgY29uc3QgZGVzYyA9IGRvY3VtZW50LmdldEVsZW1lbnRCeUlkKFwicGhhc2Utb3ZlcmxheS1kZXNjXCIpO1xuICAgIGlmICghb3ZlcmxheSB8fCAhYmFkZ2UpIHJldHVybjtcblxuICAgIGNvbnN0IGRlc2NzID0ge1xuICAgICAgbmV3czogXCJNYXJrZXQgbmV3cyBqdXN0IGxhbmRlZC5cIixcbiAgICAgIGFjdGlvbl9zdWJtaXNzaW9uOiBcIlN1Ym1pdCB5b3VyIG1vdmUuXCIsXG4gICAgICBuZWdvdGlhdGlvbjogXCJDaGF0IGlzIG9wZW4uXCIsXG4gICAgICByZXNvbHV0aW9uOiBcIlJlc29sdmluZyBhY3Rpb25zLlwiLFxuICAgICAgZGlzY2xvc3VyZTogXCJSZXN1bHRzIHBvc3RlZC5cIixcbiAgICAgIGZpbmlzaGVkOiBcIkZpbmFsIHJhbmtpbmdzIHJlYWR5LlwiLFxuICAgIH07XG5cbiAgICBiYWRnZS50ZXh0Q29udGVudCA9IHBoYXNlTGFiZWwocGhhc2UpO1xuICAgIGJhZGdlLmNsYXNzTmFtZSA9IGBwaGFzZS1iYWRnZSBwaGFzZS0ke3BoYXNlfSB0ZXh0LTJ4bCBweC04IHB5LTQgZm9udC1tb25vIHRyYWNraW5nLXdpZGVzdCB1cHBlcmNhc2VgO1xuICAgIGlmIChkZXNjKSBkZXNjLnRleHRDb250ZW50ID0gZGVzY3NbcGhhc2VdIHx8IFwiXCI7XG5cbiAgICB0aGlzLl9zaG93KG92ZXJsYXksIFwiZmxleFwiKTtcbiAgICBjbGVhclRpbWVvdXQodGhpcy5waGFzZU92ZXJsYXlUaW1lcik7XG4gICAgaWYgKHBoYXNlICE9PSBcImZpbmlzaGVkXCIpIHtcbiAgICAgIHRoaXMucGhhc2VPdmVybGF5VGltZXIgPSBzZXRUaW1lb3V0KCgpID0+IHRoaXMuX2hpZGUob3ZlcmxheSksIDI1MDApO1xuICAgIH1cbiAgfVxuXG4gIC8vIFx1MjUwMFx1MjUwMCBjb3VudGRvd24gXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXG5cbiAgX3N0YXJ0Q291bnRkb3duKGRlYWRsaW5lKSB7XG4gICAgdGhpcy5fY2xlYXJDb3VudGRvd24oKTtcblxuICAgIGNvbnN0IGVsID0gZG9jdW1lbnQuZ2V0RWxlbWVudEJ5SWQoXCJjb3VudGRvd24tdGltZXJcIik7XG4gICAgY29uc3QgdmFsRWwgPSBkb2N1bWVudC5nZXRFbGVtZW50QnlJZChcImNvdW50ZG93bi12YWx1ZVwiKTtcbiAgICBpZiAoIWVsIHx8ICF2YWxFbCkgcmV0dXJuO1xuXG4gICAgdGhpcy5fc2hvdyhlbCwgXCJmbGV4XCIpO1xuXG4gICAgY29uc3QgdGljayA9ICgpID0+IHtcbiAgICAgIGNvbnN0IGRpZmYgPSBNYXRoLm1heChcbiAgICAgICAgMCxcbiAgICAgICAgTWF0aC5mbG9vcigoZGVhZGxpbmUuZ2V0VGltZSgpIC0gRGF0ZS5ub3coKSkgLyAxMDAwKSxcbiAgICAgICk7XG4gICAgICBjb25zdCBtID0gTWF0aC5mbG9vcihkaWZmIC8gNjApO1xuICAgICAgY29uc3QgcyA9IGRpZmYgJSA2MDtcbiAgICAgIHZhbEVsLnRleHRDb250ZW50ID0gYCR7bX06JHtTdHJpbmcocykucGFkU3RhcnQoMiwgXCIwXCIpfWA7XG4gICAgICBlbC5jbGFzc0xpc3QudG9nZ2xlKFwiY291bnRkb3duLXVyZ2VudFwiLCBkaWZmIDw9IDEwICYmIGRpZmYgPiAwKTtcbiAgICAgIGlmIChkaWZmIDw9IDApIHRoaXMuX2NsZWFyQ291bnRkb3duKCk7XG4gICAgfTtcblxuICAgIHRpY2soKTtcbiAgICB0aGlzLmNvdW50ZG93biA9IHNldEludGVydmFsKHRpY2ssIDEwMDApO1xuICB9XG5cbiAgX2NsZWFyQ291bnRkb3duKCkge1xuICAgIGlmICh0aGlzLmNvdW50ZG93bikgY2xlYXJJbnRlcnZhbCh0aGlzLmNvdW50ZG93bik7XG4gICAgdGhpcy5jb3VudGRvd24gPSBudWxsO1xuXG4gICAgY29uc3QgZWwgPSBkb2N1bWVudC5nZXRFbGVtZW50QnlJZChcImNvdW50ZG93bi10aW1lclwiKTtcbiAgICBpZiAoZWwpIHtcbiAgICAgIHRoaXMuX2hpZGUoZWwpO1xuICAgICAgZWwuY2xhc3NMaXN0LnJlbW92ZShcImNvdW50ZG93bi11cmdlbnRcIik7XG4gICAgfVxuICB9XG5cbiAgLy8gXHUyNTAwXHUyNTAwIGxlYWRlcmJvYXJkIFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFxuXG4gIF9yZW5kZXJMZWFkZXJib2FyZChsZWFkZXJib2FyZCkge1xuICAgIGNvbnN0IGxpc3QgPSBkb2N1bWVudC5nZXRFbGVtZW50QnlJZChcImxlYWRlcmJvYXJkLWxpc3RcIik7XG4gICAgaWYgKCFsaXN0KSByZXR1cm47XG5cbiAgICBsaXN0LmlubmVySFRNTCA9IFwiXCI7XG4gICAgbGVhZGVyYm9hcmQuZm9yRWFjaCgoZW50cnksIGkpID0+IHtcbiAgICAgIGNvbnN0IGRpdiA9IGRvY3VtZW50LmNyZWF0ZUVsZW1lbnQoXCJkaXZcIik7XG4gICAgICBkaXYuY2xhc3NOYW1lID0gXCJmbGV4IGl0ZW1zLWNlbnRlciBnYXAtMyBwLTMgcm91bmRlZC1sZyBjYXJkIGZhZGUtaW4tdXBcIjtcblxuICAgICAgY29uc3QgbWVkYWxzID0gW1wiXHVEODNFXHVERDQ3XCIsIFwiXHVEODNFXHVERDQ4XCIsIFwiXHVEODNFXHVERDQ5XCJdO1xuICAgICAgZGl2LmlubmVySFRNTCA9IGBcbiAgICAgICAgPHNwYW4gY2xhc3M9XCJ0ZXh0LXhsIHctOCB0ZXh0LWNlbnRlclwiPiR7bWVkYWxzW2ldIHx8IGAjJHtpICsgMX1gfTwvc3Bhbj5cbiAgICAgICAgPHNwYW4gY2xhc3M9XCJmb250LW1vbm8gZm9udC1zZW1pYm9sZCB0ZXh0LXNtIHRleHQtd2hpdGUgZmxleC0xXCI+JHtlc2NhcGVIdG1sKGVudHJ5LnVzZXJuYW1lKX08L3NwYW4+XG4gICAgICAgIDxzcGFuIGNsYXNzPVwiZm9udC1tb25vIHRleHQtc20gJHtpID09PSAwID8gXCJ0ZXh0LWVtZXJhbGQtNDAwIGZvbnQtYm9sZFwiIDogXCJ0ZXh0LWdyYXktMzAwXCJ9XCI+XG4gICAgICAgICAgJCR7cGFyc2VGbG9hdChlbnRyeS5uZXRfd29ydGggfHwgMCkudG9Mb2NhbGVTdHJpbmcoXCJlbi1VU1wiLCB7XG4gICAgICAgICAgICBtaW5pbXVtRnJhY3Rpb25EaWdpdHM6IDIsXG4gICAgICAgICAgICBtYXhpbXVtRnJhY3Rpb25EaWdpdHM6IDIsXG4gICAgICAgICAgfSl9XG4gICAgICAgIDwvc3Bhbj5cbiAgICAgIGA7XG4gICAgICBsaXN0LmFwcGVuZENoaWxkKGRpdik7XG4gICAgfSk7XG4gIH1cblxuICAvLyBcdTI1MDBcdTI1MDAgYWN0aW9uIG1vZGFsIFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFxuXG4gIF9vcGVuQWN0aW9uTW9kYWwoZGF0YXNldCkge1xuICAgIGNvbnN0IG1vZGFsID0gZG9jdW1lbnQuZ2V0RWxlbWVudEJ5SWQoXCJhY3Rpb24tbW9kYWxcIik7XG4gICAgY29uc3QgdGl0bGVFbCA9IGRvY3VtZW50LmdldEVsZW1lbnRCeUlkKFwibW9kYWwtdGl0bGVcIik7XG4gICAgY29uc3QgZGVzY0VsID0gZG9jdW1lbnQuZ2V0RWxlbWVudEJ5SWQoXCJtb2RhbC1kZXNjXCIpO1xuICAgIGNvbnN0IGVyckVsID0gZG9jdW1lbnQuZ2V0RWxlbWVudEJ5SWQoXCJtb2RhbC1lcnJvclwiKTtcbiAgICBjb25zdCB0aWNrZXJEaXYgPSBkb2N1bWVudC5nZXRFbGVtZW50QnlJZChcImZpZWxkLXRpY2tlclwiKTtcbiAgICBjb25zdCBxdHlEaXYgPSBkb2N1bWVudC5nZXRFbGVtZW50QnlJZChcImZpZWxkLXF1YW50aXR5XCIpO1xuICAgIGNvbnN0IHRhcmdldERpdiA9IGRvY3VtZW50LmdldEVsZW1lbnRCeUlkKFwiZmllbGQtdGFyZ2V0XCIpO1xuICAgIGNvbnN0IGNvc3REaXYgPSBkb2N1bWVudC5nZXRFbGVtZW50QnlJZChcImNvc3QtZXN0aW1hdGVcIik7XG4gICAgY29uc3QgdGlja2VyQnRucyA9IGRvY3VtZW50LmdldEVsZW1lbnRCeUlkKFwidGlja2VyLWJ1dHRvbnNcIik7XG4gICAgY29uc3QgdGFyZ2V0QnRucyA9IGRvY3VtZW50LmdldEVsZW1lbnRCeUlkKFwidGFyZ2V0LWJ1dHRvbnNcIik7XG4gICAgY29uc3QgcXR5SW5wdXQgPSBkb2N1bWVudC5nZXRFbGVtZW50QnlJZChcImlucHV0LXF1YW50aXR5XCIpO1xuXG4gICAgaWYgKCFtb2RhbCkgcmV0dXJuO1xuXG4gICAgaWYgKHRpdGxlRWwpIHRpdGxlRWwudGV4dENvbnRlbnQgPSBkYXRhc2V0LmxhYmVsIHx8IGRhdGFzZXQuYWN0aW9uIHx8IFwiXCI7XG4gICAgaWYgKGRlc2NFbCkgZGVzY0VsLnRleHRDb250ZW50ID0gZGF0YXNldC5kZXNjIHx8IFwiXCI7XG4gICAgaWYgKGVyckVsKSB7XG4gICAgICBlcnJFbC50ZXh0Q29udGVudCA9IFwiXCI7XG4gICAgICB0aGlzLl9oaWRlKGVyckVsKTtcbiAgICB9XG5cbiAgICBjb25zdCBuZWVkc1RpY2tlciA9IGRhdGFzZXQubmVlZHNUaWNrZXIgPT09IFwidHJ1ZVwiO1xuICAgIGNvbnN0IG5lZWRzUXR5ID0gZGF0YXNldC5uZWVkc1F1YW50aXR5ID09PSBcInRydWVcIjtcbiAgICBjb25zdCBuZWVkc1RhcmdldCA9IGRhdGFzZXQubmVlZHNUYXJnZXQgPT09IFwidHJ1ZVwiO1xuXG4gICAgbmVlZHNUaWNrZXIgPyB0aGlzLl9zaG93KHRpY2tlckRpdiwgXCJibG9ja1wiKSA6IHRoaXMuX2hpZGUodGlja2VyRGl2KTtcbiAgICBuZWVkc1F0eSA/IHRoaXMuX3Nob3cocXR5RGl2LCBcImJsb2NrXCIpIDogdGhpcy5faGlkZShxdHlEaXYpO1xuICAgIG5lZWRzVGFyZ2V0ID8gdGhpcy5fc2hvdyh0YXJnZXREaXYsIFwiYmxvY2tcIikgOiB0aGlzLl9oaWRlKHRhcmdldERpdik7XG4gICAgdGhpcy5faGlkZShjb3N0RGl2KTtcblxuICAgIGlmIChuZWVkc1RpY2tlciAmJiB0aWNrZXJCdG5zKSB7XG4gICAgICB0aWNrZXJCdG5zLmlubmVySFRNTCA9IFwiXCI7XG4gICAgICB0aGlzLmNvbXBhbmllcy5mb3JFYWNoKChjKSA9PiB7XG4gICAgICAgIGNvbnN0IGJ0biA9IGRvY3VtZW50LmNyZWF0ZUVsZW1lbnQoXCJidXR0b25cIik7XG4gICAgICAgIGJ0bi50eXBlID0gXCJidXR0b25cIjtcbiAgICAgICAgYnRuLmNsYXNzTmFtZSA9IFwidGlja2VyLXBpbGxcIjtcbiAgICAgICAgYnRuLmRhdGFzZXQudGlja2VyID0gYy50aWNrZXI7XG4gICAgICAgIGJ0bi5pbm5lckhUTUwgPSBgPHNwYW4gY2xhc3M9XCJmb250LWJvbGRcIj4ke2VzY2FwZUh0bWwoYy50aWNrZXIpfTwvc3Bhbj5cbiAgICAgICAgICAgICAgICAgICAgICAgICA8c3BhbiBjbGFzcz1cInRleHQtZ3JheS01MDAgdGV4dC14cyBtbC0xXCI+JCR7cGFyc2VGbG9hdChjLnByaWNlKS50b0ZpeGVkKDIpfTwvc3Bhbj5gO1xuICAgICAgICBidG4uYWRkRXZlbnRMaXN0ZW5lcihcImNsaWNrXCIsIChlKSA9PiB7XG4gICAgICAgICAgZS5wcmV2ZW50RGVmYXVsdCgpO1xuICAgICAgICAgIHRpY2tlckJ0bnNcbiAgICAgICAgICAgIC5xdWVyeVNlbGVjdG9yQWxsKFwiLnRpY2tlci1waWxsXCIpXG4gICAgICAgICAgICAuZm9yRWFjaCgoYikgPT4gYi5jbGFzc0xpc3QucmVtb3ZlKFwic2VsZWN0ZWRcIikpO1xuICAgICAgICAgIGJ0bi5jbGFzc0xpc3QuYWRkKFwic2VsZWN0ZWRcIik7XG4gICAgICAgICAgaWYgKG5lZWRzUXR5KVxuICAgICAgICAgICAgdGhpcy5fdXBkYXRlQ29zdEVzdGltYXRlKGRhdGFzZXQuYWN0aW9uLCBjLnRpY2tlciwgcXR5SW5wdXQ/LnZhbHVlKTtcbiAgICAgICAgfSk7XG4gICAgICAgIHRpY2tlckJ0bnMuYXBwZW5kQ2hpbGQoYnRuKTtcbiAgICAgIH0pO1xuICAgIH1cblxuICAgIGlmIChuZWVkc1RhcmdldCAmJiB0YXJnZXRCdG5zKSB7XG4gICAgICB0YXJnZXRCdG5zLmlubmVySFRNTCA9IFwiXCI7XG4gICAgICB0aGlzLnBsYXllcnMuZm9yRWFjaCgocCkgPT4ge1xuICAgICAgICBpZiAoU3RyaW5nKHAudXNlcl9pZCkgPT09IFN0cmluZyh0aGlzLmNmZy51c2VySWQpKSByZXR1cm47XG5cbiAgICAgICAgY29uc3QgYnRuID0gZG9jdW1lbnQuY3JlYXRlRWxlbWVudChcImJ1dHRvblwiKTtcbiAgICAgICAgYnRuLnR5cGUgPSBcImJ1dHRvblwiO1xuICAgICAgICBidG4uY2xhc3NOYW1lID0gXCJwbGF5ZXItcGlsbFwiO1xuICAgICAgICBidG4uZGF0YXNldC50YXJnZXRJZCA9IHAudXNlcl9pZDtcbiAgICAgICAgYnRuLmlubmVySFRNTCA9IGBcbiAgICAgICAgICA8ZGl2IGNsYXNzPVwidy0yIGgtMiByb3VuZGVkLWZ1bGwgJHtwLmxpcXVpZGl0eV9mcm96ZW4gPyBcImJnLWJsdWUtNDAwXCIgOiBcImJnLWVtZXJhbGQtNDAwXCJ9XCI+PC9kaXY+XG4gICAgICAgICAgPHNwYW4gY2xhc3M9XCJmb250LW1vbm8gdGV4dC1zbSB0ZXh0LWdyYXktMjAwIGZsZXgtMVwiPiR7ZXNjYXBlSHRtbChwLnVzZXJuYW1lKX08L3NwYW4+XG4gICAgICAgICAgJHtwLmxpcXVpZGl0eV9mcm96ZW4gPyBgPHNwYW4gY2xhc3M9XCJ0ZXh0LXhzIHRleHQtYmx1ZS00MDAgZm9udC1tb25vXCI+XHUyNzQ0IGZyb3plbjwvc3Bhbj5gIDogXCJcIn1cbiAgICAgICAgICAke3AucmVndWxhdG9yeV9oZWF0ID4gMCA/IGA8c3BhbiBjbGFzcz1cInRleHQteHMgdGV4dC1hbWJlci00MDAgZm9udC1tb25vXCI+XHUyNjk2ICR7cC5yZWd1bGF0b3J5X2hlYXR9PC9zcGFuPmAgOiBcIlwifVxuICAgICAgICBgO1xuICAgICAgICBidG4uYWRkRXZlbnRMaXN0ZW5lcihcImNsaWNrXCIsIChlKSA9PiB7XG4gICAgICAgICAgZS5wcmV2ZW50RGVmYXVsdCgpO1xuICAgICAgICAgIHRhcmdldEJ0bnNcbiAgICAgICAgICAgIC5xdWVyeVNlbGVjdG9yQWxsKFwiLnBsYXllci1waWxsXCIpXG4gICAgICAgICAgICAuZm9yRWFjaCgoYikgPT4gYi5jbGFzc0xpc3QucmVtb3ZlKFwic2VsZWN0ZWRcIikpO1xuICAgICAgICAgIGJ0bi5jbGFzc0xpc3QuYWRkKFwic2VsZWN0ZWRcIik7XG4gICAgICAgIH0pO1xuICAgICAgICB0YXJnZXRCdG5zLmFwcGVuZENoaWxkKGJ0bik7XG4gICAgICB9KTtcbiAgICB9XG5cbiAgICBpZiAocXR5SW5wdXQpIHtcbiAgICAgIHF0eUlucHV0LnZhbHVlID0gXCJcIjtcbiAgICAgIHF0eUlucHV0Lm9uaW5wdXQgPSAoKSA9PiB7XG4gICAgICAgIGNvbnN0IHRpY2tlciA9IHRpY2tlckJ0bnM/LnF1ZXJ5U2VsZWN0b3IoXCIuc2VsZWN0ZWRcIik/LmRhdGFzZXQudGlja2VyO1xuICAgICAgICB0aGlzLl91cGRhdGVDb3N0RXN0aW1hdGUoZGF0YXNldC5hY3Rpb24sIHRpY2tlciwgcXR5SW5wdXQudmFsdWUpO1xuICAgICAgfTtcbiAgICB9XG5cbiAgICBjb25zdCBmb3JtID0gZG9jdW1lbnQuZ2V0RWxlbWVudEJ5SWQoXCJhY3Rpb24tZm9ybVwiKTtcbiAgICBpZiAoZm9ybSkgZm9ybS5kYXRhc2V0LmFjdGlvblR5cGUgPSBkYXRhc2V0LmFjdGlvbjtcblxuICAgIHRoaXMuX3Nob3cobW9kYWwsIFwiZmxleFwiKTtcbiAgfVxuXG4gIF9jbG9zZU1vZGFsKCkge1xuICAgIHRoaXMuX2hpZGUoZG9jdW1lbnQuZ2V0RWxlbWVudEJ5SWQoXCJhY3Rpb24tbW9kYWxcIikpO1xuICAgIHRoaXMuX2hpZGUoZG9jdW1lbnQuZ2V0RWxlbWVudEJ5SWQoXCJtb2RhbC1lcnJvclwiKSk7XG4gIH1cblxuICBfdXBkYXRlQ29zdEVzdGltYXRlKGFjdGlvbiwgdGlja2VyLCBxdHkpIHtcbiAgICBjb25zdCBjb3N0RGl2ID0gZG9jdW1lbnQuZ2V0RWxlbWVudEJ5SWQoXCJjb3N0LWVzdGltYXRlXCIpO1xuICAgIGNvbnN0IGNvc3RWYWxFbCA9IGRvY3VtZW50LmdldEVsZW1lbnRCeUlkKFwiY29zdC12YWx1ZVwiKTtcbiAgICBjb25zdCBjYXNoUmVtRWwgPSBkb2N1bWVudC5nZXRFbGVtZW50QnlJZChcImNhc2gtcmVtYWluaW5nXCIpO1xuXG4gICAgaWYgKFxuICAgICAgIWNvc3REaXYgfHxcbiAgICAgICF0aWNrZXIgfHxcbiAgICAgICFxdHkgfHxcbiAgICAgICFbXCJidXlcIiwgXCJzaG9ydFwiLCBcImFjcXVpcmVfc3Rha2VcIl0uaW5jbHVkZXMoYWN0aW9uKVxuICAgICkge1xuICAgICAgdGhpcy5faGlkZShjb3N0RGl2KTtcbiAgICAgIHJldHVybjtcbiAgICB9XG5cbiAgICBjb25zdCBjb21wYW55ID0gdGhpcy5jb21wYW5pZXMuZmluZCgoYykgPT4gYy50aWNrZXIgPT09IHRpY2tlcik7XG4gICAgaWYgKCFjb21wYW55KSB7XG4gICAgICB0aGlzLl9oaWRlKGNvc3REaXYpO1xuICAgICAgcmV0dXJuO1xuICAgIH1cblxuICAgIGNvbnN0IHByaWNlID0gcGFyc2VGbG9hdChjb21wYW55LnByaWNlKTtcbiAgICBjb25zdCBxID0gcGFyc2VJbnQocXR5LCAxMCkgfHwgMDtcbiAgICBjb25zdCB0b3RhbCA9IHByaWNlICogcTtcbiAgICBjb25zdCBteUNhc2ggPSB0aGlzLm15U3RhdGUgPyBwYXJzZUZsb2F0KHRoaXMubXlTdGF0ZS5jYXNoKSA6IDA7XG5cbiAgICB0aGlzLl9zaG93KGNvc3REaXYsIFwiYmxvY2tcIik7XG5cbiAgICBpZiAoY29zdFZhbEVsKSB7XG4gICAgICBjb3N0VmFsRWwudGV4dENvbnRlbnQgPSBgJCR7dG90YWwudG9Mb2NhbGVTdHJpbmcoXCJlbi1VU1wiLCB7XG4gICAgICAgIG1pbmltdW1GcmFjdGlvbkRpZ2l0czogMixcbiAgICAgICAgbWF4aW11bUZyYWN0aW9uRGlnaXRzOiAyLFxuICAgICAgfSl9YDtcbiAgICB9XG5cbiAgICBpZiAoY2FzaFJlbUVsKSB7XG4gICAgICBjYXNoUmVtRWwudGV4dENvbnRlbnQgPSBgJCR7TWF0aC5tYXgoMCwgbXlDYXNoIC0gdG90YWwpLnRvTG9jYWxlU3RyaW5nKFxuICAgICAgICBcImVuLVVTXCIsXG4gICAgICAgIHtcbiAgICAgICAgICBtaW5pbXVtRnJhY3Rpb25EaWdpdHM6IDIsXG4gICAgICAgICAgbWF4aW11bUZyYWN0aW9uRGlnaXRzOiAyLFxuICAgICAgICB9LFxuICAgICAgKX1gO1xuICAgICAgY2FzaFJlbUVsLnN0eWxlLmNvbG9yID0gbXlDYXNoIC0gdG90YWwgPCAwID8gXCIjZWY0NDQ0XCIgOiBcIiNkMWQ1ZGJcIjtcbiAgICB9XG4gIH1cblxuICBfc3VibWl0QWN0aW9uKCkge1xuICAgIGNvbnN0IGZvcm0gPSBkb2N1bWVudC5nZXRFbGVtZW50QnlJZChcImFjdGlvbi1mb3JtXCIpO1xuICAgIGNvbnN0IHN1Ym1pdEJ0biA9IGRvY3VtZW50LmdldEVsZW1lbnRCeUlkKFwibW9kYWwtc3VibWl0XCIpO1xuICAgIGlmICghZm9ybSkgcmV0dXJuO1xuXG4gICAgY29uc3QgYWN0aW9uVHlwZSA9IGZvcm0uZGF0YXNldC5hY3Rpb25UeXBlO1xuICAgIGlmICghYWN0aW9uVHlwZSkgcmV0dXJuO1xuXG4gICAgY29uc3QgcGFyYW1zID0ge307XG5cbiAgICBjb25zdCBzZWxlY3RlZFRpY2tlciA9IGRvY3VtZW50LnF1ZXJ5U2VsZWN0b3IoXG4gICAgICBcIiN0aWNrZXItYnV0dG9ucyAudGlja2VyLXBpbGwuc2VsZWN0ZWRcIixcbiAgICApO1xuICAgIGlmIChzZWxlY3RlZFRpY2tlcikgcGFyYW1zLnRpY2tlciA9IHNlbGVjdGVkVGlja2VyLmRhdGFzZXQudGlja2VyO1xuXG4gICAgY29uc3QgcXR5SW5wdXQgPSBkb2N1bWVudC5nZXRFbGVtZW50QnlJZChcImlucHV0LXF1YW50aXR5XCIpO1xuICAgIGNvbnN0IHF0eVBhcmVudCA9IGRvY3VtZW50LmdldEVsZW1lbnRCeUlkKFwiZmllbGQtcXVhbnRpdHlcIik7XG4gICAgY29uc3QgcXR5VmlzaWJsZSA9IHF0eVBhcmVudCAmJiBxdHlQYXJlbnQuc3R5bGUuZGlzcGxheSAhPT0gXCJub25lXCI7XG4gICAgaWYgKHF0eVZpc2libGUgJiYgcXR5SW5wdXQpIHtcbiAgICAgIGNvbnN0IHEgPSBwYXJzZUludChxdHlJbnB1dC52YWx1ZSwgMTApO1xuICAgICAgaWYgKCFxIHx8IHEgPD0gMCkge1xuICAgICAgICB0aGlzLl9zaG93TW9kYWxFcnJvcihcIlBsZWFzZSBlbnRlciBhIHZhbGlkIHF1YW50aXR5LlwiKTtcbiAgICAgICAgcmV0dXJuO1xuICAgICAgfVxuICAgICAgcGFyYW1zLnF1YW50aXR5ID0gcTtcbiAgICB9XG5cbiAgICBjb25zdCBzZWxlY3RlZFRhcmdldCA9IGRvY3VtZW50LnF1ZXJ5U2VsZWN0b3IoXG4gICAgICBcIiN0YXJnZXQtYnV0dG9ucyAucGxheWVyLXBpbGwuc2VsZWN0ZWRcIixcbiAgICApO1xuICAgIGlmIChzZWxlY3RlZFRhcmdldCkgcGFyYW1zLnRhcmdldF91c2VyX2lkID0gc2VsZWN0ZWRUYXJnZXQuZGF0YXNldC50YXJnZXRJZDtcblxuICAgIGNvbnN0IHRpY2tlclBhcmVudCA9IGRvY3VtZW50LmdldEVsZW1lbnRCeUlkKFwiZmllbGQtdGlja2VyXCIpO1xuICAgIGlmIChcbiAgICAgIHRpY2tlclBhcmVudCAmJlxuICAgICAgdGlja2VyUGFyZW50LnN0eWxlLmRpc3BsYXkgIT09IFwibm9uZVwiICYmXG4gICAgICAhcGFyYW1zLnRpY2tlclxuICAgICkge1xuICAgICAgdGhpcy5fc2hvd01vZGFsRXJyb3IoXCJQbGVhc2Ugc2VsZWN0IGEgY29tcGFueS5cIik7XG4gICAgICByZXR1cm47XG4gICAgfVxuXG4gICAgY29uc3QgdGFyZ2V0UGFyZW50ID0gZG9jdW1lbnQuZ2V0RWxlbWVudEJ5SWQoXCJmaWVsZC10YXJnZXRcIik7XG4gICAgaWYgKFxuICAgICAgdGFyZ2V0UGFyZW50ICYmXG4gICAgICB0YXJnZXRQYXJlbnQuc3R5bGUuZGlzcGxheSAhPT0gXCJub25lXCIgJiZcbiAgICAgICFwYXJhbXMudGFyZ2V0X3VzZXJfaWRcbiAgICApIHtcbiAgICAgIHRoaXMuX3Nob3dNb2RhbEVycm9yKFwiUGxlYXNlIHNlbGVjdCBhIHRhcmdldCBwbGF5ZXIuXCIpO1xuICAgICAgcmV0dXJuO1xuICAgIH1cblxuICAgIHN1Ym1pdEJ0bi5kaXNhYmxlZCA9IHRydWU7XG4gICAgc3VibWl0QnRuLnRleHRDb250ZW50ID0gXCJTdWJtaXR0aW5nLi4uXCI7XG5cbiAgICB0aGlzLmNoYW5uZWxcbiAgICAgIC5wdXNoKFwic3VibWl0X2FjdGlvblwiLCB7IGFjdGlvbl90eXBlOiBhY3Rpb25UeXBlLCBwYXJhbXMgfSlcbiAgICAgIC5yZWNlaXZlKFwib2tcIiwgKCkgPT4ge1xuICAgICAgICB0aGlzLl9jbG9zZU1vZGFsKCk7XG4gICAgICAgIHN1Ym1pdEJ0bi5kaXNhYmxlZCA9IGZhbHNlO1xuICAgICAgICBzdWJtaXRCdG4udGV4dENvbnRlbnQgPSBcIkNvbmZpcm0gQWN0aW9uXCI7XG5cbiAgICAgICAgZG9jdW1lbnQucXVlcnlTZWxlY3RvckFsbChcIi5hY3Rpb24tYnRuXCIpLmZvckVhY2goKGIpID0+IHtcbiAgICAgICAgICBiLmRpc2FibGVkID0gdHJ1ZTtcbiAgICAgICAgICBiLmNsYXNzTGlzdC5hZGQoXCJzdWJtaXR0ZWRcIik7XG4gICAgICAgIH0pO1xuXG4gICAgICAgIHRoaXMuX3Nob3coZG9jdW1lbnQuZ2V0RWxlbWVudEJ5SWQoXCJhY3Rpb24tc3VibWl0dGVkLWJhZGdlXCIpLCBcImZsZXhcIik7XG4gICAgICAgIFRvYXN0LnNob3coXCJBY3Rpb24gc3VibWl0dGVkISBXYWl0aW5nIGZvciBvdGhlciBwbGF5ZXJzLlwiLCBcInN1Y2Nlc3NcIik7XG4gICAgICB9KVxuICAgICAgLnJlY2VpdmUoXCJlcnJvclwiLCAoZSkgPT4ge1xuICAgICAgICBzdWJtaXRCdG4uZGlzYWJsZWQgPSBmYWxzZTtcbiAgICAgICAgc3VibWl0QnRuLnRleHRDb250ZW50ID0gXCJDb25maXJtIEFjdGlvblwiO1xuICAgICAgICBjb25zdCByZWFzb24gPVxuICAgICAgICAgIHR5cGVvZiBlLnJlYXNvbiA9PT0gXCJzdHJpbmdcIiA/IGUucmVhc29uIDogSlNPTi5zdHJpbmdpZnkoZS5yZWFzb24pO1xuICAgICAgICB0aGlzLl9zaG93TW9kYWxFcnJvcihcIlJlamVjdGVkOiBcIiArIHJlYXNvbik7XG4gICAgICB9KTtcbiAgfVxuXG4gIF9zaG93TW9kYWxFcnJvcihtc2cpIHtcbiAgICBjb25zdCBlbCA9IGRvY3VtZW50LmdldEVsZW1lbnRCeUlkKFwibW9kYWwtZXJyb3JcIik7XG4gICAgaWYgKCFlbCkgcmV0dXJuO1xuICAgIGVsLnRleHRDb250ZW50ID0gbXNnO1xuICAgIHRoaXMuX3Nob3coZWwsIFwiYmxvY2tcIik7XG4gIH1cblxuICBfc2hvdyhlbCwgZGlzcGxheSA9IFwiYmxvY2tcIikge1xuICAgIGlmICghZWwpIHJldHVybjtcbiAgICBlbC5jbGFzc0xpc3QucmVtb3ZlKFwiaGlkZGVuXCIpO1xuICAgIGVsLnN0eWxlLmRpc3BsYXkgPSBkaXNwbGF5O1xuICB9XG5cbiAgX2hpZGUoZWwpIHtcbiAgICBpZiAoIWVsKSByZXR1cm47XG4gICAgZWwuY2xhc3NMaXN0LmFkZChcImhpZGRlblwiKTtcbiAgICBlbC5zdHlsZS5kaXNwbGF5ID0gXCJub25lXCI7XG4gIH1cblxuICAvLyBcdTI1MDBcdTI1MDAgY2hhdCBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcblxuICBfc2VuZENoYXRNZXNzYWdlKCkge1xuICAgIGNvbnN0IGlucHV0ID0gZG9jdW1lbnQuZ2V0RWxlbWVudEJ5SWQoXCJjaGF0LWlucHV0XCIpO1xuICAgIGlmICghaW5wdXQpIHJldHVybjtcblxuICAgIGNvbnN0IGNvbnRlbnQgPSBpbnB1dC52YWx1ZS50cmltKCk7XG4gICAgaWYgKCFjb250ZW50KSByZXR1cm47XG5cbiAgICB0aGlzLmNoYW5uZWxcbiAgICAgIC5wdXNoKFwic2VuZF9tZXNzYWdlXCIsIHsgY29udGVudCB9KVxuICAgICAgLnJlY2VpdmUoXCJva1wiLCAoKSA9PiB7XG4gICAgICAgIGlucHV0LnZhbHVlID0gXCJcIjtcbiAgICAgIH0pXG4gICAgICAucmVjZWl2ZShcImVycm9yXCIsIChlKSA9PiB7XG4gICAgICAgIFRvYXN0LnNob3coYE1lc3NhZ2UgcmVqZWN0ZWQ6ICR7SlNPTi5zdHJpbmdpZnkoZS5yZWFzb24pfWAsIFwiZXJyb3JcIik7XG4gICAgICB9KTtcbiAgfVxuXG4gIF9hcHBlbmRDaGF0KG1zZykge1xuICAgIGNvbnN0IGNvbnRhaW5lciA9IGRvY3VtZW50LmdldEVsZW1lbnRCeUlkKFwiY2hhdC1tZXNzYWdlc1wiKTtcbiAgICBpZiAoIWNvbnRhaW5lcikgcmV0dXJuO1xuXG4gICAgY29uc3QgcGxhY2Vob2xkZXIgPSBjb250YWluZXIucXVlcnlTZWxlY3RvcihcInBcIik7XG4gICAgaWYgKHBsYWNlaG9sZGVyICYmIHBsYWNlaG9sZGVyLmNsYXNzTGlzdC5jb250YWlucyhcInRleHQtY2VudGVyXCIpKSB7XG4gICAgICBwbGFjZWhvbGRlci5yZW1vdmUoKTtcbiAgICB9XG5cbiAgICBjb25zdCBlbCA9IGRvY3VtZW50LmNyZWF0ZUVsZW1lbnQoXCJkaXZcIik7XG4gICAgZWwuY2xhc3NOYW1lID0gXCJmbGV4IGdhcC0xLjUgZmFkZS1pbi11cFwiO1xuXG4gICAgY29uc3QgdGltZSA9IG1zZy5pbnNlcnRlZF9hdFxuICAgICAgPyBuZXcgRGF0ZShtc2cuaW5zZXJ0ZWRfYXQpLnRvTG9jYWxlVGltZVN0cmluZyhbXSwge1xuICAgICAgICAgIGhvdXI6IFwiMi1kaWdpdFwiLFxuICAgICAgICAgIG1pbnV0ZTogXCIyLWRpZ2l0XCIsXG4gICAgICAgIH0pXG4gICAgICA6IFwiXCI7XG5cbiAgICBlbC5pbm5lckhUTUwgPSBgXG4gICAgICA8c3BhbiBjbGFzcz1cInRleHQtZ3JheS02MDAgZm9udC1tb25vIHRleHQteHMgc2hyaW5rLTAgbXQtMC41XCI+JHt0aW1lfTwvc3Bhbj5cbiAgICAgIDxkaXYgY2xhc3M9XCJtaW4tdy0wXCI+XG4gICAgICAgIDxzcGFuIGNsYXNzPVwiZm9udC1tb25vIHRleHQteHMgZm9udC1zZW1pYm9sZCB0ZXh0LWVtZXJhbGQtNDAwXCI+JHtlc2NhcGVIdG1sKG1zZy51c2VybmFtZSB8fCBcIj9cIil9Ojwvc3Bhbj5cbiAgICAgICAgPHNwYW4gY2xhc3M9XCJ0ZXh0LWdyYXktMzAwIHRleHQteHMgbWwtMSBicmVhay13b3Jkc1wiPiR7ZXNjYXBlSHRtbChtc2cuY29udGVudCl9PC9zcGFuPlxuICAgICAgPC9kaXY+XG4gICAgYDtcblxuICAgIGNvbnRhaW5lci5hcHBlbmRDaGlsZChlbCk7XG4gICAgY29udGFpbmVyLnNjcm9sbFRvcCA9IGNvbnRhaW5lci5zY3JvbGxIZWlnaHQ7XG4gIH1cbn1cblxuLy8gXHUyNTAwXHUyNTAwXHUyNTAwIEhlbHBlcnMgXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXG5mdW5jdGlvbiBwaGFzZUxhYmVsKHBoYXNlKSB7XG4gIGNvbnN0IGxhYmVscyA9IHtcbiAgICB3YWl0aW5nOiBcIldBSVRJTkdcIixcbiAgICBuZXdzOiBcIk5FV1NcIixcbiAgICBhY3Rpb25fc3VibWlzc2lvbjogXCJTVUJNSVQgQUNUSU9OXCIsXG4gICAgbmVnb3RpYXRpb246IFwiTkVHT1RJQVRJT05cIixcbiAgICByZXNvbHV0aW9uOiBcIlJFU09MVVRJT05cIixcbiAgICBkaXNjbG9zdXJlOiBcIkRJU0NMT1NVUkVcIixcbiAgICBmaW5pc2hlZDogXCJGSU5JU0hFRFwiLFxuICB9O1xuXG4gIHJldHVybiAoXG4gICAgbGFiZWxzW3BoYXNlXSB8fFxuICAgIFN0cmluZyhwaGFzZSB8fCBcIlx1MjAxNFwiKVxuICAgICAgLnJlcGxhY2VBbGwoXCJfXCIsIFwiIFwiKVxuICAgICAgLnRvVXBwZXJDYXNlKClcbiAgKTtcbn1cblxuZnVuY3Rpb24gZXNjYXBlSHRtbChzdHIpIHtcbiAgcmV0dXJuIFN0cmluZyhzdHIgPz8gXCJcIilcbiAgICAucmVwbGFjZUFsbChcIiZcIiwgXCImYW1wO1wiKVxuICAgIC5yZXBsYWNlQWxsKFwiPFwiLCBcIiZsdDtcIilcbiAgICAucmVwbGFjZUFsbChcIj5cIiwgXCImZ3Q7XCIpXG4gICAgLnJlcGxhY2VBbGwoJ1wiJywgXCImcXVvdDtcIilcbiAgICAucmVwbGFjZUFsbChcIidcIiwgXCImIzM5O1wiKTtcbn1cblxuLy8gU3RhcnQgb25seSBhZnRlciB0aGUgd2hvbGUgZmlsZSBoYXMgbG9hZGVkXG5pZiAoZG9jdW1lbnQucmVhZHlTdGF0ZSA9PT0gXCJsb2FkaW5nXCIpIHtcbiAgZG9jdW1lbnQuYWRkRXZlbnRMaXN0ZW5lcihcIkRPTUNvbnRlbnRMb2FkZWRcIiwgKCkgPT4gYm9vdChjb25maWcpLCB7XG4gICAgb25jZTogdHJ1ZSxcbiAgfSk7XG59IGVsc2Uge1xuICBib290KGNvbmZpZyk7XG59XG4iXSwKICAibWFwcGluZ3MiOiAiOztBQUNPLE1BQUksVUFBVSxDQUFDLFVBQVU7QUFDOUIsUUFBRyxPQUFPLFVBQVUsWUFBVztBQUM3QixhQUFPO0lBQ1QsT0FBTztBQUNMLFVBQUlBLFdBQVUsV0FBVztBQUFFLGVBQU87TUFBTTtBQUN4QyxhQUFPQTtJQUNUO0VBQ0Y7QUNSTyxNQUFNLGFBQWEsT0FBTyxTQUFTLGNBQWMsT0FBTztBQUN4RCxN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
