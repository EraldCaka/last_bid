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
      const container = document.getElementById("toast-container");
      if (!container)
        return;
      const el = document.createElement("div");
      el.className = `toast ${type}`;
      el.innerHTML = `
      <span class="text-base leading-none" style="flex-shrink:0">${icons[type] || "\u2022"}</span>
      <span class="flex-1 text-gray-200">${escapeHtml(msg)}</span>
    `;
      container.appendChild(el);
      setTimeout(() => {
        el.style.animation = "slide-out-right 0.25s ease forwards";
        el.addEventListener("animationend", () => el.remove(), { once: true });
      }, duration);
      el.addEventListener("click", () => el.remove());
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
      this.svg.classList.add("sparkline");
      const gradId = `sg-${Math.random().toString(36).slice(2)}`;
      this.svg.innerHTML = `
      <defs>
        <linearGradient id="${gradId}" x1="0" y1="0" x2="0" y2="1">
          <stop offset="0%" stop-color="var(--spark-color,#10b981)" stop-opacity="0.4"/>
          <stop offset="100%" stop-color="var(--spark-color,#10b981)" stop-opacity="0"/>
        </linearGradient>
      </defs>
      <path class="area" fill="url(#${gradId})"/>
      <polyline class="line" style="stroke:var(--spark-color,#10b981)"/>
    `;
      this._gradId = gradId;
      this.container.appendChild(this.svg);
    }
    push(price) {
      const p = parseFloat(price);
      if (isNaN(p))
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
      const stop = this.svg.querySelector(`#${this._gradId} stop`);
      if (stop)
        stop.setAttribute("stop-color", c);
    }
    _render() {
      if (!this.svg || this.history.length < 2)
        return;
      const pts = this.history;
      const min = Math.min(...pts);
      const max = Math.max(...pts);
      const range = max - min || 1;
      const pad = 2;
      const xStep = (this.w - pad * 2) / (pts.length - 1);
      const points = pts.map((v, i) => {
        const x = pad + i * xStep;
        const y = pad + (1 - (v - min) / range) * (this.h - pad * 2);
        return `${x.toFixed(1)},${y.toFixed(1)}`;
      });
      const polyline = this.svg.querySelector("polyline.line");
      const area = this.svg.querySelector("path.area");
      if (polyline)
        polyline.setAttribute("points", points.join(" "));
      if (area) {
        const first = points[0].split(",");
        const last = points[points.length - 1].split(",");
        const d = `M${first[0]},${this.h - pad} L${points.join(" L")} L${last[0]},${this.h - pad} Z`;
        area.setAttribute("d", d);
      }
    }
  };
  var LobbyManager = class {
    constructor(socket, cfg) {
      this.socket = socket;
      this.cfg = cfg;
      this.channel = null;
      this.presence = null;
      this.joined = false;
    }
    init() {
      this.channel = this.socket.channel("lobby:general", {});
      this.channel.on("open_matches", (p) => this._updateMatchList(p.matches));
      this.channel.on("match_created", (match) => this._addOrUpdateMatch(match));
      this.channel.on("match_updated", (match) => this._addOrUpdateMatch(match));
      this.channel.on("match_started", (p) => this._onMatchStarted(p.match_id));
      this.channel.on("presence_state", (state) => this._onPresenceState(state));
      this.channel.on("presence_diff", (diff) => this._onPresenceDiff(diff));
      this.channel.join().receive("ok", () => {
        this.joined = true;
        this._wireJoinButtons(document);
      }).receive("error", (e) => {
        console.warn("[Lobby] join error", e);
        this._wireJoinButtons(document);
      });
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
      btn.disabled = true;
      const origText = btn.textContent;
      btn.textContent = "Joining...";
      this.channel.push("join_match", { match_id: matchId }).receive("ok", () => {
        window.location = `/matches/${matchId}`;
      }).receive("error", (err) => {
        btn.disabled = false;
        btn.textContent = origText;
        const reason = (err == null ? void 0 : err.reason) || JSON.stringify(err);
        Toast.show("Could not join: " + reason, "error");
      }).receive("timeout", () => {
        btn.disabled = false;
        btn.textContent = origText;
        Toast.show("Join timed out. Please try again.", "warning");
      });
    }
    _addOrUpdateMatch(match) {
      const list = document.getElementById("open-matches-list");
      if (!list)
        return;
      const placeholder = document.getElementById("no-matches-placeholder");
      if (placeholder)
        placeholder.remove();
      const existing = list.querySelector(`[data-match-id="${match.id}"]`);
      const card = this._buildMatchCard(match);
      if (existing) {
        existing.replaceWith(card);
      } else {
        list.insertAdjacentElement("afterbegin", card);
      }
      this._wireJoinButtons(list);
    }
    _buildMatchCard(match) {
      const div = document.createElement("div");
      div.className = "card card-hover p-4 match-card fade-in-up";
      div.dataset.matchId = match.id;
      const isFull = match.player_count >= match.max_players;
      const isMyMatch = match.host_username === this._myUsername();
      div.innerHTML = `
      <div class="flex items-center justify-between">
        <div class="flex items-center gap-3">
          <div class="w-9 h-9 rounded-lg bg-emerald-500/10 border border-emerald-500/20 flex items-center justify-center shrink-0">
            <svg class="w-4 h-4 text-emerald-400" fill="none" viewBox="0 0 24 24" stroke="currentColor" stroke-width="2">
              <polyline points="22 7 13.5 15.5 8.5 10.5 2 17"/>
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
              <span class="text-white font-semibold">${match.player_count}</span>/${match.max_players}
            </div>
          </div>
          ${isFull ? `<span class="btn-ghost text-xs py-1.5 px-3 opacity-40 pointer-events-none">Full</span>` : `<button type="button" data-match-id="${escapeHtml(match.id)}" class="join-match-btn btn-primary text-xs py-1.5 px-4">
                 ${isMyMatch ? "Enter" : "Join"}
               </button>`}
        </div>
      </div>
    `;
      return div;
    }
    _updateMatchList(matches) {
      const list = document.getElementById("open-matches-list");
      if (!list)
        return;
      if (!matches || matches.length === 0)
        return;
      list.innerHTML = "";
      matches.forEach((m) => list.appendChild(this._buildMatchCard(m)));
      this._wireJoinButtons(list);
    }
    _onMatchStarted(matchId) {
      const card = document.querySelector(
        `#open-matches-list [data-match-id="${matchId}"]`
      );
      if (card) {
        card.style.animation = "slide-out-right 0.3s ease forwards";
        card.addEventListener("animationend", () => card.remove(), {
          once: true
        });
      }
    }
    _onPresenceState(state) {
      const list = document.getElementById("lobby-presence-list");
      const counter = document.getElementById("lobby-online-count");
      if (!list)
        return;
      const users = [];
      Object.values(state).forEach((entry) => {
        var _a;
        const meta = (_a = entry.metas) == null ? void 0 : _a[0];
        if (meta == null ? void 0 : meta.username)
          users.push(meta.username);
      });
      if (counter)
        counter.textContent = `${users.length} online`;
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
      Object.values(diff.joins || {}).forEach((entry) => {
        var _a;
        const meta = (_a = entry.metas) == null ? void 0 : _a[0];
        if (!(meta == null ? void 0 : meta.username))
          return;
        if (current.has(meta.username))
          return;
        const el = this._buildPresenceRow(meta.username);
        list.appendChild(el);
        current.set(meta.username, el);
      });
      Object.values(diff.leaves || {}).forEach((entry) => {
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
        counter.textContent = `${current.size} online`;
      if (current.size === 0) {
        list.innerHTML = `<p class="text-xs text-gray-600 font-mono">Nobody else online.</p>`;
      }
    }
    _renderPresenceList(list, users) {
      if (users.length === 0) {
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
    _myUsername() {
      const nav = document.querySelector("header nav .font-mono");
      return nav ? nav.textContent.trim() : null;
    }
    // Called by MatchManager to send lobby events from the match page
    push(event, payload) {
      if (this.channel && this.joined) {
        return this.channel.push(event, payload);
      }
    }
  };
  var MatchManager = class {
    constructor(socket, cfg, lobbyMgr) {
      this.socket = socket;
      this.cfg = cfg;
      this.lobbyMgr = lobbyMgr;
      this.channel = null;
      this.charts = {};
      this.prevPrices = {};
      this.players = [];
      this.companies = [];
      this.myState = null;
      this.phase = null;
      this.countdown = null;
      this.phaseOverlayTimer = null;
    }
    init() {
      this.channel = this.socket.channel(`match:${this.cfg.matchId}`, {});
      this.channel.on("state_updated", (p) => this._onStateUpdated(p));
      this.channel.on("phase_changed", (p) => this._onPhaseChanged(p));
      this.channel.on("private_state", (p) => this._onPrivateState(p));
      this.channel.on("private_events", (p) => this._onPrivateEvents(p));
      this.channel.on("new_message", (m) => this._appendChat(m));
      this.channel.on("new_whisper", (m) => this._appendChat(m, true));
      this.channel.on("match_finished", (p) => this._onMatchFinished(p));
      this.channel.on("presence_state", (s) => this._onPresenceState(s));
      this.channel.on("presence_diff", (d) => this._onPresenceDiff(d));
      this.channel.join().receive("ok", () => console.debug("[Match] joined")).receive("error", ({ reason }) => {
        Toast.show("Could not join match: " + reason, "error");
      });
      this._wireControls();
    }
    // ── Wire static controls ──────────────────────────────────────────
    _wireControls() {
      var _a, _b, _c, _d, _e;
      const startBtn = document.getElementById("start-match-btn");
      if (startBtn) {
        startBtn.addEventListener("click", (e) => {
          var _a2, _b2;
          e.preventDefault();
          startBtn.disabled = true;
          startBtn.textContent = "Starting...";
          (_b2 = (_a2 = this.lobbyMgr.push("start_match", { match_id: this.cfg.matchId })) == null ? void 0 : _a2.receive("ok", () => startBtn.remove())) == null ? void 0 : _b2.receive("error", (err) => {
            startBtn.disabled = false;
            startBtn.textContent = "Start Match";
            const reason = (err == null ? void 0 : err.reason) || JSON.stringify(err);
            Toast.show("Could not start: " + reason, "error");
          });
        });
      }
      document.querySelectorAll(".action-btn").forEach((btn) => {
        btn.addEventListener("click", (e) => {
          e.preventDefault();
          e.stopPropagation();
          if (btn.disabled)
            return;
          this._openActionModal(btn.dataset);
        });
      });
      (_a = document.getElementById("copy-invite-btn")) == null ? void 0 : _a.addEventListener("click", (e) => {
        var _a2;
        e.preventDefault();
        const url = window.location.href;
        (_a2 = navigator.clipboard) == null ? void 0 : _a2.writeText(url).then(() => Toast.show("Invite link copied to clipboard!", "success")).catch(() => {
          window.prompt("Copy this invite link:", url);
        });
      });
      (_b = document.getElementById("modal-close")) == null ? void 0 : _b.addEventListener("click", (e) => {
        e.preventDefault();
        e.stopPropagation();
        this._closeModal();
      });
      (_c = document.getElementById("action-modal")) == null ? void 0 : _c.addEventListener("click", (e) => {
        if (e.target === e.currentTarget)
          this._closeModal();
      });
      (_d = document.getElementById("modal-submit")) == null ? void 0 : _d.addEventListener("click", (e) => {
        e.preventDefault();
        e.stopPropagation();
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
    // ── State updates ─────────────────────────────────────────────────
    _onStateUpdated(payload) {
      this.players = payload.players || [];
      this.companies = payload.companies || [];
      this.phase = payload.phase;
      this._renderPhase(payload.phase, payload.round);
      this._renderRound(payload.round, payload.total_rounds);
      this._renderMarket(payload.companies);
      this._renderPlayers(payload.players);
      this._renderActionPanel(payload.phase);
      if (payload.phase === "negotiation" && payload.negotiation_deadline) {
        this._startCountdown(new Date(payload.negotiation_deadline));
      }
    }
    _onPhaseChanged(payload) {
      var _a;
      this.phase = payload.phase;
      this._renderPhase(payload.phase, payload.round);
      this._renderRound(payload.round);
      this._renderActionPanel(payload.phase);
      this._showPhaseOverlay(payload.phase);
      this._updateChatIndicator(payload.phase);
      if (payload.phase === "action_submission") {
        this._clearCountdown();
        (_a = document.getElementById("action-submitted-badge")) == null ? void 0 : _a.classList.add("hidden");
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
        Toast.show(
          ev.message || ev.type || "Event",
          ev.positive ? "success" : "warning"
        );
      });
    }
    _onMatchFinished(payload) {
      var _a, _b;
      this._clearCountdown();
      this._renderLeaderboard(payload.leaderboard);
      this._showPhaseOverlay("finished");
      (_a = document.getElementById("action-panel")) == null ? void 0 : _a.classList.add("hidden");
      (_b = document.getElementById("leaderboard-panel")) == null ? void 0 : _b.classList.remove("hidden");
      Toast.show("Match over! Final rankings are in.", "info", 8e3);
    }
    // ── Presence ──────────────────────────────────────────────────────
    _onPresenceState(state) {
      const onlineIds = /* @__PURE__ */ new Set();
      Object.entries(state).forEach(([userId, entry]) => {
        var _a;
        onlineIds.add(userId);
        const meta = (_a = entry.metas) == null ? void 0 : _a[0];
        this._setPlayerOnline(userId, meta == null ? void 0 : meta.username, true);
      });
    }
    _onPresenceDiff(diff) {
      Object.entries(diff.joins || {}).forEach(([userId, entry]) => {
        var _a;
        const meta = (_a = entry.metas) == null ? void 0 : _a[0];
        this._setPlayerOnline(userId, meta == null ? void 0 : meta.username, true);
      });
      Object.entries(diff.leaves || {}).forEach(([userId]) => {
        this._setPlayerOnline(userId, null, false);
      });
    }
    _setPlayerOnline(userId, _username, online) {
      const row = document.querySelector(`[data-player-id="${userId}"]`);
      if (!row)
        return;
      const dot = row.querySelector(".presence-dot");
      if (dot)
        dot.classList.toggle("offline", !online);
    }
    // ── Market rendering ──────────────────────────────────────────────
    _renderMarket(companies) {
      const list = document.getElementById("companies-list");
      if (!list || !companies)
        return;
      companies.forEach((c) => {
        const prev = this.prevPrices[c.ticker];
        const price = parseFloat(c.price);
        const isUp = prev !== void 0 ? price >= prev : true;
        this.prevPrices[c.ticker] = price;
        let row = list.querySelector(`[data-ticker="${c.ticker}"]`);
        if (!row) {
          row = this._createMarketRow(c);
          list.appendChild(row);
        }
        this._updateMarketRow(row, c, price, prev, isUp);
      });
    }
    _createMarketRow(c) {
      const row = document.createElement("div");
      row.className = "ticker-row";
      row.dataset.ticker = c.ticker;
      const chartId = `chart-${c.ticker}`;
      row.innerHTML = `
      <span class="font-mono font-bold text-sm ticker-symbol"></span>
      <span class="text-xs text-gray-500 truncate ticker-name"></span>
      <div class="text-right">
        <div class="font-mono font-semibold text-sm ticker-price"></div>
        <div class="text-xs font-mono ticker-change mt-0.5"></div>
      </div>
      <div id="${chartId}" class="flex items-center justify-end" style="height:28px;width:80px"></div>
    `;
      setTimeout(() => {
        this.charts[c.ticker] = new SparklineChart(chartId, {
          width: 80,
          height: 28
        });
        this.charts[c.ticker].push(parseFloat(c.price));
      }, 0);
      return row;
    }
    _updateMarketRow(row, c, price, prev, isUp) {
      const priceEl = row.querySelector(".ticker-price");
      const changeEl = row.querySelector(".ticker-change");
      const symEl = row.querySelector(".ticker-symbol");
      const nameEl = row.querySelector(".ticker-name");
      if (symEl)
        symEl.textContent = c.ticker;
      if (nameEl)
        nameEl.textContent = c.name || "";
      if (priceEl) {
        const oldPrice = priceEl.textContent;
        const newText = `$${price.toFixed(2)}`;
        if (oldPrice !== newText) {
          priceEl.textContent = newText;
          priceEl.classList.remove("price-up", "price-down");
          void priceEl.offsetWidth;
          priceEl.classList.add(isUp ? "price-up" : "price-down");
          priceEl.style.color = isUp ? "#10b981" : "#ef4444";
          row.classList.remove("row-flash-green", "row-flash-red");
          void row.offsetWidth;
          row.classList.add(isUp ? "row-flash-green" : "row-flash-red");
        }
      }
      if (changeEl && prev !== void 0) {
        const pct = (price - prev) / prev * 100;
        if (Math.abs(pct) > 0.01) {
          changeEl.textContent = (pct >= 0 ? "+" : "") + pct.toFixed(2) + "%";
          changeEl.style.color = pct >= 0 ? "#10b981" : "#ef4444";
        } else {
          changeEl.textContent = "";
        }
      }
      if (c.regulatory_heat > 0) {
        let heatEl = row.querySelector(".heat-badge");
        if (!heatEl) {
          heatEl = document.createElement("span");
          heatEl.className = "heat-badge absolute right-0 top-0";
        }
      }
      if (this.charts[c.ticker]) {
        this.charts[c.ticker].push(price);
        this.charts[c.ticker].setColor(isUp);
      }
    }
    // ── Players rendering ─────────────────────────────────────────────
    _renderPlayers(players) {
      const list = document.getElementById("players-list");
      if (!list || !players)
        return;
      players.forEach((p) => {
        const row = list.querySelector(`[data-player-id="${p.user_id}"]`);
        if (!row)
          return;
        let submitBadge = row.querySelector(".submit-badge");
        if (!submitBadge) {
          submitBadge = document.createElement("span");
          submitBadge.className = "submit-badge text-xs font-mono ml-auto";
          row.appendChild(submitBadge);
        }
        submitBadge.textContent = p.has_submitted ? "\u2713" : "";
        submitBadge.style.color = "#10b981";
        let frozenBadge = row.querySelector(".frozen-badge");
        if (p.liquidity_frozen) {
          if (!frozenBadge) {
            frozenBadge = document.createElement("span");
            frozenBadge.className = "frozen-badge text-xs font-mono text-blue-400";
            row.appendChild(frozenBadge);
          }
          frozenBadge.textContent = "\u2744";
        } else if (frozenBadge) {
          frozenBadge.remove();
        }
        let heatEl = row.querySelector(".heat-indicator");
        if (p.regulatory_heat > 0) {
          if (!heatEl) {
            heatEl = document.createElement("span");
            heatEl.className = "heat-indicator text-xs font-mono text-amber-500";
            row.appendChild(heatEl);
          }
          heatEl.textContent = `\u2696${p.regulatory_heat}`;
        } else if (heatEl) {
          heatEl.remove();
        }
      });
    }
    // ── My stats ──────────────────────────────────────────────────────
    _renderMyStats(state) {
      const panel = document.getElementById("my-stats");
      this._show(panel, "block");
      const cash = parseFloat(state.cash);
      const nw = parseFloat(state.net_worth);
      const cashEl = document.getElementById("my-cash");
      const nwEl = document.getElementById("my-networth");
      const heatEl = document.getElementById("my-heat");
      const frzEl = document.getElementById("my-frozen-badge");
      if (cashEl) {
        cashEl.textContent = "$" + cash.toLocaleString("en-US", {
          minimumFractionDigits: 2,
          maximumFractionDigits: 2
        });
      }
      if (nwEl) {
        nwEl.textContent = "$" + nw.toLocaleString("en-US", {
          minimumFractionDigits: 2,
          maximumFractionDigits: 2
        });
      }
      if (heatEl)
        heatEl.textContent = state.regulatory_heat || 0;
      if (frzEl) {
        if (state.liquidity_frozen)
          this._show(frzEl, "block");
        else
          this._hide(frzEl);
      }
      const holdingsEl = document.getElementById("my-holdings");
      if (holdingsEl && state.portfolio) {
        holdingsEl.innerHTML = "";
        Object.entries(state.portfolio).forEach(([ticker, qty]) => {
          if (qty <= 0)
            return;
          const div = document.createElement("div");
          div.className = "flex justify-between text-xs font-mono";
          div.innerHTML = `<span class="text-gray-500">${escapeHtml(ticker)}</span><span class="text-gray-300">${qty} sh</span>`;
          holdingsEl.appendChild(div);
        });
      }
    }
    // ── Phase UI ──────────────────────────────────────────────────────
    _renderPhase(phase, round) {
      const container = document.getElementById("phase-badge-container");
      if (!container)
        return;
      const label = phaseLabel(phase);
      container.innerHTML = `<span class="phase-badge phase-${phase || "waiting"}">${label}</span>`;
    }
    _renderRound(round, total) {
      const el = document.getElementById("current-round");
      if (el && round)
        el.textContent = round;
    }
    _renderActionPanel(phase) {
      const panel = document.getElementById("action-panel");
      if (!panel)
        return;
      if (phase === "action_submission") {
        this._show(panel, "block");
      } else {
        this._hide(panel);
      }
    }
    _updateChatIndicator(phase) {
      const el = document.getElementById("chat-phase-indicator");
      if (!el)
        return;
      const map = {
        negotiation: "Open",
        action_submission: "Closed",
        news: "Closed",
        resolution: "Closed",
        disclosure: "Open",
        finished: "Open"
      };
      el.textContent = map[phase] || "\u2014";
      el.style.color = phase === "negotiation" || phase === "disclosure" || phase === "finished" ? "#10b981" : "#6b7280";
    }
    _showPhaseOverlay(phase) {
      const overlay = document.getElementById("phase-overlay");
      const badge = document.getElementById("phase-overlay-badge");
      const desc = document.getElementById("phase-overlay-desc");
      if (!overlay || !badge)
        return;
      const descs = {
        news: "A market event has occurred. Analyse it carefully.",
        action_submission: "Choose your action wisely. No one else can see it.",
        negotiation: "Deals, deceptions, and alliances. Chat is open.",
        resolution: "Actions are resolving. Brace for impact.",
        disclosure: "Round complete. Results are in.",
        finished: "The market closes. Final rankings revealed."
      };
      badge.className = `phase-badge phase-${phase} text-lg px-6 py-3 font-mono tracking-widest uppercase`;
      badge.textContent = phaseLabel(phase);
      if (desc)
        desc.textContent = descs[phase] || "";
      this._show(overlay, "flex");
      if (this.phaseOverlayTimer)
        clearTimeout(this.phaseOverlayTimer);
      if (phase !== "finished") {
        this.phaseOverlayTimer = setTimeout(() => this._hide(overlay), 3e3);
      }
    }
    // ── Countdown timer ───────────────────────────────────────────────
    _startCountdown(deadline) {
      this._clearCountdown();
      const el = document.getElementById("countdown-timer");
      const valEl = document.getElementById("countdown-value");
      if (!el || !valEl)
        return;
      this._show(el, "flex");
      const tick = () => {
        const diff = Math.max(0, Math.floor((deadline - Date.now()) / 1e3));
        const m = Math.floor(diff / 60);
        const s = diff % 60;
        valEl.textContent = `${m}:${String(s).padStart(2, "0")}`;
        if (diff <= 10)
          el.classList.add("countdown-urgent");
        else
          el.classList.remove("countdown-urgent");
        if (diff === 0)
          this._clearCountdown();
      };
      tick();
      this.countdown = setInterval(tick, 1e3);
    }
    _clearCountdown() {
      if (this.countdown) {
        clearInterval(this.countdown);
        this.countdown = null;
      }
      const el = document.getElementById("countdown-timer");
      if (el) {
        this._hide(el);
        el.classList.remove("flex", "countdown-urgent");
      }
    }
    // ── Leaderboard ───────────────────────────────────────────────────
    _renderLeaderboard(leaderboard) {
      const list = document.getElementById("leaderboard-list");
      if (!list || !leaderboard)
        return;
      list.innerHTML = "";
      leaderboard.forEach((entry, i) => {
        const div = document.createElement("div");
        div.className = "flex items-center gap-3 p-3 rounded-lg card fade-in-up";
        div.style.animationDelay = `${i * 80}ms`;
        const medals = ["\u{1F947}", "\u{1F948}", "\u{1F949}"];
        div.innerHTML = `
        <span class="text-xl w-8 text-center">${medals[i] || `#${i + 1}`}</span>
        <span class="font-mono font-semibold text-sm text-white flex-1">${escapeHtml(entry.username)}</span>
        <span class="font-mono text-sm ${i === 0 ? "text-emerald-400 font-bold" : "text-gray-300"}">
          $${parseFloat(entry.net_worth).toLocaleString("en-US", { minimumFractionDigits: 2, maximumFractionDigits: 2 })}
        </span>
      `;
        list.appendChild(div);
      });
    }
    // ── Action Modal ──────────────────────────────────────────────────
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
      setTimeout(() => {
        var _a;
        const focusEl = needsTicker && (tickerBtns == null ? void 0 : tickerBtns.querySelector(".ticker-pill")) || needsQty && qtyInput || needsTarget && (targetBtns == null ? void 0 : targetBtns.querySelector(".player-pill")) || document.getElementById("modal-close");
        (_a = focusEl == null ? void 0 : focusEl.focus) == null ? void 0 : _a.call(focusEl);
      }, 20);
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
        const badge = document.getElementById("action-submitted-badge");
        this._show(badge, "flex");
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
    // ── Chat ──────────────────────────────────────────────────────────
    _sendChatMessage() {
      const input = document.getElementById("chat-input");
      if (!input)
        return;
      const content = input.value.trim();
      if (!content)
        return;
      this.channel.push("send_message", { content }).receive("ok", () => {
        input.value = "";
      }).receive(
        "error",
        (e) => Toast.show("Message rejected: " + JSON.stringify(e.reason), "error")
      );
    }
    _appendChat(msg, isWhisper = false) {
      const container = document.getElementById("chat-messages");
      if (!container)
        return;
      const placeholder = container.querySelector("p");
      if (placeholder && placeholder.classList.contains("text-center"))
        placeholder.remove();
      const el = document.createElement("div");
      el.className = `flex gap-1.5 fade-in-up ${isWhisper ? "pl-2 border-l-2 border-purple-500/30" : ""}`;
      const time = msg.inserted_at ? new Date(msg.inserted_at).toLocaleTimeString([], {
        hour: "2-digit",
        minute: "2-digit"
      }) : "";
      const typeClass = isWhisper ? "text-purple-400" : "text-emerald-400";
      el.innerHTML = `
      <span class="text-gray-600 font-mono text-xs shrink-0 mt-0.5">${time}</span>
      <div class="min-w-0">
        <span class="font-mono text-xs font-semibold ${typeClass}">${escapeHtml(msg.username || "?")}${isWhisper ? " \u2192whisper" : ""}:</span>
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
      resolution: "RESOLVING",
      disclosure: "DISCLOSURE",
      finished: "FINISHED"
    };
    return labels[phase] || (phase ? phase.replace(/_/g, " ").toUpperCase() : "\u2014");
  }
  function escapeHtml(str) {
    return String(str != null ? str : "").replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;").replace(/"/g, "&quot;").replace(/'/g, "&#39;");
  }
  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", () => boot(config), {
      once: true
    });
  } else {
    boot(config);
  }
})();
//# sourceMappingURL=data:application/json;base64,ewogICJ2ZXJzaW9uIjogMywKICAic291cmNlcyI6IFsiLi4vLi4vLi4vZGVwcy9waG9lbml4L2Fzc2V0cy9qcy9waG9lbml4L3V0aWxzLmpzIiwgIi4uLy4uLy4uL2RlcHMvcGhvZW5peC9hc3NldHMvanMvcGhvZW5peC9jb25zdGFudHMuanMiLCAiLi4vLi4vLi4vZGVwcy9waG9lbml4L2Fzc2V0cy9qcy9waG9lbml4L3B1c2guanMiLCAiLi4vLi4vLi4vZGVwcy9waG9lbml4L2Fzc2V0cy9qcy9waG9lbml4L3RpbWVyLmpzIiwgIi4uLy4uLy4uL2RlcHMvcGhvZW5peC9hc3NldHMvanMvcGhvZW5peC9jaGFubmVsLmpzIiwgIi4uLy4uLy4uL2RlcHMvcGhvZW5peC9hc3NldHMvanMvcGhvZW5peC9hamF4LmpzIiwgIi4uLy4uLy4uL2RlcHMvcGhvZW5peC9hc3NldHMvanMvcGhvZW5peC9sb25ncG9sbC5qcyIsICIuLi8uLi8uLi9kZXBzL3Bob2VuaXgvYXNzZXRzL2pzL3Bob2VuaXgvcHJlc2VuY2UuanMiLCAiLi4vLi4vLi4vZGVwcy9waG9lbml4L2Fzc2V0cy9qcy9waG9lbml4L3NlcmlhbGl6ZXIuanMiLCAiLi4vLi4vLi4vZGVwcy9waG9lbml4L2Fzc2V0cy9qcy9waG9lbml4L3NvY2tldC5qcyIsICIuLi8uLi8uLi9hc3NldHMvanMvYXBwLmpzIl0sCiAgInNvdXJjZXNDb250ZW50IjogWyIvLyB3cmFwcyB2YWx1ZSBpbiBjbG9zdXJlIG9yIHJldHVybnMgY2xvc3VyZVxuZXhwb3J0IGxldCBjbG9zdXJlID0gKHZhbHVlKSA9PiB7XG4gIGlmKHR5cGVvZiB2YWx1ZSA9PT0gXCJmdW5jdGlvblwiKXtcbiAgICByZXR1cm4gdmFsdWVcbiAgfSBlbHNlIHtcbiAgICBsZXQgY2xvc3VyZSA9IGZ1bmN0aW9uICgpeyByZXR1cm4gdmFsdWUgfVxuICAgIHJldHVybiBjbG9zdXJlXG4gIH1cbn1cbiIsICJleHBvcnQgY29uc3QgZ2xvYmFsU2VsZiA9IHR5cGVvZiBzZWxmICE9PSBcInVuZGVmaW5lZFwiID8gc2VsZiA6IG51bGxcbmV4cG9ydCBjb25zdCBwaHhXaW5kb3cgPSB0eXBlb2Ygd2luZG93ICE9PSBcInVuZGVmaW5lZFwiID8gd2luZG93IDogbnVsbFxuZXhwb3J0IGNvbnN0IGdsb2JhbCA9IGdsb2JhbFNlbGYgfHwgcGh4V2luZG93IHx8IGdsb2JhbFxuZXhwb3J0IGNvbnN0IERFRkFVTFRfVlNOID0gXCIyLjAuMFwiXG5leHBvcnQgY29uc3QgU09DS0VUX1NUQVRFUyA9IHtjb25uZWN0aW5nOiAwLCBvcGVuOiAxLCBjbG9zaW5nOiAyLCBjbG9zZWQ6IDN9XG5leHBvcnQgY29uc3QgREVGQVVMVF9USU1FT1VUID0gMTAwMDBcbmV4cG9ydCBjb25zdCBXU19DTE9TRV9OT1JNQUwgPSAxMDAwXG5leHBvcnQgY29uc3QgQ0hBTk5FTF9TVEFURVMgPSB7XG4gIGNsb3NlZDogXCJjbG9zZWRcIixcbiAgZXJyb3JlZDogXCJlcnJvcmVkXCIsXG4gIGpvaW5lZDogXCJqb2luZWRcIixcbiAgam9pbmluZzogXCJqb2luaW5nXCIsXG4gIGxlYXZpbmc6IFwibGVhdmluZ1wiLFxufVxuZXhwb3J0IGNvbnN0IENIQU5ORUxfRVZFTlRTID0ge1xuICBjbG9zZTogXCJwaHhfY2xvc2VcIixcbiAgZXJyb3I6IFwicGh4X2Vycm9yXCIsXG4gIGpvaW46IFwicGh4X2pvaW5cIixcbiAgcmVwbHk6IFwicGh4X3JlcGx5XCIsXG4gIGxlYXZlOiBcInBoeF9sZWF2ZVwiXG59XG5cbmV4cG9ydCBjb25zdCBUUkFOU1BPUlRTID0ge1xuICBsb25ncG9sbDogXCJsb25ncG9sbFwiLFxuICB3ZWJzb2NrZXQ6IFwid2Vic29ja2V0XCJcbn1cbmV4cG9ydCBjb25zdCBYSFJfU1RBVEVTID0ge1xuICBjb21wbGV0ZTogNFxufVxuIiwgIi8qKlxuICogSW5pdGlhbGl6ZXMgdGhlIFB1c2hcbiAqIEBwYXJhbSB7Q2hhbm5lbH0gY2hhbm5lbCAtIFRoZSBDaGFubmVsXG4gKiBAcGFyYW0ge3N0cmluZ30gZXZlbnQgLSBUaGUgZXZlbnQsIGZvciBleGFtcGxlIGBcInBoeF9qb2luXCJgXG4gKiBAcGFyYW0ge09iamVjdH0gcGF5bG9hZCAtIFRoZSBwYXlsb2FkLCBmb3IgZXhhbXBsZSBge3VzZXJfaWQ6IDEyM31gXG4gKiBAcGFyYW0ge251bWJlcn0gdGltZW91dCAtIFRoZSBwdXNoIHRpbWVvdXQgaW4gbWlsbGlzZWNvbmRzXG4gKi9cbmV4cG9ydCBkZWZhdWx0IGNsYXNzIFB1c2gge1xuICBjb25zdHJ1Y3RvcihjaGFubmVsLCBldmVudCwgcGF5bG9hZCwgdGltZW91dCl7XG4gICAgdGhpcy5jaGFubmVsID0gY2hhbm5lbFxuICAgIHRoaXMuZXZlbnQgPSBldmVudFxuICAgIHRoaXMucGF5bG9hZCA9IHBheWxvYWQgfHwgZnVuY3Rpb24gKCl7IHJldHVybiB7fSB9XG4gICAgdGhpcy5yZWNlaXZlZFJlc3AgPSBudWxsXG4gICAgdGhpcy50aW1lb3V0ID0gdGltZW91dFxuICAgIHRoaXMudGltZW91dFRpbWVyID0gbnVsbFxuICAgIHRoaXMucmVjSG9va3MgPSBbXVxuICAgIHRoaXMuc2VudCA9IGZhbHNlXG4gIH1cblxuICAvKipcbiAgICpcbiAgICogQHBhcmFtIHtudW1iZXJ9IHRpbWVvdXRcbiAgICovXG4gIHJlc2VuZCh0aW1lb3V0KXtcbiAgICB0aGlzLnRpbWVvdXQgPSB0aW1lb3V0XG4gICAgdGhpcy5yZXNldCgpXG4gICAgdGhpcy5zZW5kKClcbiAgfVxuXG4gIC8qKlxuICAgKlxuICAgKi9cbiAgc2VuZCgpe1xuICAgIGlmKHRoaXMuaGFzUmVjZWl2ZWQoXCJ0aW1lb3V0XCIpKXsgcmV0dXJuIH1cbiAgICB0aGlzLnN0YXJ0VGltZW91dCgpXG4gICAgdGhpcy5zZW50ID0gdHJ1ZVxuICAgIHRoaXMuY2hhbm5lbC5zb2NrZXQucHVzaCh7XG4gICAgICB0b3BpYzogdGhpcy5jaGFubmVsLnRvcGljLFxuICAgICAgZXZlbnQ6IHRoaXMuZXZlbnQsXG4gICAgICBwYXlsb2FkOiB0aGlzLnBheWxvYWQoKSxcbiAgICAgIHJlZjogdGhpcy5yZWYsXG4gICAgICBqb2luX3JlZjogdGhpcy5jaGFubmVsLmpvaW5SZWYoKVxuICAgIH0pXG4gIH1cblxuICAvKipcbiAgICpcbiAgICogQHBhcmFtIHsqfSBzdGF0dXNcbiAgICogQHBhcmFtIHsqfSBjYWxsYmFja1xuICAgKi9cbiAgcmVjZWl2ZShzdGF0dXMsIGNhbGxiYWNrKXtcbiAgICBpZih0aGlzLmhhc1JlY2VpdmVkKHN0YXR1cykpe1xuICAgICAgY2FsbGJhY2sodGhpcy5yZWNlaXZlZFJlc3AucmVzcG9uc2UpXG4gICAgfVxuXG4gICAgdGhpcy5yZWNIb29rcy5wdXNoKHtzdGF0dXMsIGNhbGxiYWNrfSlcbiAgICByZXR1cm4gdGhpc1xuICB9XG5cbiAgLyoqXG4gICAqIEBwcml2YXRlXG4gICAqL1xuICByZXNldCgpe1xuICAgIHRoaXMuY2FuY2VsUmVmRXZlbnQoKVxuICAgIHRoaXMucmVmID0gbnVsbFxuICAgIHRoaXMucmVmRXZlbnQgPSBudWxsXG4gICAgdGhpcy5yZWNlaXZlZFJlc3AgPSBudWxsXG4gICAgdGhpcy5zZW50ID0gZmFsc2VcbiAgfVxuXG4gIC8qKlxuICAgKiBAcHJpdmF0ZVxuICAgKi9cbiAgbWF0Y2hSZWNlaXZlKHtzdGF0dXMsIHJlc3BvbnNlLCBfcmVmfSl7XG4gICAgdGhpcy5yZWNIb29rcy5maWx0ZXIoaCA9PiBoLnN0YXR1cyA9PT0gc3RhdHVzKVxuICAgICAgLmZvckVhY2goaCA9PiBoLmNhbGxiYWNrKHJlc3BvbnNlKSlcbiAgfVxuXG4gIC8qKlxuICAgKiBAcHJpdmF0ZVxuICAgKi9cbiAgY2FuY2VsUmVmRXZlbnQoKXtcbiAgICBpZighdGhpcy5yZWZFdmVudCl7IHJldHVybiB9XG4gICAgdGhpcy5jaGFubmVsLm9mZih0aGlzLnJlZkV2ZW50KVxuICB9XG5cbiAgLyoqXG4gICAqIEBwcml2YXRlXG4gICAqL1xuICBjYW5jZWxUaW1lb3V0KCl7XG4gICAgY2xlYXJUaW1lb3V0KHRoaXMudGltZW91dFRpbWVyKVxuICAgIHRoaXMudGltZW91dFRpbWVyID0gbnVsbFxuICB9XG5cbiAgLyoqXG4gICAqIEBwcml2YXRlXG4gICAqL1xuICBzdGFydFRpbWVvdXQoKXtcbiAgICBpZih0aGlzLnRpbWVvdXRUaW1lcil7IHRoaXMuY2FuY2VsVGltZW91dCgpIH1cbiAgICB0aGlzLnJlZiA9IHRoaXMuY2hhbm5lbC5zb2NrZXQubWFrZVJlZigpXG4gICAgdGhpcy5yZWZFdmVudCA9IHRoaXMuY2hhbm5lbC5yZXBseUV2ZW50TmFtZSh0aGlzLnJlZilcblxuICAgIHRoaXMuY2hhbm5lbC5vbih0aGlzLnJlZkV2ZW50LCBwYXlsb2FkID0+IHtcbiAgICAgIHRoaXMuY2FuY2VsUmVmRXZlbnQoKVxuICAgICAgdGhpcy5jYW5jZWxUaW1lb3V0KClcbiAgICAgIHRoaXMucmVjZWl2ZWRSZXNwID0gcGF5bG9hZFxuICAgICAgdGhpcy5tYXRjaFJlY2VpdmUocGF5bG9hZClcbiAgICB9KVxuXG4gICAgdGhpcy50aW1lb3V0VGltZXIgPSBzZXRUaW1lb3V0KCgpID0+IHtcbiAgICAgIHRoaXMudHJpZ2dlcihcInRpbWVvdXRcIiwge30pXG4gICAgfSwgdGhpcy50aW1lb3V0KVxuICB9XG5cbiAgLyoqXG4gICAqIEBwcml2YXRlXG4gICAqL1xuICBoYXNSZWNlaXZlZChzdGF0dXMpe1xuICAgIHJldHVybiB0aGlzLnJlY2VpdmVkUmVzcCAmJiB0aGlzLnJlY2VpdmVkUmVzcC5zdGF0dXMgPT09IHN0YXR1c1xuICB9XG5cbiAgLyoqXG4gICAqIEBwcml2YXRlXG4gICAqL1xuICB0cmlnZ2VyKHN0YXR1cywgcmVzcG9uc2Upe1xuICAgIHRoaXMuY2hhbm5lbC50cmlnZ2VyKHRoaXMucmVmRXZlbnQsIHtzdGF0dXMsIHJlc3BvbnNlfSlcbiAgfVxufVxuIiwgIi8qKlxuICpcbiAqIENyZWF0ZXMgYSB0aW1lciB0aGF0IGFjY2VwdHMgYSBgdGltZXJDYWxjYCBmdW5jdGlvbiB0byBwZXJmb3JtXG4gKiBjYWxjdWxhdGVkIHRpbWVvdXQgcmV0cmllcywgc3VjaCBhcyBleHBvbmVudGlhbCBiYWNrb2ZmLlxuICpcbiAqIEBleGFtcGxlXG4gKiBsZXQgcmVjb25uZWN0VGltZXIgPSBuZXcgVGltZXIoKCkgPT4gdGhpcy5jb25uZWN0KCksIGZ1bmN0aW9uKHRyaWVzKXtcbiAqICAgcmV0dXJuIFsxMDAwLCA1MDAwLCAxMDAwMF1bdHJpZXMgLSAxXSB8fCAxMDAwMFxuICogfSlcbiAqIHJlY29ubmVjdFRpbWVyLnNjaGVkdWxlVGltZW91dCgpIC8vIGZpcmVzIGFmdGVyIDEwMDBcbiAqIHJlY29ubmVjdFRpbWVyLnNjaGVkdWxlVGltZW91dCgpIC8vIGZpcmVzIGFmdGVyIDUwMDBcbiAqIHJlY29ubmVjdFRpbWVyLnJlc2V0KClcbiAqIHJlY29ubmVjdFRpbWVyLnNjaGVkdWxlVGltZW91dCgpIC8vIGZpcmVzIGFmdGVyIDEwMDBcbiAqXG4gKiBAcGFyYW0ge0Z1bmN0aW9ufSBjYWxsYmFja1xuICogQHBhcmFtIHtGdW5jdGlvbn0gdGltZXJDYWxjXG4gKi9cbmV4cG9ydCBkZWZhdWx0IGNsYXNzIFRpbWVyIHtcbiAgY29uc3RydWN0b3IoY2FsbGJhY2ssIHRpbWVyQ2FsYyl7XG4gICAgdGhpcy5jYWxsYmFjayA9IGNhbGxiYWNrXG4gICAgdGhpcy50aW1lckNhbGMgPSB0aW1lckNhbGNcbiAgICB0aGlzLnRpbWVyID0gbnVsbFxuICAgIHRoaXMudHJpZXMgPSAwXG4gIH1cblxuICByZXNldCgpe1xuICAgIHRoaXMudHJpZXMgPSAwXG4gICAgY2xlYXJUaW1lb3V0KHRoaXMudGltZXIpXG4gIH1cblxuICAvKipcbiAgICogQ2FuY2VscyBhbnkgcHJldmlvdXMgc2NoZWR1bGVUaW1lb3V0IGFuZCBzY2hlZHVsZXMgY2FsbGJhY2tcbiAgICovXG4gIHNjaGVkdWxlVGltZW91dCgpe1xuICAgIGNsZWFyVGltZW91dCh0aGlzLnRpbWVyKVxuXG4gICAgdGhpcy50aW1lciA9IHNldFRpbWVvdXQoKCkgPT4ge1xuICAgICAgdGhpcy50cmllcyA9IHRoaXMudHJpZXMgKyAxXG4gICAgICB0aGlzLmNhbGxiYWNrKClcbiAgICB9LCB0aGlzLnRpbWVyQ2FsYyh0aGlzLnRyaWVzICsgMSkpXG4gIH1cbn1cbiIsICJpbXBvcnQge2Nsb3N1cmV9IGZyb20gXCIuL3V0aWxzXCJcbmltcG9ydCB7XG4gIENIQU5ORUxfRVZFTlRTLFxuICBDSEFOTkVMX1NUQVRFUyxcbn0gZnJvbSBcIi4vY29uc3RhbnRzXCJcblxuaW1wb3J0IFB1c2ggZnJvbSBcIi4vcHVzaFwiXG5pbXBvcnQgVGltZXIgZnJvbSBcIi4vdGltZXJcIlxuXG4vKipcbiAqXG4gKiBAcGFyYW0ge3N0cmluZ30gdG9waWNcbiAqIEBwYXJhbSB7KE9iamVjdHxmdW5jdGlvbil9IHBhcmFtc1xuICogQHBhcmFtIHtTb2NrZXR9IHNvY2tldFxuICovXG5leHBvcnQgZGVmYXVsdCBjbGFzcyBDaGFubmVsIHtcbiAgY29uc3RydWN0b3IodG9waWMsIHBhcmFtcywgc29ja2V0KXtcbiAgICB0aGlzLnN0YXRlID0gQ0hBTk5FTF9TVEFURVMuY2xvc2VkXG4gICAgdGhpcy50b3BpYyA9IHRvcGljXG4gICAgdGhpcy5wYXJhbXMgPSBjbG9zdXJlKHBhcmFtcyB8fCB7fSlcbiAgICB0aGlzLnNvY2tldCA9IHNvY2tldFxuICAgIHRoaXMuYmluZGluZ3MgPSBbXVxuICAgIHRoaXMuYmluZGluZ1JlZiA9IDBcbiAgICB0aGlzLnRpbWVvdXQgPSB0aGlzLnNvY2tldC50aW1lb3V0XG4gICAgdGhpcy5qb2luZWRPbmNlID0gZmFsc2VcbiAgICB0aGlzLmpvaW5QdXNoID0gbmV3IFB1c2godGhpcywgQ0hBTk5FTF9FVkVOVFMuam9pbiwgdGhpcy5wYXJhbXMsIHRoaXMudGltZW91dClcbiAgICB0aGlzLnB1c2hCdWZmZXIgPSBbXVxuICAgIHRoaXMuc3RhdGVDaGFuZ2VSZWZzID0gW11cblxuICAgIHRoaXMucmVqb2luVGltZXIgPSBuZXcgVGltZXIoKCkgPT4ge1xuICAgICAgaWYodGhpcy5zb2NrZXQuaXNDb25uZWN0ZWQoKSl7IHRoaXMucmVqb2luKCkgfVxuICAgIH0sIHRoaXMuc29ja2V0LnJlam9pbkFmdGVyTXMpXG4gICAgdGhpcy5zdGF0ZUNoYW5nZVJlZnMucHVzaCh0aGlzLnNvY2tldC5vbkVycm9yKCgpID0+IHRoaXMucmVqb2luVGltZXIucmVzZXQoKSkpXG4gICAgdGhpcy5zdGF0ZUNoYW5nZVJlZnMucHVzaCh0aGlzLnNvY2tldC5vbk9wZW4oKCkgPT4ge1xuICAgICAgdGhpcy5yZWpvaW5UaW1lci5yZXNldCgpXG4gICAgICBpZih0aGlzLmlzRXJyb3JlZCgpKXsgdGhpcy5yZWpvaW4oKSB9XG4gICAgfSlcbiAgICApXG4gICAgdGhpcy5qb2luUHVzaC5yZWNlaXZlKFwib2tcIiwgKCkgPT4ge1xuICAgICAgdGhpcy5zdGF0ZSA9IENIQU5ORUxfU1RBVEVTLmpvaW5lZFxuICAgICAgdGhpcy5yZWpvaW5UaW1lci5yZXNldCgpXG4gICAgICB0aGlzLnB1c2hCdWZmZXIuZm9yRWFjaChwdXNoRXZlbnQgPT4gcHVzaEV2ZW50LnNlbmQoKSlcbiAgICAgIHRoaXMucHVzaEJ1ZmZlciA9IFtdXG4gICAgfSlcbiAgICB0aGlzLmpvaW5QdXNoLnJlY2VpdmUoXCJlcnJvclwiLCAoKSA9PiB7XG4gICAgICB0aGlzLnN0YXRlID0gQ0hBTk5FTF9TVEFURVMuZXJyb3JlZFxuICAgICAgaWYodGhpcy5zb2NrZXQuaXNDb25uZWN0ZWQoKSl7IHRoaXMucmVqb2luVGltZXIuc2NoZWR1bGVUaW1lb3V0KCkgfVxuICAgIH0pXG4gICAgdGhpcy5vbkNsb3NlKCgpID0+IHtcbiAgICAgIHRoaXMucmVqb2luVGltZXIucmVzZXQoKVxuICAgICAgaWYodGhpcy5zb2NrZXQuaGFzTG9nZ2VyKCkpIHRoaXMuc29ja2V0LmxvZyhcImNoYW5uZWxcIiwgYGNsb3NlICR7dGhpcy50b3BpY30gJHt0aGlzLmpvaW5SZWYoKX1gKVxuICAgICAgdGhpcy5zdGF0ZSA9IENIQU5ORUxfU1RBVEVTLmNsb3NlZFxuICAgICAgdGhpcy5zb2NrZXQucmVtb3ZlKHRoaXMpXG4gICAgfSlcbiAgICB0aGlzLm9uRXJyb3IocmVhc29uID0+IHtcbiAgICAgIGlmKHRoaXMuc29ja2V0Lmhhc0xvZ2dlcigpKSB0aGlzLnNvY2tldC5sb2coXCJjaGFubmVsXCIsIGBlcnJvciAke3RoaXMudG9waWN9YCwgcmVhc29uKVxuICAgICAgaWYodGhpcy5pc0pvaW5pbmcoKSl7IHRoaXMuam9pblB1c2gucmVzZXQoKSB9XG4gICAgICB0aGlzLnN0YXRlID0gQ0hBTk5FTF9TVEFURVMuZXJyb3JlZFxuICAgICAgaWYodGhpcy5zb2NrZXQuaXNDb25uZWN0ZWQoKSl7IHRoaXMucmVqb2luVGltZXIuc2NoZWR1bGVUaW1lb3V0KCkgfVxuICAgIH0pXG4gICAgdGhpcy5qb2luUHVzaC5yZWNlaXZlKFwidGltZW91dFwiLCAoKSA9PiB7XG4gICAgICBpZih0aGlzLnNvY2tldC5oYXNMb2dnZXIoKSkgdGhpcy5zb2NrZXQubG9nKFwiY2hhbm5lbFwiLCBgdGltZW91dCAke3RoaXMudG9waWN9ICgke3RoaXMuam9pblJlZigpfSlgLCB0aGlzLmpvaW5QdXNoLnRpbWVvdXQpXG4gICAgICBsZXQgbGVhdmVQdXNoID0gbmV3IFB1c2godGhpcywgQ0hBTk5FTF9FVkVOVFMubGVhdmUsIGNsb3N1cmUoe30pLCB0aGlzLnRpbWVvdXQpXG4gICAgICBsZWF2ZVB1c2guc2VuZCgpXG4gICAgICB0aGlzLnN0YXRlID0gQ0hBTk5FTF9TVEFURVMuZXJyb3JlZFxuICAgICAgdGhpcy5qb2luUHVzaC5yZXNldCgpXG4gICAgICBpZih0aGlzLnNvY2tldC5pc0Nvbm5lY3RlZCgpKXsgdGhpcy5yZWpvaW5UaW1lci5zY2hlZHVsZVRpbWVvdXQoKSB9XG4gICAgfSlcbiAgICB0aGlzLm9uKENIQU5ORUxfRVZFTlRTLnJlcGx5LCAocGF5bG9hZCwgcmVmKSA9PiB7XG4gICAgICB0aGlzLnRyaWdnZXIodGhpcy5yZXBseUV2ZW50TmFtZShyZWYpLCBwYXlsb2FkKVxuICAgIH0pXG4gIH1cblxuICAvKipcbiAgICogSm9pbiB0aGUgY2hhbm5lbFxuICAgKiBAcGFyYW0ge2ludGVnZXJ9IHRpbWVvdXRcbiAgICogQHJldHVybnMge1B1c2h9XG4gICAqL1xuICBqb2luKHRpbWVvdXQgPSB0aGlzLnRpbWVvdXQpe1xuICAgIGlmKHRoaXMuam9pbmVkT25jZSl7XG4gICAgICB0aHJvdyBuZXcgRXJyb3IoXCJ0cmllZCB0byBqb2luIG11bHRpcGxlIHRpbWVzLiAnam9pbicgY2FuIG9ubHkgYmUgY2FsbGVkIGEgc2luZ2xlIHRpbWUgcGVyIGNoYW5uZWwgaW5zdGFuY2VcIilcbiAgICB9IGVsc2Uge1xuICAgICAgdGhpcy50aW1lb3V0ID0gdGltZW91dFxuICAgICAgdGhpcy5qb2luZWRPbmNlID0gdHJ1ZVxuICAgICAgdGhpcy5yZWpvaW4oKVxuICAgICAgcmV0dXJuIHRoaXMuam9pblB1c2hcbiAgICB9XG4gIH1cblxuICAvKipcbiAgICogSG9vayBpbnRvIGNoYW5uZWwgY2xvc2VcbiAgICogQHBhcmFtIHtGdW5jdGlvbn0gY2FsbGJhY2tcbiAgICovXG4gIG9uQ2xvc2UoY2FsbGJhY2spe1xuICAgIHRoaXMub24oQ0hBTk5FTF9FVkVOVFMuY2xvc2UsIGNhbGxiYWNrKVxuICB9XG5cbiAgLyoqXG4gICAqIEhvb2sgaW50byBjaGFubmVsIGVycm9yc1xuICAgKiBAcGFyYW0ge0Z1bmN0aW9ufSBjYWxsYmFja1xuICAgKi9cbiAgb25FcnJvcihjYWxsYmFjayl7XG4gICAgcmV0dXJuIHRoaXMub24oQ0hBTk5FTF9FVkVOVFMuZXJyb3IsIHJlYXNvbiA9PiBjYWxsYmFjayhyZWFzb24pKVxuICB9XG5cbiAgLyoqXG4gICAqIFN1YnNjcmliZXMgb24gY2hhbm5lbCBldmVudHNcbiAgICpcbiAgICogU3Vic2NyaXB0aW9uIHJldHVybnMgYSByZWYgY291bnRlciwgd2hpY2ggY2FuIGJlIHVzZWQgbGF0ZXIgdG9cbiAgICogdW5zdWJzY3JpYmUgdGhlIGV4YWN0IGV2ZW50IGxpc3RlbmVyXG4gICAqXG4gICAqIEBleGFtcGxlXG4gICAqIGNvbnN0IHJlZjEgPSBjaGFubmVsLm9uKFwiZXZlbnRcIiwgZG9fc3R1ZmYpXG4gICAqIGNvbnN0IHJlZjIgPSBjaGFubmVsLm9uKFwiZXZlbnRcIiwgZG9fb3RoZXJfc3R1ZmYpXG4gICAqIGNoYW5uZWwub2ZmKFwiZXZlbnRcIiwgcmVmMSlcbiAgICogLy8gU2luY2UgdW5zdWJzY3JpcHRpb24sIGRvX3N0dWZmIHdvbid0IGZpcmUsXG4gICAqIC8vIHdoaWxlIGRvX290aGVyX3N0dWZmIHdpbGwga2VlcCBmaXJpbmcgb24gdGhlIFwiZXZlbnRcIlxuICAgKlxuICAgKiBAcGFyYW0ge3N0cmluZ30gZXZlbnRcbiAgICogQHBhcmFtIHtGdW5jdGlvbn0gY2FsbGJhY2tcbiAgICogQHJldHVybnMge2ludGVnZXJ9IHJlZlxuICAgKi9cbiAgb24oZXZlbnQsIGNhbGxiYWNrKXtcbiAgICBsZXQgcmVmID0gdGhpcy5iaW5kaW5nUmVmKytcbiAgICB0aGlzLmJpbmRpbmdzLnB1c2goe2V2ZW50LCByZWYsIGNhbGxiYWNrfSlcbiAgICByZXR1cm4gcmVmXG4gIH1cblxuICAvKipcbiAgICogVW5zdWJzY3JpYmVzIG9mZiBvZiBjaGFubmVsIGV2ZW50c1xuICAgKlxuICAgKiBVc2UgdGhlIHJlZiByZXR1cm5lZCBmcm9tIGEgY2hhbm5lbC5vbigpIHRvIHVuc3Vic2NyaWJlIG9uZVxuICAgKiBoYW5kbGVyLCBvciBwYXNzIG5vdGhpbmcgZm9yIHRoZSByZWYgdG8gdW5zdWJzY3JpYmUgYWxsXG4gICAqIGhhbmRsZXJzIGZvciB0aGUgZ2l2ZW4gZXZlbnQuXG4gICAqXG4gICAqIEBleGFtcGxlXG4gICAqIC8vIFVuc3Vic2NyaWJlIHRoZSBkb19zdHVmZiBoYW5kbGVyXG4gICAqIGNvbnN0IHJlZjEgPSBjaGFubmVsLm9uKFwiZXZlbnRcIiwgZG9fc3R1ZmYpXG4gICAqIGNoYW5uZWwub2ZmKFwiZXZlbnRcIiwgcmVmMSlcbiAgICpcbiAgICogLy8gVW5zdWJzY3JpYmUgYWxsIGhhbmRsZXJzIGZyb20gZXZlbnRcbiAgICogY2hhbm5lbC5vZmYoXCJldmVudFwiKVxuICAgKlxuICAgKiBAcGFyYW0ge3N0cmluZ30gZXZlbnRcbiAgICogQHBhcmFtIHtpbnRlZ2VyfSByZWZcbiAgICovXG4gIG9mZihldmVudCwgcmVmKXtcbiAgICB0aGlzLmJpbmRpbmdzID0gdGhpcy5iaW5kaW5ncy5maWx0ZXIoKGJpbmQpID0+IHtcbiAgICAgIHJldHVybiAhKGJpbmQuZXZlbnQgPT09IGV2ZW50ICYmICh0eXBlb2YgcmVmID09PSBcInVuZGVmaW5lZFwiIHx8IHJlZiA9PT0gYmluZC5yZWYpKVxuICAgIH0pXG4gIH1cblxuICAvKipcbiAgICogQHByaXZhdGVcbiAgICovXG4gIGNhblB1c2goKXsgcmV0dXJuIHRoaXMuc29ja2V0LmlzQ29ubmVjdGVkKCkgJiYgdGhpcy5pc0pvaW5lZCgpIH1cblxuICAvKipcbiAgICogU2VuZHMgYSBtZXNzYWdlIGBldmVudGAgdG8gcGhvZW5peCB3aXRoIHRoZSBwYXlsb2FkIGBwYXlsb2FkYC5cbiAgICogUGhvZW5peCByZWNlaXZlcyB0aGlzIGluIHRoZSBgaGFuZGxlX2luKGV2ZW50LCBwYXlsb2FkLCBzb2NrZXQpYFxuICAgKiBmdW5jdGlvbi4gaWYgcGhvZW5peCByZXBsaWVzIG9yIGl0IHRpbWVzIG91dCAoZGVmYXVsdCAxMDAwMG1zKSxcbiAgICogdGhlbiBvcHRpb25hbGx5IHRoZSByZXBseSBjYW4gYmUgcmVjZWl2ZWQuXG4gICAqXG4gICAqIEBleGFtcGxlXG4gICAqIGNoYW5uZWwucHVzaChcImV2ZW50XCIpXG4gICAqICAgLnJlY2VpdmUoXCJva1wiLCBwYXlsb2FkID0+IGNvbnNvbGUubG9nKFwicGhvZW5peCByZXBsaWVkOlwiLCBwYXlsb2FkKSlcbiAgICogICAucmVjZWl2ZShcImVycm9yXCIsIGVyciA9PiBjb25zb2xlLmxvZyhcInBob2VuaXggZXJyb3JlZFwiLCBlcnIpKVxuICAgKiAgIC5yZWNlaXZlKFwidGltZW91dFwiLCAoKSA9PiBjb25zb2xlLmxvZyhcInRpbWVkIG91dCBwdXNoaW5nXCIpKVxuICAgKiBAcGFyYW0ge3N0cmluZ30gZXZlbnRcbiAgICogQHBhcmFtIHtPYmplY3R9IHBheWxvYWRcbiAgICogQHBhcmFtIHtudW1iZXJ9IFt0aW1lb3V0XVxuICAgKiBAcmV0dXJucyB7UHVzaH1cbiAgICovXG4gIHB1c2goZXZlbnQsIHBheWxvYWQsIHRpbWVvdXQgPSB0aGlzLnRpbWVvdXQpe1xuICAgIHBheWxvYWQgPSBwYXlsb2FkIHx8IHt9XG4gICAgaWYoIXRoaXMuam9pbmVkT25jZSl7XG4gICAgICB0aHJvdyBuZXcgRXJyb3IoYHRyaWVkIHRvIHB1c2ggJyR7ZXZlbnR9JyB0byAnJHt0aGlzLnRvcGljfScgYmVmb3JlIGpvaW5pbmcuIFVzZSBjaGFubmVsLmpvaW4oKSBiZWZvcmUgcHVzaGluZyBldmVudHNgKVxuICAgIH1cbiAgICBsZXQgcHVzaEV2ZW50ID0gbmV3IFB1c2godGhpcywgZXZlbnQsIGZ1bmN0aW9uICgpeyByZXR1cm4gcGF5bG9hZCB9LCB0aW1lb3V0KVxuICAgIGlmKHRoaXMuY2FuUHVzaCgpKXtcbiAgICAgIHB1c2hFdmVudC5zZW5kKClcbiAgICB9IGVsc2Uge1xuICAgICAgcHVzaEV2ZW50LnN0YXJ0VGltZW91dCgpXG4gICAgICB0aGlzLnB1c2hCdWZmZXIucHVzaChwdXNoRXZlbnQpXG4gICAgfVxuXG4gICAgcmV0dXJuIHB1c2hFdmVudFxuICB9XG5cbiAgLyoqIExlYXZlcyB0aGUgY2hhbm5lbFxuICAgKlxuICAgKiBVbnN1YnNjcmliZXMgZnJvbSBzZXJ2ZXIgZXZlbnRzLCBhbmRcbiAgICogaW5zdHJ1Y3RzIGNoYW5uZWwgdG8gdGVybWluYXRlIG9uIHNlcnZlclxuICAgKlxuICAgKiBUcmlnZ2VycyBvbkNsb3NlKCkgaG9va3NcbiAgICpcbiAgICogVG8gcmVjZWl2ZSBsZWF2ZSBhY2tub3dsZWRnZW1lbnRzLCB1c2UgdGhlIGByZWNlaXZlYFxuICAgKiBob29rIHRvIGJpbmQgdG8gdGhlIHNlcnZlciBhY2ssIGllOlxuICAgKlxuICAgKiBAZXhhbXBsZVxuICAgKiBjaGFubmVsLmxlYXZlKCkucmVjZWl2ZShcIm9rXCIsICgpID0+IGFsZXJ0KFwibGVmdCFcIikgKVxuICAgKlxuICAgKiBAcGFyYW0ge2ludGVnZXJ9IHRpbWVvdXRcbiAgICogQHJldHVybnMge1B1c2h9XG4gICAqL1xuICBsZWF2ZSh0aW1lb3V0ID0gdGhpcy50aW1lb3V0KXtcbiAgICB0aGlzLnJlam9pblRpbWVyLnJlc2V0KClcbiAgICB0aGlzLmpvaW5QdXNoLmNhbmNlbFRpbWVvdXQoKVxuXG4gICAgdGhpcy5zdGF0ZSA9IENIQU5ORUxfU1RBVEVTLmxlYXZpbmdcbiAgICBsZXQgb25DbG9zZSA9ICgpID0+IHtcbiAgICAgIGlmKHRoaXMuc29ja2V0Lmhhc0xvZ2dlcigpKSB0aGlzLnNvY2tldC5sb2coXCJjaGFubmVsXCIsIGBsZWF2ZSAke3RoaXMudG9waWN9YClcbiAgICAgIHRoaXMudHJpZ2dlcihDSEFOTkVMX0VWRU5UUy5jbG9zZSwgXCJsZWF2ZVwiKVxuICAgIH1cbiAgICBsZXQgbGVhdmVQdXNoID0gbmV3IFB1c2godGhpcywgQ0hBTk5FTF9FVkVOVFMubGVhdmUsIGNsb3N1cmUoe30pLCB0aW1lb3V0KVxuICAgIGxlYXZlUHVzaC5yZWNlaXZlKFwib2tcIiwgKCkgPT4gb25DbG9zZSgpKVxuICAgICAgLnJlY2VpdmUoXCJ0aW1lb3V0XCIsICgpID0+IG9uQ2xvc2UoKSlcbiAgICBsZWF2ZVB1c2guc2VuZCgpXG4gICAgaWYoIXRoaXMuY2FuUHVzaCgpKXsgbGVhdmVQdXNoLnRyaWdnZXIoXCJva1wiLCB7fSkgfVxuXG4gICAgcmV0dXJuIGxlYXZlUHVzaFxuICB9XG5cbiAgLyoqXG4gICAqIE92ZXJyaWRhYmxlIG1lc3NhZ2UgaG9va1xuICAgKlxuICAgKiBSZWNlaXZlcyBhbGwgZXZlbnRzIGZvciBzcGVjaWFsaXplZCBtZXNzYWdlIGhhbmRsaW5nXG4gICAqIGJlZm9yZSBkaXNwYXRjaGluZyB0byB0aGUgY2hhbm5lbCBjYWxsYmFja3MuXG4gICAqXG4gICAqIE11c3QgcmV0dXJuIHRoZSBwYXlsb2FkLCBtb2RpZmllZCBvciB1bm1vZGlmaWVkXG4gICAqIEBwYXJhbSB7c3RyaW5nfSBldmVudFxuICAgKiBAcGFyYW0ge09iamVjdH0gcGF5bG9hZFxuICAgKiBAcGFyYW0ge2ludGVnZXJ9IHJlZlxuICAgKiBAcmV0dXJucyB7T2JqZWN0fVxuICAgKi9cbiAgb25NZXNzYWdlKF9ldmVudCwgcGF5bG9hZCwgX3JlZil7IHJldHVybiBwYXlsb2FkIH1cblxuICAvKipcbiAgICogQHByaXZhdGVcbiAgICovXG4gIGlzTWVtYmVyKHRvcGljLCBldmVudCwgcGF5bG9hZCwgam9pblJlZil7XG4gICAgaWYodGhpcy50b3BpYyAhPT0gdG9waWMpeyByZXR1cm4gZmFsc2UgfVxuXG4gICAgaWYoam9pblJlZiAmJiBqb2luUmVmICE9PSB0aGlzLmpvaW5SZWYoKSl7XG4gICAgICBpZih0aGlzLnNvY2tldC5oYXNMb2dnZXIoKSkgdGhpcy5zb2NrZXQubG9nKFwiY2hhbm5lbFwiLCBcImRyb3BwaW5nIG91dGRhdGVkIG1lc3NhZ2VcIiwge3RvcGljLCBldmVudCwgcGF5bG9hZCwgam9pblJlZn0pXG4gICAgICByZXR1cm4gZmFsc2VcbiAgICB9IGVsc2Uge1xuICAgICAgcmV0dXJuIHRydWVcbiAgICB9XG4gIH1cblxuICAvKipcbiAgICogQHByaXZhdGVcbiAgICovXG4gIGpvaW5SZWYoKXsgcmV0dXJuIHRoaXMuam9pblB1c2gucmVmIH1cblxuICAvKipcbiAgICogQHByaXZhdGVcbiAgICovXG4gIHJlam9pbih0aW1lb3V0ID0gdGhpcy50aW1lb3V0KXtcbiAgICBpZih0aGlzLmlzTGVhdmluZygpKXsgcmV0dXJuIH1cbiAgICB0aGlzLnNvY2tldC5sZWF2ZU9wZW5Ub3BpYyh0aGlzLnRvcGljKVxuICAgIHRoaXMuc3RhdGUgPSBDSEFOTkVMX1NUQVRFUy5qb2luaW5nXG4gICAgdGhpcy5qb2luUHVzaC5yZXNlbmQodGltZW91dClcbiAgfVxuXG4gIC8qKlxuICAgKiBAcHJpdmF0ZVxuICAgKi9cbiAgdHJpZ2dlcihldmVudCwgcGF5bG9hZCwgcmVmLCBqb2luUmVmKXtcbiAgICBsZXQgaGFuZGxlZFBheWxvYWQgPSB0aGlzLm9uTWVzc2FnZShldmVudCwgcGF5bG9hZCwgcmVmLCBqb2luUmVmKVxuICAgIGlmKHBheWxvYWQgJiYgIWhhbmRsZWRQYXlsb2FkKXsgdGhyb3cgbmV3IEVycm9yKFwiY2hhbm5lbCBvbk1lc3NhZ2UgY2FsbGJhY2tzIG11c3QgcmV0dXJuIHRoZSBwYXlsb2FkLCBtb2RpZmllZCBvciB1bm1vZGlmaWVkXCIpIH1cblxuICAgIGxldCBldmVudEJpbmRpbmdzID0gdGhpcy5iaW5kaW5ncy5maWx0ZXIoYmluZCA9PiBiaW5kLmV2ZW50ID09PSBldmVudClcblxuICAgIGZvcihsZXQgaSA9IDA7IGkgPCBldmVudEJpbmRpbmdzLmxlbmd0aDsgaSsrKXtcbiAgICAgIGxldCBiaW5kID0gZXZlbnRCaW5kaW5nc1tpXVxuICAgICAgYmluZC5jYWxsYmFjayhoYW5kbGVkUGF5bG9hZCwgcmVmLCBqb2luUmVmIHx8IHRoaXMuam9pblJlZigpKVxuICAgIH1cbiAgfVxuXG4gIC8qKlxuICAgKiBAcHJpdmF0ZVxuICAgKi9cbiAgcmVwbHlFdmVudE5hbWUocmVmKXsgcmV0dXJuIGBjaGFuX3JlcGx5XyR7cmVmfWAgfVxuXG4gIC8qKlxuICAgKiBAcHJpdmF0ZVxuICAgKi9cbiAgaXNDbG9zZWQoKXsgcmV0dXJuIHRoaXMuc3RhdGUgPT09IENIQU5ORUxfU1RBVEVTLmNsb3NlZCB9XG5cbiAgLyoqXG4gICAqIEBwcml2YXRlXG4gICAqL1xuICBpc0Vycm9yZWQoKXsgcmV0dXJuIHRoaXMuc3RhdGUgPT09IENIQU5ORUxfU1RBVEVTLmVycm9yZWQgfVxuXG4gIC8qKlxuICAgKiBAcHJpdmF0ZVxuICAgKi9cbiAgaXNKb2luZWQoKXsgcmV0dXJuIHRoaXMuc3RhdGUgPT09IENIQU5ORUxfU1RBVEVTLmpvaW5lZCB9XG5cbiAgLyoqXG4gICAqIEBwcml2YXRlXG4gICAqL1xuICBpc0pvaW5pbmcoKXsgcmV0dXJuIHRoaXMuc3RhdGUgPT09IENIQU5ORUxfU1RBVEVTLmpvaW5pbmcgfVxuXG4gIC8qKlxuICAgKiBAcHJpdmF0ZVxuICAgKi9cbiAgaXNMZWF2aW5nKCl7IHJldHVybiB0aGlzLnN0YXRlID09PSBDSEFOTkVMX1NUQVRFUy5sZWF2aW5nIH1cbn1cbiIsICJpbXBvcnQge1xuICBnbG9iYWwsXG4gIFhIUl9TVEFURVNcbn0gZnJvbSBcIi4vY29uc3RhbnRzXCJcblxuZXhwb3J0IGRlZmF1bHQgY2xhc3MgQWpheCB7XG5cbiAgc3RhdGljIHJlcXVlc3QobWV0aG9kLCBlbmRQb2ludCwgYWNjZXB0LCBib2R5LCB0aW1lb3V0LCBvbnRpbWVvdXQsIGNhbGxiYWNrKXtcbiAgICBpZihnbG9iYWwuWERvbWFpblJlcXVlc3Qpe1xuICAgICAgbGV0IHJlcSA9IG5ldyBnbG9iYWwuWERvbWFpblJlcXVlc3QoKSAvLyBJRTgsIElFOVxuICAgICAgcmV0dXJuIHRoaXMueGRvbWFpblJlcXVlc3QocmVxLCBtZXRob2QsIGVuZFBvaW50LCBib2R5LCB0aW1lb3V0LCBvbnRpbWVvdXQsIGNhbGxiYWNrKVxuICAgIH0gZWxzZSB7XG4gICAgICBsZXQgcmVxID0gbmV3IGdsb2JhbC5YTUxIdHRwUmVxdWVzdCgpIC8vIElFNyssIEZpcmVmb3gsIENocm9tZSwgT3BlcmEsIFNhZmFyaVxuICAgICAgcmV0dXJuIHRoaXMueGhyUmVxdWVzdChyZXEsIG1ldGhvZCwgZW5kUG9pbnQsIGFjY2VwdCwgYm9keSwgdGltZW91dCwgb250aW1lb3V0LCBjYWxsYmFjaylcbiAgICB9XG4gIH1cblxuICBzdGF0aWMgeGRvbWFpblJlcXVlc3QocmVxLCBtZXRob2QsIGVuZFBvaW50LCBib2R5LCB0aW1lb3V0LCBvbnRpbWVvdXQsIGNhbGxiYWNrKXtcbiAgICByZXEudGltZW91dCA9IHRpbWVvdXRcbiAgICByZXEub3BlbihtZXRob2QsIGVuZFBvaW50KVxuICAgIHJlcS5vbmxvYWQgPSAoKSA9PiB7XG4gICAgICBsZXQgcmVzcG9uc2UgPSB0aGlzLnBhcnNlSlNPTihyZXEucmVzcG9uc2VUZXh0KVxuICAgICAgY2FsbGJhY2sgJiYgY2FsbGJhY2socmVzcG9uc2UpXG4gICAgfVxuICAgIGlmKG9udGltZW91dCl7IHJlcS5vbnRpbWVvdXQgPSBvbnRpbWVvdXQgfVxuXG4gICAgLy8gV29yayBhcm91bmQgYnVnIGluIElFOSB0aGF0IHJlcXVpcmVzIGFuIGF0dGFjaGVkIG9ucHJvZ3Jlc3MgaGFuZGxlclxuICAgIHJlcS5vbnByb2dyZXNzID0gKCkgPT4geyB9XG5cbiAgICByZXEuc2VuZChib2R5KVxuICAgIHJldHVybiByZXFcbiAgfVxuXG4gIHN0YXRpYyB4aHJSZXF1ZXN0KHJlcSwgbWV0aG9kLCBlbmRQb2ludCwgYWNjZXB0LCBib2R5LCB0aW1lb3V0LCBvbnRpbWVvdXQsIGNhbGxiYWNrKXtcbiAgICByZXEub3BlbihtZXRob2QsIGVuZFBvaW50LCB0cnVlKVxuICAgIHJlcS50aW1lb3V0ID0gdGltZW91dFxuICAgIHJlcS5zZXRSZXF1ZXN0SGVhZGVyKFwiQ29udGVudC1UeXBlXCIsIGFjY2VwdClcbiAgICByZXEub25lcnJvciA9ICgpID0+IGNhbGxiYWNrICYmIGNhbGxiYWNrKG51bGwpXG4gICAgcmVxLm9ucmVhZHlzdGF0ZWNoYW5nZSA9ICgpID0+IHtcbiAgICAgIGlmKHJlcS5yZWFkeVN0YXRlID09PSBYSFJfU1RBVEVTLmNvbXBsZXRlICYmIGNhbGxiYWNrKXtcbiAgICAgICAgbGV0IHJlc3BvbnNlID0gdGhpcy5wYXJzZUpTT04ocmVxLnJlc3BvbnNlVGV4dClcbiAgICAgICAgY2FsbGJhY2socmVzcG9uc2UpXG4gICAgICB9XG4gICAgfVxuICAgIGlmKG9udGltZW91dCl7IHJlcS5vbnRpbWVvdXQgPSBvbnRpbWVvdXQgfVxuXG4gICAgcmVxLnNlbmQoYm9keSlcbiAgICByZXR1cm4gcmVxXG4gIH1cblxuICBzdGF0aWMgcGFyc2VKU09OKHJlc3Ape1xuICAgIGlmKCFyZXNwIHx8IHJlc3AgPT09IFwiXCIpeyByZXR1cm4gbnVsbCB9XG5cbiAgICB0cnkge1xuICAgICAgcmV0dXJuIEpTT04ucGFyc2UocmVzcClcbiAgICB9IGNhdGNoIChlKXtcbiAgICAgIGNvbnNvbGUgJiYgY29uc29sZS5sb2coXCJmYWlsZWQgdG8gcGFyc2UgSlNPTiByZXNwb25zZVwiLCByZXNwKVxuICAgICAgcmV0dXJuIG51bGxcbiAgICB9XG4gIH1cblxuICBzdGF0aWMgc2VyaWFsaXplKG9iaiwgcGFyZW50S2V5KXtcbiAgICBsZXQgcXVlcnlTdHIgPSBbXVxuICAgIGZvcih2YXIga2V5IGluIG9iail7XG4gICAgICBpZighT2JqZWN0LnByb3RvdHlwZS5oYXNPd25Qcm9wZXJ0eS5jYWxsKG9iaiwga2V5KSl7IGNvbnRpbnVlIH1cbiAgICAgIGxldCBwYXJhbUtleSA9IHBhcmVudEtleSA/IGAke3BhcmVudEtleX1bJHtrZXl9XWAgOiBrZXlcbiAgICAgIGxldCBwYXJhbVZhbCA9IG9ialtrZXldXG4gICAgICBpZih0eXBlb2YgcGFyYW1WYWwgPT09IFwib2JqZWN0XCIpe1xuICAgICAgICBxdWVyeVN0ci5wdXNoKHRoaXMuc2VyaWFsaXplKHBhcmFtVmFsLCBwYXJhbUtleSkpXG4gICAgICB9IGVsc2Uge1xuICAgICAgICBxdWVyeVN0ci5wdXNoKGVuY29kZVVSSUNvbXBvbmVudChwYXJhbUtleSkgKyBcIj1cIiArIGVuY29kZVVSSUNvbXBvbmVudChwYXJhbVZhbCkpXG4gICAgICB9XG4gICAgfVxuICAgIHJldHVybiBxdWVyeVN0ci5qb2luKFwiJlwiKVxuICB9XG5cbiAgc3RhdGljIGFwcGVuZFBhcmFtcyh1cmwsIHBhcmFtcyl7XG4gICAgaWYoT2JqZWN0LmtleXMocGFyYW1zKS5sZW5ndGggPT09IDApeyByZXR1cm4gdXJsIH1cblxuICAgIGxldCBwcmVmaXggPSB1cmwubWF0Y2goL1xcPy8pID8gXCImXCIgOiBcIj9cIlxuICAgIHJldHVybiBgJHt1cmx9JHtwcmVmaXh9JHt0aGlzLnNlcmlhbGl6ZShwYXJhbXMpfWBcbiAgfVxufVxuIiwgImltcG9ydCB7XG4gIFNPQ0tFVF9TVEFURVMsXG4gIFRSQU5TUE9SVFNcbn0gZnJvbSBcIi4vY29uc3RhbnRzXCJcblxuaW1wb3J0IEFqYXggZnJvbSBcIi4vYWpheFwiXG5cbmxldCBhcnJheUJ1ZmZlclRvQmFzZTY0ID0gKGJ1ZmZlcikgPT4ge1xuICBsZXQgYmluYXJ5ID0gXCJcIlxuICBsZXQgYnl0ZXMgPSBuZXcgVWludDhBcnJheShidWZmZXIpXG4gIGxldCBsZW4gPSBieXRlcy5ieXRlTGVuZ3RoXG4gIGZvcihsZXQgaSA9IDA7IGkgPCBsZW47IGkrKyl7IGJpbmFyeSArPSBTdHJpbmcuZnJvbUNoYXJDb2RlKGJ5dGVzW2ldKSB9XG4gIHJldHVybiBidG9hKGJpbmFyeSlcbn1cblxuZXhwb3J0IGRlZmF1bHQgY2xhc3MgTG9uZ1BvbGwge1xuXG4gIGNvbnN0cnVjdG9yKGVuZFBvaW50KXtcbiAgICB0aGlzLmVuZFBvaW50ID0gbnVsbFxuICAgIHRoaXMudG9rZW4gPSBudWxsXG4gICAgdGhpcy5za2lwSGVhcnRiZWF0ID0gdHJ1ZVxuICAgIHRoaXMucmVxcyA9IG5ldyBTZXQoKVxuICAgIHRoaXMuYXdhaXRpbmdCYXRjaEFjayA9IGZhbHNlXG4gICAgdGhpcy5jdXJyZW50QmF0Y2ggPSBudWxsXG4gICAgdGhpcy5jdXJyZW50QmF0Y2hUaW1lciA9IG51bGxcbiAgICB0aGlzLmJhdGNoQnVmZmVyID0gW11cbiAgICB0aGlzLm9ub3BlbiA9IGZ1bmN0aW9uICgpeyB9IC8vIG5vb3BcbiAgICB0aGlzLm9uZXJyb3IgPSBmdW5jdGlvbiAoKXsgfSAvLyBub29wXG4gICAgdGhpcy5vbm1lc3NhZ2UgPSBmdW5jdGlvbiAoKXsgfSAvLyBub29wXG4gICAgdGhpcy5vbmNsb3NlID0gZnVuY3Rpb24gKCl7IH0gLy8gbm9vcFxuICAgIHRoaXMucG9sbEVuZHBvaW50ID0gdGhpcy5ub3JtYWxpemVFbmRwb2ludChlbmRQb2ludClcbiAgICB0aGlzLnJlYWR5U3RhdGUgPSBTT0NLRVRfU1RBVEVTLmNvbm5lY3RpbmdcbiAgICAvLyB3ZSBtdXN0IHdhaXQgZm9yIHRoZSBjYWxsZXIgdG8gZmluaXNoIHNldHRpbmcgdXAgb3VyIGNhbGxiYWNrcyBhbmQgdGltZW91dCBwcm9wZXJ0aWVzXG4gICAgc2V0VGltZW91dCgoKSA9PiB0aGlzLnBvbGwoKSwgMClcbiAgfVxuXG4gIG5vcm1hbGl6ZUVuZHBvaW50KGVuZFBvaW50KXtcbiAgICByZXR1cm4gKGVuZFBvaW50XG4gICAgICAucmVwbGFjZShcIndzOi8vXCIsIFwiaHR0cDovL1wiKVxuICAgICAgLnJlcGxhY2UoXCJ3c3M6Ly9cIiwgXCJodHRwczovL1wiKVxuICAgICAgLnJlcGxhY2UobmV3IFJlZ0V4cChcIiguKilcXC9cIiArIFRSQU5TUE9SVFMud2Vic29ja2V0KSwgXCIkMS9cIiArIFRSQU5TUE9SVFMubG9uZ3BvbGwpKVxuICB9XG5cbiAgZW5kcG9pbnRVUkwoKXtcbiAgICByZXR1cm4gQWpheC5hcHBlbmRQYXJhbXModGhpcy5wb2xsRW5kcG9pbnQsIHt0b2tlbjogdGhpcy50b2tlbn0pXG4gIH1cblxuICBjbG9zZUFuZFJldHJ5KGNvZGUsIHJlYXNvbiwgd2FzQ2xlYW4pe1xuICAgIHRoaXMuY2xvc2UoY29kZSwgcmVhc29uLCB3YXNDbGVhbilcbiAgICB0aGlzLnJlYWR5U3RhdGUgPSBTT0NLRVRfU1RBVEVTLmNvbm5lY3RpbmdcbiAgfVxuXG4gIG9udGltZW91dCgpe1xuICAgIHRoaXMub25lcnJvcihcInRpbWVvdXRcIilcbiAgICB0aGlzLmNsb3NlQW5kUmV0cnkoMTAwNSwgXCJ0aW1lb3V0XCIsIGZhbHNlKVxuICB9XG5cbiAgaXNBY3RpdmUoKXsgcmV0dXJuIHRoaXMucmVhZHlTdGF0ZSA9PT0gU09DS0VUX1NUQVRFUy5vcGVuIHx8IHRoaXMucmVhZHlTdGF0ZSA9PT0gU09DS0VUX1NUQVRFUy5jb25uZWN0aW5nIH1cblxuICBwb2xsKCl7XG4gICAgdGhpcy5hamF4KFwiR0VUXCIsIFwiYXBwbGljYXRpb24vanNvblwiLCBudWxsLCAoKSA9PiB0aGlzLm9udGltZW91dCgpLCByZXNwID0+IHtcbiAgICAgIGlmKHJlc3Ape1xuICAgICAgICB2YXIge3N0YXR1cywgdG9rZW4sIG1lc3NhZ2VzfSA9IHJlc3BcbiAgICAgICAgdGhpcy50b2tlbiA9IHRva2VuXG4gICAgICB9IGVsc2Uge1xuICAgICAgICBzdGF0dXMgPSAwXG4gICAgICB9XG5cbiAgICAgIHN3aXRjaChzdGF0dXMpe1xuICAgICAgICBjYXNlIDIwMDpcbiAgICAgICAgICBtZXNzYWdlcy5mb3JFYWNoKG1zZyA9PiB7XG4gICAgICAgICAgICAvLyBUYXNrcyBhcmUgd2hhdCB0aGluZ3MgbGlrZSBldmVudCBoYW5kbGVycywgc2V0VGltZW91dCBjYWxsYmFja3MsXG4gICAgICAgICAgICAvLyBwcm9taXNlIHJlc29sdmVzIGFuZCBtb3JlIGFyZSBydW4gd2l0aGluLlxuICAgICAgICAgICAgLy8gSW4gbW9kZXJuIGJyb3dzZXJzLCB0aGVyZSBhcmUgdHdvIGRpZmZlcmVudCBraW5kcyBvZiB0YXNrcyxcbiAgICAgICAgICAgIC8vIG1pY3JvdGFza3MgYW5kIG1hY3JvdGFza3MuXG4gICAgICAgICAgICAvLyBNaWNyb3Rhc2tzIGFyZSBtYWlubHkgdXNlZCBmb3IgUHJvbWlzZXMsIHdoaWxlIG1hY3JvdGFza3MgYXJlXG4gICAgICAgICAgICAvLyB1c2VkIGZvciBldmVyeXRoaW5nIGVsc2UuXG4gICAgICAgICAgICAvLyBNaWNyb3Rhc2tzIGFsd2F5cyBoYXZlIHByaW9yaXR5IG92ZXIgbWFjcm90YXNrcy4gSWYgdGhlIEpTIGVuZ2luZVxuICAgICAgICAgICAgLy8gaXMgbG9va2luZyBmb3IgYSB0YXNrIHRvIHJ1biwgaXQgd2lsbCBhbHdheXMgdHJ5IHRvIGVtcHR5IHRoZVxuICAgICAgICAgICAgLy8gbWljcm90YXNrIHF1ZXVlIGJlZm9yZSBhdHRlbXB0aW5nIHRvIHJ1biBhbnl0aGluZyBmcm9tIHRoZVxuICAgICAgICAgICAgLy8gbWFjcm90YXNrIHF1ZXVlLlxuICAgICAgICAgICAgLy9cbiAgICAgICAgICAgIC8vIEZvciB0aGUgV2ViU29ja2V0IHRyYW5zcG9ydCwgbWVzc2FnZXMgYWx3YXlzIGFycml2ZSBpbiB0aGVpciBvd25cbiAgICAgICAgICAgIC8vIGV2ZW50LiBUaGlzIG1lYW5zIHRoYXQgaWYgYW55IHByb21pc2VzIGFyZSByZXNvbHZlZCBmcm9tIHdpdGhpbixcbiAgICAgICAgICAgIC8vIHRoZWlyIGNhbGxiYWNrcyB3aWxsIGFsd2F5cyBmaW5pc2ggZXhlY3V0aW9uIGJ5IHRoZSB0aW1lIHRoZVxuICAgICAgICAgICAgLy8gbmV4dCBtZXNzYWdlIGV2ZW50IGhhbmRsZXIgaXMgcnVuLlxuICAgICAgICAgICAgLy9cbiAgICAgICAgICAgIC8vIEluIG9yZGVyIHRvIGVtdWxhdGUgdGhpcyBiZWhhdmlvdXIsIHdlIG5lZWQgdG8gbWFrZSBzdXJlIGVhY2hcbiAgICAgICAgICAgIC8vIG9ubWVzc2FnZSBoYW5kbGVyIGlzIHJ1biB3aXRoaW4gaXRzIG93biBtYWNyb3Rhc2suXG4gICAgICAgICAgICBzZXRUaW1lb3V0KCgpID0+IHRoaXMub25tZXNzYWdlKHtkYXRhOiBtc2d9KSwgMClcbiAgICAgICAgICB9KVxuICAgICAgICAgIHRoaXMucG9sbCgpXG4gICAgICAgICAgYnJlYWtcbiAgICAgICAgY2FzZSAyMDQ6XG4gICAgICAgICAgdGhpcy5wb2xsKClcbiAgICAgICAgICBicmVha1xuICAgICAgICBjYXNlIDQxMDpcbiAgICAgICAgICB0aGlzLnJlYWR5U3RhdGUgPSBTT0NLRVRfU1RBVEVTLm9wZW5cbiAgICAgICAgICB0aGlzLm9ub3Blbih7fSlcbiAgICAgICAgICB0aGlzLnBvbGwoKVxuICAgICAgICAgIGJyZWFrXG4gICAgICAgIGNhc2UgNDAzOlxuICAgICAgICAgIHRoaXMub25lcnJvcig0MDMpXG4gICAgICAgICAgdGhpcy5jbG9zZSgxMDA4LCBcImZvcmJpZGRlblwiLCBmYWxzZSlcbiAgICAgICAgICBicmVha1xuICAgICAgICBjYXNlIDA6XG4gICAgICAgIGNhc2UgNTAwOlxuICAgICAgICAgIHRoaXMub25lcnJvcig1MDApXG4gICAgICAgICAgdGhpcy5jbG9zZUFuZFJldHJ5KDEwMTEsIFwiaW50ZXJuYWwgc2VydmVyIGVycm9yXCIsIDUwMClcbiAgICAgICAgICBicmVha1xuICAgICAgICBkZWZhdWx0OiB0aHJvdyBuZXcgRXJyb3IoYHVuaGFuZGxlZCBwb2xsIHN0YXR1cyAke3N0YXR1c31gKVxuICAgICAgfVxuICAgIH0pXG4gIH1cblxuICAvLyB3ZSBjb2xsZWN0IGFsbCBwdXNoZXMgd2l0aGluIHRoZSBjdXJyZW50IGV2ZW50IGxvb3AgYnlcbiAgLy8gc2V0VGltZW91dCAwLCB3aGljaCBvcHRpbWl6ZXMgYmFjay10by1iYWNrIHByb2NlZHVyYWxcbiAgLy8gcHVzaGVzIGFnYWluc3QgYW4gZW1wdHkgYnVmZmVyXG5cbiAgc2VuZChib2R5KXtcbiAgICBpZih0eXBlb2YoYm9keSkgIT09IFwic3RyaW5nXCIpeyBib2R5ID0gYXJyYXlCdWZmZXJUb0Jhc2U2NChib2R5KSB9XG4gICAgaWYodGhpcy5jdXJyZW50QmF0Y2gpe1xuICAgICAgdGhpcy5jdXJyZW50QmF0Y2gucHVzaChib2R5KVxuICAgIH0gZWxzZSBpZih0aGlzLmF3YWl0aW5nQmF0Y2hBY2spe1xuICAgICAgdGhpcy5iYXRjaEJ1ZmZlci5wdXNoKGJvZHkpXG4gICAgfSBlbHNlIHtcbiAgICAgIHRoaXMuY3VycmVudEJhdGNoID0gW2JvZHldXG4gICAgICB0aGlzLmN1cnJlbnRCYXRjaFRpbWVyID0gc2V0VGltZW91dCgoKSA9PiB7XG4gICAgICAgIHRoaXMuYmF0Y2hTZW5kKHRoaXMuY3VycmVudEJhdGNoKVxuICAgICAgICB0aGlzLmN1cnJlbnRCYXRjaCA9IG51bGxcbiAgICAgIH0sIDApXG4gICAgfVxuICB9XG5cbiAgYmF0Y2hTZW5kKG1lc3NhZ2VzKXtcbiAgICB0aGlzLmF3YWl0aW5nQmF0Y2hBY2sgPSB0cnVlXG4gICAgdGhpcy5hamF4KFwiUE9TVFwiLCBcImFwcGxpY2F0aW9uL3gtbmRqc29uXCIsIG1lc3NhZ2VzLmpvaW4oXCJcXG5cIiksICgpID0+IHRoaXMub25lcnJvcihcInRpbWVvdXRcIiksIHJlc3AgPT4ge1xuICAgICAgdGhpcy5hd2FpdGluZ0JhdGNoQWNrID0gZmFsc2VcbiAgICAgIGlmKCFyZXNwIHx8IHJlc3Auc3RhdHVzICE9PSAyMDApe1xuICAgICAgICB0aGlzLm9uZXJyb3IocmVzcCAmJiByZXNwLnN0YXR1cylcbiAgICAgICAgdGhpcy5jbG9zZUFuZFJldHJ5KDEwMTEsIFwiaW50ZXJuYWwgc2VydmVyIGVycm9yXCIsIGZhbHNlKVxuICAgICAgfSBlbHNlIGlmKHRoaXMuYmF0Y2hCdWZmZXIubGVuZ3RoID4gMCl7XG4gICAgICAgIHRoaXMuYmF0Y2hTZW5kKHRoaXMuYmF0Y2hCdWZmZXIpXG4gICAgICAgIHRoaXMuYmF0Y2hCdWZmZXIgPSBbXVxuICAgICAgfVxuICAgIH0pXG4gIH1cblxuICBjbG9zZShjb2RlLCByZWFzb24sIHdhc0NsZWFuKXtcbiAgICBmb3IobGV0IHJlcSBvZiB0aGlzLnJlcXMpeyByZXEuYWJvcnQoKSB9XG4gICAgdGhpcy5yZWFkeVN0YXRlID0gU09DS0VUX1NUQVRFUy5jbG9zZWRcbiAgICBsZXQgb3B0cyA9IE9iamVjdC5hc3NpZ24oe2NvZGU6IDEwMDAsIHJlYXNvbjogdW5kZWZpbmVkLCB3YXNDbGVhbjogdHJ1ZX0sIHtjb2RlLCByZWFzb24sIHdhc0NsZWFufSlcbiAgICB0aGlzLmJhdGNoQnVmZmVyID0gW11cbiAgICBjbGVhclRpbWVvdXQodGhpcy5jdXJyZW50QmF0Y2hUaW1lcilcbiAgICB0aGlzLmN1cnJlbnRCYXRjaFRpbWVyID0gbnVsbFxuICAgIGlmKHR5cGVvZihDbG9zZUV2ZW50KSAhPT0gXCJ1bmRlZmluZWRcIil7XG4gICAgICB0aGlzLm9uY2xvc2UobmV3IENsb3NlRXZlbnQoXCJjbG9zZVwiLCBvcHRzKSlcbiAgICB9IGVsc2Uge1xuICAgICAgdGhpcy5vbmNsb3NlKG9wdHMpXG4gICAgfVxuICB9XG5cbiAgYWpheChtZXRob2QsIGNvbnRlbnRUeXBlLCBib2R5LCBvbkNhbGxlclRpbWVvdXQsIGNhbGxiYWNrKXtcbiAgICBsZXQgcmVxXG4gICAgbGV0IG9udGltZW91dCA9ICgpID0+IHtcbiAgICAgIHRoaXMucmVxcy5kZWxldGUocmVxKVxuICAgICAgb25DYWxsZXJUaW1lb3V0KClcbiAgICB9XG4gICAgcmVxID0gQWpheC5yZXF1ZXN0KG1ldGhvZCwgdGhpcy5lbmRwb2ludFVSTCgpLCBjb250ZW50VHlwZSwgYm9keSwgdGhpcy50aW1lb3V0LCBvbnRpbWVvdXQsIHJlc3AgPT4ge1xuICAgICAgdGhpcy5yZXFzLmRlbGV0ZShyZXEpXG4gICAgICBpZih0aGlzLmlzQWN0aXZlKCkpeyBjYWxsYmFjayhyZXNwKSB9XG4gICAgfSlcbiAgICB0aGlzLnJlcXMuYWRkKHJlcSlcbiAgfVxufVxuIiwgIi8qKlxuICogSW5pdGlhbGl6ZXMgdGhlIFByZXNlbmNlXG4gKiBAcGFyYW0ge0NoYW5uZWx9IGNoYW5uZWwgLSBUaGUgQ2hhbm5lbFxuICogQHBhcmFtIHtPYmplY3R9IG9wdHMgLSBUaGUgb3B0aW9ucyxcbiAqICAgICAgICBmb3IgZXhhbXBsZSBge2V2ZW50czoge3N0YXRlOiBcInN0YXRlXCIsIGRpZmY6IFwiZGlmZlwifX1gXG4gKi9cbmV4cG9ydCBkZWZhdWx0IGNsYXNzIFByZXNlbmNlIHtcblxuICBjb25zdHJ1Y3RvcihjaGFubmVsLCBvcHRzID0ge30pe1xuICAgIGxldCBldmVudHMgPSBvcHRzLmV2ZW50cyB8fCB7c3RhdGU6IFwicHJlc2VuY2Vfc3RhdGVcIiwgZGlmZjogXCJwcmVzZW5jZV9kaWZmXCJ9XG4gICAgdGhpcy5zdGF0ZSA9IHt9XG4gICAgdGhpcy5wZW5kaW5nRGlmZnMgPSBbXVxuICAgIHRoaXMuY2hhbm5lbCA9IGNoYW5uZWxcbiAgICB0aGlzLmpvaW5SZWYgPSBudWxsXG4gICAgdGhpcy5jYWxsZXIgPSB7XG4gICAgICBvbkpvaW46IGZ1bmN0aW9uICgpeyB9LFxuICAgICAgb25MZWF2ZTogZnVuY3Rpb24gKCl7IH0sXG4gICAgICBvblN5bmM6IGZ1bmN0aW9uICgpeyB9XG4gICAgfVxuXG4gICAgdGhpcy5jaGFubmVsLm9uKGV2ZW50cy5zdGF0ZSwgbmV3U3RhdGUgPT4ge1xuICAgICAgbGV0IHtvbkpvaW4sIG9uTGVhdmUsIG9uU3luY30gPSB0aGlzLmNhbGxlclxuXG4gICAgICB0aGlzLmpvaW5SZWYgPSB0aGlzLmNoYW5uZWwuam9pblJlZigpXG4gICAgICB0aGlzLnN0YXRlID0gUHJlc2VuY2Uuc3luY1N0YXRlKHRoaXMuc3RhdGUsIG5ld1N0YXRlLCBvbkpvaW4sIG9uTGVhdmUpXG5cbiAgICAgIHRoaXMucGVuZGluZ0RpZmZzLmZvckVhY2goZGlmZiA9PiB7XG4gICAgICAgIHRoaXMuc3RhdGUgPSBQcmVzZW5jZS5zeW5jRGlmZih0aGlzLnN0YXRlLCBkaWZmLCBvbkpvaW4sIG9uTGVhdmUpXG4gICAgICB9KVxuICAgICAgdGhpcy5wZW5kaW5nRGlmZnMgPSBbXVxuICAgICAgb25TeW5jKClcbiAgICB9KVxuXG4gICAgdGhpcy5jaGFubmVsLm9uKGV2ZW50cy5kaWZmLCBkaWZmID0+IHtcbiAgICAgIGxldCB7b25Kb2luLCBvbkxlYXZlLCBvblN5bmN9ID0gdGhpcy5jYWxsZXJcblxuICAgICAgaWYodGhpcy5pblBlbmRpbmdTeW5jU3RhdGUoKSl7XG4gICAgICAgIHRoaXMucGVuZGluZ0RpZmZzLnB1c2goZGlmZilcbiAgICAgIH0gZWxzZSB7XG4gICAgICAgIHRoaXMuc3RhdGUgPSBQcmVzZW5jZS5zeW5jRGlmZih0aGlzLnN0YXRlLCBkaWZmLCBvbkpvaW4sIG9uTGVhdmUpXG4gICAgICAgIG9uU3luYygpXG4gICAgICB9XG4gICAgfSlcbiAgfVxuXG4gIG9uSm9pbihjYWxsYmFjayl7IHRoaXMuY2FsbGVyLm9uSm9pbiA9IGNhbGxiYWNrIH1cblxuICBvbkxlYXZlKGNhbGxiYWNrKXsgdGhpcy5jYWxsZXIub25MZWF2ZSA9IGNhbGxiYWNrIH1cblxuICBvblN5bmMoY2FsbGJhY2speyB0aGlzLmNhbGxlci5vblN5bmMgPSBjYWxsYmFjayB9XG5cbiAgbGlzdChieSl7IHJldHVybiBQcmVzZW5jZS5saXN0KHRoaXMuc3RhdGUsIGJ5KSB9XG5cbiAgaW5QZW5kaW5nU3luY1N0YXRlKCl7XG4gICAgcmV0dXJuICF0aGlzLmpvaW5SZWYgfHwgKHRoaXMuam9pblJlZiAhPT0gdGhpcy5jaGFubmVsLmpvaW5SZWYoKSlcbiAgfVxuXG4gIC8vIGxvd2VyLWxldmVsIHB1YmxpYyBzdGF0aWMgQVBJXG5cbiAgLyoqXG4gICAqIFVzZWQgdG8gc3luYyB0aGUgbGlzdCBvZiBwcmVzZW5jZXMgb24gdGhlIHNlcnZlclxuICAgKiB3aXRoIHRoZSBjbGllbnQncyBzdGF0ZS4gQW4gb3B0aW9uYWwgYG9uSm9pbmAgYW5kIGBvbkxlYXZlYCBjYWxsYmFjayBjYW5cbiAgICogYmUgcHJvdmlkZWQgdG8gcmVhY3QgdG8gY2hhbmdlcyBpbiB0aGUgY2xpZW50J3MgbG9jYWwgcHJlc2VuY2VzIGFjcm9zc1xuICAgKiBkaXNjb25uZWN0cyBhbmQgcmVjb25uZWN0cyB3aXRoIHRoZSBzZXJ2ZXIuXG4gICAqXG4gICAqIEByZXR1cm5zIHtQcmVzZW5jZX1cbiAgICovXG4gIHN0YXRpYyBzeW5jU3RhdGUoY3VycmVudFN0YXRlLCBuZXdTdGF0ZSwgb25Kb2luLCBvbkxlYXZlKXtcbiAgICBsZXQgc3RhdGUgPSB0aGlzLmNsb25lKGN1cnJlbnRTdGF0ZSlcbiAgICBsZXQgam9pbnMgPSB7fVxuICAgIGxldCBsZWF2ZXMgPSB7fVxuXG4gICAgdGhpcy5tYXAoc3RhdGUsIChrZXksIHByZXNlbmNlKSA9PiB7XG4gICAgICBpZighbmV3U3RhdGVba2V5XSl7XG4gICAgICAgIGxlYXZlc1trZXldID0gcHJlc2VuY2VcbiAgICAgIH1cbiAgICB9KVxuICAgIHRoaXMubWFwKG5ld1N0YXRlLCAoa2V5LCBuZXdQcmVzZW5jZSkgPT4ge1xuICAgICAgbGV0IGN1cnJlbnRQcmVzZW5jZSA9IHN0YXRlW2tleV1cbiAgICAgIGlmKGN1cnJlbnRQcmVzZW5jZSl7XG4gICAgICAgIGxldCBuZXdSZWZzID0gbmV3UHJlc2VuY2UubWV0YXMubWFwKG0gPT4gbS5waHhfcmVmKVxuICAgICAgICBsZXQgY3VyUmVmcyA9IGN1cnJlbnRQcmVzZW5jZS5tZXRhcy5tYXAobSA9PiBtLnBoeF9yZWYpXG4gICAgICAgIGxldCBqb2luZWRNZXRhcyA9IG5ld1ByZXNlbmNlLm1ldGFzLmZpbHRlcihtID0+IGN1clJlZnMuaW5kZXhPZihtLnBoeF9yZWYpIDwgMClcbiAgICAgICAgbGV0IGxlZnRNZXRhcyA9IGN1cnJlbnRQcmVzZW5jZS5tZXRhcy5maWx0ZXIobSA9PiBuZXdSZWZzLmluZGV4T2YobS5waHhfcmVmKSA8IDApXG4gICAgICAgIGlmKGpvaW5lZE1ldGFzLmxlbmd0aCA+IDApe1xuICAgICAgICAgIGpvaW5zW2tleV0gPSBuZXdQcmVzZW5jZVxuICAgICAgICAgIGpvaW5zW2tleV0ubWV0YXMgPSBqb2luZWRNZXRhc1xuICAgICAgICB9XG4gICAgICAgIGlmKGxlZnRNZXRhcy5sZW5ndGggPiAwKXtcbiAgICAgICAgICBsZWF2ZXNba2V5XSA9IHRoaXMuY2xvbmUoY3VycmVudFByZXNlbmNlKVxuICAgICAgICAgIGxlYXZlc1trZXldLm1ldGFzID0gbGVmdE1ldGFzXG4gICAgICAgIH1cbiAgICAgIH0gZWxzZSB7XG4gICAgICAgIGpvaW5zW2tleV0gPSBuZXdQcmVzZW5jZVxuICAgICAgfVxuICAgIH0pXG4gICAgcmV0dXJuIHRoaXMuc3luY0RpZmYoc3RhdGUsIHtqb2luczogam9pbnMsIGxlYXZlczogbGVhdmVzfSwgb25Kb2luLCBvbkxlYXZlKVxuICB9XG5cbiAgLyoqXG4gICAqXG4gICAqIFVzZWQgdG8gc3luYyBhIGRpZmYgb2YgcHJlc2VuY2Ugam9pbiBhbmQgbGVhdmVcbiAgICogZXZlbnRzIGZyb20gdGhlIHNlcnZlciwgYXMgdGhleSBoYXBwZW4uIExpa2UgYHN5bmNTdGF0ZWAsIGBzeW5jRGlmZmBcbiAgICogYWNjZXB0cyBvcHRpb25hbCBgb25Kb2luYCBhbmQgYG9uTGVhdmVgIGNhbGxiYWNrcyB0byByZWFjdCB0byBhIHVzZXJcbiAgICogam9pbmluZyBvciBsZWF2aW5nIGZyb20gYSBkZXZpY2UuXG4gICAqXG4gICAqIEByZXR1cm5zIHtQcmVzZW5jZX1cbiAgICovXG4gIHN0YXRpYyBzeW5jRGlmZihzdGF0ZSwgZGlmZiwgb25Kb2luLCBvbkxlYXZlKXtcbiAgICBsZXQge2pvaW5zLCBsZWF2ZXN9ID0gdGhpcy5jbG9uZShkaWZmKVxuICAgIGlmKCFvbkpvaW4peyBvbkpvaW4gPSBmdW5jdGlvbiAoKXsgfSB9XG4gICAgaWYoIW9uTGVhdmUpeyBvbkxlYXZlID0gZnVuY3Rpb24gKCl7IH0gfVxuXG4gICAgdGhpcy5tYXAoam9pbnMsIChrZXksIG5ld1ByZXNlbmNlKSA9PiB7XG4gICAgICBsZXQgY3VycmVudFByZXNlbmNlID0gc3RhdGVba2V5XVxuICAgICAgc3RhdGVba2V5XSA9IHRoaXMuY2xvbmUobmV3UHJlc2VuY2UpXG4gICAgICBpZihjdXJyZW50UHJlc2VuY2Upe1xuICAgICAgICBsZXQgam9pbmVkUmVmcyA9IHN0YXRlW2tleV0ubWV0YXMubWFwKG0gPT4gbS5waHhfcmVmKVxuICAgICAgICBsZXQgY3VyTWV0YXMgPSBjdXJyZW50UHJlc2VuY2UubWV0YXMuZmlsdGVyKG0gPT4gam9pbmVkUmVmcy5pbmRleE9mKG0ucGh4X3JlZikgPCAwKVxuICAgICAgICBzdGF0ZVtrZXldLm1ldGFzLnVuc2hpZnQoLi4uY3VyTWV0YXMpXG4gICAgICB9XG4gICAgICBvbkpvaW4oa2V5LCBjdXJyZW50UHJlc2VuY2UsIG5ld1ByZXNlbmNlKVxuICAgIH0pXG4gICAgdGhpcy5tYXAobGVhdmVzLCAoa2V5LCBsZWZ0UHJlc2VuY2UpID0+IHtcbiAgICAgIGxldCBjdXJyZW50UHJlc2VuY2UgPSBzdGF0ZVtrZXldXG4gICAgICBpZighY3VycmVudFByZXNlbmNlKXsgcmV0dXJuIH1cbiAgICAgIGxldCByZWZzVG9SZW1vdmUgPSBsZWZ0UHJlc2VuY2UubWV0YXMubWFwKG0gPT4gbS5waHhfcmVmKVxuICAgICAgY3VycmVudFByZXNlbmNlLm1ldGFzID0gY3VycmVudFByZXNlbmNlLm1ldGFzLmZpbHRlcihwID0+IHtcbiAgICAgICAgcmV0dXJuIHJlZnNUb1JlbW92ZS5pbmRleE9mKHAucGh4X3JlZikgPCAwXG4gICAgICB9KVxuICAgICAgb25MZWF2ZShrZXksIGN1cnJlbnRQcmVzZW5jZSwgbGVmdFByZXNlbmNlKVxuICAgICAgaWYoY3VycmVudFByZXNlbmNlLm1ldGFzLmxlbmd0aCA9PT0gMCl7XG4gICAgICAgIGRlbGV0ZSBzdGF0ZVtrZXldXG4gICAgICB9XG4gICAgfSlcbiAgICByZXR1cm4gc3RhdGVcbiAgfVxuXG4gIC8qKlxuICAgKiBSZXR1cm5zIHRoZSBhcnJheSBvZiBwcmVzZW5jZXMsIHdpdGggc2VsZWN0ZWQgbWV0YWRhdGEuXG4gICAqXG4gICAqIEBwYXJhbSB7T2JqZWN0fSBwcmVzZW5jZXNcbiAgICogQHBhcmFtIHtGdW5jdGlvbn0gY2hvb3NlclxuICAgKlxuICAgKiBAcmV0dXJucyB7UHJlc2VuY2V9XG4gICAqL1xuICBzdGF0aWMgbGlzdChwcmVzZW5jZXMsIGNob29zZXIpe1xuICAgIGlmKCFjaG9vc2VyKXsgY2hvb3NlciA9IGZ1bmN0aW9uIChrZXksIHByZXMpeyByZXR1cm4gcHJlcyB9IH1cblxuICAgIHJldHVybiB0aGlzLm1hcChwcmVzZW5jZXMsIChrZXksIHByZXNlbmNlKSA9PiB7XG4gICAgICByZXR1cm4gY2hvb3NlcihrZXksIHByZXNlbmNlKVxuICAgIH0pXG4gIH1cblxuICAvLyBwcml2YXRlXG5cbiAgc3RhdGljIG1hcChvYmosIGZ1bmMpe1xuICAgIHJldHVybiBPYmplY3QuZ2V0T3duUHJvcGVydHlOYW1lcyhvYmopLm1hcChrZXkgPT4gZnVuYyhrZXksIG9ialtrZXldKSlcbiAgfVxuXG4gIHN0YXRpYyBjbG9uZShvYmopeyByZXR1cm4gSlNPTi5wYXJzZShKU09OLnN0cmluZ2lmeShvYmopKSB9XG59XG4iLCAiLyogVGhlIGRlZmF1bHQgc2VyaWFsaXplciBmb3IgZW5jb2RpbmcgYW5kIGRlY29kaW5nIG1lc3NhZ2VzICovXG5pbXBvcnQge1xuICBDSEFOTkVMX0VWRU5UU1xufSBmcm9tIFwiLi9jb25zdGFudHNcIlxuXG5leHBvcnQgZGVmYXVsdCB7XG4gIEhFQURFUl9MRU5HVEg6IDEsXG4gIE1FVEFfTEVOR1RIOiA0LFxuICBLSU5EUzoge3B1c2g6IDAsIHJlcGx5OiAxLCBicm9hZGNhc3Q6IDJ9LFxuXG4gIGVuY29kZShtc2csIGNhbGxiYWNrKXtcbiAgICBpZihtc2cucGF5bG9hZC5jb25zdHJ1Y3RvciA9PT0gQXJyYXlCdWZmZXIpe1xuICAgICAgcmV0dXJuIGNhbGxiYWNrKHRoaXMuYmluYXJ5RW5jb2RlKG1zZykpXG4gICAgfSBlbHNlIHtcbiAgICAgIGxldCBwYXlsb2FkID0gW21zZy5qb2luX3JlZiwgbXNnLnJlZiwgbXNnLnRvcGljLCBtc2cuZXZlbnQsIG1zZy5wYXlsb2FkXVxuICAgICAgcmV0dXJuIGNhbGxiYWNrKEpTT04uc3RyaW5naWZ5KHBheWxvYWQpKVxuICAgIH1cbiAgfSxcblxuICBkZWNvZGUocmF3UGF5bG9hZCwgY2FsbGJhY2spe1xuICAgIGlmKHJhd1BheWxvYWQuY29uc3RydWN0b3IgPT09IEFycmF5QnVmZmVyKXtcbiAgICAgIHJldHVybiBjYWxsYmFjayh0aGlzLmJpbmFyeURlY29kZShyYXdQYXlsb2FkKSlcbiAgICB9IGVsc2Uge1xuICAgICAgbGV0IFtqb2luX3JlZiwgcmVmLCB0b3BpYywgZXZlbnQsIHBheWxvYWRdID0gSlNPTi5wYXJzZShyYXdQYXlsb2FkKVxuICAgICAgcmV0dXJuIGNhbGxiYWNrKHtqb2luX3JlZiwgcmVmLCB0b3BpYywgZXZlbnQsIHBheWxvYWR9KVxuICAgIH1cbiAgfSxcblxuICAvLyBwcml2YXRlXG5cbiAgYmluYXJ5RW5jb2RlKG1lc3NhZ2Upe1xuICAgIGxldCB7am9pbl9yZWYsIHJlZiwgZXZlbnQsIHRvcGljLCBwYXlsb2FkfSA9IG1lc3NhZ2VcbiAgICBsZXQgbWV0YUxlbmd0aCA9IHRoaXMuTUVUQV9MRU5HVEggKyBqb2luX3JlZi5sZW5ndGggKyByZWYubGVuZ3RoICsgdG9waWMubGVuZ3RoICsgZXZlbnQubGVuZ3RoXG4gICAgbGV0IGhlYWRlciA9IG5ldyBBcnJheUJ1ZmZlcih0aGlzLkhFQURFUl9MRU5HVEggKyBtZXRhTGVuZ3RoKVxuICAgIGxldCB2aWV3ID0gbmV3IERhdGFWaWV3KGhlYWRlcilcbiAgICBsZXQgb2Zmc2V0ID0gMFxuXG4gICAgdmlldy5zZXRVaW50OChvZmZzZXQrKywgdGhpcy5LSU5EUy5wdXNoKSAvLyBraW5kXG4gICAgdmlldy5zZXRVaW50OChvZmZzZXQrKywgam9pbl9yZWYubGVuZ3RoKVxuICAgIHZpZXcuc2V0VWludDgob2Zmc2V0KyssIHJlZi5sZW5ndGgpXG4gICAgdmlldy5zZXRVaW50OChvZmZzZXQrKywgdG9waWMubGVuZ3RoKVxuICAgIHZpZXcuc2V0VWludDgob2Zmc2V0KyssIGV2ZW50Lmxlbmd0aClcbiAgICBBcnJheS5mcm9tKGpvaW5fcmVmLCBjaGFyID0+IHZpZXcuc2V0VWludDgob2Zmc2V0KyssIGNoYXIuY2hhckNvZGVBdCgwKSkpXG4gICAgQXJyYXkuZnJvbShyZWYsIGNoYXIgPT4gdmlldy5zZXRVaW50OChvZmZzZXQrKywgY2hhci5jaGFyQ29kZUF0KDApKSlcbiAgICBBcnJheS5mcm9tKHRvcGljLCBjaGFyID0+IHZpZXcuc2V0VWludDgob2Zmc2V0KyssIGNoYXIuY2hhckNvZGVBdCgwKSkpXG4gICAgQXJyYXkuZnJvbShldmVudCwgY2hhciA9PiB2aWV3LnNldFVpbnQ4KG9mZnNldCsrLCBjaGFyLmNoYXJDb2RlQXQoMCkpKVxuXG4gICAgdmFyIGNvbWJpbmVkID0gbmV3IFVpbnQ4QXJyYXkoaGVhZGVyLmJ5dGVMZW5ndGggKyBwYXlsb2FkLmJ5dGVMZW5ndGgpXG4gICAgY29tYmluZWQuc2V0KG5ldyBVaW50OEFycmF5KGhlYWRlciksIDApXG4gICAgY29tYmluZWQuc2V0KG5ldyBVaW50OEFycmF5KHBheWxvYWQpLCBoZWFkZXIuYnl0ZUxlbmd0aClcblxuICAgIHJldHVybiBjb21iaW5lZC5idWZmZXJcbiAgfSxcblxuICBiaW5hcnlEZWNvZGUoYnVmZmVyKXtcbiAgICBsZXQgdmlldyA9IG5ldyBEYXRhVmlldyhidWZmZXIpXG4gICAgbGV0IGtpbmQgPSB2aWV3LmdldFVpbnQ4KDApXG4gICAgbGV0IGRlY29kZXIgPSBuZXcgVGV4dERlY29kZXIoKVxuICAgIHN3aXRjaChraW5kKXtcbiAgICAgIGNhc2UgdGhpcy5LSU5EUy5wdXNoOiByZXR1cm4gdGhpcy5kZWNvZGVQdXNoKGJ1ZmZlciwgdmlldywgZGVjb2RlcilcbiAgICAgIGNhc2UgdGhpcy5LSU5EUy5yZXBseTogcmV0dXJuIHRoaXMuZGVjb2RlUmVwbHkoYnVmZmVyLCB2aWV3LCBkZWNvZGVyKVxuICAgICAgY2FzZSB0aGlzLktJTkRTLmJyb2FkY2FzdDogcmV0dXJuIHRoaXMuZGVjb2RlQnJvYWRjYXN0KGJ1ZmZlciwgdmlldywgZGVjb2RlcilcbiAgICB9XG4gIH0sXG5cbiAgZGVjb2RlUHVzaChidWZmZXIsIHZpZXcsIGRlY29kZXIpe1xuICAgIGxldCBqb2luUmVmU2l6ZSA9IHZpZXcuZ2V0VWludDgoMSlcbiAgICBsZXQgdG9waWNTaXplID0gdmlldy5nZXRVaW50OCgyKVxuICAgIGxldCBldmVudFNpemUgPSB2aWV3LmdldFVpbnQ4KDMpXG4gICAgbGV0IG9mZnNldCA9IHRoaXMuSEVBREVSX0xFTkdUSCArIHRoaXMuTUVUQV9MRU5HVEggLSAxIC8vIHB1c2hlcyBoYXZlIG5vIHJlZlxuICAgIGxldCBqb2luUmVmID0gZGVjb2Rlci5kZWNvZGUoYnVmZmVyLnNsaWNlKG9mZnNldCwgb2Zmc2V0ICsgam9pblJlZlNpemUpKVxuICAgIG9mZnNldCA9IG9mZnNldCArIGpvaW5SZWZTaXplXG4gICAgbGV0IHRvcGljID0gZGVjb2Rlci5kZWNvZGUoYnVmZmVyLnNsaWNlKG9mZnNldCwgb2Zmc2V0ICsgdG9waWNTaXplKSlcbiAgICBvZmZzZXQgPSBvZmZzZXQgKyB0b3BpY1NpemVcbiAgICBsZXQgZXZlbnQgPSBkZWNvZGVyLmRlY29kZShidWZmZXIuc2xpY2Uob2Zmc2V0LCBvZmZzZXQgKyBldmVudFNpemUpKVxuICAgIG9mZnNldCA9IG9mZnNldCArIGV2ZW50U2l6ZVxuICAgIGxldCBkYXRhID0gYnVmZmVyLnNsaWNlKG9mZnNldCwgYnVmZmVyLmJ5dGVMZW5ndGgpXG4gICAgcmV0dXJuIHtqb2luX3JlZjogam9pblJlZiwgcmVmOiBudWxsLCB0b3BpYzogdG9waWMsIGV2ZW50OiBldmVudCwgcGF5bG9hZDogZGF0YX1cbiAgfSxcblxuICBkZWNvZGVSZXBseShidWZmZXIsIHZpZXcsIGRlY29kZXIpe1xuICAgIGxldCBqb2luUmVmU2l6ZSA9IHZpZXcuZ2V0VWludDgoMSlcbiAgICBsZXQgcmVmU2l6ZSA9IHZpZXcuZ2V0VWludDgoMilcbiAgICBsZXQgdG9waWNTaXplID0gdmlldy5nZXRVaW50OCgzKVxuICAgIGxldCBldmVudFNpemUgPSB2aWV3LmdldFVpbnQ4KDQpXG4gICAgbGV0IG9mZnNldCA9IHRoaXMuSEVBREVSX0xFTkdUSCArIHRoaXMuTUVUQV9MRU5HVEhcbiAgICBsZXQgam9pblJlZiA9IGRlY29kZXIuZGVjb2RlKGJ1ZmZlci5zbGljZShvZmZzZXQsIG9mZnNldCArIGpvaW5SZWZTaXplKSlcbiAgICBvZmZzZXQgPSBvZmZzZXQgKyBqb2luUmVmU2l6ZVxuICAgIGxldCByZWYgPSBkZWNvZGVyLmRlY29kZShidWZmZXIuc2xpY2Uob2Zmc2V0LCBvZmZzZXQgKyByZWZTaXplKSlcbiAgICBvZmZzZXQgPSBvZmZzZXQgKyByZWZTaXplXG4gICAgbGV0IHRvcGljID0gZGVjb2Rlci5kZWNvZGUoYnVmZmVyLnNsaWNlKG9mZnNldCwgb2Zmc2V0ICsgdG9waWNTaXplKSlcbiAgICBvZmZzZXQgPSBvZmZzZXQgKyB0b3BpY1NpemVcbiAgICBsZXQgZXZlbnQgPSBkZWNvZGVyLmRlY29kZShidWZmZXIuc2xpY2Uob2Zmc2V0LCBvZmZzZXQgKyBldmVudFNpemUpKVxuICAgIG9mZnNldCA9IG9mZnNldCArIGV2ZW50U2l6ZVxuICAgIGxldCBkYXRhID0gYnVmZmVyLnNsaWNlKG9mZnNldCwgYnVmZmVyLmJ5dGVMZW5ndGgpXG4gICAgbGV0IHBheWxvYWQgPSB7c3RhdHVzOiBldmVudCwgcmVzcG9uc2U6IGRhdGF9XG4gICAgcmV0dXJuIHtqb2luX3JlZjogam9pblJlZiwgcmVmOiByZWYsIHRvcGljOiB0b3BpYywgZXZlbnQ6IENIQU5ORUxfRVZFTlRTLnJlcGx5LCBwYXlsb2FkOiBwYXlsb2FkfVxuICB9LFxuXG4gIGRlY29kZUJyb2FkY2FzdChidWZmZXIsIHZpZXcsIGRlY29kZXIpe1xuICAgIGxldCB0b3BpY1NpemUgPSB2aWV3LmdldFVpbnQ4KDEpXG4gICAgbGV0IGV2ZW50U2l6ZSA9IHZpZXcuZ2V0VWludDgoMilcbiAgICBsZXQgb2Zmc2V0ID0gdGhpcy5IRUFERVJfTEVOR1RIICsgMlxuICAgIGxldCB0b3BpYyA9IGRlY29kZXIuZGVjb2RlKGJ1ZmZlci5zbGljZShvZmZzZXQsIG9mZnNldCArIHRvcGljU2l6ZSkpXG4gICAgb2Zmc2V0ID0gb2Zmc2V0ICsgdG9waWNTaXplXG4gICAgbGV0IGV2ZW50ID0gZGVjb2Rlci5kZWNvZGUoYnVmZmVyLnNsaWNlKG9mZnNldCwgb2Zmc2V0ICsgZXZlbnRTaXplKSlcbiAgICBvZmZzZXQgPSBvZmZzZXQgKyBldmVudFNpemVcbiAgICBsZXQgZGF0YSA9IGJ1ZmZlci5zbGljZShvZmZzZXQsIGJ1ZmZlci5ieXRlTGVuZ3RoKVxuXG4gICAgcmV0dXJuIHtqb2luX3JlZjogbnVsbCwgcmVmOiBudWxsLCB0b3BpYzogdG9waWMsIGV2ZW50OiBldmVudCwgcGF5bG9hZDogZGF0YX1cbiAgfVxufVxuIiwgImltcG9ydCB7XG4gIGdsb2JhbCxcbiAgcGh4V2luZG93LFxuICBDSEFOTkVMX0VWRU5UUyxcbiAgREVGQVVMVF9USU1FT1VULFxuICBERUZBVUxUX1ZTTixcbiAgU09DS0VUX1NUQVRFUyxcbiAgVFJBTlNQT1JUUyxcbiAgV1NfQ0xPU0VfTk9STUFMXG59IGZyb20gXCIuL2NvbnN0YW50c1wiXG5cbmltcG9ydCB7XG4gIGNsb3N1cmVcbn0gZnJvbSBcIi4vdXRpbHNcIlxuXG5pbXBvcnQgQWpheCBmcm9tIFwiLi9hamF4XCJcbmltcG9ydCBDaGFubmVsIGZyb20gXCIuL2NoYW5uZWxcIlxuaW1wb3J0IExvbmdQb2xsIGZyb20gXCIuL2xvbmdwb2xsXCJcbmltcG9ydCBTZXJpYWxpemVyIGZyb20gXCIuL3NlcmlhbGl6ZXJcIlxuaW1wb3J0IFRpbWVyIGZyb20gXCIuL3RpbWVyXCJcblxuLyoqIEluaXRpYWxpemVzIHRoZSBTb2NrZXQgKlxuICpcbiAqIEZvciBJRTggc3VwcG9ydCB1c2UgYW4gRVM1LXNoaW0gKGh0dHBzOi8vZ2l0aHViLmNvbS9lcy1zaGltcy9lczUtc2hpbSlcbiAqXG4gKiBAcGFyYW0ge3N0cmluZ30gZW5kUG9pbnQgLSBUaGUgc3RyaW5nIFdlYlNvY2tldCBlbmRwb2ludCwgaWUsIGBcIndzOi8vZXhhbXBsZS5jb20vc29ja2V0XCJgLFxuICogICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgIGBcIndzczovL2V4YW1wbGUuY29tXCJgXG4gKiAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgYFwiL3NvY2tldFwiYCAoaW5oZXJpdGVkIGhvc3QgJiBwcm90b2NvbClcbiAqIEBwYXJhbSB7T2JqZWN0fSBbb3B0c10gLSBPcHRpb25hbCBjb25maWd1cmF0aW9uXG4gKiBAcGFyYW0ge0Z1bmN0aW9ufSBbb3B0cy50cmFuc3BvcnRdIC0gVGhlIFdlYnNvY2tldCBUcmFuc3BvcnQsIGZvciBleGFtcGxlIFdlYlNvY2tldCBvciBQaG9lbml4LkxvbmdQb2xsLlxuICpcbiAqIERlZmF1bHRzIHRvIFdlYlNvY2tldCB3aXRoIGF1dG9tYXRpYyBMb25nUG9sbCBmYWxsYmFjayBpZiBXZWJTb2NrZXQgaXMgbm90IGRlZmluZWQuXG4gKiBUbyBmYWxsYmFjayB0byBMb25nUG9sbCB3aGVuIFdlYlNvY2tldCBhdHRlbXB0cyBmYWlsLCB1c2UgYGxvbmdQb2xsRmFsbGJhY2tNczogMjUwMGAuXG4gKlxuICogQHBhcmFtIHtGdW5jdGlvbn0gW29wdHMubG9uZ1BvbGxGYWxsYmFja01zXSAtIFRoZSBtaWxsaXNlY29uZCB0aW1lIHRvIGF0dGVtcHQgdGhlIHByaW1hcnkgdHJhbnNwb3J0XG4gKiBiZWZvcmUgZmFsbGluZyBiYWNrIHRvIHRoZSBMb25nUG9sbCB0cmFuc3BvcnQuIERpc2FibGVkIGJ5IGRlZmF1bHQuXG4gKlxuICogQHBhcmFtIHtGdW5jdGlvbn0gW29wdHMuZGVidWddIC0gV2hlbiB0cnVlLCBlbmFibGVzIGRlYnVnIGxvZ2dpbmcuIERlZmF1bHQgZmFsc2UuXG4gKlxuICogQHBhcmFtIHtGdW5jdGlvbn0gW29wdHMuZW5jb2RlXSAtIFRoZSBmdW5jdGlvbiB0byBlbmNvZGUgb3V0Z29pbmcgbWVzc2FnZXMuXG4gKlxuICogRGVmYXVsdHMgdG8gSlNPTiBlbmNvZGVyLlxuICpcbiAqIEBwYXJhbSB7RnVuY3Rpb259IFtvcHRzLmRlY29kZV0gLSBUaGUgZnVuY3Rpb24gdG8gZGVjb2RlIGluY29taW5nIG1lc3NhZ2VzLlxuICpcbiAqIERlZmF1bHRzIHRvIEpTT046XG4gKlxuICogYGBgamF2YXNjcmlwdFxuICogKHBheWxvYWQsIGNhbGxiYWNrKSA9PiBjYWxsYmFjayhKU09OLnBhcnNlKHBheWxvYWQpKVxuICogYGBgXG4gKlxuICogQHBhcmFtIHtudW1iZXJ9IFtvcHRzLnRpbWVvdXRdIC0gVGhlIGRlZmF1bHQgdGltZW91dCBpbiBtaWxsaXNlY29uZHMgdG8gdHJpZ2dlciBwdXNoIHRpbWVvdXRzLlxuICpcbiAqIERlZmF1bHRzIGBERUZBVUxUX1RJTUVPVVRgXG4gKiBAcGFyYW0ge251bWJlcn0gW29wdHMuaGVhcnRiZWF0SW50ZXJ2YWxNc10gLSBUaGUgbWlsbGlzZWMgaW50ZXJ2YWwgdG8gc2VuZCBhIGhlYXJ0YmVhdCBtZXNzYWdlXG4gKiBAcGFyYW0ge251bWJlcn0gW29wdHMucmVjb25uZWN0QWZ0ZXJNc10gLSBUaGUgb3B0aW9uYWwgZnVuY3Rpb24gdGhhdCByZXR1cm5zIHRoZSBtaWxsaXNlY1xuICogc29ja2V0IHJlY29ubmVjdCBpbnRlcnZhbC5cbiAqXG4gKiBEZWZhdWx0cyB0byBzdGVwcGVkIGJhY2tvZmYgb2Y6XG4gKlxuICogYGBgamF2YXNjcmlwdFxuICogZnVuY3Rpb24odHJpZXMpe1xuICogICByZXR1cm4gWzEwLCA1MCwgMTAwLCAxNTAsIDIwMCwgMjUwLCA1MDAsIDEwMDAsIDIwMDBdW3RyaWVzIC0gMV0gfHwgNTAwMFxuICogfVxuICogYGBgYFxuICpcbiAqIEBwYXJhbSB7bnVtYmVyfSBbb3B0cy5yZWpvaW5BZnRlck1zXSAtIFRoZSBvcHRpb25hbCBmdW5jdGlvbiB0aGF0IHJldHVybnMgdGhlIG1pbGxpc2VjXG4gKiByZWpvaW4gaW50ZXJ2YWwgZm9yIGluZGl2aWR1YWwgY2hhbm5lbHMuXG4gKlxuICogYGBgamF2YXNjcmlwdFxuICogZnVuY3Rpb24odHJpZXMpe1xuICogICByZXR1cm4gWzEwMDAsIDIwMDAsIDUwMDBdW3RyaWVzIC0gMV0gfHwgMTAwMDBcbiAqIH1cbiAqIGBgYGBcbiAqXG4gKiBAcGFyYW0ge0Z1bmN0aW9ufSBbb3B0cy5sb2dnZXJdIC0gVGhlIG9wdGlvbmFsIGZ1bmN0aW9uIGZvciBzcGVjaWFsaXplZCBsb2dnaW5nLCBpZTpcbiAqXG4gKiBgYGBqYXZhc2NyaXB0XG4gKiBmdW5jdGlvbihraW5kLCBtc2csIGRhdGEpIHtcbiAqICAgY29uc29sZS5sb2coYCR7a2luZH06ICR7bXNnfWAsIGRhdGEpXG4gKiB9XG4gKiBgYGBcbiAqXG4gKiBAcGFyYW0ge251bWJlcn0gW29wdHMubG9uZ3BvbGxlclRpbWVvdXRdIC0gVGhlIG1heGltdW0gdGltZW91dCBvZiBhIGxvbmcgcG9sbCBBSkFYIHJlcXVlc3QuXG4gKlxuICogRGVmYXVsdHMgdG8gMjBzIChkb3VibGUgdGhlIHNlcnZlciBsb25nIHBvbGwgdGltZXIpLlxuICpcbiAqIEBwYXJhbSB7KE9iamVjdHxmdW5jdGlvbil9IFtvcHRzLnBhcmFtc10gLSBUaGUgb3B0aW9uYWwgcGFyYW1zIHRvIHBhc3Mgd2hlbiBjb25uZWN0aW5nXG4gKiBAcGFyYW0ge3N0cmluZ30gW29wdHMuYmluYXJ5VHlwZV0gLSBUaGUgYmluYXJ5IHR5cGUgdG8gdXNlIGZvciBiaW5hcnkgV2ViU29ja2V0IGZyYW1lcy5cbiAqXG4gKiBEZWZhdWx0cyB0byBcImFycmF5YnVmZmVyXCJcbiAqXG4gKiBAcGFyYW0ge3Zzbn0gW29wdHMudnNuXSAtIFRoZSBzZXJpYWxpemVyJ3MgcHJvdG9jb2wgdmVyc2lvbiB0byBzZW5kIG9uIGNvbm5lY3QuXG4gKlxuICogRGVmYXVsdHMgdG8gREVGQVVMVF9WU04uXG4gKlxuICogQHBhcmFtIHtPYmplY3R9IFtvcHRzLnNlc3Npb25TdG9yYWdlXSAtIEFuIG9wdGlvbmFsIFN0b3JhZ2UgY29tcGF0aWJsZSBvYmplY3RcbiAqIFBob2VuaXggdXNlcyBzZXNzaW9uU3RvcmFnZSBmb3IgbG9uZ3BvbGwgZmFsbGJhY2sgaGlzdG9yeS4gT3ZlcnJpZGluZyB0aGUgc3RvcmUgaXNcbiAqIHVzZWZ1bCB3aGVuIFBob2VuaXggd29uJ3QgaGF2ZSBhY2Nlc3MgdG8gYHNlc3Npb25TdG9yYWdlYC4gRm9yIGV4YW1wbGUsIFRoaXMgY291bGRcbiAqIGhhcHBlbiBpZiBhIHNpdGUgbG9hZHMgYSBjcm9zcy1kb21haW4gY2hhbm5lbCBpbiBhbiBpZnJhbWUuIEV4YW1wbGUgdXNhZ2U6XG4gKlxuICogICAgIGNsYXNzIEluTWVtb3J5U3RvcmFnZSB7XG4gKiAgICAgICBjb25zdHJ1Y3RvcigpIHsgdGhpcy5zdG9yYWdlID0ge30gfVxuICogICAgICAgZ2V0SXRlbShrZXlOYW1lKSB7IHJldHVybiB0aGlzLnN0b3JhZ2Vba2V5TmFtZV0gfHwgbnVsbCB9XG4gKiAgICAgICByZW1vdmVJdGVtKGtleU5hbWUpIHsgZGVsZXRlIHRoaXMuc3RvcmFnZVtrZXlOYW1lXSB9XG4gKiAgICAgICBzZXRJdGVtKGtleU5hbWUsIGtleVZhbHVlKSB7IHRoaXMuc3RvcmFnZVtrZXlOYW1lXSA9IGtleVZhbHVlIH1cbiAqICAgICB9XG4gKlxuKi9cbmV4cG9ydCBkZWZhdWx0IGNsYXNzIFNvY2tldCB7XG4gIGNvbnN0cnVjdG9yKGVuZFBvaW50LCBvcHRzID0ge30pe1xuICAgIHRoaXMuc3RhdGVDaGFuZ2VDYWxsYmFja3MgPSB7b3BlbjogW10sIGNsb3NlOiBbXSwgZXJyb3I6IFtdLCBtZXNzYWdlOiBbXX1cbiAgICB0aGlzLmNoYW5uZWxzID0gW11cbiAgICB0aGlzLnNlbmRCdWZmZXIgPSBbXVxuICAgIHRoaXMucmVmID0gMFxuICAgIHRoaXMudGltZW91dCA9IG9wdHMudGltZW91dCB8fCBERUZBVUxUX1RJTUVPVVRcbiAgICB0aGlzLnRyYW5zcG9ydCA9IG9wdHMudHJhbnNwb3J0IHx8IGdsb2JhbC5XZWJTb2NrZXQgfHwgTG9uZ1BvbGxcbiAgICB0aGlzLnByaW1hcnlQYXNzZWRIZWFsdGhDaGVjayA9IGZhbHNlXG4gICAgdGhpcy5sb25nUG9sbEZhbGxiYWNrTXMgPSBvcHRzLmxvbmdQb2xsRmFsbGJhY2tNc1xuICAgIHRoaXMuZmFsbGJhY2tUaW1lciA9IG51bGxcbiAgICB0aGlzLnNlc3Npb25TdG9yZSA9IG9wdHMuc2Vzc2lvblN0b3JhZ2UgfHwgKGdsb2JhbCAmJiBnbG9iYWwuc2Vzc2lvblN0b3JhZ2UpXG4gICAgdGhpcy5lc3RhYmxpc2hlZENvbm5lY3Rpb25zID0gMFxuICAgIHRoaXMuZGVmYXVsdEVuY29kZXIgPSBTZXJpYWxpemVyLmVuY29kZS5iaW5kKFNlcmlhbGl6ZXIpXG4gICAgdGhpcy5kZWZhdWx0RGVjb2RlciA9IFNlcmlhbGl6ZXIuZGVjb2RlLmJpbmQoU2VyaWFsaXplcilcbiAgICB0aGlzLmNsb3NlV2FzQ2xlYW4gPSBmYWxzZVxuICAgIHRoaXMuZGlzY29ubmVjdGluZyA9IGZhbHNlXG4gICAgdGhpcy5iaW5hcnlUeXBlID0gb3B0cy5iaW5hcnlUeXBlIHx8IFwiYXJyYXlidWZmZXJcIlxuICAgIHRoaXMuY29ubmVjdENsb2NrID0gMVxuICAgIGlmKHRoaXMudHJhbnNwb3J0ICE9PSBMb25nUG9sbCl7XG4gICAgICB0aGlzLmVuY29kZSA9IG9wdHMuZW5jb2RlIHx8IHRoaXMuZGVmYXVsdEVuY29kZXJcbiAgICAgIHRoaXMuZGVjb2RlID0gb3B0cy5kZWNvZGUgfHwgdGhpcy5kZWZhdWx0RGVjb2RlclxuICAgIH0gZWxzZSB7XG4gICAgICB0aGlzLmVuY29kZSA9IHRoaXMuZGVmYXVsdEVuY29kZXJcbiAgICAgIHRoaXMuZGVjb2RlID0gdGhpcy5kZWZhdWx0RGVjb2RlclxuICAgIH1cbiAgICBsZXQgYXdhaXRpbmdDb25uZWN0aW9uT25QYWdlU2hvdyA9IG51bGxcbiAgICBpZihwaHhXaW5kb3cgJiYgcGh4V2luZG93LmFkZEV2ZW50TGlzdGVuZXIpe1xuICAgICAgcGh4V2luZG93LmFkZEV2ZW50TGlzdGVuZXIoXCJwYWdlaGlkZVwiLCBfZSA9PiB7XG4gICAgICAgIGlmKHRoaXMuY29ubil7XG4gICAgICAgICAgdGhpcy5kaXNjb25uZWN0KClcbiAgICAgICAgICBhd2FpdGluZ0Nvbm5lY3Rpb25PblBhZ2VTaG93ID0gdGhpcy5jb25uZWN0Q2xvY2tcbiAgICAgICAgfVxuICAgICAgfSlcbiAgICAgIHBoeFdpbmRvdy5hZGRFdmVudExpc3RlbmVyKFwicGFnZXNob3dcIiwgX2UgPT4ge1xuICAgICAgICBpZihhd2FpdGluZ0Nvbm5lY3Rpb25PblBhZ2VTaG93ID09PSB0aGlzLmNvbm5lY3RDbG9jayl7XG4gICAgICAgICAgYXdhaXRpbmdDb25uZWN0aW9uT25QYWdlU2hvdyA9IG51bGxcbiAgICAgICAgICB0aGlzLmNvbm5lY3QoKVxuICAgICAgICB9XG4gICAgICB9KVxuICAgIH1cbiAgICB0aGlzLmhlYXJ0YmVhdEludGVydmFsTXMgPSBvcHRzLmhlYXJ0YmVhdEludGVydmFsTXMgfHwgMzAwMDBcbiAgICB0aGlzLnJlam9pbkFmdGVyTXMgPSAodHJpZXMpID0+IHtcbiAgICAgIGlmKG9wdHMucmVqb2luQWZ0ZXJNcyl7XG4gICAgICAgIHJldHVybiBvcHRzLnJlam9pbkFmdGVyTXModHJpZXMpXG4gICAgICB9IGVsc2Uge1xuICAgICAgICByZXR1cm4gWzEwMDAsIDIwMDAsIDUwMDBdW3RyaWVzIC0gMV0gfHwgMTAwMDBcbiAgICAgIH1cbiAgICB9XG4gICAgdGhpcy5yZWNvbm5lY3RBZnRlck1zID0gKHRyaWVzKSA9PiB7XG4gICAgICBpZihvcHRzLnJlY29ubmVjdEFmdGVyTXMpe1xuICAgICAgICByZXR1cm4gb3B0cy5yZWNvbm5lY3RBZnRlck1zKHRyaWVzKVxuICAgICAgfSBlbHNlIHtcbiAgICAgICAgcmV0dXJuIFsxMCwgNTAsIDEwMCwgMTUwLCAyMDAsIDI1MCwgNTAwLCAxMDAwLCAyMDAwXVt0cmllcyAtIDFdIHx8IDUwMDBcbiAgICAgIH1cbiAgICB9XG4gICAgdGhpcy5sb2dnZXIgPSBvcHRzLmxvZ2dlciB8fCBudWxsXG4gICAgaWYoIXRoaXMubG9nZ2VyICYmIG9wdHMuZGVidWcpe1xuICAgICAgdGhpcy5sb2dnZXIgPSAoa2luZCwgbXNnLCBkYXRhKSA9PiB7IGNvbnNvbGUubG9nKGAke2tpbmR9OiAke21zZ31gLCBkYXRhKSB9XG4gICAgfVxuICAgIHRoaXMubG9uZ3BvbGxlclRpbWVvdXQgPSBvcHRzLmxvbmdwb2xsZXJUaW1lb3V0IHx8IDIwMDAwXG4gICAgdGhpcy5wYXJhbXMgPSBjbG9zdXJlKG9wdHMucGFyYW1zIHx8IHt9KVxuICAgIHRoaXMuZW5kUG9pbnQgPSBgJHtlbmRQb2ludH0vJHtUUkFOU1BPUlRTLndlYnNvY2tldH1gXG4gICAgdGhpcy52c24gPSBvcHRzLnZzbiB8fCBERUZBVUxUX1ZTTlxuICAgIHRoaXMuaGVhcnRiZWF0VGltZW91dFRpbWVyID0gbnVsbFxuICAgIHRoaXMuaGVhcnRiZWF0VGltZXIgPSBudWxsXG4gICAgdGhpcy5wZW5kaW5nSGVhcnRiZWF0UmVmID0gbnVsbFxuICAgIHRoaXMucmVjb25uZWN0VGltZXIgPSBuZXcgVGltZXIoKCkgPT4ge1xuICAgICAgdGhpcy50ZWFyZG93bigoKSA9PiB0aGlzLmNvbm5lY3QoKSlcbiAgICB9LCB0aGlzLnJlY29ubmVjdEFmdGVyTXMpXG4gIH1cblxuICAvKipcbiAgICogUmV0dXJucyB0aGUgTG9uZ1BvbGwgdHJhbnNwb3J0IHJlZmVyZW5jZVxuICAgKi9cbiAgZ2V0TG9uZ1BvbGxUcmFuc3BvcnQoKXsgcmV0dXJuIExvbmdQb2xsIH1cblxuICAvKipcbiAgICogRGlzY29ubmVjdHMgYW5kIHJlcGxhY2VzIHRoZSBhY3RpdmUgdHJhbnNwb3J0XG4gICAqXG4gICAqIEBwYXJhbSB7RnVuY3Rpb259IG5ld1RyYW5zcG9ydCAtIFRoZSBuZXcgdHJhbnNwb3J0IGNsYXNzIHRvIGluc3RhbnRpYXRlXG4gICAqXG4gICAqL1xuICByZXBsYWNlVHJhbnNwb3J0KG5ld1RyYW5zcG9ydCl7XG4gICAgdGhpcy5jb25uZWN0Q2xvY2srK1xuICAgIHRoaXMuY2xvc2VXYXNDbGVhbiA9IHRydWVcbiAgICBjbGVhclRpbWVvdXQodGhpcy5mYWxsYmFja1RpbWVyKVxuICAgIHRoaXMucmVjb25uZWN0VGltZXIucmVzZXQoKVxuICAgIGlmKHRoaXMuY29ubil7XG4gICAgICB0aGlzLmNvbm4uY2xvc2UoKVxuICAgICAgdGhpcy5jb25uID0gbnVsbFxuICAgIH1cbiAgICB0aGlzLnRyYW5zcG9ydCA9IG5ld1RyYW5zcG9ydFxuICB9XG5cbiAgLyoqXG4gICAqIFJldHVybnMgdGhlIHNvY2tldCBwcm90b2NvbFxuICAgKlxuICAgKiBAcmV0dXJucyB7c3RyaW5nfVxuICAgKi9cbiAgcHJvdG9jb2woKXsgcmV0dXJuIGxvY2F0aW9uLnByb3RvY29sLm1hdGNoKC9eaHR0cHMvKSA/IFwid3NzXCIgOiBcIndzXCIgfVxuXG4gIC8qKlxuICAgKiBUaGUgZnVsbHkgcXVhbGlmaWVkIHNvY2tldCB1cmxcbiAgICpcbiAgICogQHJldHVybnMge3N0cmluZ31cbiAgICovXG4gIGVuZFBvaW50VVJMKCl7XG4gICAgbGV0IHVyaSA9IEFqYXguYXBwZW5kUGFyYW1zKFxuICAgICAgQWpheC5hcHBlbmRQYXJhbXModGhpcy5lbmRQb2ludCwgdGhpcy5wYXJhbXMoKSksIHt2c246IHRoaXMudnNufSlcbiAgICBpZih1cmkuY2hhckF0KDApICE9PSBcIi9cIil7IHJldHVybiB1cmkgfVxuICAgIGlmKHVyaS5jaGFyQXQoMSkgPT09IFwiL1wiKXsgcmV0dXJuIGAke3RoaXMucHJvdG9jb2woKX06JHt1cml9YCB9XG5cbiAgICByZXR1cm4gYCR7dGhpcy5wcm90b2NvbCgpfTovLyR7bG9jYXRpb24uaG9zdH0ke3VyaX1gXG4gIH1cblxuICAvKipcbiAgICogRGlzY29ubmVjdHMgdGhlIHNvY2tldFxuICAgKlxuICAgKiBTZWUgaHR0cHM6Ly9kZXZlbG9wZXIubW96aWxsYS5vcmcvZW4tVVMvZG9jcy9XZWIvQVBJL0Nsb3NlRXZlbnQjU3RhdHVzX2NvZGVzIGZvciB2YWxpZCBzdGF0dXMgY29kZXMuXG4gICAqXG4gICAqIEBwYXJhbSB7RnVuY3Rpb259IGNhbGxiYWNrIC0gT3B0aW9uYWwgY2FsbGJhY2sgd2hpY2ggaXMgY2FsbGVkIGFmdGVyIHNvY2tldCBpcyBkaXNjb25uZWN0ZWQuXG4gICAqIEBwYXJhbSB7aW50ZWdlcn0gY29kZSAtIEEgc3RhdHVzIGNvZGUgZm9yIGRpc2Nvbm5lY3Rpb24gKE9wdGlvbmFsKS5cbiAgICogQHBhcmFtIHtzdHJpbmd9IHJlYXNvbiAtIEEgdGV4dHVhbCBkZXNjcmlwdGlvbiBvZiB0aGUgcmVhc29uIHRvIGRpc2Nvbm5lY3QuIChPcHRpb25hbClcbiAgICovXG4gIGRpc2Nvbm5lY3QoY2FsbGJhY2ssIGNvZGUsIHJlYXNvbil7XG4gICAgdGhpcy5jb25uZWN0Q2xvY2srK1xuICAgIHRoaXMuZGlzY29ubmVjdGluZyA9IHRydWVcbiAgICB0aGlzLmNsb3NlV2FzQ2xlYW4gPSB0cnVlXG4gICAgY2xlYXJUaW1lb3V0KHRoaXMuZmFsbGJhY2tUaW1lcilcbiAgICB0aGlzLnJlY29ubmVjdFRpbWVyLnJlc2V0KClcbiAgICB0aGlzLnRlYXJkb3duKCgpID0+IHtcbiAgICAgIHRoaXMuZGlzY29ubmVjdGluZyA9IGZhbHNlXG4gICAgICBjYWxsYmFjayAmJiBjYWxsYmFjaygpXG4gICAgfSwgY29kZSwgcmVhc29uKVxuICB9XG5cbiAgLyoqXG4gICAqXG4gICAqIEBwYXJhbSB7T2JqZWN0fSBwYXJhbXMgLSBUaGUgcGFyYW1zIHRvIHNlbmQgd2hlbiBjb25uZWN0aW5nLCBmb3IgZXhhbXBsZSBge3VzZXJfaWQ6IHVzZXJUb2tlbn1gXG4gICAqXG4gICAqIFBhc3NpbmcgcGFyYW1zIHRvIGNvbm5lY3QgaXMgZGVwcmVjYXRlZDsgcGFzcyB0aGVtIGluIHRoZSBTb2NrZXQgY29uc3RydWN0b3IgaW5zdGVhZDpcbiAgICogYG5ldyBTb2NrZXQoXCIvc29ja2V0XCIsIHtwYXJhbXM6IHt1c2VyX2lkOiB1c2VyVG9rZW59fSlgLlxuICAgKi9cbiAgY29ubmVjdChwYXJhbXMpe1xuICAgIGlmKHBhcmFtcyl7XG4gICAgICBjb25zb2xlICYmIGNvbnNvbGUubG9nKFwicGFzc2luZyBwYXJhbXMgdG8gY29ubmVjdCBpcyBkZXByZWNhdGVkLiBJbnN0ZWFkIHBhc3MgOnBhcmFtcyB0byB0aGUgU29ja2V0IGNvbnN0cnVjdG9yXCIpXG4gICAgICB0aGlzLnBhcmFtcyA9IGNsb3N1cmUocGFyYW1zKVxuICAgIH1cbiAgICBpZih0aGlzLmNvbm4gJiYgIXRoaXMuZGlzY29ubmVjdGluZyl7IHJldHVybiB9XG4gICAgaWYodGhpcy5sb25nUG9sbEZhbGxiYWNrTXMgJiYgdGhpcy50cmFuc3BvcnQgIT09IExvbmdQb2xsKXtcbiAgICAgIHRoaXMuY29ubmVjdFdpdGhGYWxsYmFjayhMb25nUG9sbCwgdGhpcy5sb25nUG9sbEZhbGxiYWNrTXMpXG4gICAgfSBlbHNlIHtcbiAgICAgIHRoaXMudHJhbnNwb3J0Q29ubmVjdCgpXG4gICAgfVxuICB9XG5cbiAgLyoqXG4gICAqIExvZ3MgdGhlIG1lc3NhZ2UuIE92ZXJyaWRlIGB0aGlzLmxvZ2dlcmAgZm9yIHNwZWNpYWxpemVkIGxvZ2dpbmcuIG5vb3BzIGJ5IGRlZmF1bHRcbiAgICogQHBhcmFtIHtzdHJpbmd9IGtpbmRcbiAgICogQHBhcmFtIHtzdHJpbmd9IG1zZ1xuICAgKiBAcGFyYW0ge09iamVjdH0gZGF0YVxuICAgKi9cbiAgbG9nKGtpbmQsIG1zZywgZGF0YSl7IHRoaXMubG9nZ2VyICYmIHRoaXMubG9nZ2VyKGtpbmQsIG1zZywgZGF0YSkgfVxuXG4gIC8qKlxuICAgKiBSZXR1cm5zIHRydWUgaWYgYSBsb2dnZXIgaGFzIGJlZW4gc2V0IG9uIHRoaXMgc29ja2V0LlxuICAgKi9cbiAgaGFzTG9nZ2VyKCl7IHJldHVybiB0aGlzLmxvZ2dlciAhPT0gbnVsbCB9XG5cbiAgLyoqXG4gICAqIFJlZ2lzdGVycyBjYWxsYmFja3MgZm9yIGNvbm5lY3Rpb24gb3BlbiBldmVudHNcbiAgICpcbiAgICogQGV4YW1wbGUgc29ja2V0Lm9uT3BlbihmdW5jdGlvbigpeyBjb25zb2xlLmluZm8oXCJ0aGUgc29ja2V0IHdhcyBvcGVuZWRcIikgfSlcbiAgICpcbiAgICogQHBhcmFtIHtGdW5jdGlvbn0gY2FsbGJhY2tcbiAgICovXG4gIG9uT3BlbihjYWxsYmFjayl7XG4gICAgbGV0IHJlZiA9IHRoaXMubWFrZVJlZigpXG4gICAgdGhpcy5zdGF0ZUNoYW5nZUNhbGxiYWNrcy5vcGVuLnB1c2goW3JlZiwgY2FsbGJhY2tdKVxuICAgIHJldHVybiByZWZcbiAgfVxuXG4gIC8qKlxuICAgKiBSZWdpc3RlcnMgY2FsbGJhY2tzIGZvciBjb25uZWN0aW9uIGNsb3NlIGV2ZW50c1xuICAgKiBAcGFyYW0ge0Z1bmN0aW9ufSBjYWxsYmFja1xuICAgKi9cbiAgb25DbG9zZShjYWxsYmFjayl7XG4gICAgbGV0IHJlZiA9IHRoaXMubWFrZVJlZigpXG4gICAgdGhpcy5zdGF0ZUNoYW5nZUNhbGxiYWNrcy5jbG9zZS5wdXNoKFtyZWYsIGNhbGxiYWNrXSlcbiAgICByZXR1cm4gcmVmXG4gIH1cblxuICAvKipcbiAgICogUmVnaXN0ZXJzIGNhbGxiYWNrcyBmb3IgY29ubmVjdGlvbiBlcnJvciBldmVudHNcbiAgICpcbiAgICogQGV4YW1wbGUgc29ja2V0Lm9uRXJyb3IoZnVuY3Rpb24oZXJyb3IpeyBhbGVydChcIkFuIGVycm9yIG9jY3VycmVkXCIpIH0pXG4gICAqXG4gICAqIEBwYXJhbSB7RnVuY3Rpb259IGNhbGxiYWNrXG4gICAqL1xuICBvbkVycm9yKGNhbGxiYWNrKXtcbiAgICBsZXQgcmVmID0gdGhpcy5tYWtlUmVmKClcbiAgICB0aGlzLnN0YXRlQ2hhbmdlQ2FsbGJhY2tzLmVycm9yLnB1c2goW3JlZiwgY2FsbGJhY2tdKVxuICAgIHJldHVybiByZWZcbiAgfVxuXG4gIC8qKlxuICAgKiBSZWdpc3RlcnMgY2FsbGJhY2tzIGZvciBjb25uZWN0aW9uIG1lc3NhZ2UgZXZlbnRzXG4gICAqIEBwYXJhbSB7RnVuY3Rpb259IGNhbGxiYWNrXG4gICAqL1xuICBvbk1lc3NhZ2UoY2FsbGJhY2spe1xuICAgIGxldCByZWYgPSB0aGlzLm1ha2VSZWYoKVxuICAgIHRoaXMuc3RhdGVDaGFuZ2VDYWxsYmFja3MubWVzc2FnZS5wdXNoKFtyZWYsIGNhbGxiYWNrXSlcbiAgICByZXR1cm4gcmVmXG4gIH1cblxuICAvKipcbiAgICogUGluZ3MgdGhlIHNlcnZlciBhbmQgaW52b2tlcyB0aGUgY2FsbGJhY2sgd2l0aCB0aGUgUlRUIGluIG1pbGxpc2Vjb25kc1xuICAgKiBAcGFyYW0ge0Z1bmN0aW9ufSBjYWxsYmFja1xuICAgKlxuICAgKiBSZXR1cm5zIHRydWUgaWYgdGhlIHBpbmcgd2FzIHB1c2hlZCBvciBmYWxzZSBpZiB1bmFibGUgdG8gYmUgcHVzaGVkLlxuICAgKi9cbiAgcGluZyhjYWxsYmFjayl7XG4gICAgaWYoIXRoaXMuaXNDb25uZWN0ZWQoKSl7IHJldHVybiBmYWxzZSB9XG4gICAgbGV0IHJlZiA9IHRoaXMubWFrZVJlZigpXG4gICAgbGV0IHN0YXJ0VGltZSA9IERhdGUubm93KClcbiAgICB0aGlzLnB1c2goe3RvcGljOiBcInBob2VuaXhcIiwgZXZlbnQ6IFwiaGVhcnRiZWF0XCIsIHBheWxvYWQ6IHt9LCByZWY6IHJlZn0pXG4gICAgbGV0IG9uTXNnUmVmID0gdGhpcy5vbk1lc3NhZ2UobXNnID0+IHtcbiAgICAgIGlmKG1zZy5yZWYgPT09IHJlZil7XG4gICAgICAgIHRoaXMub2ZmKFtvbk1zZ1JlZl0pXG4gICAgICAgIGNhbGxiYWNrKERhdGUubm93KCkgLSBzdGFydFRpbWUpXG4gICAgICB9XG4gICAgfSlcbiAgICByZXR1cm4gdHJ1ZVxuICB9XG5cbiAgLyoqXG4gICAqIEBwcml2YXRlXG4gICAqL1xuXG4gIHRyYW5zcG9ydENvbm5lY3QoKXtcbiAgICB0aGlzLmNvbm5lY3RDbG9jaysrXG4gICAgdGhpcy5jbG9zZVdhc0NsZWFuID0gZmFsc2VcbiAgICB0aGlzLmNvbm4gPSBuZXcgdGhpcy50cmFuc3BvcnQodGhpcy5lbmRQb2ludFVSTCgpKVxuICAgIHRoaXMuY29ubi5iaW5hcnlUeXBlID0gdGhpcy5iaW5hcnlUeXBlXG4gICAgdGhpcy5jb25uLnRpbWVvdXQgPSB0aGlzLmxvbmdwb2xsZXJUaW1lb3V0XG4gICAgdGhpcy5jb25uLm9ub3BlbiA9ICgpID0+IHRoaXMub25Db25uT3BlbigpXG4gICAgdGhpcy5jb25uLm9uZXJyb3IgPSBlcnJvciA9PiB0aGlzLm9uQ29ubkVycm9yKGVycm9yKVxuICAgIHRoaXMuY29ubi5vbm1lc3NhZ2UgPSBldmVudCA9PiB0aGlzLm9uQ29ubk1lc3NhZ2UoZXZlbnQpXG4gICAgdGhpcy5jb25uLm9uY2xvc2UgPSBldmVudCA9PiB0aGlzLm9uQ29ubkNsb3NlKGV2ZW50KVxuICB9XG5cbiAgZ2V0U2Vzc2lvbihrZXkpeyByZXR1cm4gdGhpcy5zZXNzaW9uU3RvcmUgJiYgdGhpcy5zZXNzaW9uU3RvcmUuZ2V0SXRlbShrZXkpIH1cblxuICBzdG9yZVNlc3Npb24oa2V5LCB2YWwpeyB0aGlzLnNlc3Npb25TdG9yZSAmJiB0aGlzLnNlc3Npb25TdG9yZS5zZXRJdGVtKGtleSwgdmFsKSB9XG5cbiAgY29ubmVjdFdpdGhGYWxsYmFjayhmYWxsYmFja1RyYW5zcG9ydCwgZmFsbGJhY2tUaHJlc2hvbGQgPSAyNTAwKXtcbiAgICBjbGVhclRpbWVvdXQodGhpcy5mYWxsYmFja1RpbWVyKVxuICAgIGxldCBlc3RhYmxpc2hlZCA9IGZhbHNlXG4gICAgbGV0IHByaW1hcnlUcmFuc3BvcnQgPSB0cnVlXG4gICAgbGV0IG9wZW5SZWYsIGVycm9yUmVmXG4gICAgbGV0IGZhbGxiYWNrID0gKHJlYXNvbikgPT4ge1xuICAgICAgdGhpcy5sb2coXCJ0cmFuc3BvcnRcIiwgYGZhbGxpbmcgYmFjayB0byAke2ZhbGxiYWNrVHJhbnNwb3J0Lm5hbWV9Li4uYCwgcmVhc29uKVxuICAgICAgdGhpcy5vZmYoW29wZW5SZWYsIGVycm9yUmVmXSlcbiAgICAgIHByaW1hcnlUcmFuc3BvcnQgPSBmYWxzZVxuICAgICAgdGhpcy5yZXBsYWNlVHJhbnNwb3J0KGZhbGxiYWNrVHJhbnNwb3J0KVxuICAgICAgdGhpcy50cmFuc3BvcnRDb25uZWN0KClcbiAgICB9XG4gICAgaWYodGhpcy5nZXRTZXNzaW9uKGBwaHg6ZmFsbGJhY2s6JHtmYWxsYmFja1RyYW5zcG9ydC5uYW1lfWApKXsgcmV0dXJuIGZhbGxiYWNrKFwibWVtb3JpemVkXCIpIH1cblxuICAgIHRoaXMuZmFsbGJhY2tUaW1lciA9IHNldFRpbWVvdXQoZmFsbGJhY2ssIGZhbGxiYWNrVGhyZXNob2xkKVxuXG4gICAgZXJyb3JSZWYgPSB0aGlzLm9uRXJyb3IocmVhc29uID0+IHtcbiAgICAgIHRoaXMubG9nKFwidHJhbnNwb3J0XCIsIFwiZXJyb3JcIiwgcmVhc29uKVxuICAgICAgaWYocHJpbWFyeVRyYW5zcG9ydCAmJiAhZXN0YWJsaXNoZWQpe1xuICAgICAgICBjbGVhclRpbWVvdXQodGhpcy5mYWxsYmFja1RpbWVyKVxuICAgICAgICBmYWxsYmFjayhyZWFzb24pXG4gICAgICB9XG4gICAgfSlcbiAgICB0aGlzLm9uT3BlbigoKSA9PiB7XG4gICAgICBlc3RhYmxpc2hlZCA9IHRydWVcbiAgICAgIGlmKCFwcmltYXJ5VHJhbnNwb3J0KXtcbiAgICAgICAgLy8gb25seSBtZW1vcml6ZSBMUCBpZiB3ZSBuZXZlciBjb25uZWN0ZWQgdG8gcHJpbWFyeVxuICAgICAgICBpZighdGhpcy5wcmltYXJ5UGFzc2VkSGVhbHRoQ2hlY2speyB0aGlzLnN0b3JlU2Vzc2lvbihgcGh4OmZhbGxiYWNrOiR7ZmFsbGJhY2tUcmFuc3BvcnQubmFtZX1gLCBcInRydWVcIikgfVxuICAgICAgICByZXR1cm4gdGhpcy5sb2coXCJ0cmFuc3BvcnRcIiwgYGVzdGFibGlzaGVkICR7ZmFsbGJhY2tUcmFuc3BvcnQubmFtZX0gZmFsbGJhY2tgKVxuICAgICAgfVxuICAgICAgLy8gaWYgd2UndmUgZXN0YWJsaXNoZWQgcHJpbWFyeSwgZ2l2ZSB0aGUgZmFsbGJhY2sgYSBuZXcgcGVyaW9kIHRvIGF0dGVtcHQgcGluZ1xuICAgICAgY2xlYXJUaW1lb3V0KHRoaXMuZmFsbGJhY2tUaW1lcilcbiAgICAgIHRoaXMuZmFsbGJhY2tUaW1lciA9IHNldFRpbWVvdXQoZmFsbGJhY2ssIGZhbGxiYWNrVGhyZXNob2xkKVxuICAgICAgdGhpcy5waW5nKHJ0dCA9PiB7XG4gICAgICAgIHRoaXMubG9nKFwidHJhbnNwb3J0XCIsIFwiY29ubmVjdGVkIHRvIHByaW1hcnkgYWZ0ZXJcIiwgcnR0KVxuICAgICAgICB0aGlzLnByaW1hcnlQYXNzZWRIZWFsdGhDaGVjayA9IHRydWVcbiAgICAgICAgY2xlYXJUaW1lb3V0KHRoaXMuZmFsbGJhY2tUaW1lcilcbiAgICAgIH0pXG4gICAgfSlcbiAgICB0aGlzLnRyYW5zcG9ydENvbm5lY3QoKVxuICB9XG5cbiAgY2xlYXJIZWFydGJlYXRzKCl7XG4gICAgY2xlYXJUaW1lb3V0KHRoaXMuaGVhcnRiZWF0VGltZXIpXG4gICAgY2xlYXJUaW1lb3V0KHRoaXMuaGVhcnRiZWF0VGltZW91dFRpbWVyKVxuICB9XG5cbiAgb25Db25uT3Blbigpe1xuICAgIGlmKHRoaXMuaGFzTG9nZ2VyKCkpIHRoaXMubG9nKFwidHJhbnNwb3J0XCIsIGAke3RoaXMudHJhbnNwb3J0Lm5hbWV9IGNvbm5lY3RlZCB0byAke3RoaXMuZW5kUG9pbnRVUkwoKX1gKVxuICAgIHRoaXMuY2xvc2VXYXNDbGVhbiA9IGZhbHNlXG4gICAgdGhpcy5kaXNjb25uZWN0aW5nID0gZmFsc2VcbiAgICB0aGlzLmVzdGFibGlzaGVkQ29ubmVjdGlvbnMrK1xuICAgIHRoaXMuZmx1c2hTZW5kQnVmZmVyKClcbiAgICB0aGlzLnJlY29ubmVjdFRpbWVyLnJlc2V0KClcbiAgICB0aGlzLnJlc2V0SGVhcnRiZWF0KClcbiAgICB0aGlzLnN0YXRlQ2hhbmdlQ2FsbGJhY2tzLm9wZW4uZm9yRWFjaCgoWywgY2FsbGJhY2tdKSA9PiBjYWxsYmFjaygpKVxuICB9XG5cbiAgLyoqXG4gICAqIEBwcml2YXRlXG4gICAqL1xuXG4gIGhlYXJ0YmVhdFRpbWVvdXQoKXtcbiAgICBpZih0aGlzLnBlbmRpbmdIZWFydGJlYXRSZWYpe1xuICAgICAgdGhpcy5wZW5kaW5nSGVhcnRiZWF0UmVmID0gbnVsbFxuICAgICAgaWYodGhpcy5oYXNMb2dnZXIoKSl7IHRoaXMubG9nKFwidHJhbnNwb3J0XCIsIFwiaGVhcnRiZWF0IHRpbWVvdXQuIEF0dGVtcHRpbmcgdG8gcmUtZXN0YWJsaXNoIGNvbm5lY3Rpb25cIikgfVxuICAgICAgdGhpcy50cmlnZ2VyQ2hhbkVycm9yKClcbiAgICAgIHRoaXMuY2xvc2VXYXNDbGVhbiA9IGZhbHNlXG4gICAgICB0aGlzLnRlYXJkb3duKCgpID0+IHRoaXMucmVjb25uZWN0VGltZXIuc2NoZWR1bGVUaW1lb3V0KCksIFdTX0NMT1NFX05PUk1BTCwgXCJoZWFydGJlYXQgdGltZW91dFwiKVxuICAgIH1cbiAgfVxuXG4gIHJlc2V0SGVhcnRiZWF0KCl7XG4gICAgaWYodGhpcy5jb25uICYmIHRoaXMuY29ubi5za2lwSGVhcnRiZWF0KXsgcmV0dXJuIH1cbiAgICB0aGlzLnBlbmRpbmdIZWFydGJlYXRSZWYgPSBudWxsXG4gICAgdGhpcy5jbGVhckhlYXJ0YmVhdHMoKVxuICAgIHRoaXMuaGVhcnRiZWF0VGltZXIgPSBzZXRUaW1lb3V0KCgpID0+IHRoaXMuc2VuZEhlYXJ0YmVhdCgpLCB0aGlzLmhlYXJ0YmVhdEludGVydmFsTXMpXG4gIH1cblxuICB0ZWFyZG93bihjYWxsYmFjaywgY29kZSwgcmVhc29uKXtcbiAgICBpZighdGhpcy5jb25uKXtcbiAgICAgIHJldHVybiBjYWxsYmFjayAmJiBjYWxsYmFjaygpXG4gICAgfVxuICAgIGxldCBjb25uZWN0Q2xvY2sgPSB0aGlzLmNvbm5lY3RDbG9ja1xuXG4gICAgdGhpcy53YWl0Rm9yQnVmZmVyRG9uZSgoKSA9PiB7XG4gICAgICBpZihjb25uZWN0Q2xvY2sgIT09IHRoaXMuY29ubmVjdENsb2NrKXsgcmV0dXJuIH1cbiAgICAgIGlmKHRoaXMuY29ubil7XG4gICAgICAgIGlmKGNvZGUpeyB0aGlzLmNvbm4uY2xvc2UoY29kZSwgcmVhc29uIHx8IFwiXCIpIH0gZWxzZSB7IHRoaXMuY29ubi5jbG9zZSgpIH1cbiAgICAgIH1cblxuICAgICAgdGhpcy53YWl0Rm9yU29ja2V0Q2xvc2VkKCgpID0+IHtcbiAgICAgICAgaWYoY29ubmVjdENsb2NrICE9PSB0aGlzLmNvbm5lY3RDbG9jayl7IHJldHVybiB9XG4gICAgICAgIGlmKHRoaXMuY29ubil7XG4gICAgICAgICAgdGhpcy5jb25uLm9ub3BlbiA9IGZ1bmN0aW9uICgpeyB9IC8vIG5vb3BcbiAgICAgICAgICB0aGlzLmNvbm4ub25lcnJvciA9IGZ1bmN0aW9uICgpeyB9IC8vIG5vb3BcbiAgICAgICAgICB0aGlzLmNvbm4ub25tZXNzYWdlID0gZnVuY3Rpb24gKCl7IH0gLy8gbm9vcFxuICAgICAgICAgIHRoaXMuY29ubi5vbmNsb3NlID0gZnVuY3Rpb24gKCl7IH0gLy8gbm9vcFxuICAgICAgICAgIHRoaXMuY29ubiA9IG51bGxcbiAgICAgICAgfVxuXG4gICAgICAgIGNhbGxiYWNrICYmIGNhbGxiYWNrKClcbiAgICAgIH0pXG4gICAgfSlcbiAgfVxuXG4gIHdhaXRGb3JCdWZmZXJEb25lKGNhbGxiYWNrLCB0cmllcyA9IDEpe1xuICAgIGlmKHRyaWVzID09PSA1IHx8ICF0aGlzLmNvbm4gfHwgIXRoaXMuY29ubi5idWZmZXJlZEFtb3VudCl7XG4gICAgICBjYWxsYmFjaygpXG4gICAgICByZXR1cm5cbiAgICB9XG5cbiAgICBzZXRUaW1lb3V0KCgpID0+IHtcbiAgICAgIHRoaXMud2FpdEZvckJ1ZmZlckRvbmUoY2FsbGJhY2ssIHRyaWVzICsgMSlcbiAgICB9LCAxNTAgKiB0cmllcylcbiAgfVxuXG4gIHdhaXRGb3JTb2NrZXRDbG9zZWQoY2FsbGJhY2ssIHRyaWVzID0gMSl7XG4gICAgaWYodHJpZXMgPT09IDUgfHwgIXRoaXMuY29ubiB8fCB0aGlzLmNvbm4ucmVhZHlTdGF0ZSA9PT0gU09DS0VUX1NUQVRFUy5jbG9zZWQpe1xuICAgICAgY2FsbGJhY2soKVxuICAgICAgcmV0dXJuXG4gICAgfVxuXG4gICAgc2V0VGltZW91dCgoKSA9PiB7XG4gICAgICB0aGlzLndhaXRGb3JTb2NrZXRDbG9zZWQoY2FsbGJhY2ssIHRyaWVzICsgMSlcbiAgICB9LCAxNTAgKiB0cmllcylcbiAgfVxuXG4gIG9uQ29ubkNsb3NlKGV2ZW50KXtcbiAgICBsZXQgY2xvc2VDb2RlID0gZXZlbnQgJiYgZXZlbnQuY29kZVxuICAgIGlmKHRoaXMuaGFzTG9nZ2VyKCkpIHRoaXMubG9nKFwidHJhbnNwb3J0XCIsIFwiY2xvc2VcIiwgZXZlbnQpXG4gICAgdGhpcy50cmlnZ2VyQ2hhbkVycm9yKClcbiAgICB0aGlzLmNsZWFySGVhcnRiZWF0cygpXG4gICAgaWYoIXRoaXMuY2xvc2VXYXNDbGVhbiAmJiBjbG9zZUNvZGUgIT09IDEwMDApe1xuICAgICAgdGhpcy5yZWNvbm5lY3RUaW1lci5zY2hlZHVsZVRpbWVvdXQoKVxuICAgIH1cbiAgICB0aGlzLnN0YXRlQ2hhbmdlQ2FsbGJhY2tzLmNsb3NlLmZvckVhY2goKFssIGNhbGxiYWNrXSkgPT4gY2FsbGJhY2soZXZlbnQpKVxuICB9XG5cbiAgLyoqXG4gICAqIEBwcml2YXRlXG4gICAqL1xuICBvbkNvbm5FcnJvcihlcnJvcil7XG4gICAgaWYodGhpcy5oYXNMb2dnZXIoKSkgdGhpcy5sb2coXCJ0cmFuc3BvcnRcIiwgZXJyb3IpXG4gICAgbGV0IHRyYW5zcG9ydEJlZm9yZSA9IHRoaXMudHJhbnNwb3J0XG4gICAgbGV0IGVzdGFibGlzaGVkQmVmb3JlID0gdGhpcy5lc3RhYmxpc2hlZENvbm5lY3Rpb25zXG4gICAgdGhpcy5zdGF0ZUNoYW5nZUNhbGxiYWNrcy5lcnJvci5mb3JFYWNoKChbLCBjYWxsYmFja10pID0+IHtcbiAgICAgIGNhbGxiYWNrKGVycm9yLCB0cmFuc3BvcnRCZWZvcmUsIGVzdGFibGlzaGVkQmVmb3JlKVxuICAgIH0pXG4gICAgaWYodHJhbnNwb3J0QmVmb3JlID09PSB0aGlzLnRyYW5zcG9ydCB8fCBlc3RhYmxpc2hlZEJlZm9yZSA+IDApe1xuICAgICAgdGhpcy50cmlnZ2VyQ2hhbkVycm9yKClcbiAgICB9XG4gIH1cblxuICAvKipcbiAgICogQHByaXZhdGVcbiAgICovXG4gIHRyaWdnZXJDaGFuRXJyb3IoKXtcbiAgICB0aGlzLmNoYW5uZWxzLmZvckVhY2goY2hhbm5lbCA9PiB7XG4gICAgICBpZighKGNoYW5uZWwuaXNFcnJvcmVkKCkgfHwgY2hhbm5lbC5pc0xlYXZpbmcoKSB8fCBjaGFubmVsLmlzQ2xvc2VkKCkpKXtcbiAgICAgICAgY2hhbm5lbC50cmlnZ2VyKENIQU5ORUxfRVZFTlRTLmVycm9yKVxuICAgICAgfVxuICAgIH0pXG4gIH1cblxuICAvKipcbiAgICogQHJldHVybnMge3N0cmluZ31cbiAgICovXG4gIGNvbm5lY3Rpb25TdGF0ZSgpe1xuICAgIHN3aXRjaCh0aGlzLmNvbm4gJiYgdGhpcy5jb25uLnJlYWR5U3RhdGUpe1xuICAgICAgY2FzZSBTT0NLRVRfU1RBVEVTLmNvbm5lY3Rpbmc6IHJldHVybiBcImNvbm5lY3RpbmdcIlxuICAgICAgY2FzZSBTT0NLRVRfU1RBVEVTLm9wZW46IHJldHVybiBcIm9wZW5cIlxuICAgICAgY2FzZSBTT0NLRVRfU1RBVEVTLmNsb3Npbmc6IHJldHVybiBcImNsb3NpbmdcIlxuICAgICAgZGVmYXVsdDogcmV0dXJuIFwiY2xvc2VkXCJcbiAgICB9XG4gIH1cblxuICAvKipcbiAgICogQHJldHVybnMge2Jvb2xlYW59XG4gICAqL1xuICBpc0Nvbm5lY3RlZCgpeyByZXR1cm4gdGhpcy5jb25uZWN0aW9uU3RhdGUoKSA9PT0gXCJvcGVuXCIgfVxuXG4gIC8qKlxuICAgKiBAcHJpdmF0ZVxuICAgKlxuICAgKiBAcGFyYW0ge0NoYW5uZWx9XG4gICAqL1xuICByZW1vdmUoY2hhbm5lbCl7XG4gICAgdGhpcy5vZmYoY2hhbm5lbC5zdGF0ZUNoYW5nZVJlZnMpXG4gICAgdGhpcy5jaGFubmVscyA9IHRoaXMuY2hhbm5lbHMuZmlsdGVyKGMgPT4gYyAhPT0gY2hhbm5lbClcbiAgfVxuXG4gIC8qKlxuICAgKiBSZW1vdmVzIGBvbk9wZW5gLCBgb25DbG9zZWAsIGBvbkVycm9yLGAgYW5kIGBvbk1lc3NhZ2VgIHJlZ2lzdHJhdGlvbnMuXG4gICAqXG4gICAqIEBwYXJhbSB7cmVmc30gLSBsaXN0IG9mIHJlZnMgcmV0dXJuZWQgYnkgY2FsbHMgdG9cbiAgICogICAgICAgICAgICAgICAgIGBvbk9wZW5gLCBgb25DbG9zZWAsIGBvbkVycm9yLGAgYW5kIGBvbk1lc3NhZ2VgXG4gICAqL1xuICBvZmYocmVmcyl7XG4gICAgZm9yKGxldCBrZXkgaW4gdGhpcy5zdGF0ZUNoYW5nZUNhbGxiYWNrcyl7XG4gICAgICB0aGlzLnN0YXRlQ2hhbmdlQ2FsbGJhY2tzW2tleV0gPSB0aGlzLnN0YXRlQ2hhbmdlQ2FsbGJhY2tzW2tleV0uZmlsdGVyKChbcmVmXSkgPT4ge1xuICAgICAgICByZXR1cm4gcmVmcy5pbmRleE9mKHJlZikgPT09IC0xXG4gICAgICB9KVxuICAgIH1cbiAgfVxuXG4gIC8qKlxuICAgKiBJbml0aWF0ZXMgYSBuZXcgY2hhbm5lbCBmb3IgdGhlIGdpdmVuIHRvcGljXG4gICAqXG4gICAqIEBwYXJhbSB7c3RyaW5nfSB0b3BpY1xuICAgKiBAcGFyYW0ge09iamVjdH0gY2hhblBhcmFtcyAtIFBhcmFtZXRlcnMgZm9yIHRoZSBjaGFubmVsXG4gICAqIEByZXR1cm5zIHtDaGFubmVsfVxuICAgKi9cbiAgY2hhbm5lbCh0b3BpYywgY2hhblBhcmFtcyA9IHt9KXtcbiAgICBsZXQgY2hhbiA9IG5ldyBDaGFubmVsKHRvcGljLCBjaGFuUGFyYW1zLCB0aGlzKVxuICAgIHRoaXMuY2hhbm5lbHMucHVzaChjaGFuKVxuICAgIHJldHVybiBjaGFuXG4gIH1cblxuICAvKipcbiAgICogQHBhcmFtIHtPYmplY3R9IGRhdGFcbiAgICovXG4gIHB1c2goZGF0YSl7XG4gICAgaWYodGhpcy5oYXNMb2dnZXIoKSl7XG4gICAgICBsZXQge3RvcGljLCBldmVudCwgcGF5bG9hZCwgcmVmLCBqb2luX3JlZn0gPSBkYXRhXG4gICAgICB0aGlzLmxvZyhcInB1c2hcIiwgYCR7dG9waWN9ICR7ZXZlbnR9ICgke2pvaW5fcmVmfSwgJHtyZWZ9KWAsIHBheWxvYWQpXG4gICAgfVxuXG4gICAgaWYodGhpcy5pc0Nvbm5lY3RlZCgpKXtcbiAgICAgIHRoaXMuZW5jb2RlKGRhdGEsIHJlc3VsdCA9PiB0aGlzLmNvbm4uc2VuZChyZXN1bHQpKVxuICAgIH0gZWxzZSB7XG4gICAgICB0aGlzLnNlbmRCdWZmZXIucHVzaCgoKSA9PiB0aGlzLmVuY29kZShkYXRhLCByZXN1bHQgPT4gdGhpcy5jb25uLnNlbmQocmVzdWx0KSkpXG4gICAgfVxuICB9XG5cbiAgLyoqXG4gICAqIFJldHVybiB0aGUgbmV4dCBtZXNzYWdlIHJlZiwgYWNjb3VudGluZyBmb3Igb3ZlcmZsb3dzXG4gICAqIEByZXR1cm5zIHtzdHJpbmd9XG4gICAqL1xuICBtYWtlUmVmKCl7XG4gICAgbGV0IG5ld1JlZiA9IHRoaXMucmVmICsgMVxuICAgIGlmKG5ld1JlZiA9PT0gdGhpcy5yZWYpeyB0aGlzLnJlZiA9IDAgfSBlbHNlIHsgdGhpcy5yZWYgPSBuZXdSZWYgfVxuXG4gICAgcmV0dXJuIHRoaXMucmVmLnRvU3RyaW5nKClcbiAgfVxuXG4gIHNlbmRIZWFydGJlYXQoKXtcbiAgICBpZih0aGlzLnBlbmRpbmdIZWFydGJlYXRSZWYgJiYgIXRoaXMuaXNDb25uZWN0ZWQoKSl7IHJldHVybiB9XG4gICAgdGhpcy5wZW5kaW5nSGVhcnRiZWF0UmVmID0gdGhpcy5tYWtlUmVmKClcbiAgICB0aGlzLnB1c2goe3RvcGljOiBcInBob2VuaXhcIiwgZXZlbnQ6IFwiaGVhcnRiZWF0XCIsIHBheWxvYWQ6IHt9LCByZWY6IHRoaXMucGVuZGluZ0hlYXJ0YmVhdFJlZn0pXG4gICAgdGhpcy5oZWFydGJlYXRUaW1lb3V0VGltZXIgPSBzZXRUaW1lb3V0KCgpID0+IHRoaXMuaGVhcnRiZWF0VGltZW91dCgpLCB0aGlzLmhlYXJ0YmVhdEludGVydmFsTXMpXG4gIH1cblxuICBmbHVzaFNlbmRCdWZmZXIoKXtcbiAgICBpZih0aGlzLmlzQ29ubmVjdGVkKCkgJiYgdGhpcy5zZW5kQnVmZmVyLmxlbmd0aCA+IDApe1xuICAgICAgdGhpcy5zZW5kQnVmZmVyLmZvckVhY2goY2FsbGJhY2sgPT4gY2FsbGJhY2soKSlcbiAgICAgIHRoaXMuc2VuZEJ1ZmZlciA9IFtdXG4gICAgfVxuICB9XG5cbiAgb25Db25uTWVzc2FnZShyYXdNZXNzYWdlKXtcbiAgICB0aGlzLmRlY29kZShyYXdNZXNzYWdlLmRhdGEsIG1zZyA9PiB7XG4gICAgICBsZXQge3RvcGljLCBldmVudCwgcGF5bG9hZCwgcmVmLCBqb2luX3JlZn0gPSBtc2dcbiAgICAgIGlmKHJlZiAmJiByZWYgPT09IHRoaXMucGVuZGluZ0hlYXJ0YmVhdFJlZil7XG4gICAgICAgIHRoaXMuY2xlYXJIZWFydGJlYXRzKClcbiAgICAgICAgdGhpcy5wZW5kaW5nSGVhcnRiZWF0UmVmID0gbnVsbFxuICAgICAgICB0aGlzLmhlYXJ0YmVhdFRpbWVyID0gc2V0VGltZW91dCgoKSA9PiB0aGlzLnNlbmRIZWFydGJlYXQoKSwgdGhpcy5oZWFydGJlYXRJbnRlcnZhbE1zKVxuICAgICAgfVxuXG4gICAgICBpZih0aGlzLmhhc0xvZ2dlcigpKSB0aGlzLmxvZyhcInJlY2VpdmVcIiwgYCR7cGF5bG9hZC5zdGF0dXMgfHwgXCJcIn0gJHt0b3BpY30gJHtldmVudH0gJHtyZWYgJiYgXCIoXCIgKyByZWYgKyBcIilcIiB8fCBcIlwifWAsIHBheWxvYWQpXG5cbiAgICAgIGZvcihsZXQgaSA9IDA7IGkgPCB0aGlzLmNoYW5uZWxzLmxlbmd0aDsgaSsrKXtcbiAgICAgICAgY29uc3QgY2hhbm5lbCA9IHRoaXMuY2hhbm5lbHNbaV1cbiAgICAgICAgaWYoIWNoYW5uZWwuaXNNZW1iZXIodG9waWMsIGV2ZW50LCBwYXlsb2FkLCBqb2luX3JlZikpeyBjb250aW51ZSB9XG4gICAgICAgIGNoYW5uZWwudHJpZ2dlcihldmVudCwgcGF5bG9hZCwgcmVmLCBqb2luX3JlZilcbiAgICAgIH1cblxuICAgICAgZm9yKGxldCBpID0gMDsgaSA8IHRoaXMuc3RhdGVDaGFuZ2VDYWxsYmFja3MubWVzc2FnZS5sZW5ndGg7IGkrKyl7XG4gICAgICAgIGxldCBbLCBjYWxsYmFja10gPSB0aGlzLnN0YXRlQ2hhbmdlQ2FsbGJhY2tzLm1lc3NhZ2VbaV1cbiAgICAgICAgY2FsbGJhY2sobXNnKVxuICAgICAgfVxuICAgIH0pXG4gIH1cblxuICBsZWF2ZU9wZW5Ub3BpYyh0b3BpYyl7XG4gICAgbGV0IGR1cENoYW5uZWwgPSB0aGlzLmNoYW5uZWxzLmZpbmQoYyA9PiBjLnRvcGljID09PSB0b3BpYyAmJiAoYy5pc0pvaW5lZCgpIHx8IGMuaXNKb2luaW5nKCkpKVxuICAgIGlmKGR1cENoYW5uZWwpe1xuICAgICAgaWYodGhpcy5oYXNMb2dnZXIoKSkgdGhpcy5sb2coXCJ0cmFuc3BvcnRcIiwgYGxlYXZpbmcgZHVwbGljYXRlIHRvcGljIFwiJHt0b3BpY31cImApXG4gICAgICBkdXBDaGFubmVsLmxlYXZlKClcbiAgICB9XG4gIH1cbn1cbiIsICIvLyBEYXJrIFBvb2wgXHUyMDE0IGNvbXBsZXRlIGdhbWUgY2xpZW50XG4vLyBIYW5kbGVzIGxvYmJ5IHJlYWwtdGltZSB1cGRhdGVzLCBtYXRjaCBnYW1lcGxheSwgY2hhcnRzLCBhbmQgcmljaCBVSS5cblxuaW1wb3J0IHsgU29ja2V0IH0gZnJvbSBcInBob2VuaXhcIjtcblxuLy8gXHUyNTAwXHUyNTAwXHUyNTAwIEJvb3QgXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXG5jb25zdCBjb25maWcgPSB3aW5kb3cuRGFya1Bvb2wgfHwge307XG5cbmZ1bmN0aW9uIGJvb3QoY2ZnKSB7XG4gIGlmICghY2ZnLnNvY2tldFRva2VuKSB7XG4gICAgY29uc29sZS5kZWJ1ZyhcIltEYXJrUG9vbF0gTm8gc29ja2V0IHRva2VuIGZvdW5kIFx1MjAxNCBXZWJTb2NrZXQgbm90IHN0YXJ0ZWQuXCIpO1xuICAgIHJldHVybjtcbiAgfVxuXG4gIGNvbnN0IHNvY2tldCA9IG5ldyBTb2NrZXQoXCIvc29ja2V0XCIsIHtcbiAgICBwYXJhbXM6IHsgdG9rZW46IGNmZy5zb2NrZXRUb2tlbiB9LFxuICB9KTtcblxuICBzb2NrZXQuY29ubmVjdCgpO1xuXG4gIGNvbnN0IGxvYmJ5TWdyID0gbmV3IExvYmJ5TWFuYWdlcihzb2NrZXQsIGNmZyk7XG4gIGxvYmJ5TWdyLmluaXQoKTtcblxuICBpZiAoY2ZnLm1hdGNoSWQpIHtcbiAgICBjb25zdCBtYXRjaE1nciA9IG5ldyBNYXRjaE1hbmFnZXIoc29ja2V0LCBjZmcsIGxvYmJ5TWdyKTtcbiAgICBtYXRjaE1nci5pbml0KCk7XG4gIH1cbn1cblxuLy8gXHUyNTAwXHUyNTAwXHUyNTAwIFRvYXN0IFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFxuY29uc3QgVG9hc3QgPSB7XG4gIHNob3cobXNnLCB0eXBlID0gXCJpbmZvXCIsIGR1cmF0aW9uID0gNDAwMCkge1xuICAgIGNvbnN0IGljb25zID0ge1xuICAgICAgaW5mbzogXCJcdTIxMzlcIixcbiAgICAgIHN1Y2Nlc3M6IFwiXHUyNzEzXCIsXG4gICAgICB3YXJuaW5nOiBcIlx1MjZBMFwiLFxuICAgICAgZXJyb3I6IFwiXHUyNzE1XCIsXG4gICAgfTtcbiAgICBjb25zdCBjb250YWluZXIgPSBkb2N1bWVudC5nZXRFbGVtZW50QnlJZChcInRvYXN0LWNvbnRhaW5lclwiKTtcbiAgICBpZiAoIWNvbnRhaW5lcikgcmV0dXJuO1xuXG4gICAgY29uc3QgZWwgPSBkb2N1bWVudC5jcmVhdGVFbGVtZW50KFwiZGl2XCIpO1xuICAgIGVsLmNsYXNzTmFtZSA9IGB0b2FzdCAke3R5cGV9YDtcbiAgICBlbC5pbm5lckhUTUwgPSBgXG4gICAgICA8c3BhbiBjbGFzcz1cInRleHQtYmFzZSBsZWFkaW5nLW5vbmVcIiBzdHlsZT1cImZsZXgtc2hyaW5rOjBcIj4ke2ljb25zW3R5cGVdIHx8IFwiXHUyMDIyXCJ9PC9zcGFuPlxuICAgICAgPHNwYW4gY2xhc3M9XCJmbGV4LTEgdGV4dC1ncmF5LTIwMFwiPiR7ZXNjYXBlSHRtbChtc2cpfTwvc3Bhbj5cbiAgICBgO1xuICAgIGNvbnRhaW5lci5hcHBlbmRDaGlsZChlbCk7XG5cbiAgICBzZXRUaW1lb3V0KCgpID0+IHtcbiAgICAgIGVsLnN0eWxlLmFuaW1hdGlvbiA9IFwic2xpZGUtb3V0LXJpZ2h0IDAuMjVzIGVhc2UgZm9yd2FyZHNcIjtcbiAgICAgIGVsLmFkZEV2ZW50TGlzdGVuZXIoXCJhbmltYXRpb25lbmRcIiwgKCkgPT4gZWwucmVtb3ZlKCksIHsgb25jZTogdHJ1ZSB9KTtcbiAgICB9LCBkdXJhdGlvbik7XG5cbiAgICBlbC5hZGRFdmVudExpc3RlbmVyKFwiY2xpY2tcIiwgKCkgPT4gZWwucmVtb3ZlKCkpO1xuICB9LFxufTtcblxuLy8gXHUyNTAwXHUyNTAwXHUyNTAwIFNwYXJrbGluZSBDaGFydCBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcbmNsYXNzIFNwYXJrbGluZUNoYXJ0IHtcbiAgY29uc3RydWN0b3IoY29udGFpbmVySWQsIG9wdGlvbnMgPSB7fSkge1xuICAgIHRoaXMuY29udGFpbmVyID0gZG9jdW1lbnQuZ2V0RWxlbWVudEJ5SWQoY29udGFpbmVySWQpO1xuICAgIHRoaXMuaGlzdG9yeSA9IFtdO1xuICAgIHRoaXMubWF4UG9pbnRzID0gb3B0aW9ucy5tYXhQb2ludHMgfHwgMjA7XG4gICAgdGhpcy53ID0gb3B0aW9ucy53aWR0aCB8fCA4MDtcbiAgICB0aGlzLmggPSBvcHRpb25zLmhlaWdodCB8fCAyODtcbiAgICBpZiAodGhpcy5jb250YWluZXIpIHRoaXMuX2NyZWF0ZVN2ZygpO1xuICB9XG5cbiAgX2NyZWF0ZVN2ZygpIHtcbiAgICB0aGlzLnN2ZyA9IGRvY3VtZW50LmNyZWF0ZUVsZW1lbnROUyhcImh0dHA6Ly93d3cudzMub3JnLzIwMDAvc3ZnXCIsIFwic3ZnXCIpO1xuICAgIHRoaXMuc3ZnLnNldEF0dHJpYnV0ZShcInZpZXdCb3hcIiwgYDAgMCAke3RoaXMud30gJHt0aGlzLmh9YCk7XG4gICAgdGhpcy5zdmcuc2V0QXR0cmlidXRlKFwid2lkdGhcIiwgdGhpcy53KTtcbiAgICB0aGlzLnN2Zy5zZXRBdHRyaWJ1dGUoXCJoZWlnaHRcIiwgdGhpcy5oKTtcbiAgICB0aGlzLnN2Zy5jbGFzc0xpc3QuYWRkKFwic3BhcmtsaW5lXCIpO1xuXG4gICAgY29uc3QgZ3JhZElkID0gYHNnLSR7TWF0aC5yYW5kb20oKS50b1N0cmluZygzNikuc2xpY2UoMil9YDtcbiAgICB0aGlzLnN2Zy5pbm5lckhUTUwgPSBgXG4gICAgICA8ZGVmcz5cbiAgICAgICAgPGxpbmVhckdyYWRpZW50IGlkPVwiJHtncmFkSWR9XCIgeDE9XCIwXCIgeTE9XCIwXCIgeDI9XCIwXCIgeTI9XCIxXCI+XG4gICAgICAgICAgPHN0b3Agb2Zmc2V0PVwiMCVcIiBzdG9wLWNvbG9yPVwidmFyKC0tc3BhcmstY29sb3IsIzEwYjk4MSlcIiBzdG9wLW9wYWNpdHk9XCIwLjRcIi8+XG4gICAgICAgICAgPHN0b3Agb2Zmc2V0PVwiMTAwJVwiIHN0b3AtY29sb3I9XCJ2YXIoLS1zcGFyay1jb2xvciwjMTBiOTgxKVwiIHN0b3Atb3BhY2l0eT1cIjBcIi8+XG4gICAgICAgIDwvbGluZWFyR3JhZGllbnQ+XG4gICAgICA8L2RlZnM+XG4gICAgICA8cGF0aCBjbGFzcz1cImFyZWFcIiBmaWxsPVwidXJsKCMke2dyYWRJZH0pXCIvPlxuICAgICAgPHBvbHlsaW5lIGNsYXNzPVwibGluZVwiIHN0eWxlPVwic3Ryb2tlOnZhcigtLXNwYXJrLWNvbG9yLCMxMGI5ODEpXCIvPlxuICAgIGA7XG4gICAgdGhpcy5fZ3JhZElkID0gZ3JhZElkO1xuICAgIHRoaXMuY29udGFpbmVyLmFwcGVuZENoaWxkKHRoaXMuc3ZnKTtcbiAgfVxuXG4gIHB1c2gocHJpY2UpIHtcbiAgICBjb25zdCBwID0gcGFyc2VGbG9hdChwcmljZSk7XG4gICAgaWYgKGlzTmFOKHApKSByZXR1cm47XG4gICAgdGhpcy5oaXN0b3J5LnB1c2gocCk7XG4gICAgaWYgKHRoaXMuaGlzdG9yeS5sZW5ndGggPiB0aGlzLm1heFBvaW50cykgdGhpcy5oaXN0b3J5LnNoaWZ0KCk7XG4gICAgdGhpcy5fcmVuZGVyKCk7XG4gIH1cblxuICBzZXRDb2xvcihpc1VwKSB7XG4gICAgaWYgKCF0aGlzLnN2ZykgcmV0dXJuO1xuICAgIGNvbnN0IGMgPSBpc1VwID8gXCIjMTBiOTgxXCIgOiBcIiNlZjQ0NDRcIjtcbiAgICB0aGlzLnN2Zy5zdHlsZS5zZXRQcm9wZXJ0eShcIi0tc3BhcmstY29sb3JcIiwgYyk7XG4gICAgY29uc3Qgc3RvcCA9IHRoaXMuc3ZnLnF1ZXJ5U2VsZWN0b3IoYCMke3RoaXMuX2dyYWRJZH0gc3RvcGApO1xuICAgIGlmIChzdG9wKSBzdG9wLnNldEF0dHJpYnV0ZShcInN0b3AtY29sb3JcIiwgYyk7XG4gIH1cblxuICBfcmVuZGVyKCkge1xuICAgIGlmICghdGhpcy5zdmcgfHwgdGhpcy5oaXN0b3J5Lmxlbmd0aCA8IDIpIHJldHVybjtcbiAgICBjb25zdCBwdHMgPSB0aGlzLmhpc3Rvcnk7XG4gICAgY29uc3QgbWluID0gTWF0aC5taW4oLi4ucHRzKTtcbiAgICBjb25zdCBtYXggPSBNYXRoLm1heCguLi5wdHMpO1xuICAgIGNvbnN0IHJhbmdlID0gbWF4IC0gbWluIHx8IDE7XG4gICAgY29uc3QgcGFkID0gMjtcbiAgICBjb25zdCB4U3RlcCA9ICh0aGlzLncgLSBwYWQgKiAyKSAvIChwdHMubGVuZ3RoIC0gMSk7XG5cbiAgICBjb25zdCBwb2ludHMgPSBwdHMubWFwKCh2LCBpKSA9PiB7XG4gICAgICBjb25zdCB4ID0gcGFkICsgaSAqIHhTdGVwO1xuICAgICAgY29uc3QgeSA9IHBhZCArICgxIC0gKHYgLSBtaW4pIC8gcmFuZ2UpICogKHRoaXMuaCAtIHBhZCAqIDIpO1xuICAgICAgcmV0dXJuIGAke3gudG9GaXhlZCgxKX0sJHt5LnRvRml4ZWQoMSl9YDtcbiAgICB9KTtcblxuICAgIGNvbnN0IHBvbHlsaW5lID0gdGhpcy5zdmcucXVlcnlTZWxlY3RvcihcInBvbHlsaW5lLmxpbmVcIik7XG4gICAgY29uc3QgYXJlYSA9IHRoaXMuc3ZnLnF1ZXJ5U2VsZWN0b3IoXCJwYXRoLmFyZWFcIik7XG4gICAgaWYgKHBvbHlsaW5lKSBwb2x5bGluZS5zZXRBdHRyaWJ1dGUoXCJwb2ludHNcIiwgcG9pbnRzLmpvaW4oXCIgXCIpKTtcbiAgICBpZiAoYXJlYSkge1xuICAgICAgY29uc3QgZmlyc3QgPSBwb2ludHNbMF0uc3BsaXQoXCIsXCIpO1xuICAgICAgY29uc3QgbGFzdCA9IHBvaW50c1twb2ludHMubGVuZ3RoIC0gMV0uc3BsaXQoXCIsXCIpO1xuICAgICAgY29uc3QgZCA9IGBNJHtmaXJzdFswXX0sJHt0aGlzLmggLSBwYWR9IEwke3BvaW50cy5qb2luKFwiIExcIil9IEwke2xhc3RbMF19LCR7dGhpcy5oIC0gcGFkfSBaYDtcbiAgICAgIGFyZWEuc2V0QXR0cmlidXRlKFwiZFwiLCBkKTtcbiAgICB9XG4gIH1cbn1cblxuLy8gXHUyNTAwXHUyNTAwXHUyNTAwIExvYmJ5TWFuYWdlciBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcbmNsYXNzIExvYmJ5TWFuYWdlciB7XG4gIGNvbnN0cnVjdG9yKHNvY2tldCwgY2ZnKSB7XG4gICAgdGhpcy5zb2NrZXQgPSBzb2NrZXQ7XG4gICAgdGhpcy5jZmcgPSBjZmc7XG4gICAgdGhpcy5jaGFubmVsID0gbnVsbDtcbiAgICB0aGlzLnByZXNlbmNlID0gbnVsbDtcbiAgICB0aGlzLmpvaW5lZCA9IGZhbHNlO1xuICB9XG5cbiAgaW5pdCgpIHtcbiAgICB0aGlzLmNoYW5uZWwgPSB0aGlzLnNvY2tldC5jaGFubmVsKFwibG9iYnk6Z2VuZXJhbFwiLCB7fSk7XG5cbiAgICB0aGlzLmNoYW5uZWwub24oXCJvcGVuX21hdGNoZXNcIiwgKHApID0+IHRoaXMuX3VwZGF0ZU1hdGNoTGlzdChwLm1hdGNoZXMpKTtcbiAgICB0aGlzLmNoYW5uZWwub24oXCJtYXRjaF9jcmVhdGVkXCIsIChtYXRjaCkgPT4gdGhpcy5fYWRkT3JVcGRhdGVNYXRjaChtYXRjaCkpO1xuICAgIHRoaXMuY2hhbm5lbC5vbihcIm1hdGNoX3VwZGF0ZWRcIiwgKG1hdGNoKSA9PiB0aGlzLl9hZGRPclVwZGF0ZU1hdGNoKG1hdGNoKSk7XG4gICAgdGhpcy5jaGFubmVsLm9uKFwibWF0Y2hfc3RhcnRlZFwiLCAocCkgPT4gdGhpcy5fb25NYXRjaFN0YXJ0ZWQocC5tYXRjaF9pZCkpO1xuICAgIHRoaXMuY2hhbm5lbC5vbihcInByZXNlbmNlX3N0YXRlXCIsIChzdGF0ZSkgPT4gdGhpcy5fb25QcmVzZW5jZVN0YXRlKHN0YXRlKSk7XG4gICAgdGhpcy5jaGFubmVsLm9uKFwicHJlc2VuY2VfZGlmZlwiLCAoZGlmZikgPT4gdGhpcy5fb25QcmVzZW5jZURpZmYoZGlmZikpO1xuXG4gICAgdGhpcy5jaGFubmVsXG4gICAgICAuam9pbigpXG4gICAgICAucmVjZWl2ZShcIm9rXCIsICgpID0+IHtcbiAgICAgICAgdGhpcy5qb2luZWQgPSB0cnVlO1xuICAgICAgICB0aGlzLl93aXJlSm9pbkJ1dHRvbnMoZG9jdW1lbnQpO1xuICAgICAgfSlcbiAgICAgIC5yZWNlaXZlKFwiZXJyb3JcIiwgKGUpID0+IHtcbiAgICAgICAgY29uc29sZS53YXJuKFwiW0xvYmJ5XSBqb2luIGVycm9yXCIsIGUpO1xuICAgICAgICB0aGlzLl93aXJlSm9pbkJ1dHRvbnMoZG9jdW1lbnQpO1xuICAgICAgfSk7XG4gIH1cblxuICBfd2lyZUpvaW5CdXR0b25zKHJvb3QgPSBkb2N1bWVudCkge1xuICAgIHJvb3RcbiAgICAgIC5xdWVyeVNlbGVjdG9yQWxsKFwiLmpvaW4tbWF0Y2gtYnRuOm5vdChbZGF0YS13aXJlZF0pXCIpXG4gICAgICAuZm9yRWFjaCgoYnRuKSA9PiB7XG4gICAgICAgIGJ0bi5kYXRhc2V0LndpcmVkID0gXCIxXCI7XG4gICAgICAgIGJ0bi5hZGRFdmVudExpc3RlbmVyKFwiY2xpY2tcIiwgKGUpID0+IHtcbiAgICAgICAgICBlLnByZXZlbnREZWZhdWx0KCk7XG4gICAgICAgICAgY29uc3QgbWF0Y2hJZCA9XG4gICAgICAgICAgICBidG4uZGF0YXNldC5tYXRjaElkIHx8XG4gICAgICAgICAgICBidG4uY2xvc2VzdChcIltkYXRhLW1hdGNoLWlkXVwiKT8uZGF0YXNldC5tYXRjaElkO1xuICAgICAgICAgIGlmICghbWF0Y2hJZCkgcmV0dXJuO1xuICAgICAgICAgIHRoaXMuX2pvaW5NYXRjaChtYXRjaElkLCBidG4pO1xuICAgICAgICB9KTtcbiAgICAgIH0pO1xuICB9XG5cbiAgX2pvaW5NYXRjaChtYXRjaElkLCBidG4pIHtcbiAgICBidG4uZGlzYWJsZWQgPSB0cnVlO1xuICAgIGNvbnN0IG9yaWdUZXh0ID0gYnRuLnRleHRDb250ZW50O1xuICAgIGJ0bi50ZXh0Q29udGVudCA9IFwiSm9pbmluZy4uLlwiO1xuXG4gICAgLy8gUGhvZW5peCBjaGFubmVscyBidWZmZXIgcHVzaGVzIHdoaWxlIGpvaW5pbmcgXHUyMDE0IG5vIG5lZWQgdG8gZ2F0ZSBvbiB0aGlzLmpvaW5lZC5cbiAgICAvLyBJZiB0aGUgY2hhbm5lbCBpcyBub3QgeWV0IG9wZW4sIHRoZSBwdXNoIHdpbGwgYmUgZmx1c2hlZCBvbmNlIGl0IGNvbm5lY3RzLlxuICAgIHRoaXMuY2hhbm5lbFxuICAgICAgLnB1c2goXCJqb2luX21hdGNoXCIsIHsgbWF0Y2hfaWQ6IG1hdGNoSWQgfSlcbiAgICAgIC5yZWNlaXZlKFwib2tcIiwgKCkgPT4ge1xuICAgICAgICB3aW5kb3cubG9jYXRpb24gPSBgL21hdGNoZXMvJHttYXRjaElkfWA7XG4gICAgICB9KVxuICAgICAgLnJlY2VpdmUoXCJlcnJvclwiLCAoZXJyKSA9PiB7XG4gICAgICAgIGJ0bi5kaXNhYmxlZCA9IGZhbHNlO1xuICAgICAgICBidG4udGV4dENvbnRlbnQgPSBvcmlnVGV4dDtcbiAgICAgICAgY29uc3QgcmVhc29uID0gZXJyPy5yZWFzb24gfHwgSlNPTi5zdHJpbmdpZnkoZXJyKTtcbiAgICAgICAgVG9hc3Quc2hvdyhcIkNvdWxkIG5vdCBqb2luOiBcIiArIHJlYXNvbiwgXCJlcnJvclwiKTtcbiAgICAgIH0pXG4gICAgICAucmVjZWl2ZShcInRpbWVvdXRcIiwgKCkgPT4ge1xuICAgICAgICBidG4uZGlzYWJsZWQgPSBmYWxzZTtcbiAgICAgICAgYnRuLnRleHRDb250ZW50ID0gb3JpZ1RleHQ7XG4gICAgICAgIFRvYXN0LnNob3coXCJKb2luIHRpbWVkIG91dC4gUGxlYXNlIHRyeSBhZ2Fpbi5cIiwgXCJ3YXJuaW5nXCIpO1xuICAgICAgfSk7XG4gIH1cblxuICBfYWRkT3JVcGRhdGVNYXRjaChtYXRjaCkge1xuICAgIGNvbnN0IGxpc3QgPSBkb2N1bWVudC5nZXRFbGVtZW50QnlJZChcIm9wZW4tbWF0Y2hlcy1saXN0XCIpO1xuICAgIGlmICghbGlzdCkgcmV0dXJuO1xuXG4gICAgLy8gUmVtb3ZlIHBsYWNlaG9sZGVyXG4gICAgY29uc3QgcGxhY2Vob2xkZXIgPSBkb2N1bWVudC5nZXRFbGVtZW50QnlJZChcIm5vLW1hdGNoZXMtcGxhY2Vob2xkZXJcIik7XG4gICAgaWYgKHBsYWNlaG9sZGVyKSBwbGFjZWhvbGRlci5yZW1vdmUoKTtcblxuICAgIGNvbnN0IGV4aXN0aW5nID0gbGlzdC5xdWVyeVNlbGVjdG9yKGBbZGF0YS1tYXRjaC1pZD1cIiR7bWF0Y2guaWR9XCJdYCk7XG4gICAgY29uc3QgY2FyZCA9IHRoaXMuX2J1aWxkTWF0Y2hDYXJkKG1hdGNoKTtcblxuICAgIGlmIChleGlzdGluZykge1xuICAgICAgZXhpc3RpbmcucmVwbGFjZVdpdGgoY2FyZCk7XG4gICAgfSBlbHNlIHtcbiAgICAgIGxpc3QuaW5zZXJ0QWRqYWNlbnRFbGVtZW50KFwiYWZ0ZXJiZWdpblwiLCBjYXJkKTtcbiAgICB9XG5cbiAgICB0aGlzLl93aXJlSm9pbkJ1dHRvbnMobGlzdCk7XG4gIH1cblxuICBfYnVpbGRNYXRjaENhcmQobWF0Y2gpIHtcbiAgICBjb25zdCBkaXYgPSBkb2N1bWVudC5jcmVhdGVFbGVtZW50KFwiZGl2XCIpO1xuICAgIGRpdi5jbGFzc05hbWUgPSBcImNhcmQgY2FyZC1ob3ZlciBwLTQgbWF0Y2gtY2FyZCBmYWRlLWluLXVwXCI7XG4gICAgZGl2LmRhdGFzZXQubWF0Y2hJZCA9IG1hdGNoLmlkO1xuXG4gICAgY29uc3QgaXNGdWxsID0gbWF0Y2gucGxheWVyX2NvdW50ID49IG1hdGNoLm1heF9wbGF5ZXJzO1xuICAgIGNvbnN0IGlzTXlNYXRjaCA9IG1hdGNoLmhvc3RfdXNlcm5hbWUgPT09IHRoaXMuX215VXNlcm5hbWUoKTtcblxuICAgIGRpdi5pbm5lckhUTUwgPSBgXG4gICAgICA8ZGl2IGNsYXNzPVwiZmxleCBpdGVtcy1jZW50ZXIganVzdGlmeS1iZXR3ZWVuXCI+XG4gICAgICAgIDxkaXYgY2xhc3M9XCJmbGV4IGl0ZW1zLWNlbnRlciBnYXAtM1wiPlxuICAgICAgICAgIDxkaXYgY2xhc3M9XCJ3LTkgaC05IHJvdW5kZWQtbGcgYmctZW1lcmFsZC01MDAvMTAgYm9yZGVyIGJvcmRlci1lbWVyYWxkLTUwMC8yMCBmbGV4IGl0ZW1zLWNlbnRlciBqdXN0aWZ5LWNlbnRlciBzaHJpbmstMFwiPlxuICAgICAgICAgICAgPHN2ZyBjbGFzcz1cInctNCBoLTQgdGV4dC1lbWVyYWxkLTQwMFwiIGZpbGw9XCJub25lXCIgdmlld0JveD1cIjAgMCAyNCAyNFwiIHN0cm9rZT1cImN1cnJlbnRDb2xvclwiIHN0cm9rZS13aWR0aD1cIjJcIj5cbiAgICAgICAgICAgICAgPHBvbHlsaW5lIHBvaW50cz1cIjIyIDcgMTMuNSAxNS41IDguNSAxMC41IDIgMTdcIi8+XG4gICAgICAgICAgICA8L3N2Zz5cbiAgICAgICAgICA8L2Rpdj5cbiAgICAgICAgICA8ZGl2PlxuICAgICAgICAgICAgPGRpdiBjbGFzcz1cImZvbnQtc2VtaWJvbGQgdGV4dC1zbSB0ZXh0LXdoaXRlIGZvbnQtbW9ub1wiPiR7ZXNjYXBlSHRtbChtYXRjaC5uYW1lKX08L2Rpdj5cbiAgICAgICAgICAgIDxkaXYgY2xhc3M9XCJ0ZXh0LXhzIHRleHQtZ3JheS01MDAgbXQtMC41XCI+XG4gICAgICAgICAgICAgIGhvc3RlZCBieSA8c3BhbiBjbGFzcz1cInRleHQtZ3JheS00MDBcIj4ke2VzY2FwZUh0bWwobWF0Y2guaG9zdF91c2VybmFtZSB8fCBcIlx1MjAxNFwiKX08L3NwYW4+XG4gICAgICAgICAgICA8L2Rpdj5cbiAgICAgICAgICA8L2Rpdj5cbiAgICAgICAgPC9kaXY+XG4gICAgICAgIDxkaXYgY2xhc3M9XCJmbGV4IGl0ZW1zLWNlbnRlciBnYXAtM1wiPlxuICAgICAgICAgIDxkaXYgY2xhc3M9XCJ0ZXh0LXJpZ2h0XCI+XG4gICAgICAgICAgICA8ZGl2IGNsYXNzPVwidGV4dC14cyBmb250LW1vbm8gdGV4dC1ncmF5LTQwMFwiPlxuICAgICAgICAgICAgICA8c3BhbiBjbGFzcz1cInRleHQtd2hpdGUgZm9udC1zZW1pYm9sZFwiPiR7bWF0Y2gucGxheWVyX2NvdW50fTwvc3Bhbj4vJHttYXRjaC5tYXhfcGxheWVyc31cbiAgICAgICAgICAgIDwvZGl2PlxuICAgICAgICAgIDwvZGl2PlxuICAgICAgICAgICR7XG4gICAgICAgICAgICBpc0Z1bGxcbiAgICAgICAgICAgICAgPyBgPHNwYW4gY2xhc3M9XCJidG4tZ2hvc3QgdGV4dC14cyBweS0xLjUgcHgtMyBvcGFjaXR5LTQwIHBvaW50ZXItZXZlbnRzLW5vbmVcIj5GdWxsPC9zcGFuPmBcbiAgICAgICAgICAgICAgOiBgPGJ1dHRvbiB0eXBlPVwiYnV0dG9uXCIgZGF0YS1tYXRjaC1pZD1cIiR7ZXNjYXBlSHRtbChtYXRjaC5pZCl9XCIgY2xhc3M9XCJqb2luLW1hdGNoLWJ0biBidG4tcHJpbWFyeSB0ZXh0LXhzIHB5LTEuNSBweC00XCI+XG4gICAgICAgICAgICAgICAgICR7aXNNeU1hdGNoID8gXCJFbnRlclwiIDogXCJKb2luXCJ9XG4gICAgICAgICAgICAgICA8L2J1dHRvbj5gXG4gICAgICAgICAgfVxuICAgICAgICA8L2Rpdj5cbiAgICAgIDwvZGl2PlxuICAgIGA7XG4gICAgcmV0dXJuIGRpdjtcbiAgfVxuXG4gIF91cGRhdGVNYXRjaExpc3QobWF0Y2hlcykge1xuICAgIGNvbnN0IGxpc3QgPSBkb2N1bWVudC5nZXRFbGVtZW50QnlJZChcIm9wZW4tbWF0Y2hlcy1saXN0XCIpO1xuICAgIGlmICghbGlzdCkgcmV0dXJuO1xuICAgIGlmICghbWF0Y2hlcyB8fCBtYXRjaGVzLmxlbmd0aCA9PT0gMCkgcmV0dXJuO1xuXG4gICAgbGlzdC5pbm5lckhUTUwgPSBcIlwiO1xuICAgIG1hdGNoZXMuZm9yRWFjaCgobSkgPT4gbGlzdC5hcHBlbmRDaGlsZCh0aGlzLl9idWlsZE1hdGNoQ2FyZChtKSkpO1xuICAgIHRoaXMuX3dpcmVKb2luQnV0dG9ucyhsaXN0KTtcbiAgfVxuXG4gIF9vbk1hdGNoU3RhcnRlZChtYXRjaElkKSB7XG4gICAgLy8gUmVtb3ZlIHRoZSBjYXJkIGZyb20gdGhlIG9wZW4tbWF0Y2hlcyBsaXN0IHNpbmNlIGl0J3Mgbm8gbG9uZ2VyIHdhaXRpbmdcbiAgICBjb25zdCBjYXJkID0gZG9jdW1lbnQucXVlcnlTZWxlY3RvcihcbiAgICAgIGAjb3Blbi1tYXRjaGVzLWxpc3QgW2RhdGEtbWF0Y2gtaWQ9XCIke21hdGNoSWR9XCJdYCxcbiAgICApO1xuICAgIGlmIChjYXJkKSB7XG4gICAgICBjYXJkLnN0eWxlLmFuaW1hdGlvbiA9IFwic2xpZGUtb3V0LXJpZ2h0IDAuM3MgZWFzZSBmb3J3YXJkc1wiO1xuICAgICAgY2FyZC5hZGRFdmVudExpc3RlbmVyKFwiYW5pbWF0aW9uZW5kXCIsICgpID0+IGNhcmQucmVtb3ZlKCksIHtcbiAgICAgICAgb25jZTogdHJ1ZSxcbiAgICAgIH0pO1xuICAgIH1cbiAgfVxuXG4gIF9vblByZXNlbmNlU3RhdGUoc3RhdGUpIHtcbiAgICBjb25zdCBsaXN0ID0gZG9jdW1lbnQuZ2V0RWxlbWVudEJ5SWQoXCJsb2JieS1wcmVzZW5jZS1saXN0XCIpO1xuICAgIGNvbnN0IGNvdW50ZXIgPSBkb2N1bWVudC5nZXRFbGVtZW50QnlJZChcImxvYmJ5LW9ubGluZS1jb3VudFwiKTtcbiAgICBpZiAoIWxpc3QpIHJldHVybjtcblxuICAgIGNvbnN0IHVzZXJzID0gW107XG4gICAgT2JqZWN0LnZhbHVlcyhzdGF0ZSkuZm9yRWFjaCgoZW50cnkpID0+IHtcbiAgICAgIGNvbnN0IG1ldGEgPSBlbnRyeS5tZXRhcz8uWzBdO1xuICAgICAgaWYgKG1ldGE/LnVzZXJuYW1lKSB1c2Vycy5wdXNoKG1ldGEudXNlcm5hbWUpO1xuICAgIH0pO1xuXG4gICAgaWYgKGNvdW50ZXIpIGNvdW50ZXIudGV4dENvbnRlbnQgPSBgJHt1c2Vycy5sZW5ndGh9IG9ubGluZWA7XG4gICAgdGhpcy5fcmVuZGVyUHJlc2VuY2VMaXN0KGxpc3QsIHVzZXJzKTtcbiAgfVxuXG4gIF9vblByZXNlbmNlRGlmZihkaWZmKSB7XG4gICAgY29uc3QgbGlzdCA9IGRvY3VtZW50LmdldEVsZW1lbnRCeUlkKFwibG9iYnktcHJlc2VuY2UtbGlzdFwiKTtcbiAgICBpZiAoIWxpc3QpIHJldHVybjtcblxuICAgIC8vIENvbGxlY3QgZXhpc3RpbmcgZnJvbSBET01cbiAgICBjb25zdCBjdXJyZW50ID0gbmV3IE1hcCgpO1xuICAgIGxpc3QucXVlcnlTZWxlY3RvckFsbChcIltkYXRhLXByZXNlbmNlLXVzZXJdXCIpLmZvckVhY2goKGVsKSA9PiB7XG4gICAgICBjdXJyZW50LnNldChlbC5kYXRhc2V0LnByZXNlbmNlVXNlciwgZWwpO1xuICAgIH0pO1xuXG4gICAgLy8gQWRkIGpvaW5zXG4gICAgT2JqZWN0LnZhbHVlcyhkaWZmLmpvaW5zIHx8IHt9KS5mb3JFYWNoKChlbnRyeSkgPT4ge1xuICAgICAgY29uc3QgbWV0YSA9IGVudHJ5Lm1ldGFzPy5bMF07XG4gICAgICBpZiAoIW1ldGE/LnVzZXJuYW1lKSByZXR1cm47XG4gICAgICBpZiAoY3VycmVudC5oYXMobWV0YS51c2VybmFtZSkpIHJldHVybjtcblxuICAgICAgY29uc3QgZWwgPSB0aGlzLl9idWlsZFByZXNlbmNlUm93KG1ldGEudXNlcm5hbWUpO1xuICAgICAgbGlzdC5hcHBlbmRDaGlsZChlbCk7XG4gICAgICBjdXJyZW50LnNldChtZXRhLnVzZXJuYW1lLCBlbCk7XG4gICAgfSk7XG5cbiAgICAvLyBSZW1vdmUgbGVhdmVzXG4gICAgT2JqZWN0LnZhbHVlcyhkaWZmLmxlYXZlcyB8fCB7fSkuZm9yRWFjaCgoZW50cnkpID0+IHtcbiAgICAgIGNvbnN0IG1ldGEgPSBlbnRyeS5tZXRhcz8uWzBdO1xuICAgICAgaWYgKCFtZXRhPy51c2VybmFtZSkgcmV0dXJuO1xuICAgICAgY29uc3QgZWwgPSBjdXJyZW50LmdldChtZXRhLnVzZXJuYW1lKTtcbiAgICAgIGlmIChlbCkge1xuICAgICAgICBlbC5yZW1vdmUoKTtcbiAgICAgICAgY3VycmVudC5kZWxldGUobWV0YS51c2VybmFtZSk7XG4gICAgICB9XG4gICAgfSk7XG5cbiAgICBjb25zdCBjb3VudGVyID0gZG9jdW1lbnQuZ2V0RWxlbWVudEJ5SWQoXCJsb2JieS1vbmxpbmUtY291bnRcIik7XG4gICAgaWYgKGNvdW50ZXIpIGNvdW50ZXIudGV4dENvbnRlbnQgPSBgJHtjdXJyZW50LnNpemV9IG9ubGluZWA7XG5cbiAgICBpZiAoY3VycmVudC5zaXplID09PSAwKSB7XG4gICAgICBsaXN0LmlubmVySFRNTCA9IGA8cCBjbGFzcz1cInRleHQteHMgdGV4dC1ncmF5LTYwMCBmb250LW1vbm9cIj5Ob2JvZHkgZWxzZSBvbmxpbmUuPC9wPmA7XG4gICAgfVxuICB9XG5cbiAgX3JlbmRlclByZXNlbmNlTGlzdChsaXN0LCB1c2Vycykge1xuICAgIGlmICh1c2Vycy5sZW5ndGggPT09IDApIHtcbiAgICAgIGxpc3QuaW5uZXJIVE1MID0gYDxwIGNsYXNzPVwidGV4dC14cyB0ZXh0LWdyYXktNjAwIGZvbnQtbW9ub1wiPk5vYm9keSBlbHNlIG9ubGluZS48L3A+YDtcbiAgICAgIHJldHVybjtcbiAgICB9XG4gICAgbGlzdC5pbm5lckhUTUwgPSBcIlwiO1xuICAgIHVzZXJzLmZvckVhY2goKHUpID0+IGxpc3QuYXBwZW5kQ2hpbGQodGhpcy5fYnVpbGRQcmVzZW5jZVJvdyh1KSkpO1xuICB9XG5cbiAgX2J1aWxkUHJlc2VuY2VSb3codXNlcm5hbWUpIHtcbiAgICBjb25zdCBlbCA9IGRvY3VtZW50LmNyZWF0ZUVsZW1lbnQoXCJkaXZcIik7XG4gICAgZWwuY2xhc3NOYW1lID0gXCJmbGV4IGl0ZW1zLWNlbnRlciBnYXAtMiBmYWRlLWluLXVwXCI7XG4gICAgZWwuZGF0YXNldC5wcmVzZW5jZVVzZXIgPSB1c2VybmFtZTtcbiAgICBlbC5pbm5lckhUTUwgPSBgXG4gICAgICA8ZGl2IGNsYXNzPVwicHJlc2VuY2UtZG90XCI+PC9kaXY+XG4gICAgICA8c3BhbiBjbGFzcz1cInRleHQteHMgZm9udC1tb25vIHRleHQtZ3JheS0zMDBcIj4ke2VzY2FwZUh0bWwodXNlcm5hbWUpfTwvc3Bhbj5cbiAgICBgO1xuICAgIHJldHVybiBlbDtcbiAgfVxuXG4gIF9teVVzZXJuYW1lKCkge1xuICAgIGNvbnN0IG5hdiA9IGRvY3VtZW50LnF1ZXJ5U2VsZWN0b3IoXCJoZWFkZXIgbmF2IC5mb250LW1vbm9cIik7XG4gICAgcmV0dXJuIG5hdiA/IG5hdi50ZXh0Q29udGVudC50cmltKCkgOiBudWxsO1xuICB9XG5cbiAgLy8gQ2FsbGVkIGJ5IE1hdGNoTWFuYWdlciB0byBzZW5kIGxvYmJ5IGV2ZW50cyBmcm9tIHRoZSBtYXRjaCBwYWdlXG4gIHB1c2goZXZlbnQsIHBheWxvYWQpIHtcbiAgICBpZiAodGhpcy5jaGFubmVsICYmIHRoaXMuam9pbmVkKSB7XG4gICAgICByZXR1cm4gdGhpcy5jaGFubmVsLnB1c2goZXZlbnQsIHBheWxvYWQpO1xuICAgIH1cbiAgfVxufVxuXG4vLyBcdTI1MDBcdTI1MDBcdTI1MDAgTWF0Y2hNYW5hZ2VyIFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFxuY2xhc3MgTWF0Y2hNYW5hZ2VyIHtcbiAgY29uc3RydWN0b3Ioc29ja2V0LCBjZmcsIGxvYmJ5TWdyKSB7XG4gICAgdGhpcy5zb2NrZXQgPSBzb2NrZXQ7XG4gICAgdGhpcy5jZmcgPSBjZmc7XG4gICAgdGhpcy5sb2JieU1nciA9IGxvYmJ5TWdyO1xuICAgIHRoaXMuY2hhbm5lbCA9IG51bGw7XG4gICAgdGhpcy5jaGFydHMgPSB7fTsgLy8gdGlja2VyIFx1MjE5MiBTcGFya2xpbmVDaGFydFxuICAgIHRoaXMucHJldlByaWNlcyA9IHt9OyAvLyB0aWNrZXIgXHUyMTkyIGxhc3Qga25vd24gcHJpY2VcbiAgICB0aGlzLnBsYXllcnMgPSBbXTsgLy8gY3VycmVudCBwbGF5ZXJzIGxpc3QgZnJvbSBwdWJsaWMgc3RhdGVcbiAgICB0aGlzLmNvbXBhbmllcyA9IFtdOyAvLyBjdXJyZW50IGNvbXBhbmllcyBsaXN0IGZyb20gcHVibGljIHN0YXRlXG4gICAgdGhpcy5teVN0YXRlID0gbnVsbDsgLy8gbXkgcHJpdmF0ZSBzdGF0ZVxuICAgIHRoaXMucGhhc2UgPSBudWxsO1xuICAgIHRoaXMuY291bnRkb3duID0gbnVsbDsgLy8gc2V0SW50ZXJ2YWwgaGFuZGxlXG4gICAgdGhpcy5waGFzZU92ZXJsYXlUaW1lciA9IG51bGw7XG4gIH1cblxuICBpbml0KCkge1xuICAgIHRoaXMuY2hhbm5lbCA9IHRoaXMuc29ja2V0LmNoYW5uZWwoYG1hdGNoOiR7dGhpcy5jZmcubWF0Y2hJZH1gLCB7fSk7XG5cbiAgICB0aGlzLmNoYW5uZWwub24oXCJzdGF0ZV91cGRhdGVkXCIsIChwKSA9PiB0aGlzLl9vblN0YXRlVXBkYXRlZChwKSk7XG4gICAgdGhpcy5jaGFubmVsLm9uKFwicGhhc2VfY2hhbmdlZFwiLCAocCkgPT4gdGhpcy5fb25QaGFzZUNoYW5nZWQocCkpO1xuICAgIHRoaXMuY2hhbm5lbC5vbihcInByaXZhdGVfc3RhdGVcIiwgKHApID0+IHRoaXMuX29uUHJpdmF0ZVN0YXRlKHApKTtcbiAgICB0aGlzLmNoYW5uZWwub24oXCJwcml2YXRlX2V2ZW50c1wiLCAocCkgPT4gdGhpcy5fb25Qcml2YXRlRXZlbnRzKHApKTtcbiAgICB0aGlzLmNoYW5uZWwub24oXCJuZXdfbWVzc2FnZVwiLCAobSkgPT4gdGhpcy5fYXBwZW5kQ2hhdChtKSk7XG4gICAgdGhpcy5jaGFubmVsLm9uKFwibmV3X3doaXNwZXJcIiwgKG0pID0+IHRoaXMuX2FwcGVuZENoYXQobSwgdHJ1ZSkpO1xuICAgIHRoaXMuY2hhbm5lbC5vbihcIm1hdGNoX2ZpbmlzaGVkXCIsIChwKSA9PiB0aGlzLl9vbk1hdGNoRmluaXNoZWQocCkpO1xuICAgIHRoaXMuY2hhbm5lbC5vbihcInByZXNlbmNlX3N0YXRlXCIsIChzKSA9PiB0aGlzLl9vblByZXNlbmNlU3RhdGUocykpO1xuICAgIHRoaXMuY2hhbm5lbC5vbihcInByZXNlbmNlX2RpZmZcIiwgKGQpID0+IHRoaXMuX29uUHJlc2VuY2VEaWZmKGQpKTtcblxuICAgIHRoaXMuY2hhbm5lbFxuICAgICAgLmpvaW4oKVxuICAgICAgLnJlY2VpdmUoXCJva1wiLCAoKSA9PiBjb25zb2xlLmRlYnVnKFwiW01hdGNoXSBqb2luZWRcIikpXG4gICAgICAucmVjZWl2ZShcImVycm9yXCIsICh7IHJlYXNvbiB9KSA9PiB7XG4gICAgICAgIFRvYXN0LnNob3coXCJDb3VsZCBub3Qgam9pbiBtYXRjaDogXCIgKyByZWFzb24sIFwiZXJyb3JcIik7XG4gICAgICB9KTtcblxuICAgIHRoaXMuX3dpcmVDb250cm9scygpO1xuICB9XG5cbiAgLy8gXHUyNTAwXHUyNTAwIFdpcmUgc3RhdGljIGNvbnRyb2xzIFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFxuXG4gIF93aXJlQ29udHJvbHMoKSB7XG4gICAgY29uc3Qgc3RhcnRCdG4gPSBkb2N1bWVudC5nZXRFbGVtZW50QnlJZChcInN0YXJ0LW1hdGNoLWJ0blwiKTtcbiAgICBpZiAoc3RhcnRCdG4pIHtcbiAgICAgIHN0YXJ0QnRuLmFkZEV2ZW50TGlzdGVuZXIoXCJjbGlja1wiLCAoZSkgPT4ge1xuICAgICAgICBlLnByZXZlbnREZWZhdWx0KCk7XG4gICAgICAgIHN0YXJ0QnRuLmRpc2FibGVkID0gdHJ1ZTtcbiAgICAgICAgc3RhcnRCdG4udGV4dENvbnRlbnQgPSBcIlN0YXJ0aW5nLi4uXCI7XG4gICAgICAgIHRoaXMubG9iYnlNZ3JcbiAgICAgICAgICAucHVzaChcInN0YXJ0X21hdGNoXCIsIHsgbWF0Y2hfaWQ6IHRoaXMuY2ZnLm1hdGNoSWQgfSlcbiAgICAgICAgICA/LnJlY2VpdmUoXCJva1wiLCAoKSA9PiBzdGFydEJ0bi5yZW1vdmUoKSlcbiAgICAgICAgICA/LnJlY2VpdmUoXCJlcnJvclwiLCAoZXJyKSA9PiB7XG4gICAgICAgICAgICBzdGFydEJ0bi5kaXNhYmxlZCA9IGZhbHNlO1xuICAgICAgICAgICAgc3RhcnRCdG4udGV4dENvbnRlbnQgPSBcIlN0YXJ0IE1hdGNoXCI7XG4gICAgICAgICAgICBjb25zdCByZWFzb24gPSBlcnI/LnJlYXNvbiB8fCBKU09OLnN0cmluZ2lmeShlcnIpO1xuICAgICAgICAgICAgVG9hc3Quc2hvdyhcIkNvdWxkIG5vdCBzdGFydDogXCIgKyByZWFzb24sIFwiZXJyb3JcIik7XG4gICAgICAgICAgfSk7XG4gICAgICB9KTtcbiAgICB9XG5cbiAgICBkb2N1bWVudC5xdWVyeVNlbGVjdG9yQWxsKFwiLmFjdGlvbi1idG5cIikuZm9yRWFjaCgoYnRuKSA9PiB7XG4gICAgICBidG4uYWRkRXZlbnRMaXN0ZW5lcihcImNsaWNrXCIsIChlKSA9PiB7XG4gICAgICAgIGUucHJldmVudERlZmF1bHQoKTtcbiAgICAgICAgZS5zdG9wUHJvcGFnYXRpb24oKTtcbiAgICAgICAgaWYgKGJ0bi5kaXNhYmxlZCkgcmV0dXJuO1xuICAgICAgICB0aGlzLl9vcGVuQWN0aW9uTW9kYWwoYnRuLmRhdGFzZXQpO1xuICAgICAgfSk7XG4gICAgfSk7XG5cbiAgICBkb2N1bWVudFxuICAgICAgLmdldEVsZW1lbnRCeUlkKFwiY29weS1pbnZpdGUtYnRuXCIpXG4gICAgICA/LmFkZEV2ZW50TGlzdGVuZXIoXCJjbGlja1wiLCAoZSkgPT4ge1xuICAgICAgICBlLnByZXZlbnREZWZhdWx0KCk7XG4gICAgICAgIGNvbnN0IHVybCA9IHdpbmRvdy5sb2NhdGlvbi5ocmVmO1xuICAgICAgICBuYXZpZ2F0b3IuY2xpcGJvYXJkXG4gICAgICAgICAgPy53cml0ZVRleHQodXJsKVxuICAgICAgICAgIC50aGVuKCgpID0+IFRvYXN0LnNob3coXCJJbnZpdGUgbGluayBjb3BpZWQgdG8gY2xpcGJvYXJkIVwiLCBcInN1Y2Nlc3NcIikpXG4gICAgICAgICAgLmNhdGNoKCgpID0+IHtcbiAgICAgICAgICAgIHdpbmRvdy5wcm9tcHQoXCJDb3B5IHRoaXMgaW52aXRlIGxpbms6XCIsIHVybCk7XG4gICAgICAgICAgfSk7XG4gICAgICB9KTtcblxuICAgIGRvY3VtZW50LmdldEVsZW1lbnRCeUlkKFwibW9kYWwtY2xvc2VcIik/LmFkZEV2ZW50TGlzdGVuZXIoXCJjbGlja1wiLCAoZSkgPT4ge1xuICAgICAgZS5wcmV2ZW50RGVmYXVsdCgpO1xuICAgICAgZS5zdG9wUHJvcGFnYXRpb24oKTtcbiAgICAgIHRoaXMuX2Nsb3NlTW9kYWwoKTtcbiAgICB9KTtcblxuICAgIGRvY3VtZW50LmdldEVsZW1lbnRCeUlkKFwiYWN0aW9uLW1vZGFsXCIpPy5hZGRFdmVudExpc3RlbmVyKFwiY2xpY2tcIiwgKGUpID0+IHtcbiAgICAgIGlmIChlLnRhcmdldCA9PT0gZS5jdXJyZW50VGFyZ2V0KSB0aGlzLl9jbG9zZU1vZGFsKCk7XG4gICAgfSk7XG5cbiAgICBkb2N1bWVudC5nZXRFbGVtZW50QnlJZChcIm1vZGFsLXN1Ym1pdFwiKT8uYWRkRXZlbnRMaXN0ZW5lcihcImNsaWNrXCIsIChlKSA9PiB7XG4gICAgICBlLnByZXZlbnREZWZhdWx0KCk7XG4gICAgICBlLnN0b3BQcm9wYWdhdGlvbigpO1xuICAgICAgdGhpcy5fc3VibWl0QWN0aW9uKCk7XG4gICAgfSk7XG5cbiAgICBkb2N1bWVudFxuICAgICAgLmdldEVsZW1lbnRCeUlkKFwiaW5wdXQtcXVhbnRpdHlcIilcbiAgICAgID8uYWRkRXZlbnRMaXN0ZW5lcihcImtleWRvd25cIiwgKGUpID0+IHtcbiAgICAgICAgaWYgKGUua2V5ID09PSBcIkVudGVyXCIpIHtcbiAgICAgICAgICBlLnByZXZlbnREZWZhdWx0KCk7XG4gICAgICAgICAgdGhpcy5fc3VibWl0QWN0aW9uKCk7XG4gICAgICAgIH1cbiAgICAgIH0pO1xuXG4gICAgY29uc3Qgc2VuZEJ0biA9IGRvY3VtZW50LmdldEVsZW1lbnRCeUlkKFwiY2hhdC1zZW5kXCIpO1xuICAgIGNvbnN0IGNoYXRJbnB1dCA9IGRvY3VtZW50LmdldEVsZW1lbnRCeUlkKFwiY2hhdC1pbnB1dFwiKTtcbiAgICBpZiAoc2VuZEJ0biAmJiBjaGF0SW5wdXQpIHtcbiAgICAgIHNlbmRCdG4uYWRkRXZlbnRMaXN0ZW5lcihcImNsaWNrXCIsIChlKSA9PiB7XG4gICAgICAgIGUucHJldmVudERlZmF1bHQoKTtcbiAgICAgICAgdGhpcy5fc2VuZENoYXRNZXNzYWdlKCk7XG4gICAgICB9KTtcblxuICAgICAgY2hhdElucHV0LmFkZEV2ZW50TGlzdGVuZXIoXCJrZXlkb3duXCIsIChlKSA9PiB7XG4gICAgICAgIGlmIChlLmtleSA9PT0gXCJFbnRlclwiICYmICFlLnNoaWZ0S2V5KSB7XG4gICAgICAgICAgZS5wcmV2ZW50RGVmYXVsdCgpO1xuICAgICAgICAgIHRoaXMuX3NlbmRDaGF0TWVzc2FnZSgpO1xuICAgICAgICB9XG4gICAgICB9KTtcbiAgICB9XG4gIH1cblxuICAvLyBcdTI1MDBcdTI1MDAgU3RhdGUgdXBkYXRlcyBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcblxuICBfb25TdGF0ZVVwZGF0ZWQocGF5bG9hZCkge1xuICAgIHRoaXMucGxheWVycyA9IHBheWxvYWQucGxheWVycyB8fCBbXTtcbiAgICB0aGlzLmNvbXBhbmllcyA9IHBheWxvYWQuY29tcGFuaWVzIHx8IFtdO1xuICAgIHRoaXMucGhhc2UgPSBwYXlsb2FkLnBoYXNlO1xuXG4gICAgdGhpcy5fcmVuZGVyUGhhc2UocGF5bG9hZC5waGFzZSwgcGF5bG9hZC5yb3VuZCk7XG4gICAgdGhpcy5fcmVuZGVyUm91bmQocGF5bG9hZC5yb3VuZCwgcGF5bG9hZC50b3RhbF9yb3VuZHMpO1xuICAgIHRoaXMuX3JlbmRlck1hcmtldChwYXlsb2FkLmNvbXBhbmllcyk7XG4gICAgdGhpcy5fcmVuZGVyUGxheWVycyhwYXlsb2FkLnBsYXllcnMpO1xuICAgIHRoaXMuX3JlbmRlckFjdGlvblBhbmVsKHBheWxvYWQucGhhc2UpO1xuXG4gICAgLy8gU3RhcnQgbmVnb3RpYXRpb24gY291bnRkb3duIGlmIGFwcGxpY2FibGVcbiAgICBpZiAocGF5bG9hZC5waGFzZSA9PT0gXCJuZWdvdGlhdGlvblwiICYmIHBheWxvYWQubmVnb3RpYXRpb25fZGVhZGxpbmUpIHtcbiAgICAgIHRoaXMuX3N0YXJ0Q291bnRkb3duKG5ldyBEYXRlKHBheWxvYWQubmVnb3RpYXRpb25fZGVhZGxpbmUpKTtcbiAgICB9XG4gIH1cblxuICBfb25QaGFzZUNoYW5nZWQocGF5bG9hZCkge1xuICAgIHRoaXMucGhhc2UgPSBwYXlsb2FkLnBoYXNlO1xuICAgIHRoaXMuX3JlbmRlclBoYXNlKHBheWxvYWQucGhhc2UsIHBheWxvYWQucm91bmQpO1xuICAgIHRoaXMuX3JlbmRlclJvdW5kKHBheWxvYWQucm91bmQpO1xuICAgIHRoaXMuX3JlbmRlckFjdGlvblBhbmVsKHBheWxvYWQucGhhc2UpO1xuICAgIHRoaXMuX3Nob3dQaGFzZU92ZXJsYXkocGF5bG9hZC5waGFzZSk7XG4gICAgdGhpcy5fdXBkYXRlQ2hhdEluZGljYXRvcihwYXlsb2FkLnBoYXNlKTtcblxuICAgIGlmIChwYXlsb2FkLnBoYXNlID09PSBcImFjdGlvbl9zdWJtaXNzaW9uXCIpIHtcbiAgICAgIHRoaXMuX2NsZWFyQ291bnRkb3duKCk7XG4gICAgICAvLyBSZXNldCBhY3Rpb24gc3VibWl0dGVkIHN0YXRlXG4gICAgICBkb2N1bWVudFxuICAgICAgICAuZ2V0RWxlbWVudEJ5SWQoXCJhY3Rpb24tc3VibWl0dGVkLWJhZGdlXCIpXG4gICAgICAgID8uY2xhc3NMaXN0LmFkZChcImhpZGRlblwiKTtcbiAgICAgIGRvY3VtZW50LnF1ZXJ5U2VsZWN0b3JBbGwoXCIuYWN0aW9uLWJ0blwiKS5mb3JFYWNoKChiKSA9PiB7XG4gICAgICAgIGIuZGlzYWJsZWQgPSBmYWxzZTtcbiAgICAgICAgYi5jbGFzc0xpc3QucmVtb3ZlKFwic3VibWl0dGVkXCIpO1xuICAgICAgfSk7XG4gICAgfVxuXG4gICAgaWYgKHBheWxvYWQucGhhc2UgPT09IFwibmVnb3RpYXRpb25cIiAmJiBwYXlsb2FkLm5lZ290aWF0aW9uX2RlYWRsaW5lKSB7XG4gICAgICB0aGlzLl9zdGFydENvdW50ZG93bihuZXcgRGF0ZShwYXlsb2FkLm5lZ290aWF0aW9uX2RlYWRsaW5lKSk7XG4gICAgfSBlbHNlIGlmIChwYXlsb2FkLnBoYXNlICE9PSBcIm5lZ290aWF0aW9uXCIpIHtcbiAgICAgIHRoaXMuX2NsZWFyQ291bnRkb3duKCk7XG4gICAgfVxuICB9XG5cbiAgX29uUHJpdmF0ZVN0YXRlKHN0YXRlKSB7XG4gICAgdGhpcy5teVN0YXRlID0gc3RhdGU7XG4gICAgdGhpcy5fcmVuZGVyTXlTdGF0cyhzdGF0ZSk7XG4gIH1cblxuICBfb25Qcml2YXRlRXZlbnRzKHBheWxvYWQpIHtcbiAgICAocGF5bG9hZC5ldmVudHMgfHwgW10pLmZvckVhY2goKGV2KSA9PiB7XG4gICAgICBUb2FzdC5zaG93KFxuICAgICAgICBldi5tZXNzYWdlIHx8IGV2LnR5cGUgfHwgXCJFdmVudFwiLFxuICAgICAgICBldi5wb3NpdGl2ZSA/IFwic3VjY2Vzc1wiIDogXCJ3YXJuaW5nXCIsXG4gICAgICApO1xuICAgIH0pO1xuICB9XG5cbiAgX29uTWF0Y2hGaW5pc2hlZChwYXlsb2FkKSB7XG4gICAgdGhpcy5fY2xlYXJDb3VudGRvd24oKTtcbiAgICB0aGlzLl9yZW5kZXJMZWFkZXJib2FyZChwYXlsb2FkLmxlYWRlcmJvYXJkKTtcbiAgICB0aGlzLl9zaG93UGhhc2VPdmVybGF5KFwiZmluaXNoZWRcIik7XG4gICAgZG9jdW1lbnQuZ2V0RWxlbWVudEJ5SWQoXCJhY3Rpb24tcGFuZWxcIik/LmNsYXNzTGlzdC5hZGQoXCJoaWRkZW5cIik7XG4gICAgZG9jdW1lbnQuZ2V0RWxlbWVudEJ5SWQoXCJsZWFkZXJib2FyZC1wYW5lbFwiKT8uY2xhc3NMaXN0LnJlbW92ZShcImhpZGRlblwiKTtcbiAgICBUb2FzdC5zaG93KFwiTWF0Y2ggb3ZlciEgRmluYWwgcmFua2luZ3MgYXJlIGluLlwiLCBcImluZm9cIiwgODAwMCk7XG4gIH1cblxuICAvLyBcdTI1MDBcdTI1MDAgUHJlc2VuY2UgXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXG5cbiAgX29uUHJlc2VuY2VTdGF0ZShzdGF0ZSkge1xuICAgIGNvbnN0IG9ubGluZUlkcyA9IG5ldyBTZXQoKTtcbiAgICBPYmplY3QuZW50cmllcyhzdGF0ZSkuZm9yRWFjaCgoW3VzZXJJZCwgZW50cnldKSA9PiB7XG4gICAgICBvbmxpbmVJZHMuYWRkKHVzZXJJZCk7XG4gICAgICBjb25zdCBtZXRhID0gZW50cnkubWV0YXM/LlswXTtcbiAgICAgIHRoaXMuX3NldFBsYXllck9ubGluZSh1c2VySWQsIG1ldGE/LnVzZXJuYW1lLCB0cnVlKTtcbiAgICB9KTtcbiAgfVxuXG4gIF9vblByZXNlbmNlRGlmZihkaWZmKSB7XG4gICAgT2JqZWN0LmVudHJpZXMoZGlmZi5qb2lucyB8fCB7fSkuZm9yRWFjaCgoW3VzZXJJZCwgZW50cnldKSA9PiB7XG4gICAgICBjb25zdCBtZXRhID0gZW50cnkubWV0YXM/LlswXTtcbiAgICAgIHRoaXMuX3NldFBsYXllck9ubGluZSh1c2VySWQsIG1ldGE/LnVzZXJuYW1lLCB0cnVlKTtcbiAgICB9KTtcbiAgICBPYmplY3QuZW50cmllcyhkaWZmLmxlYXZlcyB8fCB7fSkuZm9yRWFjaCgoW3VzZXJJZF0pID0+IHtcbiAgICAgIHRoaXMuX3NldFBsYXllck9ubGluZSh1c2VySWQsIG51bGwsIGZhbHNlKTtcbiAgICB9KTtcbiAgfVxuXG4gIF9zZXRQbGF5ZXJPbmxpbmUodXNlcklkLCBfdXNlcm5hbWUsIG9ubGluZSkge1xuICAgIGNvbnN0IHJvdyA9IGRvY3VtZW50LnF1ZXJ5U2VsZWN0b3IoYFtkYXRhLXBsYXllci1pZD1cIiR7dXNlcklkfVwiXWApO1xuICAgIGlmICghcm93KSByZXR1cm47XG4gICAgY29uc3QgZG90ID0gcm93LnF1ZXJ5U2VsZWN0b3IoXCIucHJlc2VuY2UtZG90XCIpO1xuICAgIGlmIChkb3QpIGRvdC5jbGFzc0xpc3QudG9nZ2xlKFwib2ZmbGluZVwiLCAhb25saW5lKTtcbiAgfVxuXG4gIC8vIFx1MjUwMFx1MjUwMCBNYXJrZXQgcmVuZGVyaW5nIFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFxuXG4gIF9yZW5kZXJNYXJrZXQoY29tcGFuaWVzKSB7XG4gICAgY29uc3QgbGlzdCA9IGRvY3VtZW50LmdldEVsZW1lbnRCeUlkKFwiY29tcGFuaWVzLWxpc3RcIik7XG4gICAgaWYgKCFsaXN0IHx8ICFjb21wYW5pZXMpIHJldHVybjtcblxuICAgIGNvbXBhbmllcy5mb3JFYWNoKChjKSA9PiB7XG4gICAgICBjb25zdCBwcmV2ID0gdGhpcy5wcmV2UHJpY2VzW2MudGlja2VyXTtcbiAgICAgIGNvbnN0IHByaWNlID0gcGFyc2VGbG9hdChjLnByaWNlKTtcbiAgICAgIGNvbnN0IGlzVXAgPSBwcmV2ICE9PSB1bmRlZmluZWQgPyBwcmljZSA+PSBwcmV2IDogdHJ1ZTtcbiAgICAgIHRoaXMucHJldlByaWNlc1tjLnRpY2tlcl0gPSBwcmljZTtcblxuICAgICAgbGV0IHJvdyA9IGxpc3QucXVlcnlTZWxlY3RvcihgW2RhdGEtdGlja2VyPVwiJHtjLnRpY2tlcn1cIl1gKTtcbiAgICAgIGlmICghcm93KSB7XG4gICAgICAgIHJvdyA9IHRoaXMuX2NyZWF0ZU1hcmtldFJvdyhjKTtcbiAgICAgICAgbGlzdC5hcHBlbmRDaGlsZChyb3cpO1xuICAgICAgfVxuXG4gICAgICB0aGlzLl91cGRhdGVNYXJrZXRSb3cocm93LCBjLCBwcmljZSwgcHJldiwgaXNVcCk7XG4gICAgfSk7XG4gIH1cblxuICBfY3JlYXRlTWFya2V0Um93KGMpIHtcbiAgICBjb25zdCByb3cgPSBkb2N1bWVudC5jcmVhdGVFbGVtZW50KFwiZGl2XCIpO1xuICAgIHJvdy5jbGFzc05hbWUgPSBcInRpY2tlci1yb3dcIjtcbiAgICByb3cuZGF0YXNldC50aWNrZXIgPSBjLnRpY2tlcjtcblxuICAgIGNvbnN0IGNoYXJ0SWQgPSBgY2hhcnQtJHtjLnRpY2tlcn1gO1xuICAgIHJvdy5pbm5lckhUTUwgPSBgXG4gICAgICA8c3BhbiBjbGFzcz1cImZvbnQtbW9ubyBmb250LWJvbGQgdGV4dC1zbSB0aWNrZXItc3ltYm9sXCI+PC9zcGFuPlxuICAgICAgPHNwYW4gY2xhc3M9XCJ0ZXh0LXhzIHRleHQtZ3JheS01MDAgdHJ1bmNhdGUgdGlja2VyLW5hbWVcIj48L3NwYW4+XG4gICAgICA8ZGl2IGNsYXNzPVwidGV4dC1yaWdodFwiPlxuICAgICAgICA8ZGl2IGNsYXNzPVwiZm9udC1tb25vIGZvbnQtc2VtaWJvbGQgdGV4dC1zbSB0aWNrZXItcHJpY2VcIj48L2Rpdj5cbiAgICAgICAgPGRpdiBjbGFzcz1cInRleHQteHMgZm9udC1tb25vIHRpY2tlci1jaGFuZ2UgbXQtMC41XCI+PC9kaXY+XG4gICAgICA8L2Rpdj5cbiAgICAgIDxkaXYgaWQ9XCIke2NoYXJ0SWR9XCIgY2xhc3M9XCJmbGV4IGl0ZW1zLWNlbnRlciBqdXN0aWZ5LWVuZFwiIHN0eWxlPVwiaGVpZ2h0OjI4cHg7d2lkdGg6ODBweFwiPjwvZGl2PlxuICAgIGA7XG5cbiAgICAvLyBDcmVhdGUgc3BhcmtsaW5lXG4gICAgc2V0VGltZW91dCgoKSA9PiB7XG4gICAgICB0aGlzLmNoYXJ0c1tjLnRpY2tlcl0gPSBuZXcgU3BhcmtsaW5lQ2hhcnQoY2hhcnRJZCwge1xuICAgICAgICB3aWR0aDogODAsXG4gICAgICAgIGhlaWdodDogMjgsXG4gICAgICB9KTtcbiAgICAgIHRoaXMuY2hhcnRzW2MudGlja2VyXS5wdXNoKHBhcnNlRmxvYXQoYy5wcmljZSkpO1xuICAgIH0sIDApO1xuXG4gICAgcmV0dXJuIHJvdztcbiAgfVxuXG4gIF91cGRhdGVNYXJrZXRSb3cocm93LCBjLCBwcmljZSwgcHJldiwgaXNVcCkge1xuICAgIGNvbnN0IHByaWNlRWwgPSByb3cucXVlcnlTZWxlY3RvcihcIi50aWNrZXItcHJpY2VcIik7XG4gICAgY29uc3QgY2hhbmdlRWwgPSByb3cucXVlcnlTZWxlY3RvcihcIi50aWNrZXItY2hhbmdlXCIpO1xuICAgIGNvbnN0IHN5bUVsID0gcm93LnF1ZXJ5U2VsZWN0b3IoXCIudGlja2VyLXN5bWJvbFwiKTtcbiAgICBjb25zdCBuYW1lRWwgPSByb3cucXVlcnlTZWxlY3RvcihcIi50aWNrZXItbmFtZVwiKTtcblxuICAgIGlmIChzeW1FbCkgc3ltRWwudGV4dENvbnRlbnQgPSBjLnRpY2tlcjtcbiAgICBpZiAobmFtZUVsKSBuYW1lRWwudGV4dENvbnRlbnQgPSBjLm5hbWUgfHwgXCJcIjtcblxuICAgIGlmIChwcmljZUVsKSB7XG4gICAgICBjb25zdCBvbGRQcmljZSA9IHByaWNlRWwudGV4dENvbnRlbnQ7XG4gICAgICBjb25zdCBuZXdUZXh0ID0gYCQke3ByaWNlLnRvRml4ZWQoMil9YDtcbiAgICAgIGlmIChvbGRQcmljZSAhPT0gbmV3VGV4dCkge1xuICAgICAgICBwcmljZUVsLnRleHRDb250ZW50ID0gbmV3VGV4dDtcbiAgICAgICAgcHJpY2VFbC5jbGFzc0xpc3QucmVtb3ZlKFwicHJpY2UtdXBcIiwgXCJwcmljZS1kb3duXCIpO1xuICAgICAgICB2b2lkIHByaWNlRWwub2Zmc2V0V2lkdGg7IC8vIHJlZmxvd1xuICAgICAgICBwcmljZUVsLmNsYXNzTGlzdC5hZGQoaXNVcCA/IFwicHJpY2UtdXBcIiA6IFwicHJpY2UtZG93blwiKTtcbiAgICAgICAgcHJpY2VFbC5zdHlsZS5jb2xvciA9IGlzVXAgPyBcIiMxMGI5ODFcIiA6IFwiI2VmNDQ0NFwiO1xuXG4gICAgICAgIHJvdy5jbGFzc0xpc3QucmVtb3ZlKFwicm93LWZsYXNoLWdyZWVuXCIsIFwicm93LWZsYXNoLXJlZFwiKTtcbiAgICAgICAgdm9pZCByb3cub2Zmc2V0V2lkdGg7XG4gICAgICAgIHJvdy5jbGFzc0xpc3QuYWRkKGlzVXAgPyBcInJvdy1mbGFzaC1ncmVlblwiIDogXCJyb3ctZmxhc2gtcmVkXCIpO1xuICAgICAgfVxuICAgIH1cblxuICAgIGlmIChjaGFuZ2VFbCAmJiBwcmV2ICE9PSB1bmRlZmluZWQpIHtcbiAgICAgIGNvbnN0IHBjdCA9ICgocHJpY2UgLSBwcmV2KSAvIHByZXYpICogMTAwO1xuICAgICAgaWYgKE1hdGguYWJzKHBjdCkgPiAwLjAxKSB7XG4gICAgICAgIGNoYW5nZUVsLnRleHRDb250ZW50ID0gKHBjdCA+PSAwID8gXCIrXCIgOiBcIlwiKSArIHBjdC50b0ZpeGVkKDIpICsgXCIlXCI7XG4gICAgICAgIGNoYW5nZUVsLnN0eWxlLmNvbG9yID0gcGN0ID49IDAgPyBcIiMxMGI5ODFcIiA6IFwiI2VmNDQ0NFwiO1xuICAgICAgfSBlbHNlIHtcbiAgICAgICAgY2hhbmdlRWwudGV4dENvbnRlbnQgPSBcIlwiO1xuICAgICAgfVxuICAgIH1cblxuICAgIC8vIEhlYXQgaW5kaWNhdG9yXG4gICAgaWYgKGMucmVndWxhdG9yeV9oZWF0ID4gMCkge1xuICAgICAgbGV0IGhlYXRFbCA9IHJvdy5xdWVyeVNlbGVjdG9yKFwiLmhlYXQtYmFkZ2VcIik7XG4gICAgICBpZiAoIWhlYXRFbCkge1xuICAgICAgICBoZWF0RWwgPSBkb2N1bWVudC5jcmVhdGVFbGVtZW50KFwic3BhblwiKTtcbiAgICAgICAgaGVhdEVsLmNsYXNzTmFtZSA9IFwiaGVhdC1iYWRnZSBhYnNvbHV0ZSByaWdodC0wIHRvcC0wXCI7XG4gICAgICAgIC8vIFdlJ2xsIGp1c3QgYWRkIGEgdGV4dHVhbCBpbmRpY2F0b3IgaW4gdGhlIHJvd1xuICAgICAgfVxuICAgIH1cblxuICAgIC8vIFVwZGF0ZSBzcGFya2xpbmVcbiAgICBpZiAodGhpcy5jaGFydHNbYy50aWNrZXJdKSB7XG4gICAgICB0aGlzLmNoYXJ0c1tjLnRpY2tlcl0ucHVzaChwcmljZSk7XG4gICAgICB0aGlzLmNoYXJ0c1tjLnRpY2tlcl0uc2V0Q29sb3IoaXNVcCk7XG4gICAgfVxuICB9XG5cbiAgLy8gXHUyNTAwXHUyNTAwIFBsYXllcnMgcmVuZGVyaW5nIFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFxuXG4gIF9yZW5kZXJQbGF5ZXJzKHBsYXllcnMpIHtcbiAgICBjb25zdCBsaXN0ID0gZG9jdW1lbnQuZ2V0RWxlbWVudEJ5SWQoXCJwbGF5ZXJzLWxpc3RcIik7XG4gICAgaWYgKCFsaXN0IHx8ICFwbGF5ZXJzKSByZXR1cm47XG5cbiAgICAvLyBVcGRhdGUgc3VibWl0IHN0YXR1cyBmb3Iga25vd24gcGxheWVyIHJvd3NcbiAgICBwbGF5ZXJzLmZvckVhY2goKHApID0+IHtcbiAgICAgIGNvbnN0IHJvdyA9IGxpc3QucXVlcnlTZWxlY3RvcihgW2RhdGEtcGxheWVyLWlkPVwiJHtwLnVzZXJfaWR9XCJdYCk7XG4gICAgICBpZiAoIXJvdykgcmV0dXJuO1xuXG4gICAgICBsZXQgc3VibWl0QmFkZ2UgPSByb3cucXVlcnlTZWxlY3RvcihcIi5zdWJtaXQtYmFkZ2VcIik7XG4gICAgICBpZiAoIXN1Ym1pdEJhZGdlKSB7XG4gICAgICAgIHN1Ym1pdEJhZGdlID0gZG9jdW1lbnQuY3JlYXRlRWxlbWVudChcInNwYW5cIik7XG4gICAgICAgIHN1Ym1pdEJhZGdlLmNsYXNzTmFtZSA9IFwic3VibWl0LWJhZGdlIHRleHQteHMgZm9udC1tb25vIG1sLWF1dG9cIjtcbiAgICAgICAgcm93LmFwcGVuZENoaWxkKHN1Ym1pdEJhZGdlKTtcbiAgICAgIH1cbiAgICAgIHN1Ym1pdEJhZGdlLnRleHRDb250ZW50ID0gcC5oYXNfc3VibWl0dGVkID8gXCJcdTI3MTNcIiA6IFwiXCI7XG4gICAgICBzdWJtaXRCYWRnZS5zdHlsZS5jb2xvciA9IFwiIzEwYjk4MVwiO1xuXG4gICAgICBsZXQgZnJvemVuQmFkZ2UgPSByb3cucXVlcnlTZWxlY3RvcihcIi5mcm96ZW4tYmFkZ2VcIik7XG4gICAgICBpZiAocC5saXF1aWRpdHlfZnJvemVuKSB7XG4gICAgICAgIGlmICghZnJvemVuQmFkZ2UpIHtcbiAgICAgICAgICBmcm96ZW5CYWRnZSA9IGRvY3VtZW50LmNyZWF0ZUVsZW1lbnQoXCJzcGFuXCIpO1xuICAgICAgICAgIGZyb3plbkJhZGdlLmNsYXNzTmFtZSA9XG4gICAgICAgICAgICBcImZyb3plbi1iYWRnZSB0ZXh0LXhzIGZvbnQtbW9ubyB0ZXh0LWJsdWUtNDAwXCI7XG4gICAgICAgICAgcm93LmFwcGVuZENoaWxkKGZyb3plbkJhZGdlKTtcbiAgICAgICAgfVxuICAgICAgICBmcm96ZW5CYWRnZS50ZXh0Q29udGVudCA9IFwiXHUyNzQ0XCI7XG4gICAgICB9IGVsc2UgaWYgKGZyb3plbkJhZGdlKSB7XG4gICAgICAgIGZyb3plbkJhZGdlLnJlbW92ZSgpO1xuICAgICAgfVxuXG4gICAgICAvLyBIZWF0IGluZGljYXRvclxuICAgICAgbGV0IGhlYXRFbCA9IHJvdy5xdWVyeVNlbGVjdG9yKFwiLmhlYXQtaW5kaWNhdG9yXCIpO1xuICAgICAgaWYgKHAucmVndWxhdG9yeV9oZWF0ID4gMCkge1xuICAgICAgICBpZiAoIWhlYXRFbCkge1xuICAgICAgICAgIGhlYXRFbCA9IGRvY3VtZW50LmNyZWF0ZUVsZW1lbnQoXCJzcGFuXCIpO1xuICAgICAgICAgIGhlYXRFbC5jbGFzc05hbWUgPSBcImhlYXQtaW5kaWNhdG9yIHRleHQteHMgZm9udC1tb25vIHRleHQtYW1iZXItNTAwXCI7XG4gICAgICAgICAgcm93LmFwcGVuZENoaWxkKGhlYXRFbCk7XG4gICAgICAgIH1cbiAgICAgICAgaGVhdEVsLnRleHRDb250ZW50ID0gYFx1MjY5NiR7cC5yZWd1bGF0b3J5X2hlYXR9YDtcbiAgICAgIH0gZWxzZSBpZiAoaGVhdEVsKSB7XG4gICAgICAgIGhlYXRFbC5yZW1vdmUoKTtcbiAgICAgIH1cbiAgICB9KTtcbiAgfVxuXG4gIC8vIFx1MjUwMFx1MjUwMCBNeSBzdGF0cyBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcblxuICBfcmVuZGVyTXlTdGF0cyhzdGF0ZSkge1xuICAgIGNvbnN0IHBhbmVsID0gZG9jdW1lbnQuZ2V0RWxlbWVudEJ5SWQoXCJteS1zdGF0c1wiKTtcbiAgICB0aGlzLl9zaG93KHBhbmVsLCBcImJsb2NrXCIpO1xuXG4gICAgY29uc3QgY2FzaCA9IHBhcnNlRmxvYXQoc3RhdGUuY2FzaCk7XG4gICAgY29uc3QgbncgPSBwYXJzZUZsb2F0KHN0YXRlLm5ldF93b3J0aCk7XG5cbiAgICBjb25zdCBjYXNoRWwgPSBkb2N1bWVudC5nZXRFbGVtZW50QnlJZChcIm15LWNhc2hcIik7XG4gICAgY29uc3QgbndFbCA9IGRvY3VtZW50LmdldEVsZW1lbnRCeUlkKFwibXktbmV0d29ydGhcIik7XG4gICAgY29uc3QgaGVhdEVsID0gZG9jdW1lbnQuZ2V0RWxlbWVudEJ5SWQoXCJteS1oZWF0XCIpO1xuICAgIGNvbnN0IGZyekVsID0gZG9jdW1lbnQuZ2V0RWxlbWVudEJ5SWQoXCJteS1mcm96ZW4tYmFkZ2VcIik7XG5cbiAgICBpZiAoY2FzaEVsKSB7XG4gICAgICBjYXNoRWwudGV4dENvbnRlbnQgPVxuICAgICAgICBcIiRcIiArXG4gICAgICAgIGNhc2gudG9Mb2NhbGVTdHJpbmcoXCJlbi1VU1wiLCB7XG4gICAgICAgICAgbWluaW11bUZyYWN0aW9uRGlnaXRzOiAyLFxuICAgICAgICAgIG1heGltdW1GcmFjdGlvbkRpZ2l0czogMixcbiAgICAgICAgfSk7XG4gICAgfVxuXG4gICAgaWYgKG53RWwpIHtcbiAgICAgIG53RWwudGV4dENvbnRlbnQgPVxuICAgICAgICBcIiRcIiArXG4gICAgICAgIG53LnRvTG9jYWxlU3RyaW5nKFwiZW4tVVNcIiwge1xuICAgICAgICAgIG1pbmltdW1GcmFjdGlvbkRpZ2l0czogMixcbiAgICAgICAgICBtYXhpbXVtRnJhY3Rpb25EaWdpdHM6IDIsXG4gICAgICAgIH0pO1xuICAgIH1cblxuICAgIGlmIChoZWF0RWwpIGhlYXRFbC50ZXh0Q29udGVudCA9IHN0YXRlLnJlZ3VsYXRvcnlfaGVhdCB8fCAwO1xuXG4gICAgaWYgKGZyekVsKSB7XG4gICAgICBpZiAoc3RhdGUubGlxdWlkaXR5X2Zyb3plbikgdGhpcy5fc2hvdyhmcnpFbCwgXCJibG9ja1wiKTtcbiAgICAgIGVsc2UgdGhpcy5faGlkZShmcnpFbCk7XG4gICAgfVxuXG4gICAgY29uc3QgaG9sZGluZ3NFbCA9IGRvY3VtZW50LmdldEVsZW1lbnRCeUlkKFwibXktaG9sZGluZ3NcIik7XG4gICAgaWYgKGhvbGRpbmdzRWwgJiYgc3RhdGUucG9ydGZvbGlvKSB7XG4gICAgICBob2xkaW5nc0VsLmlubmVySFRNTCA9IFwiXCI7XG4gICAgICBPYmplY3QuZW50cmllcyhzdGF0ZS5wb3J0Zm9saW8pLmZvckVhY2goKFt0aWNrZXIsIHF0eV0pID0+IHtcbiAgICAgICAgaWYgKHF0eSA8PSAwKSByZXR1cm47XG4gICAgICAgIGNvbnN0IGRpdiA9IGRvY3VtZW50LmNyZWF0ZUVsZW1lbnQoXCJkaXZcIik7XG4gICAgICAgIGRpdi5jbGFzc05hbWUgPSBcImZsZXgganVzdGlmeS1iZXR3ZWVuIHRleHQteHMgZm9udC1tb25vXCI7XG4gICAgICAgIGRpdi5pbm5lckhUTUwgPSBgPHNwYW4gY2xhc3M9XCJ0ZXh0LWdyYXktNTAwXCI+JHtlc2NhcGVIdG1sKHRpY2tlcil9PC9zcGFuPjxzcGFuIGNsYXNzPVwidGV4dC1ncmF5LTMwMFwiPiR7cXR5fSBzaDwvc3Bhbj5gO1xuICAgICAgICBob2xkaW5nc0VsLmFwcGVuZENoaWxkKGRpdik7XG4gICAgICB9KTtcbiAgICB9XG4gIH1cblxuICAvLyBcdTI1MDBcdTI1MDAgUGhhc2UgVUkgXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXG5cbiAgX3JlbmRlclBoYXNlKHBoYXNlLCByb3VuZCkge1xuICAgIGNvbnN0IGNvbnRhaW5lciA9IGRvY3VtZW50LmdldEVsZW1lbnRCeUlkKFwicGhhc2UtYmFkZ2UtY29udGFpbmVyXCIpO1xuICAgIGlmICghY29udGFpbmVyKSByZXR1cm47XG4gICAgY29uc3QgbGFiZWwgPSBwaGFzZUxhYmVsKHBoYXNlKTtcbiAgICBjb250YWluZXIuaW5uZXJIVE1MID0gYDxzcGFuIGNsYXNzPVwicGhhc2UtYmFkZ2UgcGhhc2UtJHtwaGFzZSB8fCBcIndhaXRpbmdcIn1cIj4ke2xhYmVsfTwvc3Bhbj5gO1xuICB9XG5cbiAgX3JlbmRlclJvdW5kKHJvdW5kLCB0b3RhbCkge1xuICAgIGNvbnN0IGVsID0gZG9jdW1lbnQuZ2V0RWxlbWVudEJ5SWQoXCJjdXJyZW50LXJvdW5kXCIpO1xuICAgIGlmIChlbCAmJiByb3VuZCkgZWwudGV4dENvbnRlbnQgPSByb3VuZDtcbiAgfVxuXG4gIF9yZW5kZXJBY3Rpb25QYW5lbChwaGFzZSkge1xuICAgIGNvbnN0IHBhbmVsID0gZG9jdW1lbnQuZ2V0RWxlbWVudEJ5SWQoXCJhY3Rpb24tcGFuZWxcIik7XG4gICAgaWYgKCFwYW5lbCkgcmV0dXJuO1xuXG4gICAgaWYgKHBoYXNlID09PSBcImFjdGlvbl9zdWJtaXNzaW9uXCIpIHtcbiAgICAgIHRoaXMuX3Nob3cocGFuZWwsIFwiYmxvY2tcIik7XG4gICAgfSBlbHNlIHtcbiAgICAgIHRoaXMuX2hpZGUocGFuZWwpO1xuICAgIH1cbiAgfVxuICBfdXBkYXRlQ2hhdEluZGljYXRvcihwaGFzZSkge1xuICAgIGNvbnN0IGVsID0gZG9jdW1lbnQuZ2V0RWxlbWVudEJ5SWQoXCJjaGF0LXBoYXNlLWluZGljYXRvclwiKTtcbiAgICBpZiAoIWVsKSByZXR1cm47XG4gICAgY29uc3QgbWFwID0ge1xuICAgICAgbmVnb3RpYXRpb246IFwiT3BlblwiLFxuICAgICAgYWN0aW9uX3N1Ym1pc3Npb246IFwiQ2xvc2VkXCIsXG4gICAgICBuZXdzOiBcIkNsb3NlZFwiLFxuICAgICAgcmVzb2x1dGlvbjogXCJDbG9zZWRcIixcbiAgICAgIGRpc2Nsb3N1cmU6IFwiT3BlblwiLFxuICAgICAgZmluaXNoZWQ6IFwiT3BlblwiLFxuICAgIH07XG4gICAgZWwudGV4dENvbnRlbnQgPSBtYXBbcGhhc2VdIHx8IFwiXHUyMDE0XCI7XG4gICAgZWwuc3R5bGUuY29sb3IgPVxuICAgICAgcGhhc2UgPT09IFwibmVnb3RpYXRpb25cIiB8fCBwaGFzZSA9PT0gXCJkaXNjbG9zdXJlXCIgfHwgcGhhc2UgPT09IFwiZmluaXNoZWRcIlxuICAgICAgICA/IFwiIzEwYjk4MVwiXG4gICAgICAgIDogXCIjNmI3MjgwXCI7XG4gIH1cblxuICBfc2hvd1BoYXNlT3ZlcmxheShwaGFzZSkge1xuICAgIGNvbnN0IG92ZXJsYXkgPSBkb2N1bWVudC5nZXRFbGVtZW50QnlJZChcInBoYXNlLW92ZXJsYXlcIik7XG4gICAgY29uc3QgYmFkZ2UgPSBkb2N1bWVudC5nZXRFbGVtZW50QnlJZChcInBoYXNlLW92ZXJsYXktYmFkZ2VcIik7XG4gICAgY29uc3QgZGVzYyA9IGRvY3VtZW50LmdldEVsZW1lbnRCeUlkKFwicGhhc2Utb3ZlcmxheS1kZXNjXCIpO1xuICAgIGlmICghb3ZlcmxheSB8fCAhYmFkZ2UpIHJldHVybjtcblxuICAgIGNvbnN0IGRlc2NzID0ge1xuICAgICAgbmV3czogXCJBIG1hcmtldCBldmVudCBoYXMgb2NjdXJyZWQuIEFuYWx5c2UgaXQgY2FyZWZ1bGx5LlwiLFxuICAgICAgYWN0aW9uX3N1Ym1pc3Npb246IFwiQ2hvb3NlIHlvdXIgYWN0aW9uIHdpc2VseS4gTm8gb25lIGVsc2UgY2FuIHNlZSBpdC5cIixcbiAgICAgIG5lZ290aWF0aW9uOiBcIkRlYWxzLCBkZWNlcHRpb25zLCBhbmQgYWxsaWFuY2VzLiBDaGF0IGlzIG9wZW4uXCIsXG4gICAgICByZXNvbHV0aW9uOiBcIkFjdGlvbnMgYXJlIHJlc29sdmluZy4gQnJhY2UgZm9yIGltcGFjdC5cIixcbiAgICAgIGRpc2Nsb3N1cmU6IFwiUm91bmQgY29tcGxldGUuIFJlc3VsdHMgYXJlIGluLlwiLFxuICAgICAgZmluaXNoZWQ6IFwiVGhlIG1hcmtldCBjbG9zZXMuIEZpbmFsIHJhbmtpbmdzIHJldmVhbGVkLlwiLFxuICAgIH07XG5cbiAgICBiYWRnZS5jbGFzc05hbWUgPSBgcGhhc2UtYmFkZ2UgcGhhc2UtJHtwaGFzZX0gdGV4dC1sZyBweC02IHB5LTMgZm9udC1tb25vIHRyYWNraW5nLXdpZGVzdCB1cHBlcmNhc2VgO1xuICAgIGJhZGdlLnRleHRDb250ZW50ID0gcGhhc2VMYWJlbChwaGFzZSk7XG4gICAgaWYgKGRlc2MpIGRlc2MudGV4dENvbnRlbnQgPSBkZXNjc1twaGFzZV0gfHwgXCJcIjtcblxuICAgIHRoaXMuX3Nob3cob3ZlcmxheSwgXCJmbGV4XCIpO1xuXG4gICAgaWYgKHRoaXMucGhhc2VPdmVybGF5VGltZXIpIGNsZWFyVGltZW91dCh0aGlzLnBoYXNlT3ZlcmxheVRpbWVyKTtcbiAgICBpZiAocGhhc2UgIT09IFwiZmluaXNoZWRcIikge1xuICAgICAgdGhpcy5waGFzZU92ZXJsYXlUaW1lciA9IHNldFRpbWVvdXQoKCkgPT4gdGhpcy5faGlkZShvdmVybGF5KSwgMzAwMCk7XG4gICAgfVxuICB9XG5cbiAgLy8gXHUyNTAwXHUyNTAwIENvdW50ZG93biB0aW1lciBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcblxuICBfc3RhcnRDb3VudGRvd24oZGVhZGxpbmUpIHtcbiAgICB0aGlzLl9jbGVhckNvdW50ZG93bigpO1xuICAgIGNvbnN0IGVsID0gZG9jdW1lbnQuZ2V0RWxlbWVudEJ5SWQoXCJjb3VudGRvd24tdGltZXJcIik7XG4gICAgY29uc3QgdmFsRWwgPSBkb2N1bWVudC5nZXRFbGVtZW50QnlJZChcImNvdW50ZG93bi12YWx1ZVwiKTtcbiAgICBpZiAoIWVsIHx8ICF2YWxFbCkgcmV0dXJuO1xuXG4gICAgdGhpcy5fc2hvdyhlbCwgXCJmbGV4XCIpO1xuXG4gICAgY29uc3QgdGljayA9ICgpID0+IHtcbiAgICAgIGNvbnN0IGRpZmYgPSBNYXRoLm1heCgwLCBNYXRoLmZsb29yKChkZWFkbGluZSAtIERhdGUubm93KCkpIC8gMTAwMCkpO1xuICAgICAgY29uc3QgbSA9IE1hdGguZmxvb3IoZGlmZiAvIDYwKTtcbiAgICAgIGNvbnN0IHMgPSBkaWZmICUgNjA7XG4gICAgICB2YWxFbC50ZXh0Q29udGVudCA9IGAke219OiR7U3RyaW5nKHMpLnBhZFN0YXJ0KDIsIFwiMFwiKX1gO1xuXG4gICAgICBpZiAoZGlmZiA8PSAxMCkgZWwuY2xhc3NMaXN0LmFkZChcImNvdW50ZG93bi11cmdlbnRcIik7XG4gICAgICBlbHNlIGVsLmNsYXNzTGlzdC5yZW1vdmUoXCJjb3VudGRvd24tdXJnZW50XCIpO1xuXG4gICAgICBpZiAoZGlmZiA9PT0gMCkgdGhpcy5fY2xlYXJDb3VudGRvd24oKTtcbiAgICB9O1xuXG4gICAgdGljaygpO1xuICAgIHRoaXMuY291bnRkb3duID0gc2V0SW50ZXJ2YWwodGljaywgMTAwMCk7XG4gIH1cblxuICBfY2xlYXJDb3VudGRvd24oKSB7XG4gICAgaWYgKHRoaXMuY291bnRkb3duKSB7XG4gICAgICBjbGVhckludGVydmFsKHRoaXMuY291bnRkb3duKTtcbiAgICAgIHRoaXMuY291bnRkb3duID0gbnVsbDtcbiAgICB9XG5cbiAgICBjb25zdCBlbCA9IGRvY3VtZW50LmdldEVsZW1lbnRCeUlkKFwiY291bnRkb3duLXRpbWVyXCIpO1xuICAgIGlmIChlbCkge1xuICAgICAgdGhpcy5faGlkZShlbCk7XG4gICAgICBlbC5jbGFzc0xpc3QucmVtb3ZlKFwiZmxleFwiLCBcImNvdW50ZG93bi11cmdlbnRcIik7XG4gICAgfVxuICB9XG5cbiAgLy8gXHUyNTAwXHUyNTAwIExlYWRlcmJvYXJkIFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFxuXG4gIF9yZW5kZXJMZWFkZXJib2FyZChsZWFkZXJib2FyZCkge1xuICAgIGNvbnN0IGxpc3QgPSBkb2N1bWVudC5nZXRFbGVtZW50QnlJZChcImxlYWRlcmJvYXJkLWxpc3RcIik7XG4gICAgaWYgKCFsaXN0IHx8ICFsZWFkZXJib2FyZCkgcmV0dXJuO1xuXG4gICAgbGlzdC5pbm5lckhUTUwgPSBcIlwiO1xuICAgIGxlYWRlcmJvYXJkLmZvckVhY2goKGVudHJ5LCBpKSA9PiB7XG4gICAgICBjb25zdCBkaXYgPSBkb2N1bWVudC5jcmVhdGVFbGVtZW50KFwiZGl2XCIpO1xuICAgICAgZGl2LmNsYXNzTmFtZSA9IFwiZmxleCBpdGVtcy1jZW50ZXIgZ2FwLTMgcC0zIHJvdW5kZWQtbGcgY2FyZCBmYWRlLWluLXVwXCI7XG4gICAgICBkaXYuc3R5bGUuYW5pbWF0aW9uRGVsYXkgPSBgJHtpICogODB9bXNgO1xuXG4gICAgICBjb25zdCBtZWRhbHMgPSBbXCJcdUQ4M0VcdURENDdcIiwgXCJcdUQ4M0VcdURENDhcIiwgXCJcdUQ4M0VcdURENDlcIl07XG4gICAgICBkaXYuaW5uZXJIVE1MID0gYFxuICAgICAgICA8c3BhbiBjbGFzcz1cInRleHQteGwgdy04IHRleHQtY2VudGVyXCI+JHttZWRhbHNbaV0gfHwgYCMke2kgKyAxfWB9PC9zcGFuPlxuICAgICAgICA8c3BhbiBjbGFzcz1cImZvbnQtbW9ubyBmb250LXNlbWlib2xkIHRleHQtc20gdGV4dC13aGl0ZSBmbGV4LTFcIj4ke2VzY2FwZUh0bWwoZW50cnkudXNlcm5hbWUpfTwvc3Bhbj5cbiAgICAgICAgPHNwYW4gY2xhc3M9XCJmb250LW1vbm8gdGV4dC1zbSAke2kgPT09IDAgPyBcInRleHQtZW1lcmFsZC00MDAgZm9udC1ib2xkXCIgOiBcInRleHQtZ3JheS0zMDBcIn1cIj5cbiAgICAgICAgICAkJHtwYXJzZUZsb2F0KGVudHJ5Lm5ldF93b3J0aCkudG9Mb2NhbGVTdHJpbmcoXCJlbi1VU1wiLCB7IG1pbmltdW1GcmFjdGlvbkRpZ2l0czogMiwgbWF4aW11bUZyYWN0aW9uRGlnaXRzOiAyIH0pfVxuICAgICAgICA8L3NwYW4+XG4gICAgICBgO1xuICAgICAgbGlzdC5hcHBlbmRDaGlsZChkaXYpO1xuICAgIH0pO1xuICB9XG5cbiAgLy8gXHUyNTAwXHUyNTAwIEFjdGlvbiBNb2RhbCBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcblxuICBfb3BlbkFjdGlvbk1vZGFsKGRhdGFzZXQpIHtcbiAgICBjb25zdCBtb2RhbCA9IGRvY3VtZW50LmdldEVsZW1lbnRCeUlkKFwiYWN0aW9uLW1vZGFsXCIpO1xuICAgIGNvbnN0IHRpdGxlRWwgPSBkb2N1bWVudC5nZXRFbGVtZW50QnlJZChcIm1vZGFsLXRpdGxlXCIpO1xuICAgIGNvbnN0IGRlc2NFbCA9IGRvY3VtZW50LmdldEVsZW1lbnRCeUlkKFwibW9kYWwtZGVzY1wiKTtcbiAgICBjb25zdCBlcnJFbCA9IGRvY3VtZW50LmdldEVsZW1lbnRCeUlkKFwibW9kYWwtZXJyb3JcIik7XG4gICAgY29uc3QgdGlja2VyRGl2ID0gZG9jdW1lbnQuZ2V0RWxlbWVudEJ5SWQoXCJmaWVsZC10aWNrZXJcIik7XG4gICAgY29uc3QgcXR5RGl2ID0gZG9jdW1lbnQuZ2V0RWxlbWVudEJ5SWQoXCJmaWVsZC1xdWFudGl0eVwiKTtcbiAgICBjb25zdCB0YXJnZXREaXYgPSBkb2N1bWVudC5nZXRFbGVtZW50QnlJZChcImZpZWxkLXRhcmdldFwiKTtcbiAgICBjb25zdCBjb3N0RGl2ID0gZG9jdW1lbnQuZ2V0RWxlbWVudEJ5SWQoXCJjb3N0LWVzdGltYXRlXCIpO1xuICAgIGNvbnN0IHRpY2tlckJ0bnMgPSBkb2N1bWVudC5nZXRFbGVtZW50QnlJZChcInRpY2tlci1idXR0b25zXCIpO1xuICAgIGNvbnN0IHRhcmdldEJ0bnMgPSBkb2N1bWVudC5nZXRFbGVtZW50QnlJZChcInRhcmdldC1idXR0b25zXCIpO1xuICAgIGNvbnN0IHF0eUlucHV0ID0gZG9jdW1lbnQuZ2V0RWxlbWVudEJ5SWQoXCJpbnB1dC1xdWFudGl0eVwiKTtcblxuICAgIGlmICghbW9kYWwpIHJldHVybjtcblxuICAgIGlmICh0aXRsZUVsKSB0aXRsZUVsLnRleHRDb250ZW50ID0gZGF0YXNldC5sYWJlbCB8fCBkYXRhc2V0LmFjdGlvbiB8fCBcIlwiO1xuICAgIGlmIChkZXNjRWwpIGRlc2NFbC50ZXh0Q29udGVudCA9IGRhdGFzZXQuZGVzYyB8fCBcIlwiO1xuICAgIGlmIChlcnJFbCkge1xuICAgICAgZXJyRWwudGV4dENvbnRlbnQgPSBcIlwiO1xuICAgICAgdGhpcy5faGlkZShlcnJFbCk7XG4gICAgfVxuXG4gICAgY29uc3QgbmVlZHNUaWNrZXIgPSBkYXRhc2V0Lm5lZWRzVGlja2VyID09PSBcInRydWVcIjtcbiAgICBjb25zdCBuZWVkc1F0eSA9IGRhdGFzZXQubmVlZHNRdWFudGl0eSA9PT0gXCJ0cnVlXCI7XG4gICAgY29uc3QgbmVlZHNUYXJnZXQgPSBkYXRhc2V0Lm5lZWRzVGFyZ2V0ID09PSBcInRydWVcIjtcblxuICAgIG5lZWRzVGlja2VyID8gdGhpcy5fc2hvdyh0aWNrZXJEaXYsIFwiYmxvY2tcIikgOiB0aGlzLl9oaWRlKHRpY2tlckRpdik7XG4gICAgbmVlZHNRdHkgPyB0aGlzLl9zaG93KHF0eURpdiwgXCJibG9ja1wiKSA6IHRoaXMuX2hpZGUocXR5RGl2KTtcbiAgICBuZWVkc1RhcmdldCA/IHRoaXMuX3Nob3codGFyZ2V0RGl2LCBcImJsb2NrXCIpIDogdGhpcy5faGlkZSh0YXJnZXREaXYpO1xuICAgIHRoaXMuX2hpZGUoY29zdERpdik7XG5cbiAgICBpZiAobmVlZHNUaWNrZXIgJiYgdGlja2VyQnRucykge1xuICAgICAgdGlja2VyQnRucy5pbm5lckhUTUwgPSBcIlwiO1xuICAgICAgdGhpcy5jb21wYW5pZXMuZm9yRWFjaCgoYykgPT4ge1xuICAgICAgICBjb25zdCBidG4gPSBkb2N1bWVudC5jcmVhdGVFbGVtZW50KFwiYnV0dG9uXCIpO1xuICAgICAgICBidG4udHlwZSA9IFwiYnV0dG9uXCI7XG4gICAgICAgIGJ0bi5jbGFzc05hbWUgPSBcInRpY2tlci1waWxsXCI7XG4gICAgICAgIGJ0bi5kYXRhc2V0LnRpY2tlciA9IGMudGlja2VyO1xuICAgICAgICBidG4uaW5uZXJIVE1MID0gYDxzcGFuIGNsYXNzPVwiZm9udC1ib2xkXCI+JHtlc2NhcGVIdG1sKGMudGlja2VyKX08L3NwYW4+XG4gICAgICAgICAgICAgICAgICAgICAgICAgPHNwYW4gY2xhc3M9XCJ0ZXh0LWdyYXktNTAwIHRleHQteHMgbWwtMVwiPiQke3BhcnNlRmxvYXQoYy5wcmljZSkudG9GaXhlZCgyKX08L3NwYW4+YDtcbiAgICAgICAgYnRuLmFkZEV2ZW50TGlzdGVuZXIoXCJjbGlja1wiLCAoZSkgPT4ge1xuICAgICAgICAgIGUucHJldmVudERlZmF1bHQoKTtcbiAgICAgICAgICB0aWNrZXJCdG5zXG4gICAgICAgICAgICAucXVlcnlTZWxlY3RvckFsbChcIi50aWNrZXItcGlsbFwiKVxuICAgICAgICAgICAgLmZvckVhY2goKGIpID0+IGIuY2xhc3NMaXN0LnJlbW92ZShcInNlbGVjdGVkXCIpKTtcbiAgICAgICAgICBidG4uY2xhc3NMaXN0LmFkZChcInNlbGVjdGVkXCIpO1xuICAgICAgICAgIGlmIChuZWVkc1F0eSlcbiAgICAgICAgICAgIHRoaXMuX3VwZGF0ZUNvc3RFc3RpbWF0ZShkYXRhc2V0LmFjdGlvbiwgYy50aWNrZXIsIHF0eUlucHV0Py52YWx1ZSk7XG4gICAgICAgIH0pO1xuICAgICAgICB0aWNrZXJCdG5zLmFwcGVuZENoaWxkKGJ0bik7XG4gICAgICB9KTtcbiAgICB9XG5cbiAgICBpZiAobmVlZHNUYXJnZXQgJiYgdGFyZ2V0QnRucykge1xuICAgICAgdGFyZ2V0QnRucy5pbm5lckhUTUwgPSBcIlwiO1xuICAgICAgdGhpcy5wbGF5ZXJzLmZvckVhY2goKHApID0+IHtcbiAgICAgICAgaWYgKFN0cmluZyhwLnVzZXJfaWQpID09PSBTdHJpbmcodGhpcy5jZmcudXNlcklkKSkgcmV0dXJuO1xuXG4gICAgICAgIGNvbnN0IGJ0biA9IGRvY3VtZW50LmNyZWF0ZUVsZW1lbnQoXCJidXR0b25cIik7XG4gICAgICAgIGJ0bi50eXBlID0gXCJidXR0b25cIjtcbiAgICAgICAgYnRuLmNsYXNzTmFtZSA9IFwicGxheWVyLXBpbGxcIjtcbiAgICAgICAgYnRuLmRhdGFzZXQudGFyZ2V0SWQgPSBwLnVzZXJfaWQ7XG4gICAgICAgIGJ0bi5pbm5lckhUTUwgPSBgXG4gICAgICAgICAgPGRpdiBjbGFzcz1cInctMiBoLTIgcm91bmRlZC1mdWxsICR7cC5saXF1aWRpdHlfZnJvemVuID8gXCJiZy1ibHVlLTQwMFwiIDogXCJiZy1lbWVyYWxkLTQwMFwifVwiPjwvZGl2PlxuICAgICAgICAgIDxzcGFuIGNsYXNzPVwiZm9udC1tb25vIHRleHQtc20gdGV4dC1ncmF5LTIwMCBmbGV4LTFcIj4ke2VzY2FwZUh0bWwocC51c2VybmFtZSl9PC9zcGFuPlxuICAgICAgICAgICR7cC5saXF1aWRpdHlfZnJvemVuID8gYDxzcGFuIGNsYXNzPVwidGV4dC14cyB0ZXh0LWJsdWUtNDAwIGZvbnQtbW9ub1wiPlx1Mjc0NCBmcm96ZW48L3NwYW4+YCA6IFwiXCJ9XG4gICAgICAgICAgJHtwLnJlZ3VsYXRvcnlfaGVhdCA+IDAgPyBgPHNwYW4gY2xhc3M9XCJ0ZXh0LXhzIHRleHQtYW1iZXItNDAwIGZvbnQtbW9ub1wiPlx1MjY5NiAke3AucmVndWxhdG9yeV9oZWF0fTwvc3Bhbj5gIDogXCJcIn1cbiAgICAgICAgYDtcbiAgICAgICAgYnRuLmFkZEV2ZW50TGlzdGVuZXIoXCJjbGlja1wiLCAoZSkgPT4ge1xuICAgICAgICAgIGUucHJldmVudERlZmF1bHQoKTtcbiAgICAgICAgICB0YXJnZXRCdG5zXG4gICAgICAgICAgICAucXVlcnlTZWxlY3RvckFsbChcIi5wbGF5ZXItcGlsbFwiKVxuICAgICAgICAgICAgLmZvckVhY2goKGIpID0+IGIuY2xhc3NMaXN0LnJlbW92ZShcInNlbGVjdGVkXCIpKTtcbiAgICAgICAgICBidG4uY2xhc3NMaXN0LmFkZChcInNlbGVjdGVkXCIpO1xuICAgICAgICB9KTtcbiAgICAgICAgdGFyZ2V0QnRucy5hcHBlbmRDaGlsZChidG4pO1xuICAgICAgfSk7XG4gICAgfVxuXG4gICAgaWYgKHF0eUlucHV0KSB7XG4gICAgICBxdHlJbnB1dC52YWx1ZSA9IFwiXCI7XG4gICAgICBxdHlJbnB1dC5vbmlucHV0ID0gKCkgPT4ge1xuICAgICAgICBjb25zdCB0aWNrZXIgPSB0aWNrZXJCdG5zPy5xdWVyeVNlbGVjdG9yKFwiLnNlbGVjdGVkXCIpPy5kYXRhc2V0LnRpY2tlcjtcbiAgICAgICAgdGhpcy5fdXBkYXRlQ29zdEVzdGltYXRlKGRhdGFzZXQuYWN0aW9uLCB0aWNrZXIsIHF0eUlucHV0LnZhbHVlKTtcbiAgICAgIH07XG4gICAgfVxuXG4gICAgY29uc3QgZm9ybSA9IGRvY3VtZW50LmdldEVsZW1lbnRCeUlkKFwiYWN0aW9uLWZvcm1cIik7XG4gICAgaWYgKGZvcm0pIGZvcm0uZGF0YXNldC5hY3Rpb25UeXBlID0gZGF0YXNldC5hY3Rpb247XG5cbiAgICB0aGlzLl9zaG93KG1vZGFsLCBcImZsZXhcIik7XG5cbiAgICBzZXRUaW1lb3V0KCgpID0+IHtcbiAgICAgIGNvbnN0IGZvY3VzRWwgPVxuICAgICAgICAobmVlZHNUaWNrZXIgJiYgdGlja2VyQnRucz8ucXVlcnlTZWxlY3RvcihcIi50aWNrZXItcGlsbFwiKSkgfHxcbiAgICAgICAgKG5lZWRzUXR5ICYmIHF0eUlucHV0KSB8fFxuICAgICAgICAobmVlZHNUYXJnZXQgJiYgdGFyZ2V0QnRucz8ucXVlcnlTZWxlY3RvcihcIi5wbGF5ZXItcGlsbFwiKSkgfHxcbiAgICAgICAgZG9jdW1lbnQuZ2V0RWxlbWVudEJ5SWQoXCJtb2RhbC1jbG9zZVwiKTtcblxuICAgICAgZm9jdXNFbD8uZm9jdXM/LigpO1xuICAgIH0sIDIwKTtcbiAgfVxuXG4gIF9jbG9zZU1vZGFsKCkge1xuICAgIHRoaXMuX2hpZGUoZG9jdW1lbnQuZ2V0RWxlbWVudEJ5SWQoXCJhY3Rpb24tbW9kYWxcIikpO1xuICAgIHRoaXMuX2hpZGUoZG9jdW1lbnQuZ2V0RWxlbWVudEJ5SWQoXCJtb2RhbC1lcnJvclwiKSk7XG4gIH1cblxuICBfdXBkYXRlQ29zdEVzdGltYXRlKGFjdGlvbiwgdGlja2VyLCBxdHkpIHtcbiAgICBjb25zdCBjb3N0RGl2ID0gZG9jdW1lbnQuZ2V0RWxlbWVudEJ5SWQoXCJjb3N0LWVzdGltYXRlXCIpO1xuICAgIGNvbnN0IGNvc3RWYWxFbCA9IGRvY3VtZW50LmdldEVsZW1lbnRCeUlkKFwiY29zdC12YWx1ZVwiKTtcbiAgICBjb25zdCBjYXNoUmVtRWwgPSBkb2N1bWVudC5nZXRFbGVtZW50QnlJZChcImNhc2gtcmVtYWluaW5nXCIpO1xuXG4gICAgaWYgKFxuICAgICAgIWNvc3REaXYgfHxcbiAgICAgICF0aWNrZXIgfHxcbiAgICAgICFxdHkgfHxcbiAgICAgICFbXCJidXlcIiwgXCJzaG9ydFwiLCBcImFjcXVpcmVfc3Rha2VcIl0uaW5jbHVkZXMoYWN0aW9uKVxuICAgICkge1xuICAgICAgdGhpcy5faGlkZShjb3N0RGl2KTtcbiAgICAgIHJldHVybjtcbiAgICB9XG5cbiAgICBjb25zdCBjb21wYW55ID0gdGhpcy5jb21wYW5pZXMuZmluZCgoYykgPT4gYy50aWNrZXIgPT09IHRpY2tlcik7XG4gICAgaWYgKCFjb21wYW55KSB7XG4gICAgICB0aGlzLl9oaWRlKGNvc3REaXYpO1xuICAgICAgcmV0dXJuO1xuICAgIH1cblxuICAgIGNvbnN0IHByaWNlID0gcGFyc2VGbG9hdChjb21wYW55LnByaWNlKTtcbiAgICBjb25zdCBxID0gcGFyc2VJbnQocXR5LCAxMCkgfHwgMDtcbiAgICBjb25zdCB0b3RhbCA9IHByaWNlICogcTtcbiAgICBjb25zdCBteUNhc2ggPSB0aGlzLm15U3RhdGUgPyBwYXJzZUZsb2F0KHRoaXMubXlTdGF0ZS5jYXNoKSA6IDA7XG5cbiAgICB0aGlzLl9zaG93KGNvc3REaXYsIFwiYmxvY2tcIik7XG5cbiAgICBpZiAoY29zdFZhbEVsKSB7XG4gICAgICBjb3N0VmFsRWwudGV4dENvbnRlbnQgPSBgJCR7dG90YWwudG9Mb2NhbGVTdHJpbmcoXCJlbi1VU1wiLCB7XG4gICAgICAgIG1pbmltdW1GcmFjdGlvbkRpZ2l0czogMixcbiAgICAgICAgbWF4aW11bUZyYWN0aW9uRGlnaXRzOiAyLFxuICAgICAgfSl9YDtcbiAgICB9XG5cbiAgICBpZiAoY2FzaFJlbUVsKSB7XG4gICAgICBjYXNoUmVtRWwudGV4dENvbnRlbnQgPSBgJCR7TWF0aC5tYXgoMCwgbXlDYXNoIC0gdG90YWwpLnRvTG9jYWxlU3RyaW5nKFxuICAgICAgICBcImVuLVVTXCIsXG4gICAgICAgIHtcbiAgICAgICAgICBtaW5pbXVtRnJhY3Rpb25EaWdpdHM6IDIsXG4gICAgICAgICAgbWF4aW11bUZyYWN0aW9uRGlnaXRzOiAyLFxuICAgICAgICB9LFxuICAgICAgKX1gO1xuICAgICAgY2FzaFJlbUVsLnN0eWxlLmNvbG9yID0gbXlDYXNoIC0gdG90YWwgPCAwID8gXCIjZWY0NDQ0XCIgOiBcIiNkMWQ1ZGJcIjtcbiAgICB9XG4gIH1cblxuICBfc3VibWl0QWN0aW9uKCkge1xuICAgIGNvbnN0IGZvcm0gPSBkb2N1bWVudC5nZXRFbGVtZW50QnlJZChcImFjdGlvbi1mb3JtXCIpO1xuICAgIGNvbnN0IHN1Ym1pdEJ0biA9IGRvY3VtZW50LmdldEVsZW1lbnRCeUlkKFwibW9kYWwtc3VibWl0XCIpO1xuICAgIGlmICghZm9ybSkgcmV0dXJuO1xuXG4gICAgY29uc3QgYWN0aW9uVHlwZSA9IGZvcm0uZGF0YXNldC5hY3Rpb25UeXBlO1xuICAgIGlmICghYWN0aW9uVHlwZSkgcmV0dXJuO1xuXG4gICAgY29uc3QgcGFyYW1zID0ge307XG5cbiAgICBjb25zdCBzZWxlY3RlZFRpY2tlciA9IGRvY3VtZW50LnF1ZXJ5U2VsZWN0b3IoXG4gICAgICBcIiN0aWNrZXItYnV0dG9ucyAudGlja2VyLXBpbGwuc2VsZWN0ZWRcIixcbiAgICApO1xuICAgIGlmIChzZWxlY3RlZFRpY2tlcikgcGFyYW1zLnRpY2tlciA9IHNlbGVjdGVkVGlja2VyLmRhdGFzZXQudGlja2VyO1xuXG4gICAgY29uc3QgcXR5SW5wdXQgPSBkb2N1bWVudC5nZXRFbGVtZW50QnlJZChcImlucHV0LXF1YW50aXR5XCIpO1xuICAgIGNvbnN0IHF0eVBhcmVudCA9IGRvY3VtZW50LmdldEVsZW1lbnRCeUlkKFwiZmllbGQtcXVhbnRpdHlcIik7XG4gICAgY29uc3QgcXR5VmlzaWJsZSA9IHF0eVBhcmVudCAmJiBxdHlQYXJlbnQuc3R5bGUuZGlzcGxheSAhPT0gXCJub25lXCI7XG4gICAgaWYgKHF0eVZpc2libGUgJiYgcXR5SW5wdXQpIHtcbiAgICAgIGNvbnN0IHEgPSBwYXJzZUludChxdHlJbnB1dC52YWx1ZSwgMTApO1xuICAgICAgaWYgKCFxIHx8IHEgPD0gMCkge1xuICAgICAgICB0aGlzLl9zaG93TW9kYWxFcnJvcihcIlBsZWFzZSBlbnRlciBhIHZhbGlkIHF1YW50aXR5LlwiKTtcbiAgICAgICAgcmV0dXJuO1xuICAgICAgfVxuICAgICAgcGFyYW1zLnF1YW50aXR5ID0gcTtcbiAgICB9XG5cbiAgICBjb25zdCBzZWxlY3RlZFRhcmdldCA9IGRvY3VtZW50LnF1ZXJ5U2VsZWN0b3IoXG4gICAgICBcIiN0YXJnZXQtYnV0dG9ucyAucGxheWVyLXBpbGwuc2VsZWN0ZWRcIixcbiAgICApO1xuICAgIGlmIChzZWxlY3RlZFRhcmdldCkgcGFyYW1zLnRhcmdldF91c2VyX2lkID0gc2VsZWN0ZWRUYXJnZXQuZGF0YXNldC50YXJnZXRJZDtcblxuICAgIGNvbnN0IHRpY2tlclBhcmVudCA9IGRvY3VtZW50LmdldEVsZW1lbnRCeUlkKFwiZmllbGQtdGlja2VyXCIpO1xuICAgIGlmIChcbiAgICAgIHRpY2tlclBhcmVudCAmJlxuICAgICAgdGlja2VyUGFyZW50LnN0eWxlLmRpc3BsYXkgIT09IFwibm9uZVwiICYmXG4gICAgICAhcGFyYW1zLnRpY2tlclxuICAgICkge1xuICAgICAgdGhpcy5fc2hvd01vZGFsRXJyb3IoXCJQbGVhc2Ugc2VsZWN0IGEgY29tcGFueS5cIik7XG4gICAgICByZXR1cm47XG4gICAgfVxuXG4gICAgY29uc3QgdGFyZ2V0UGFyZW50ID0gZG9jdW1lbnQuZ2V0RWxlbWVudEJ5SWQoXCJmaWVsZC10YXJnZXRcIik7XG4gICAgaWYgKFxuICAgICAgdGFyZ2V0UGFyZW50ICYmXG4gICAgICB0YXJnZXRQYXJlbnQuc3R5bGUuZGlzcGxheSAhPT0gXCJub25lXCIgJiZcbiAgICAgICFwYXJhbXMudGFyZ2V0X3VzZXJfaWRcbiAgICApIHtcbiAgICAgIHRoaXMuX3Nob3dNb2RhbEVycm9yKFwiUGxlYXNlIHNlbGVjdCBhIHRhcmdldCBwbGF5ZXIuXCIpO1xuICAgICAgcmV0dXJuO1xuICAgIH1cblxuICAgIHN1Ym1pdEJ0bi5kaXNhYmxlZCA9IHRydWU7XG4gICAgc3VibWl0QnRuLnRleHRDb250ZW50ID0gXCJTdWJtaXR0aW5nLi4uXCI7XG5cbiAgICB0aGlzLmNoYW5uZWxcbiAgICAgIC5wdXNoKFwic3VibWl0X2FjdGlvblwiLCB7IGFjdGlvbl90eXBlOiBhY3Rpb25UeXBlLCBwYXJhbXMgfSlcbiAgICAgIC5yZWNlaXZlKFwib2tcIiwgKCkgPT4ge1xuICAgICAgICB0aGlzLl9jbG9zZU1vZGFsKCk7XG4gICAgICAgIHN1Ym1pdEJ0bi5kaXNhYmxlZCA9IGZhbHNlO1xuICAgICAgICBzdWJtaXRCdG4udGV4dENvbnRlbnQgPSBcIkNvbmZpcm0gQWN0aW9uXCI7XG5cbiAgICAgICAgZG9jdW1lbnQucXVlcnlTZWxlY3RvckFsbChcIi5hY3Rpb24tYnRuXCIpLmZvckVhY2goKGIpID0+IHtcbiAgICAgICAgICBiLmRpc2FibGVkID0gdHJ1ZTtcbiAgICAgICAgICBiLmNsYXNzTGlzdC5hZGQoXCJzdWJtaXR0ZWRcIik7XG4gICAgICAgIH0pO1xuXG4gICAgICAgIGNvbnN0IGJhZGdlID0gZG9jdW1lbnQuZ2V0RWxlbWVudEJ5SWQoXCJhY3Rpb24tc3VibWl0dGVkLWJhZGdlXCIpO1xuICAgICAgICB0aGlzLl9zaG93KGJhZGdlLCBcImZsZXhcIik7XG5cbiAgICAgICAgVG9hc3Quc2hvdyhcIkFjdGlvbiBzdWJtaXR0ZWQhIFdhaXRpbmcgZm9yIG90aGVyIHBsYXllcnMuXCIsIFwic3VjY2Vzc1wiKTtcbiAgICAgIH0pXG4gICAgICAucmVjZWl2ZShcImVycm9yXCIsIChlKSA9PiB7XG4gICAgICAgIHN1Ym1pdEJ0bi5kaXNhYmxlZCA9IGZhbHNlO1xuICAgICAgICBzdWJtaXRCdG4udGV4dENvbnRlbnQgPSBcIkNvbmZpcm0gQWN0aW9uXCI7XG4gICAgICAgIGNvbnN0IHJlYXNvbiA9XG4gICAgICAgICAgdHlwZW9mIGUucmVhc29uID09PSBcInN0cmluZ1wiID8gZS5yZWFzb24gOiBKU09OLnN0cmluZ2lmeShlLnJlYXNvbik7XG4gICAgICAgIHRoaXMuX3Nob3dNb2RhbEVycm9yKFwiUmVqZWN0ZWQ6IFwiICsgcmVhc29uKTtcbiAgICAgIH0pO1xuICB9XG5cbiAgX3Nob3dNb2RhbEVycm9yKG1zZykge1xuICAgIGNvbnN0IGVsID0gZG9jdW1lbnQuZ2V0RWxlbWVudEJ5SWQoXCJtb2RhbC1lcnJvclwiKTtcbiAgICBpZiAoIWVsKSByZXR1cm47XG4gICAgZWwudGV4dENvbnRlbnQgPSBtc2c7XG4gICAgdGhpcy5fc2hvdyhlbCwgXCJibG9ja1wiKTtcbiAgfVxuXG4gIF9zaG93KGVsLCBkaXNwbGF5ID0gXCJibG9ja1wiKSB7XG4gICAgaWYgKCFlbCkgcmV0dXJuO1xuICAgIGVsLmNsYXNzTGlzdC5yZW1vdmUoXCJoaWRkZW5cIik7XG4gICAgZWwuc3R5bGUuZGlzcGxheSA9IGRpc3BsYXk7XG4gIH1cblxuICBfaGlkZShlbCkge1xuICAgIGlmICghZWwpIHJldHVybjtcbiAgICBlbC5jbGFzc0xpc3QuYWRkKFwiaGlkZGVuXCIpO1xuICAgIGVsLnN0eWxlLmRpc3BsYXkgPSBcIm5vbmVcIjtcbiAgfVxuXG4gIC8vIFx1MjUwMFx1MjUwMCBDaGF0IFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFxuXG4gIF9zZW5kQ2hhdE1lc3NhZ2UoKSB7XG4gICAgY29uc3QgaW5wdXQgPSBkb2N1bWVudC5nZXRFbGVtZW50QnlJZChcImNoYXQtaW5wdXRcIik7XG4gICAgaWYgKCFpbnB1dCkgcmV0dXJuO1xuICAgIGNvbnN0IGNvbnRlbnQgPSBpbnB1dC52YWx1ZS50cmltKCk7XG4gICAgaWYgKCFjb250ZW50KSByZXR1cm47XG5cbiAgICB0aGlzLmNoYW5uZWxcbiAgICAgIC5wdXNoKFwic2VuZF9tZXNzYWdlXCIsIHsgY29udGVudCB9KVxuICAgICAgLnJlY2VpdmUoXCJva1wiLCAoKSA9PiB7XG4gICAgICAgIGlucHV0LnZhbHVlID0gXCJcIjtcbiAgICAgIH0pXG4gICAgICAucmVjZWl2ZShcImVycm9yXCIsIChlKSA9PlxuICAgICAgICBUb2FzdC5zaG93KFwiTWVzc2FnZSByZWplY3RlZDogXCIgKyBKU09OLnN0cmluZ2lmeShlLnJlYXNvbiksIFwiZXJyb3JcIiksXG4gICAgICApO1xuICB9XG5cbiAgX2FwcGVuZENoYXQobXNnLCBpc1doaXNwZXIgPSBmYWxzZSkge1xuICAgIGNvbnN0IGNvbnRhaW5lciA9IGRvY3VtZW50LmdldEVsZW1lbnRCeUlkKFwiY2hhdC1tZXNzYWdlc1wiKTtcbiAgICBpZiAoIWNvbnRhaW5lcikgcmV0dXJuO1xuXG4gICAgLy8gQ2xlYXIgcGxhY2Vob2xkZXJcbiAgICBjb25zdCBwbGFjZWhvbGRlciA9IGNvbnRhaW5lci5xdWVyeVNlbGVjdG9yKFwicFwiKTtcbiAgICBpZiAocGxhY2Vob2xkZXIgJiYgcGxhY2Vob2xkZXIuY2xhc3NMaXN0LmNvbnRhaW5zKFwidGV4dC1jZW50ZXJcIikpXG4gICAgICBwbGFjZWhvbGRlci5yZW1vdmUoKTtcblxuICAgIGNvbnN0IGVsID0gZG9jdW1lbnQuY3JlYXRlRWxlbWVudChcImRpdlwiKTtcbiAgICBlbC5jbGFzc05hbWUgPSBgZmxleCBnYXAtMS41IGZhZGUtaW4tdXAgJHtpc1doaXNwZXIgPyBcInBsLTIgYm9yZGVyLWwtMiBib3JkZXItcHVycGxlLTUwMC8zMFwiIDogXCJcIn1gO1xuXG4gICAgY29uc3QgdGltZSA9IG1zZy5pbnNlcnRlZF9hdFxuICAgICAgPyBuZXcgRGF0ZShtc2cuaW5zZXJ0ZWRfYXQpLnRvTG9jYWxlVGltZVN0cmluZyhbXSwge1xuICAgICAgICAgIGhvdXI6IFwiMi1kaWdpdFwiLFxuICAgICAgICAgIG1pbnV0ZTogXCIyLWRpZ2l0XCIsXG4gICAgICAgIH0pXG4gICAgICA6IFwiXCI7XG5cbiAgICBjb25zdCB0eXBlQ2xhc3MgPSBpc1doaXNwZXIgPyBcInRleHQtcHVycGxlLTQwMFwiIDogXCJ0ZXh0LWVtZXJhbGQtNDAwXCI7XG5cbiAgICBlbC5pbm5lckhUTUwgPSBgXG4gICAgICA8c3BhbiBjbGFzcz1cInRleHQtZ3JheS02MDAgZm9udC1tb25vIHRleHQteHMgc2hyaW5rLTAgbXQtMC41XCI+JHt0aW1lfTwvc3Bhbj5cbiAgICAgIDxkaXYgY2xhc3M9XCJtaW4tdy0wXCI+XG4gICAgICAgIDxzcGFuIGNsYXNzPVwiZm9udC1tb25vIHRleHQteHMgZm9udC1zZW1pYm9sZCAke3R5cGVDbGFzc31cIj4ke2VzY2FwZUh0bWwobXNnLnVzZXJuYW1lIHx8IFwiP1wiKX0ke2lzV2hpc3BlciA/IFwiIFx1MjE5MndoaXNwZXJcIiA6IFwiXCJ9Ojwvc3Bhbj5cbiAgICAgICAgPHNwYW4gY2xhc3M9XCJ0ZXh0LWdyYXktMzAwIHRleHQteHMgbWwtMSBicmVhay13b3Jkc1wiPiR7ZXNjYXBlSHRtbChtc2cuY29udGVudCl9PC9zcGFuPlxuICAgICAgPC9kaXY+XG4gICAgYDtcbiAgICBjb250YWluZXIuYXBwZW5kQ2hpbGQoZWwpO1xuICAgIGNvbnRhaW5lci5zY3JvbGxUb3AgPSBjb250YWluZXIuc2Nyb2xsSGVpZ2h0O1xuICB9XG59XG5cbi8vIFx1MjUwMFx1MjUwMFx1MjUwMCBIZWxwZXJzIFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFxuXG5mdW5jdGlvbiBwaGFzZUxhYmVsKHBoYXNlKSB7XG4gIGNvbnN0IGxhYmVscyA9IHtcbiAgICB3YWl0aW5nOiBcIldBSVRJTkdcIixcbiAgICBuZXdzOiBcIk5FV1NcIixcbiAgICBhY3Rpb25fc3VibWlzc2lvbjogXCJTVUJNSVQgQUNUSU9OXCIsXG4gICAgbmVnb3RpYXRpb246IFwiTkVHT1RJQVRJT05cIixcbiAgICByZXNvbHV0aW9uOiBcIlJFU09MVklOR1wiLFxuICAgIGRpc2Nsb3N1cmU6IFwiRElTQ0xPU1VSRVwiLFxuICAgIGZpbmlzaGVkOiBcIkZJTklTSEVEXCIsXG4gIH07XG4gIHJldHVybiAoXG4gICAgbGFiZWxzW3BoYXNlXSB8fCAocGhhc2UgPyBwaGFzZS5yZXBsYWNlKC9fL2csIFwiIFwiKS50b1VwcGVyQ2FzZSgpIDogXCJcdTIwMTRcIilcbiAgKTtcbn1cblxuZnVuY3Rpb24gZXNjYXBlSHRtbChzdHIpIHtcbiAgcmV0dXJuIFN0cmluZyhzdHIgPz8gXCJcIilcbiAgICAucmVwbGFjZSgvJi9nLCBcIiZhbXA7XCIpXG4gICAgLnJlcGxhY2UoLzwvZywgXCImbHQ7XCIpXG4gICAgLnJlcGxhY2UoLz4vZywgXCImZ3Q7XCIpXG4gICAgLnJlcGxhY2UoL1wiL2csIFwiJnF1b3Q7XCIpXG4gICAgLnJlcGxhY2UoLycvZywgXCImIzM5O1wiKTtcbn1cbmlmIChkb2N1bWVudC5yZWFkeVN0YXRlID09PSBcImxvYWRpbmdcIikge1xuICBkb2N1bWVudC5hZGRFdmVudExpc3RlbmVyKFwiRE9NQ29udGVudExvYWRlZFwiLCAoKSA9PiBib290KGNvbmZpZyksIHtcbiAgICBvbmNlOiB0cnVlLFxuICB9KTtcbn0gZWxzZSB7XG4gIGJvb3QoY29uZmlnKTtcbn1cbiJdLAogICJtYXBwaW5ncyI6ICI7O0FBQ08sTUFBSSxVQUFVLENBQUMsVUFBVTtBQUM5QixRQUFHLE9BQU8sVUFBVSxZQUFXO0FBQzdCLGFBQU87SUFDVCxPQUFPO0FBQ0wsVUFBSUEsV0FBVSxXQUFXO0FBQUUsZUFBTztNQUFNO0FBQ3hDLGFBQU9BO0lBQ1Q7RUFDRjtBQ1JPLE1BQU0sYUFBYSxPQUFPLFNBQVMs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
