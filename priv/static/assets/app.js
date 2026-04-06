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
      this.onlineUsers = /* @__PURE__ */ new Set();
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
      const events = payload.public_events || [];
      if (events.length > 0) {
        this._renderEvents(events);
        this._renderNews(events);
      }
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
      if (online)
        this.onlineUsers.add(String(userId));
      else
        this.onlineUsers.delete(String(userId));
      this._applyOnlineState();
    }
    _applyOnlineState() {
      document.querySelectorAll("[data-player-id]").forEach((row) => {
        const dot = row.querySelector(".presence-dot");
        if (dot)
          dot.classList.toggle("offline", !this.onlineUsers.has(row.dataset.playerId));
      });
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
      this._applyOnlineState();
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
          this.sparklineCharts[c.ticker] = new SparklineChart(chartId, {
            width: 80,
            height: 28
          });
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
//# sourceMappingURL=data:application/json;base64,ewogICJ2ZXJzaW9uIjogMywKICAic291cmNlcyI6IFsiLi4vLi4vLi4vZGVwcy9waG9lbml4L2Fzc2V0cy9qcy9waG9lbml4L3V0aWxzLmpzIiwgIi4uLy4uLy4uL2RlcHMvcGhvZW5peC9hc3NldHMvanMvcGhvZW5peC9jb25zdGFudHMuanMiLCAiLi4vLi4vLi4vZGVwcy9waG9lbml4L2Fzc2V0cy9qcy9waG9lbml4L3B1c2guanMiLCAiLi4vLi4vLi4vZGVwcy9waG9lbml4L2Fzc2V0cy9qcy9waG9lbml4L3RpbWVyLmpzIiwgIi4uLy4uLy4uL2RlcHMvcGhvZW5peC9hc3NldHMvanMvcGhvZW5peC9jaGFubmVsLmpzIiwgIi4uLy4uLy4uL2RlcHMvcGhvZW5peC9hc3NldHMvanMvcGhvZW5peC9hamF4LmpzIiwgIi4uLy4uLy4uL2RlcHMvcGhvZW5peC9hc3NldHMvanMvcGhvZW5peC9sb25ncG9sbC5qcyIsICIuLi8uLi8uLi9kZXBzL3Bob2VuaXgvYXNzZXRzL2pzL3Bob2VuaXgvcHJlc2VuY2UuanMiLCAiLi4vLi4vLi4vZGVwcy9waG9lbml4L2Fzc2V0cy9qcy9waG9lbml4L3NlcmlhbGl6ZXIuanMiLCAiLi4vLi4vLi4vZGVwcy9waG9lbml4L2Fzc2V0cy9qcy9waG9lbml4L3NvY2tldC5qcyIsICIuLi8uLi8uLi9hc3NldHMvanMvYXBwLmpzIl0sCiAgInNvdXJjZXNDb250ZW50IjogWyIvLyB3cmFwcyB2YWx1ZSBpbiBjbG9zdXJlIG9yIHJldHVybnMgY2xvc3VyZVxuZXhwb3J0IGxldCBjbG9zdXJlID0gKHZhbHVlKSA9PiB7XG4gIGlmKHR5cGVvZiB2YWx1ZSA9PT0gXCJmdW5jdGlvblwiKXtcbiAgICByZXR1cm4gdmFsdWVcbiAgfSBlbHNlIHtcbiAgICBsZXQgY2xvc3VyZSA9IGZ1bmN0aW9uICgpeyByZXR1cm4gdmFsdWUgfVxuICAgIHJldHVybiBjbG9zdXJlXG4gIH1cbn1cbiIsICJleHBvcnQgY29uc3QgZ2xvYmFsU2VsZiA9IHR5cGVvZiBzZWxmICE9PSBcInVuZGVmaW5lZFwiID8gc2VsZiA6IG51bGxcbmV4cG9ydCBjb25zdCBwaHhXaW5kb3cgPSB0eXBlb2Ygd2luZG93ICE9PSBcInVuZGVmaW5lZFwiID8gd2luZG93IDogbnVsbFxuZXhwb3J0IGNvbnN0IGdsb2JhbCA9IGdsb2JhbFNlbGYgfHwgcGh4V2luZG93IHx8IGdsb2JhbFxuZXhwb3J0IGNvbnN0IERFRkFVTFRfVlNOID0gXCIyLjAuMFwiXG5leHBvcnQgY29uc3QgU09DS0VUX1NUQVRFUyA9IHtjb25uZWN0aW5nOiAwLCBvcGVuOiAxLCBjbG9zaW5nOiAyLCBjbG9zZWQ6IDN9XG5leHBvcnQgY29uc3QgREVGQVVMVF9USU1FT1VUID0gMTAwMDBcbmV4cG9ydCBjb25zdCBXU19DTE9TRV9OT1JNQUwgPSAxMDAwXG5leHBvcnQgY29uc3QgQ0hBTk5FTF9TVEFURVMgPSB7XG4gIGNsb3NlZDogXCJjbG9zZWRcIixcbiAgZXJyb3JlZDogXCJlcnJvcmVkXCIsXG4gIGpvaW5lZDogXCJqb2luZWRcIixcbiAgam9pbmluZzogXCJqb2luaW5nXCIsXG4gIGxlYXZpbmc6IFwibGVhdmluZ1wiLFxufVxuZXhwb3J0IGNvbnN0IENIQU5ORUxfRVZFTlRTID0ge1xuICBjbG9zZTogXCJwaHhfY2xvc2VcIixcbiAgZXJyb3I6IFwicGh4X2Vycm9yXCIsXG4gIGpvaW46IFwicGh4X2pvaW5cIixcbiAgcmVwbHk6IFwicGh4X3JlcGx5XCIsXG4gIGxlYXZlOiBcInBoeF9sZWF2ZVwiXG59XG5cbmV4cG9ydCBjb25zdCBUUkFOU1BPUlRTID0ge1xuICBsb25ncG9sbDogXCJsb25ncG9sbFwiLFxuICB3ZWJzb2NrZXQ6IFwid2Vic29ja2V0XCJcbn1cbmV4cG9ydCBjb25zdCBYSFJfU1RBVEVTID0ge1xuICBjb21wbGV0ZTogNFxufVxuIiwgIi8qKlxuICogSW5pdGlhbGl6ZXMgdGhlIFB1c2hcbiAqIEBwYXJhbSB7Q2hhbm5lbH0gY2hhbm5lbCAtIFRoZSBDaGFubmVsXG4gKiBAcGFyYW0ge3N0cmluZ30gZXZlbnQgLSBUaGUgZXZlbnQsIGZvciBleGFtcGxlIGBcInBoeF9qb2luXCJgXG4gKiBAcGFyYW0ge09iamVjdH0gcGF5bG9hZCAtIFRoZSBwYXlsb2FkLCBmb3IgZXhhbXBsZSBge3VzZXJfaWQ6IDEyM31gXG4gKiBAcGFyYW0ge251bWJlcn0gdGltZW91dCAtIFRoZSBwdXNoIHRpbWVvdXQgaW4gbWlsbGlzZWNvbmRzXG4gKi9cbmV4cG9ydCBkZWZhdWx0IGNsYXNzIFB1c2gge1xuICBjb25zdHJ1Y3RvcihjaGFubmVsLCBldmVudCwgcGF5bG9hZCwgdGltZW91dCl7XG4gICAgdGhpcy5jaGFubmVsID0gY2hhbm5lbFxuICAgIHRoaXMuZXZlbnQgPSBldmVudFxuICAgIHRoaXMucGF5bG9hZCA9IHBheWxvYWQgfHwgZnVuY3Rpb24gKCl7IHJldHVybiB7fSB9XG4gICAgdGhpcy5yZWNlaXZlZFJlc3AgPSBudWxsXG4gICAgdGhpcy50aW1lb3V0ID0gdGltZW91dFxuICAgIHRoaXMudGltZW91dFRpbWVyID0gbnVsbFxuICAgIHRoaXMucmVjSG9va3MgPSBbXVxuICAgIHRoaXMuc2VudCA9IGZhbHNlXG4gIH1cblxuICAvKipcbiAgICpcbiAgICogQHBhcmFtIHtudW1iZXJ9IHRpbWVvdXRcbiAgICovXG4gIHJlc2VuZCh0aW1lb3V0KXtcbiAgICB0aGlzLnRpbWVvdXQgPSB0aW1lb3V0XG4gICAgdGhpcy5yZXNldCgpXG4gICAgdGhpcy5zZW5kKClcbiAgfVxuXG4gIC8qKlxuICAgKlxuICAgKi9cbiAgc2VuZCgpe1xuICAgIGlmKHRoaXMuaGFzUmVjZWl2ZWQoXCJ0aW1lb3V0XCIpKXsgcmV0dXJuIH1cbiAgICB0aGlzLnN0YXJ0VGltZW91dCgpXG4gICAgdGhpcy5zZW50ID0gdHJ1ZVxuICAgIHRoaXMuY2hhbm5lbC5zb2NrZXQucHVzaCh7XG4gICAgICB0b3BpYzogdGhpcy5jaGFubmVsLnRvcGljLFxuICAgICAgZXZlbnQ6IHRoaXMuZXZlbnQsXG4gICAgICBwYXlsb2FkOiB0aGlzLnBheWxvYWQoKSxcbiAgICAgIHJlZjogdGhpcy5yZWYsXG4gICAgICBqb2luX3JlZjogdGhpcy5jaGFubmVsLmpvaW5SZWYoKVxuICAgIH0pXG4gIH1cblxuICAvKipcbiAgICpcbiAgICogQHBhcmFtIHsqfSBzdGF0dXNcbiAgICogQHBhcmFtIHsqfSBjYWxsYmFja1xuICAgKi9cbiAgcmVjZWl2ZShzdGF0dXMsIGNhbGxiYWNrKXtcbiAgICBpZih0aGlzLmhhc1JlY2VpdmVkKHN0YXR1cykpe1xuICAgICAgY2FsbGJhY2sodGhpcy5yZWNlaXZlZFJlc3AucmVzcG9uc2UpXG4gICAgfVxuXG4gICAgdGhpcy5yZWNIb29rcy5wdXNoKHtzdGF0dXMsIGNhbGxiYWNrfSlcbiAgICByZXR1cm4gdGhpc1xuICB9XG5cbiAgLyoqXG4gICAqIEBwcml2YXRlXG4gICAqL1xuICByZXNldCgpe1xuICAgIHRoaXMuY2FuY2VsUmVmRXZlbnQoKVxuICAgIHRoaXMucmVmID0gbnVsbFxuICAgIHRoaXMucmVmRXZlbnQgPSBudWxsXG4gICAgdGhpcy5yZWNlaXZlZFJlc3AgPSBudWxsXG4gICAgdGhpcy5zZW50ID0gZmFsc2VcbiAgfVxuXG4gIC8qKlxuICAgKiBAcHJpdmF0ZVxuICAgKi9cbiAgbWF0Y2hSZWNlaXZlKHtzdGF0dXMsIHJlc3BvbnNlLCBfcmVmfSl7XG4gICAgdGhpcy5yZWNIb29rcy5maWx0ZXIoaCA9PiBoLnN0YXR1cyA9PT0gc3RhdHVzKVxuICAgICAgLmZvckVhY2goaCA9PiBoLmNhbGxiYWNrKHJlc3BvbnNlKSlcbiAgfVxuXG4gIC8qKlxuICAgKiBAcHJpdmF0ZVxuICAgKi9cbiAgY2FuY2VsUmVmRXZlbnQoKXtcbiAgICBpZighdGhpcy5yZWZFdmVudCl7IHJldHVybiB9XG4gICAgdGhpcy5jaGFubmVsLm9mZih0aGlzLnJlZkV2ZW50KVxuICB9XG5cbiAgLyoqXG4gICAqIEBwcml2YXRlXG4gICAqL1xuICBjYW5jZWxUaW1lb3V0KCl7XG4gICAgY2xlYXJUaW1lb3V0KHRoaXMudGltZW91dFRpbWVyKVxuICAgIHRoaXMudGltZW91dFRpbWVyID0gbnVsbFxuICB9XG5cbiAgLyoqXG4gICAqIEBwcml2YXRlXG4gICAqL1xuICBzdGFydFRpbWVvdXQoKXtcbiAgICBpZih0aGlzLnRpbWVvdXRUaW1lcil7IHRoaXMuY2FuY2VsVGltZW91dCgpIH1cbiAgICB0aGlzLnJlZiA9IHRoaXMuY2hhbm5lbC5zb2NrZXQubWFrZVJlZigpXG4gICAgdGhpcy5yZWZFdmVudCA9IHRoaXMuY2hhbm5lbC5yZXBseUV2ZW50TmFtZSh0aGlzLnJlZilcblxuICAgIHRoaXMuY2hhbm5lbC5vbih0aGlzLnJlZkV2ZW50LCBwYXlsb2FkID0+IHtcbiAgICAgIHRoaXMuY2FuY2VsUmVmRXZlbnQoKVxuICAgICAgdGhpcy5jYW5jZWxUaW1lb3V0KClcbiAgICAgIHRoaXMucmVjZWl2ZWRSZXNwID0gcGF5bG9hZFxuICAgICAgdGhpcy5tYXRjaFJlY2VpdmUocGF5bG9hZClcbiAgICB9KVxuXG4gICAgdGhpcy50aW1lb3V0VGltZXIgPSBzZXRUaW1lb3V0KCgpID0+IHtcbiAgICAgIHRoaXMudHJpZ2dlcihcInRpbWVvdXRcIiwge30pXG4gICAgfSwgdGhpcy50aW1lb3V0KVxuICB9XG5cbiAgLyoqXG4gICAqIEBwcml2YXRlXG4gICAqL1xuICBoYXNSZWNlaXZlZChzdGF0dXMpe1xuICAgIHJldHVybiB0aGlzLnJlY2VpdmVkUmVzcCAmJiB0aGlzLnJlY2VpdmVkUmVzcC5zdGF0dXMgPT09IHN0YXR1c1xuICB9XG5cbiAgLyoqXG4gICAqIEBwcml2YXRlXG4gICAqL1xuICB0cmlnZ2VyKHN0YXR1cywgcmVzcG9uc2Upe1xuICAgIHRoaXMuY2hhbm5lbC50cmlnZ2VyKHRoaXMucmVmRXZlbnQsIHtzdGF0dXMsIHJlc3BvbnNlfSlcbiAgfVxufVxuIiwgIi8qKlxuICpcbiAqIENyZWF0ZXMgYSB0aW1lciB0aGF0IGFjY2VwdHMgYSBgdGltZXJDYWxjYCBmdW5jdGlvbiB0byBwZXJmb3JtXG4gKiBjYWxjdWxhdGVkIHRpbWVvdXQgcmV0cmllcywgc3VjaCBhcyBleHBvbmVudGlhbCBiYWNrb2ZmLlxuICpcbiAqIEBleGFtcGxlXG4gKiBsZXQgcmVjb25uZWN0VGltZXIgPSBuZXcgVGltZXIoKCkgPT4gdGhpcy5jb25uZWN0KCksIGZ1bmN0aW9uKHRyaWVzKXtcbiAqICAgcmV0dXJuIFsxMDAwLCA1MDAwLCAxMDAwMF1bdHJpZXMgLSAxXSB8fCAxMDAwMFxuICogfSlcbiAqIHJlY29ubmVjdFRpbWVyLnNjaGVkdWxlVGltZW91dCgpIC8vIGZpcmVzIGFmdGVyIDEwMDBcbiAqIHJlY29ubmVjdFRpbWVyLnNjaGVkdWxlVGltZW91dCgpIC8vIGZpcmVzIGFmdGVyIDUwMDBcbiAqIHJlY29ubmVjdFRpbWVyLnJlc2V0KClcbiAqIHJlY29ubmVjdFRpbWVyLnNjaGVkdWxlVGltZW91dCgpIC8vIGZpcmVzIGFmdGVyIDEwMDBcbiAqXG4gKiBAcGFyYW0ge0Z1bmN0aW9ufSBjYWxsYmFja1xuICogQHBhcmFtIHtGdW5jdGlvbn0gdGltZXJDYWxjXG4gKi9cbmV4cG9ydCBkZWZhdWx0IGNsYXNzIFRpbWVyIHtcbiAgY29uc3RydWN0b3IoY2FsbGJhY2ssIHRpbWVyQ2FsYyl7XG4gICAgdGhpcy5jYWxsYmFjayA9IGNhbGxiYWNrXG4gICAgdGhpcy50aW1lckNhbGMgPSB0aW1lckNhbGNcbiAgICB0aGlzLnRpbWVyID0gbnVsbFxuICAgIHRoaXMudHJpZXMgPSAwXG4gIH1cblxuICByZXNldCgpe1xuICAgIHRoaXMudHJpZXMgPSAwXG4gICAgY2xlYXJUaW1lb3V0KHRoaXMudGltZXIpXG4gIH1cblxuICAvKipcbiAgICogQ2FuY2VscyBhbnkgcHJldmlvdXMgc2NoZWR1bGVUaW1lb3V0IGFuZCBzY2hlZHVsZXMgY2FsbGJhY2tcbiAgICovXG4gIHNjaGVkdWxlVGltZW91dCgpe1xuICAgIGNsZWFyVGltZW91dCh0aGlzLnRpbWVyKVxuXG4gICAgdGhpcy50aW1lciA9IHNldFRpbWVvdXQoKCkgPT4ge1xuICAgICAgdGhpcy50cmllcyA9IHRoaXMudHJpZXMgKyAxXG4gICAgICB0aGlzLmNhbGxiYWNrKClcbiAgICB9LCB0aGlzLnRpbWVyQ2FsYyh0aGlzLnRyaWVzICsgMSkpXG4gIH1cbn1cbiIsICJpbXBvcnQge2Nsb3N1cmV9IGZyb20gXCIuL3V0aWxzXCJcbmltcG9ydCB7XG4gIENIQU5ORUxfRVZFTlRTLFxuICBDSEFOTkVMX1NUQVRFUyxcbn0gZnJvbSBcIi4vY29uc3RhbnRzXCJcblxuaW1wb3J0IFB1c2ggZnJvbSBcIi4vcHVzaFwiXG5pbXBvcnQgVGltZXIgZnJvbSBcIi4vdGltZXJcIlxuXG4vKipcbiAqXG4gKiBAcGFyYW0ge3N0cmluZ30gdG9waWNcbiAqIEBwYXJhbSB7KE9iamVjdHxmdW5jdGlvbil9IHBhcmFtc1xuICogQHBhcmFtIHtTb2NrZXR9IHNvY2tldFxuICovXG5leHBvcnQgZGVmYXVsdCBjbGFzcyBDaGFubmVsIHtcbiAgY29uc3RydWN0b3IodG9waWMsIHBhcmFtcywgc29ja2V0KXtcbiAgICB0aGlzLnN0YXRlID0gQ0hBTk5FTF9TVEFURVMuY2xvc2VkXG4gICAgdGhpcy50b3BpYyA9IHRvcGljXG4gICAgdGhpcy5wYXJhbXMgPSBjbG9zdXJlKHBhcmFtcyB8fCB7fSlcbiAgICB0aGlzLnNvY2tldCA9IHNvY2tldFxuICAgIHRoaXMuYmluZGluZ3MgPSBbXVxuICAgIHRoaXMuYmluZGluZ1JlZiA9IDBcbiAgICB0aGlzLnRpbWVvdXQgPSB0aGlzLnNvY2tldC50aW1lb3V0XG4gICAgdGhpcy5qb2luZWRPbmNlID0gZmFsc2VcbiAgICB0aGlzLmpvaW5QdXNoID0gbmV3IFB1c2godGhpcywgQ0hBTk5FTF9FVkVOVFMuam9pbiwgdGhpcy5wYXJhbXMsIHRoaXMudGltZW91dClcbiAgICB0aGlzLnB1c2hCdWZmZXIgPSBbXVxuICAgIHRoaXMuc3RhdGVDaGFuZ2VSZWZzID0gW11cblxuICAgIHRoaXMucmVqb2luVGltZXIgPSBuZXcgVGltZXIoKCkgPT4ge1xuICAgICAgaWYodGhpcy5zb2NrZXQuaXNDb25uZWN0ZWQoKSl7IHRoaXMucmVqb2luKCkgfVxuICAgIH0sIHRoaXMuc29ja2V0LnJlam9pbkFmdGVyTXMpXG4gICAgdGhpcy5zdGF0ZUNoYW5nZVJlZnMucHVzaCh0aGlzLnNvY2tldC5vbkVycm9yKCgpID0+IHRoaXMucmVqb2luVGltZXIucmVzZXQoKSkpXG4gICAgdGhpcy5zdGF0ZUNoYW5nZVJlZnMucHVzaCh0aGlzLnNvY2tldC5vbk9wZW4oKCkgPT4ge1xuICAgICAgdGhpcy5yZWpvaW5UaW1lci5yZXNldCgpXG4gICAgICBpZih0aGlzLmlzRXJyb3JlZCgpKXsgdGhpcy5yZWpvaW4oKSB9XG4gICAgfSlcbiAgICApXG4gICAgdGhpcy5qb2luUHVzaC5yZWNlaXZlKFwib2tcIiwgKCkgPT4ge1xuICAgICAgdGhpcy5zdGF0ZSA9IENIQU5ORUxfU1RBVEVTLmpvaW5lZFxuICAgICAgdGhpcy5yZWpvaW5UaW1lci5yZXNldCgpXG4gICAgICB0aGlzLnB1c2hCdWZmZXIuZm9yRWFjaChwdXNoRXZlbnQgPT4gcHVzaEV2ZW50LnNlbmQoKSlcbiAgICAgIHRoaXMucHVzaEJ1ZmZlciA9IFtdXG4gICAgfSlcbiAgICB0aGlzLmpvaW5QdXNoLnJlY2VpdmUoXCJlcnJvclwiLCAoKSA9PiB7XG4gICAgICB0aGlzLnN0YXRlID0gQ0hBTk5FTF9TVEFURVMuZXJyb3JlZFxuICAgICAgaWYodGhpcy5zb2NrZXQuaXNDb25uZWN0ZWQoKSl7IHRoaXMucmVqb2luVGltZXIuc2NoZWR1bGVUaW1lb3V0KCkgfVxuICAgIH0pXG4gICAgdGhpcy5vbkNsb3NlKCgpID0+IHtcbiAgICAgIHRoaXMucmVqb2luVGltZXIucmVzZXQoKVxuICAgICAgaWYodGhpcy5zb2NrZXQuaGFzTG9nZ2VyKCkpIHRoaXMuc29ja2V0LmxvZyhcImNoYW5uZWxcIiwgYGNsb3NlICR7dGhpcy50b3BpY30gJHt0aGlzLmpvaW5SZWYoKX1gKVxuICAgICAgdGhpcy5zdGF0ZSA9IENIQU5ORUxfU1RBVEVTLmNsb3NlZFxuICAgICAgdGhpcy5zb2NrZXQucmVtb3ZlKHRoaXMpXG4gICAgfSlcbiAgICB0aGlzLm9uRXJyb3IocmVhc29uID0+IHtcbiAgICAgIGlmKHRoaXMuc29ja2V0Lmhhc0xvZ2dlcigpKSB0aGlzLnNvY2tldC5sb2coXCJjaGFubmVsXCIsIGBlcnJvciAke3RoaXMudG9waWN9YCwgcmVhc29uKVxuICAgICAgaWYodGhpcy5pc0pvaW5pbmcoKSl7IHRoaXMuam9pblB1c2gucmVzZXQoKSB9XG4gICAgICB0aGlzLnN0YXRlID0gQ0hBTk5FTF9TVEFURVMuZXJyb3JlZFxuICAgICAgaWYodGhpcy5zb2NrZXQuaXNDb25uZWN0ZWQoKSl7IHRoaXMucmVqb2luVGltZXIuc2NoZWR1bGVUaW1lb3V0KCkgfVxuICAgIH0pXG4gICAgdGhpcy5qb2luUHVzaC5yZWNlaXZlKFwidGltZW91dFwiLCAoKSA9PiB7XG4gICAgICBpZih0aGlzLnNvY2tldC5oYXNMb2dnZXIoKSkgdGhpcy5zb2NrZXQubG9nKFwiY2hhbm5lbFwiLCBgdGltZW91dCAke3RoaXMudG9waWN9ICgke3RoaXMuam9pblJlZigpfSlgLCB0aGlzLmpvaW5QdXNoLnRpbWVvdXQpXG4gICAgICBsZXQgbGVhdmVQdXNoID0gbmV3IFB1c2godGhpcywgQ0hBTk5FTF9FVkVOVFMubGVhdmUsIGNsb3N1cmUoe30pLCB0aGlzLnRpbWVvdXQpXG4gICAgICBsZWF2ZVB1c2guc2VuZCgpXG4gICAgICB0aGlzLnN0YXRlID0gQ0hBTk5FTF9TVEFURVMuZXJyb3JlZFxuICAgICAgdGhpcy5qb2luUHVzaC5yZXNldCgpXG4gICAgICBpZih0aGlzLnNvY2tldC5pc0Nvbm5lY3RlZCgpKXsgdGhpcy5yZWpvaW5UaW1lci5zY2hlZHVsZVRpbWVvdXQoKSB9XG4gICAgfSlcbiAgICB0aGlzLm9uKENIQU5ORUxfRVZFTlRTLnJlcGx5LCAocGF5bG9hZCwgcmVmKSA9PiB7XG4gICAgICB0aGlzLnRyaWdnZXIodGhpcy5yZXBseUV2ZW50TmFtZShyZWYpLCBwYXlsb2FkKVxuICAgIH0pXG4gIH1cblxuICAvKipcbiAgICogSm9pbiB0aGUgY2hhbm5lbFxuICAgKiBAcGFyYW0ge2ludGVnZXJ9IHRpbWVvdXRcbiAgICogQHJldHVybnMge1B1c2h9XG4gICAqL1xuICBqb2luKHRpbWVvdXQgPSB0aGlzLnRpbWVvdXQpe1xuICAgIGlmKHRoaXMuam9pbmVkT25jZSl7XG4gICAgICB0aHJvdyBuZXcgRXJyb3IoXCJ0cmllZCB0byBqb2luIG11bHRpcGxlIHRpbWVzLiAnam9pbicgY2FuIG9ubHkgYmUgY2FsbGVkIGEgc2luZ2xlIHRpbWUgcGVyIGNoYW5uZWwgaW5zdGFuY2VcIilcbiAgICB9IGVsc2Uge1xuICAgICAgdGhpcy50aW1lb3V0ID0gdGltZW91dFxuICAgICAgdGhpcy5qb2luZWRPbmNlID0gdHJ1ZVxuICAgICAgdGhpcy5yZWpvaW4oKVxuICAgICAgcmV0dXJuIHRoaXMuam9pblB1c2hcbiAgICB9XG4gIH1cblxuICAvKipcbiAgICogSG9vayBpbnRvIGNoYW5uZWwgY2xvc2VcbiAgICogQHBhcmFtIHtGdW5jdGlvbn0gY2FsbGJhY2tcbiAgICovXG4gIG9uQ2xvc2UoY2FsbGJhY2spe1xuICAgIHRoaXMub24oQ0hBTk5FTF9FVkVOVFMuY2xvc2UsIGNhbGxiYWNrKVxuICB9XG5cbiAgLyoqXG4gICAqIEhvb2sgaW50byBjaGFubmVsIGVycm9yc1xuICAgKiBAcGFyYW0ge0Z1bmN0aW9ufSBjYWxsYmFja1xuICAgKi9cbiAgb25FcnJvcihjYWxsYmFjayl7XG4gICAgcmV0dXJuIHRoaXMub24oQ0hBTk5FTF9FVkVOVFMuZXJyb3IsIHJlYXNvbiA9PiBjYWxsYmFjayhyZWFzb24pKVxuICB9XG5cbiAgLyoqXG4gICAqIFN1YnNjcmliZXMgb24gY2hhbm5lbCBldmVudHNcbiAgICpcbiAgICogU3Vic2NyaXB0aW9uIHJldHVybnMgYSByZWYgY291bnRlciwgd2hpY2ggY2FuIGJlIHVzZWQgbGF0ZXIgdG9cbiAgICogdW5zdWJzY3JpYmUgdGhlIGV4YWN0IGV2ZW50IGxpc3RlbmVyXG4gICAqXG4gICAqIEBleGFtcGxlXG4gICAqIGNvbnN0IHJlZjEgPSBjaGFubmVsLm9uKFwiZXZlbnRcIiwgZG9fc3R1ZmYpXG4gICAqIGNvbnN0IHJlZjIgPSBjaGFubmVsLm9uKFwiZXZlbnRcIiwgZG9fb3RoZXJfc3R1ZmYpXG4gICAqIGNoYW5uZWwub2ZmKFwiZXZlbnRcIiwgcmVmMSlcbiAgICogLy8gU2luY2UgdW5zdWJzY3JpcHRpb24sIGRvX3N0dWZmIHdvbid0IGZpcmUsXG4gICAqIC8vIHdoaWxlIGRvX290aGVyX3N0dWZmIHdpbGwga2VlcCBmaXJpbmcgb24gdGhlIFwiZXZlbnRcIlxuICAgKlxuICAgKiBAcGFyYW0ge3N0cmluZ30gZXZlbnRcbiAgICogQHBhcmFtIHtGdW5jdGlvbn0gY2FsbGJhY2tcbiAgICogQHJldHVybnMge2ludGVnZXJ9IHJlZlxuICAgKi9cbiAgb24oZXZlbnQsIGNhbGxiYWNrKXtcbiAgICBsZXQgcmVmID0gdGhpcy5iaW5kaW5nUmVmKytcbiAgICB0aGlzLmJpbmRpbmdzLnB1c2goe2V2ZW50LCByZWYsIGNhbGxiYWNrfSlcbiAgICByZXR1cm4gcmVmXG4gIH1cblxuICAvKipcbiAgICogVW5zdWJzY3JpYmVzIG9mZiBvZiBjaGFubmVsIGV2ZW50c1xuICAgKlxuICAgKiBVc2UgdGhlIHJlZiByZXR1cm5lZCBmcm9tIGEgY2hhbm5lbC5vbigpIHRvIHVuc3Vic2NyaWJlIG9uZVxuICAgKiBoYW5kbGVyLCBvciBwYXNzIG5vdGhpbmcgZm9yIHRoZSByZWYgdG8gdW5zdWJzY3JpYmUgYWxsXG4gICAqIGhhbmRsZXJzIGZvciB0aGUgZ2l2ZW4gZXZlbnQuXG4gICAqXG4gICAqIEBleGFtcGxlXG4gICAqIC8vIFVuc3Vic2NyaWJlIHRoZSBkb19zdHVmZiBoYW5kbGVyXG4gICAqIGNvbnN0IHJlZjEgPSBjaGFubmVsLm9uKFwiZXZlbnRcIiwgZG9fc3R1ZmYpXG4gICAqIGNoYW5uZWwub2ZmKFwiZXZlbnRcIiwgcmVmMSlcbiAgICpcbiAgICogLy8gVW5zdWJzY3JpYmUgYWxsIGhhbmRsZXJzIGZyb20gZXZlbnRcbiAgICogY2hhbm5lbC5vZmYoXCJldmVudFwiKVxuICAgKlxuICAgKiBAcGFyYW0ge3N0cmluZ30gZXZlbnRcbiAgICogQHBhcmFtIHtpbnRlZ2VyfSByZWZcbiAgICovXG4gIG9mZihldmVudCwgcmVmKXtcbiAgICB0aGlzLmJpbmRpbmdzID0gdGhpcy5iaW5kaW5ncy5maWx0ZXIoKGJpbmQpID0+IHtcbiAgICAgIHJldHVybiAhKGJpbmQuZXZlbnQgPT09IGV2ZW50ICYmICh0eXBlb2YgcmVmID09PSBcInVuZGVmaW5lZFwiIHx8IHJlZiA9PT0gYmluZC5yZWYpKVxuICAgIH0pXG4gIH1cblxuICAvKipcbiAgICogQHByaXZhdGVcbiAgICovXG4gIGNhblB1c2goKXsgcmV0dXJuIHRoaXMuc29ja2V0LmlzQ29ubmVjdGVkKCkgJiYgdGhpcy5pc0pvaW5lZCgpIH1cblxuICAvKipcbiAgICogU2VuZHMgYSBtZXNzYWdlIGBldmVudGAgdG8gcGhvZW5peCB3aXRoIHRoZSBwYXlsb2FkIGBwYXlsb2FkYC5cbiAgICogUGhvZW5peCByZWNlaXZlcyB0aGlzIGluIHRoZSBgaGFuZGxlX2luKGV2ZW50LCBwYXlsb2FkLCBzb2NrZXQpYFxuICAgKiBmdW5jdGlvbi4gaWYgcGhvZW5peCByZXBsaWVzIG9yIGl0IHRpbWVzIG91dCAoZGVmYXVsdCAxMDAwMG1zKSxcbiAgICogdGhlbiBvcHRpb25hbGx5IHRoZSByZXBseSBjYW4gYmUgcmVjZWl2ZWQuXG4gICAqXG4gICAqIEBleGFtcGxlXG4gICAqIGNoYW5uZWwucHVzaChcImV2ZW50XCIpXG4gICAqICAgLnJlY2VpdmUoXCJva1wiLCBwYXlsb2FkID0+IGNvbnNvbGUubG9nKFwicGhvZW5peCByZXBsaWVkOlwiLCBwYXlsb2FkKSlcbiAgICogICAucmVjZWl2ZShcImVycm9yXCIsIGVyciA9PiBjb25zb2xlLmxvZyhcInBob2VuaXggZXJyb3JlZFwiLCBlcnIpKVxuICAgKiAgIC5yZWNlaXZlKFwidGltZW91dFwiLCAoKSA9PiBjb25zb2xlLmxvZyhcInRpbWVkIG91dCBwdXNoaW5nXCIpKVxuICAgKiBAcGFyYW0ge3N0cmluZ30gZXZlbnRcbiAgICogQHBhcmFtIHtPYmplY3R9IHBheWxvYWRcbiAgICogQHBhcmFtIHtudW1iZXJ9IFt0aW1lb3V0XVxuICAgKiBAcmV0dXJucyB7UHVzaH1cbiAgICovXG4gIHB1c2goZXZlbnQsIHBheWxvYWQsIHRpbWVvdXQgPSB0aGlzLnRpbWVvdXQpe1xuICAgIHBheWxvYWQgPSBwYXlsb2FkIHx8IHt9XG4gICAgaWYoIXRoaXMuam9pbmVkT25jZSl7XG4gICAgICB0aHJvdyBuZXcgRXJyb3IoYHRyaWVkIHRvIHB1c2ggJyR7ZXZlbnR9JyB0byAnJHt0aGlzLnRvcGljfScgYmVmb3JlIGpvaW5pbmcuIFVzZSBjaGFubmVsLmpvaW4oKSBiZWZvcmUgcHVzaGluZyBldmVudHNgKVxuICAgIH1cbiAgICBsZXQgcHVzaEV2ZW50ID0gbmV3IFB1c2godGhpcywgZXZlbnQsIGZ1bmN0aW9uICgpeyByZXR1cm4gcGF5bG9hZCB9LCB0aW1lb3V0KVxuICAgIGlmKHRoaXMuY2FuUHVzaCgpKXtcbiAgICAgIHB1c2hFdmVudC5zZW5kKClcbiAgICB9IGVsc2Uge1xuICAgICAgcHVzaEV2ZW50LnN0YXJ0VGltZW91dCgpXG4gICAgICB0aGlzLnB1c2hCdWZmZXIucHVzaChwdXNoRXZlbnQpXG4gICAgfVxuXG4gICAgcmV0dXJuIHB1c2hFdmVudFxuICB9XG5cbiAgLyoqIExlYXZlcyB0aGUgY2hhbm5lbFxuICAgKlxuICAgKiBVbnN1YnNjcmliZXMgZnJvbSBzZXJ2ZXIgZXZlbnRzLCBhbmRcbiAgICogaW5zdHJ1Y3RzIGNoYW5uZWwgdG8gdGVybWluYXRlIG9uIHNlcnZlclxuICAgKlxuICAgKiBUcmlnZ2VycyBvbkNsb3NlKCkgaG9va3NcbiAgICpcbiAgICogVG8gcmVjZWl2ZSBsZWF2ZSBhY2tub3dsZWRnZW1lbnRzLCB1c2UgdGhlIGByZWNlaXZlYFxuICAgKiBob29rIHRvIGJpbmQgdG8gdGhlIHNlcnZlciBhY2ssIGllOlxuICAgKlxuICAgKiBAZXhhbXBsZVxuICAgKiBjaGFubmVsLmxlYXZlKCkucmVjZWl2ZShcIm9rXCIsICgpID0+IGFsZXJ0KFwibGVmdCFcIikgKVxuICAgKlxuICAgKiBAcGFyYW0ge2ludGVnZXJ9IHRpbWVvdXRcbiAgICogQHJldHVybnMge1B1c2h9XG4gICAqL1xuICBsZWF2ZSh0aW1lb3V0ID0gdGhpcy50aW1lb3V0KXtcbiAgICB0aGlzLnJlam9pblRpbWVyLnJlc2V0KClcbiAgICB0aGlzLmpvaW5QdXNoLmNhbmNlbFRpbWVvdXQoKVxuXG4gICAgdGhpcy5zdGF0ZSA9IENIQU5ORUxfU1RBVEVTLmxlYXZpbmdcbiAgICBsZXQgb25DbG9zZSA9ICgpID0+IHtcbiAgICAgIGlmKHRoaXMuc29ja2V0Lmhhc0xvZ2dlcigpKSB0aGlzLnNvY2tldC5sb2coXCJjaGFubmVsXCIsIGBsZWF2ZSAke3RoaXMudG9waWN9YClcbiAgICAgIHRoaXMudHJpZ2dlcihDSEFOTkVMX0VWRU5UUy5jbG9zZSwgXCJsZWF2ZVwiKVxuICAgIH1cbiAgICBsZXQgbGVhdmVQdXNoID0gbmV3IFB1c2godGhpcywgQ0hBTk5FTF9FVkVOVFMubGVhdmUsIGNsb3N1cmUoe30pLCB0aW1lb3V0KVxuICAgIGxlYXZlUHVzaC5yZWNlaXZlKFwib2tcIiwgKCkgPT4gb25DbG9zZSgpKVxuICAgICAgLnJlY2VpdmUoXCJ0aW1lb3V0XCIsICgpID0+IG9uQ2xvc2UoKSlcbiAgICBsZWF2ZVB1c2guc2VuZCgpXG4gICAgaWYoIXRoaXMuY2FuUHVzaCgpKXsgbGVhdmVQdXNoLnRyaWdnZXIoXCJva1wiLCB7fSkgfVxuXG4gICAgcmV0dXJuIGxlYXZlUHVzaFxuICB9XG5cbiAgLyoqXG4gICAqIE92ZXJyaWRhYmxlIG1lc3NhZ2UgaG9va1xuICAgKlxuICAgKiBSZWNlaXZlcyBhbGwgZXZlbnRzIGZvciBzcGVjaWFsaXplZCBtZXNzYWdlIGhhbmRsaW5nXG4gICAqIGJlZm9yZSBkaXNwYXRjaGluZyB0byB0aGUgY2hhbm5lbCBjYWxsYmFja3MuXG4gICAqXG4gICAqIE11c3QgcmV0dXJuIHRoZSBwYXlsb2FkLCBtb2RpZmllZCBvciB1bm1vZGlmaWVkXG4gICAqIEBwYXJhbSB7c3RyaW5nfSBldmVudFxuICAgKiBAcGFyYW0ge09iamVjdH0gcGF5bG9hZFxuICAgKiBAcGFyYW0ge2ludGVnZXJ9IHJlZlxuICAgKiBAcmV0dXJucyB7T2JqZWN0fVxuICAgKi9cbiAgb25NZXNzYWdlKF9ldmVudCwgcGF5bG9hZCwgX3JlZil7IHJldHVybiBwYXlsb2FkIH1cblxuICAvKipcbiAgICogQHByaXZhdGVcbiAgICovXG4gIGlzTWVtYmVyKHRvcGljLCBldmVudCwgcGF5bG9hZCwgam9pblJlZil7XG4gICAgaWYodGhpcy50b3BpYyAhPT0gdG9waWMpeyByZXR1cm4gZmFsc2UgfVxuXG4gICAgaWYoam9pblJlZiAmJiBqb2luUmVmICE9PSB0aGlzLmpvaW5SZWYoKSl7XG4gICAgICBpZih0aGlzLnNvY2tldC5oYXNMb2dnZXIoKSkgdGhpcy5zb2NrZXQubG9nKFwiY2hhbm5lbFwiLCBcImRyb3BwaW5nIG91dGRhdGVkIG1lc3NhZ2VcIiwge3RvcGljLCBldmVudCwgcGF5bG9hZCwgam9pblJlZn0pXG4gICAgICByZXR1cm4gZmFsc2VcbiAgICB9IGVsc2Uge1xuICAgICAgcmV0dXJuIHRydWVcbiAgICB9XG4gIH1cblxuICAvKipcbiAgICogQHByaXZhdGVcbiAgICovXG4gIGpvaW5SZWYoKXsgcmV0dXJuIHRoaXMuam9pblB1c2gucmVmIH1cblxuICAvKipcbiAgICogQHByaXZhdGVcbiAgICovXG4gIHJlam9pbih0aW1lb3V0ID0gdGhpcy50aW1lb3V0KXtcbiAgICBpZih0aGlzLmlzTGVhdmluZygpKXsgcmV0dXJuIH1cbiAgICB0aGlzLnNvY2tldC5sZWF2ZU9wZW5Ub3BpYyh0aGlzLnRvcGljKVxuICAgIHRoaXMuc3RhdGUgPSBDSEFOTkVMX1NUQVRFUy5qb2luaW5nXG4gICAgdGhpcy5qb2luUHVzaC5yZXNlbmQodGltZW91dClcbiAgfVxuXG4gIC8qKlxuICAgKiBAcHJpdmF0ZVxuICAgKi9cbiAgdHJpZ2dlcihldmVudCwgcGF5bG9hZCwgcmVmLCBqb2luUmVmKXtcbiAgICBsZXQgaGFuZGxlZFBheWxvYWQgPSB0aGlzLm9uTWVzc2FnZShldmVudCwgcGF5bG9hZCwgcmVmLCBqb2luUmVmKVxuICAgIGlmKHBheWxvYWQgJiYgIWhhbmRsZWRQYXlsb2FkKXsgdGhyb3cgbmV3IEVycm9yKFwiY2hhbm5lbCBvbk1lc3NhZ2UgY2FsbGJhY2tzIG11c3QgcmV0dXJuIHRoZSBwYXlsb2FkLCBtb2RpZmllZCBvciB1bm1vZGlmaWVkXCIpIH1cblxuICAgIGxldCBldmVudEJpbmRpbmdzID0gdGhpcy5iaW5kaW5ncy5maWx0ZXIoYmluZCA9PiBiaW5kLmV2ZW50ID09PSBldmVudClcblxuICAgIGZvcihsZXQgaSA9IDA7IGkgPCBldmVudEJpbmRpbmdzLmxlbmd0aDsgaSsrKXtcbiAgICAgIGxldCBiaW5kID0gZXZlbnRCaW5kaW5nc1tpXVxuICAgICAgYmluZC5jYWxsYmFjayhoYW5kbGVkUGF5bG9hZCwgcmVmLCBqb2luUmVmIHx8IHRoaXMuam9pblJlZigpKVxuICAgIH1cbiAgfVxuXG4gIC8qKlxuICAgKiBAcHJpdmF0ZVxuICAgKi9cbiAgcmVwbHlFdmVudE5hbWUocmVmKXsgcmV0dXJuIGBjaGFuX3JlcGx5XyR7cmVmfWAgfVxuXG4gIC8qKlxuICAgKiBAcHJpdmF0ZVxuICAgKi9cbiAgaXNDbG9zZWQoKXsgcmV0dXJuIHRoaXMuc3RhdGUgPT09IENIQU5ORUxfU1RBVEVTLmNsb3NlZCB9XG5cbiAgLyoqXG4gICAqIEBwcml2YXRlXG4gICAqL1xuICBpc0Vycm9yZWQoKXsgcmV0dXJuIHRoaXMuc3RhdGUgPT09IENIQU5ORUxfU1RBVEVTLmVycm9yZWQgfVxuXG4gIC8qKlxuICAgKiBAcHJpdmF0ZVxuICAgKi9cbiAgaXNKb2luZWQoKXsgcmV0dXJuIHRoaXMuc3RhdGUgPT09IENIQU5ORUxfU1RBVEVTLmpvaW5lZCB9XG5cbiAgLyoqXG4gICAqIEBwcml2YXRlXG4gICAqL1xuICBpc0pvaW5pbmcoKXsgcmV0dXJuIHRoaXMuc3RhdGUgPT09IENIQU5ORUxfU1RBVEVTLmpvaW5pbmcgfVxuXG4gIC8qKlxuICAgKiBAcHJpdmF0ZVxuICAgKi9cbiAgaXNMZWF2aW5nKCl7IHJldHVybiB0aGlzLnN0YXRlID09PSBDSEFOTkVMX1NUQVRFUy5sZWF2aW5nIH1cbn1cbiIsICJpbXBvcnQge1xuICBnbG9iYWwsXG4gIFhIUl9TVEFURVNcbn0gZnJvbSBcIi4vY29uc3RhbnRzXCJcblxuZXhwb3J0IGRlZmF1bHQgY2xhc3MgQWpheCB7XG5cbiAgc3RhdGljIHJlcXVlc3QobWV0aG9kLCBlbmRQb2ludCwgYWNjZXB0LCBib2R5LCB0aW1lb3V0LCBvbnRpbWVvdXQsIGNhbGxiYWNrKXtcbiAgICBpZihnbG9iYWwuWERvbWFpblJlcXVlc3Qpe1xuICAgICAgbGV0IHJlcSA9IG5ldyBnbG9iYWwuWERvbWFpblJlcXVlc3QoKSAvLyBJRTgsIElFOVxuICAgICAgcmV0dXJuIHRoaXMueGRvbWFpblJlcXVlc3QocmVxLCBtZXRob2QsIGVuZFBvaW50LCBib2R5LCB0aW1lb3V0LCBvbnRpbWVvdXQsIGNhbGxiYWNrKVxuICAgIH0gZWxzZSB7XG4gICAgICBsZXQgcmVxID0gbmV3IGdsb2JhbC5YTUxIdHRwUmVxdWVzdCgpIC8vIElFNyssIEZpcmVmb3gsIENocm9tZSwgT3BlcmEsIFNhZmFyaVxuICAgICAgcmV0dXJuIHRoaXMueGhyUmVxdWVzdChyZXEsIG1ldGhvZCwgZW5kUG9pbnQsIGFjY2VwdCwgYm9keSwgdGltZW91dCwgb250aW1lb3V0LCBjYWxsYmFjaylcbiAgICB9XG4gIH1cblxuICBzdGF0aWMgeGRvbWFpblJlcXVlc3QocmVxLCBtZXRob2QsIGVuZFBvaW50LCBib2R5LCB0aW1lb3V0LCBvbnRpbWVvdXQsIGNhbGxiYWNrKXtcbiAgICByZXEudGltZW91dCA9IHRpbWVvdXRcbiAgICByZXEub3BlbihtZXRob2QsIGVuZFBvaW50KVxuICAgIHJlcS5vbmxvYWQgPSAoKSA9PiB7XG4gICAgICBsZXQgcmVzcG9uc2UgPSB0aGlzLnBhcnNlSlNPTihyZXEucmVzcG9uc2VUZXh0KVxuICAgICAgY2FsbGJhY2sgJiYgY2FsbGJhY2socmVzcG9uc2UpXG4gICAgfVxuICAgIGlmKG9udGltZW91dCl7IHJlcS5vbnRpbWVvdXQgPSBvbnRpbWVvdXQgfVxuXG4gICAgLy8gV29yayBhcm91bmQgYnVnIGluIElFOSB0aGF0IHJlcXVpcmVzIGFuIGF0dGFjaGVkIG9ucHJvZ3Jlc3MgaGFuZGxlclxuICAgIHJlcS5vbnByb2dyZXNzID0gKCkgPT4geyB9XG5cbiAgICByZXEuc2VuZChib2R5KVxuICAgIHJldHVybiByZXFcbiAgfVxuXG4gIHN0YXRpYyB4aHJSZXF1ZXN0KHJlcSwgbWV0aG9kLCBlbmRQb2ludCwgYWNjZXB0LCBib2R5LCB0aW1lb3V0LCBvbnRpbWVvdXQsIGNhbGxiYWNrKXtcbiAgICByZXEub3BlbihtZXRob2QsIGVuZFBvaW50LCB0cnVlKVxuICAgIHJlcS50aW1lb3V0ID0gdGltZW91dFxuICAgIHJlcS5zZXRSZXF1ZXN0SGVhZGVyKFwiQ29udGVudC1UeXBlXCIsIGFjY2VwdClcbiAgICByZXEub25lcnJvciA9ICgpID0+IGNhbGxiYWNrICYmIGNhbGxiYWNrKG51bGwpXG4gICAgcmVxLm9ucmVhZHlzdGF0ZWNoYW5nZSA9ICgpID0+IHtcbiAgICAgIGlmKHJlcS5yZWFkeVN0YXRlID09PSBYSFJfU1RBVEVTLmNvbXBsZXRlICYmIGNhbGxiYWNrKXtcbiAgICAgICAgbGV0IHJlc3BvbnNlID0gdGhpcy5wYXJzZUpTT04ocmVxLnJlc3BvbnNlVGV4dClcbiAgICAgICAgY2FsbGJhY2socmVzcG9uc2UpXG4gICAgICB9XG4gICAgfVxuICAgIGlmKG9udGltZW91dCl7IHJlcS5vbnRpbWVvdXQgPSBvbnRpbWVvdXQgfVxuXG4gICAgcmVxLnNlbmQoYm9keSlcbiAgICByZXR1cm4gcmVxXG4gIH1cblxuICBzdGF0aWMgcGFyc2VKU09OKHJlc3Ape1xuICAgIGlmKCFyZXNwIHx8IHJlc3AgPT09IFwiXCIpeyByZXR1cm4gbnVsbCB9XG5cbiAgICB0cnkge1xuICAgICAgcmV0dXJuIEpTT04ucGFyc2UocmVzcClcbiAgICB9IGNhdGNoIChlKXtcbiAgICAgIGNvbnNvbGUgJiYgY29uc29sZS5sb2coXCJmYWlsZWQgdG8gcGFyc2UgSlNPTiByZXNwb25zZVwiLCByZXNwKVxuICAgICAgcmV0dXJuIG51bGxcbiAgICB9XG4gIH1cblxuICBzdGF0aWMgc2VyaWFsaXplKG9iaiwgcGFyZW50S2V5KXtcbiAgICBsZXQgcXVlcnlTdHIgPSBbXVxuICAgIGZvcih2YXIga2V5IGluIG9iail7XG4gICAgICBpZighT2JqZWN0LnByb3RvdHlwZS5oYXNPd25Qcm9wZXJ0eS5jYWxsKG9iaiwga2V5KSl7IGNvbnRpbnVlIH1cbiAgICAgIGxldCBwYXJhbUtleSA9IHBhcmVudEtleSA/IGAke3BhcmVudEtleX1bJHtrZXl9XWAgOiBrZXlcbiAgICAgIGxldCBwYXJhbVZhbCA9IG9ialtrZXldXG4gICAgICBpZih0eXBlb2YgcGFyYW1WYWwgPT09IFwib2JqZWN0XCIpe1xuICAgICAgICBxdWVyeVN0ci5wdXNoKHRoaXMuc2VyaWFsaXplKHBhcmFtVmFsLCBwYXJhbUtleSkpXG4gICAgICB9IGVsc2Uge1xuICAgICAgICBxdWVyeVN0ci5wdXNoKGVuY29kZVVSSUNvbXBvbmVudChwYXJhbUtleSkgKyBcIj1cIiArIGVuY29kZVVSSUNvbXBvbmVudChwYXJhbVZhbCkpXG4gICAgICB9XG4gICAgfVxuICAgIHJldHVybiBxdWVyeVN0ci5qb2luKFwiJlwiKVxuICB9XG5cbiAgc3RhdGljIGFwcGVuZFBhcmFtcyh1cmwsIHBhcmFtcyl7XG4gICAgaWYoT2JqZWN0LmtleXMocGFyYW1zKS5sZW5ndGggPT09IDApeyByZXR1cm4gdXJsIH1cblxuICAgIGxldCBwcmVmaXggPSB1cmwubWF0Y2goL1xcPy8pID8gXCImXCIgOiBcIj9cIlxuICAgIHJldHVybiBgJHt1cmx9JHtwcmVmaXh9JHt0aGlzLnNlcmlhbGl6ZShwYXJhbXMpfWBcbiAgfVxufVxuIiwgImltcG9ydCB7XG4gIFNPQ0tFVF9TVEFURVMsXG4gIFRSQU5TUE9SVFNcbn0gZnJvbSBcIi4vY29uc3RhbnRzXCJcblxuaW1wb3J0IEFqYXggZnJvbSBcIi4vYWpheFwiXG5cbmxldCBhcnJheUJ1ZmZlclRvQmFzZTY0ID0gKGJ1ZmZlcikgPT4ge1xuICBsZXQgYmluYXJ5ID0gXCJcIlxuICBsZXQgYnl0ZXMgPSBuZXcgVWludDhBcnJheShidWZmZXIpXG4gIGxldCBsZW4gPSBieXRlcy5ieXRlTGVuZ3RoXG4gIGZvcihsZXQgaSA9IDA7IGkgPCBsZW47IGkrKyl7IGJpbmFyeSArPSBTdHJpbmcuZnJvbUNoYXJDb2RlKGJ5dGVzW2ldKSB9XG4gIHJldHVybiBidG9hKGJpbmFyeSlcbn1cblxuZXhwb3J0IGRlZmF1bHQgY2xhc3MgTG9uZ1BvbGwge1xuXG4gIGNvbnN0cnVjdG9yKGVuZFBvaW50KXtcbiAgICB0aGlzLmVuZFBvaW50ID0gbnVsbFxuICAgIHRoaXMudG9rZW4gPSBudWxsXG4gICAgdGhpcy5za2lwSGVhcnRiZWF0ID0gdHJ1ZVxuICAgIHRoaXMucmVxcyA9IG5ldyBTZXQoKVxuICAgIHRoaXMuYXdhaXRpbmdCYXRjaEFjayA9IGZhbHNlXG4gICAgdGhpcy5jdXJyZW50QmF0Y2ggPSBudWxsXG4gICAgdGhpcy5jdXJyZW50QmF0Y2hUaW1lciA9IG51bGxcbiAgICB0aGlzLmJhdGNoQnVmZmVyID0gW11cbiAgICB0aGlzLm9ub3BlbiA9IGZ1bmN0aW9uICgpeyB9IC8vIG5vb3BcbiAgICB0aGlzLm9uZXJyb3IgPSBmdW5jdGlvbiAoKXsgfSAvLyBub29wXG4gICAgdGhpcy5vbm1lc3NhZ2UgPSBmdW5jdGlvbiAoKXsgfSAvLyBub29wXG4gICAgdGhpcy5vbmNsb3NlID0gZnVuY3Rpb24gKCl7IH0gLy8gbm9vcFxuICAgIHRoaXMucG9sbEVuZHBvaW50ID0gdGhpcy5ub3JtYWxpemVFbmRwb2ludChlbmRQb2ludClcbiAgICB0aGlzLnJlYWR5U3RhdGUgPSBTT0NLRVRfU1RBVEVTLmNvbm5lY3RpbmdcbiAgICAvLyB3ZSBtdXN0IHdhaXQgZm9yIHRoZSBjYWxsZXIgdG8gZmluaXNoIHNldHRpbmcgdXAgb3VyIGNhbGxiYWNrcyBhbmQgdGltZW91dCBwcm9wZXJ0aWVzXG4gICAgc2V0VGltZW91dCgoKSA9PiB0aGlzLnBvbGwoKSwgMClcbiAgfVxuXG4gIG5vcm1hbGl6ZUVuZHBvaW50KGVuZFBvaW50KXtcbiAgICByZXR1cm4gKGVuZFBvaW50XG4gICAgICAucmVwbGFjZShcIndzOi8vXCIsIFwiaHR0cDovL1wiKVxuICAgICAgLnJlcGxhY2UoXCJ3c3M6Ly9cIiwgXCJodHRwczovL1wiKVxuICAgICAgLnJlcGxhY2UobmV3IFJlZ0V4cChcIiguKilcXC9cIiArIFRSQU5TUE9SVFMud2Vic29ja2V0KSwgXCIkMS9cIiArIFRSQU5TUE9SVFMubG9uZ3BvbGwpKVxuICB9XG5cbiAgZW5kcG9pbnRVUkwoKXtcbiAgICByZXR1cm4gQWpheC5hcHBlbmRQYXJhbXModGhpcy5wb2xsRW5kcG9pbnQsIHt0b2tlbjogdGhpcy50b2tlbn0pXG4gIH1cblxuICBjbG9zZUFuZFJldHJ5KGNvZGUsIHJlYXNvbiwgd2FzQ2xlYW4pe1xuICAgIHRoaXMuY2xvc2UoY29kZSwgcmVhc29uLCB3YXNDbGVhbilcbiAgICB0aGlzLnJlYWR5U3RhdGUgPSBTT0NLRVRfU1RBVEVTLmNvbm5lY3RpbmdcbiAgfVxuXG4gIG9udGltZW91dCgpe1xuICAgIHRoaXMub25lcnJvcihcInRpbWVvdXRcIilcbiAgICB0aGlzLmNsb3NlQW5kUmV0cnkoMTAwNSwgXCJ0aW1lb3V0XCIsIGZhbHNlKVxuICB9XG5cbiAgaXNBY3RpdmUoKXsgcmV0dXJuIHRoaXMucmVhZHlTdGF0ZSA9PT0gU09DS0VUX1NUQVRFUy5vcGVuIHx8IHRoaXMucmVhZHlTdGF0ZSA9PT0gU09DS0VUX1NUQVRFUy5jb25uZWN0aW5nIH1cblxuICBwb2xsKCl7XG4gICAgdGhpcy5hamF4KFwiR0VUXCIsIFwiYXBwbGljYXRpb24vanNvblwiLCBudWxsLCAoKSA9PiB0aGlzLm9udGltZW91dCgpLCByZXNwID0+IHtcbiAgICAgIGlmKHJlc3Ape1xuICAgICAgICB2YXIge3N0YXR1cywgdG9rZW4sIG1lc3NhZ2VzfSA9IHJlc3BcbiAgICAgICAgdGhpcy50b2tlbiA9IHRva2VuXG4gICAgICB9IGVsc2Uge1xuICAgICAgICBzdGF0dXMgPSAwXG4gICAgICB9XG5cbiAgICAgIHN3aXRjaChzdGF0dXMpe1xuICAgICAgICBjYXNlIDIwMDpcbiAgICAgICAgICBtZXNzYWdlcy5mb3JFYWNoKG1zZyA9PiB7XG4gICAgICAgICAgICAvLyBUYXNrcyBhcmUgd2hhdCB0aGluZ3MgbGlrZSBldmVudCBoYW5kbGVycywgc2V0VGltZW91dCBjYWxsYmFja3MsXG4gICAgICAgICAgICAvLyBwcm9taXNlIHJlc29sdmVzIGFuZCBtb3JlIGFyZSBydW4gd2l0aGluLlxuICAgICAgICAgICAgLy8gSW4gbW9kZXJuIGJyb3dzZXJzLCB0aGVyZSBhcmUgdHdvIGRpZmZlcmVudCBraW5kcyBvZiB0YXNrcyxcbiAgICAgICAgICAgIC8vIG1pY3JvdGFza3MgYW5kIG1hY3JvdGFza3MuXG4gICAgICAgICAgICAvLyBNaWNyb3Rhc2tzIGFyZSBtYWlubHkgdXNlZCBmb3IgUHJvbWlzZXMsIHdoaWxlIG1hY3JvdGFza3MgYXJlXG4gICAgICAgICAgICAvLyB1c2VkIGZvciBldmVyeXRoaW5nIGVsc2UuXG4gICAgICAgICAgICAvLyBNaWNyb3Rhc2tzIGFsd2F5cyBoYXZlIHByaW9yaXR5IG92ZXIgbWFjcm90YXNrcy4gSWYgdGhlIEpTIGVuZ2luZVxuICAgICAgICAgICAgLy8gaXMgbG9va2luZyBmb3IgYSB0YXNrIHRvIHJ1biwgaXQgd2lsbCBhbHdheXMgdHJ5IHRvIGVtcHR5IHRoZVxuICAgICAgICAgICAgLy8gbWljcm90YXNrIHF1ZXVlIGJlZm9yZSBhdHRlbXB0aW5nIHRvIHJ1biBhbnl0aGluZyBmcm9tIHRoZVxuICAgICAgICAgICAgLy8gbWFjcm90YXNrIHF1ZXVlLlxuICAgICAgICAgICAgLy9cbiAgICAgICAgICAgIC8vIEZvciB0aGUgV2ViU29ja2V0IHRyYW5zcG9ydCwgbWVzc2FnZXMgYWx3YXlzIGFycml2ZSBpbiB0aGVpciBvd25cbiAgICAgICAgICAgIC8vIGV2ZW50LiBUaGlzIG1lYW5zIHRoYXQgaWYgYW55IHByb21pc2VzIGFyZSByZXNvbHZlZCBmcm9tIHdpdGhpbixcbiAgICAgICAgICAgIC8vIHRoZWlyIGNhbGxiYWNrcyB3aWxsIGFsd2F5cyBmaW5pc2ggZXhlY3V0aW9uIGJ5IHRoZSB0aW1lIHRoZVxuICAgICAgICAgICAgLy8gbmV4dCBtZXNzYWdlIGV2ZW50IGhhbmRsZXIgaXMgcnVuLlxuICAgICAgICAgICAgLy9cbiAgICAgICAgICAgIC8vIEluIG9yZGVyIHRvIGVtdWxhdGUgdGhpcyBiZWhhdmlvdXIsIHdlIG5lZWQgdG8gbWFrZSBzdXJlIGVhY2hcbiAgICAgICAgICAgIC8vIG9ubWVzc2FnZSBoYW5kbGVyIGlzIHJ1biB3aXRoaW4gaXRzIG93biBtYWNyb3Rhc2suXG4gICAgICAgICAgICBzZXRUaW1lb3V0KCgpID0+IHRoaXMub25tZXNzYWdlKHtkYXRhOiBtc2d9KSwgMClcbiAgICAgICAgICB9KVxuICAgICAgICAgIHRoaXMucG9sbCgpXG4gICAgICAgICAgYnJlYWtcbiAgICAgICAgY2FzZSAyMDQ6XG4gICAgICAgICAgdGhpcy5wb2xsKClcbiAgICAgICAgICBicmVha1xuICAgICAgICBjYXNlIDQxMDpcbiAgICAgICAgICB0aGlzLnJlYWR5U3RhdGUgPSBTT0NLRVRfU1RBVEVTLm9wZW5cbiAgICAgICAgICB0aGlzLm9ub3Blbih7fSlcbiAgICAgICAgICB0aGlzLnBvbGwoKVxuICAgICAgICAgIGJyZWFrXG4gICAgICAgIGNhc2UgNDAzOlxuICAgICAgICAgIHRoaXMub25lcnJvcig0MDMpXG4gICAgICAgICAgdGhpcy5jbG9zZSgxMDA4LCBcImZvcmJpZGRlblwiLCBmYWxzZSlcbiAgICAgICAgICBicmVha1xuICAgICAgICBjYXNlIDA6XG4gICAgICAgIGNhc2UgNTAwOlxuICAgICAgICAgIHRoaXMub25lcnJvcig1MDApXG4gICAgICAgICAgdGhpcy5jbG9zZUFuZFJldHJ5KDEwMTEsIFwiaW50ZXJuYWwgc2VydmVyIGVycm9yXCIsIDUwMClcbiAgICAgICAgICBicmVha1xuICAgICAgICBkZWZhdWx0OiB0aHJvdyBuZXcgRXJyb3IoYHVuaGFuZGxlZCBwb2xsIHN0YXR1cyAke3N0YXR1c31gKVxuICAgICAgfVxuICAgIH0pXG4gIH1cblxuICAvLyB3ZSBjb2xsZWN0IGFsbCBwdXNoZXMgd2l0aGluIHRoZSBjdXJyZW50IGV2ZW50IGxvb3AgYnlcbiAgLy8gc2V0VGltZW91dCAwLCB3aGljaCBvcHRpbWl6ZXMgYmFjay10by1iYWNrIHByb2NlZHVyYWxcbiAgLy8gcHVzaGVzIGFnYWluc3QgYW4gZW1wdHkgYnVmZmVyXG5cbiAgc2VuZChib2R5KXtcbiAgICBpZih0eXBlb2YoYm9keSkgIT09IFwic3RyaW5nXCIpeyBib2R5ID0gYXJyYXlCdWZmZXJUb0Jhc2U2NChib2R5KSB9XG4gICAgaWYodGhpcy5jdXJyZW50QmF0Y2gpe1xuICAgICAgdGhpcy5jdXJyZW50QmF0Y2gucHVzaChib2R5KVxuICAgIH0gZWxzZSBpZih0aGlzLmF3YWl0aW5nQmF0Y2hBY2spe1xuICAgICAgdGhpcy5iYXRjaEJ1ZmZlci5wdXNoKGJvZHkpXG4gICAgfSBlbHNlIHtcbiAgICAgIHRoaXMuY3VycmVudEJhdGNoID0gW2JvZHldXG4gICAgICB0aGlzLmN1cnJlbnRCYXRjaFRpbWVyID0gc2V0VGltZW91dCgoKSA9PiB7XG4gICAgICAgIHRoaXMuYmF0Y2hTZW5kKHRoaXMuY3VycmVudEJhdGNoKVxuICAgICAgICB0aGlzLmN1cnJlbnRCYXRjaCA9IG51bGxcbiAgICAgIH0sIDApXG4gICAgfVxuICB9XG5cbiAgYmF0Y2hTZW5kKG1lc3NhZ2VzKXtcbiAgICB0aGlzLmF3YWl0aW5nQmF0Y2hBY2sgPSB0cnVlXG4gICAgdGhpcy5hamF4KFwiUE9TVFwiLCBcImFwcGxpY2F0aW9uL3gtbmRqc29uXCIsIG1lc3NhZ2VzLmpvaW4oXCJcXG5cIiksICgpID0+IHRoaXMub25lcnJvcihcInRpbWVvdXRcIiksIHJlc3AgPT4ge1xuICAgICAgdGhpcy5hd2FpdGluZ0JhdGNoQWNrID0gZmFsc2VcbiAgICAgIGlmKCFyZXNwIHx8IHJlc3Auc3RhdHVzICE9PSAyMDApe1xuICAgICAgICB0aGlzLm9uZXJyb3IocmVzcCAmJiByZXNwLnN0YXR1cylcbiAgICAgICAgdGhpcy5jbG9zZUFuZFJldHJ5KDEwMTEsIFwiaW50ZXJuYWwgc2VydmVyIGVycm9yXCIsIGZhbHNlKVxuICAgICAgfSBlbHNlIGlmKHRoaXMuYmF0Y2hCdWZmZXIubGVuZ3RoID4gMCl7XG4gICAgICAgIHRoaXMuYmF0Y2hTZW5kKHRoaXMuYmF0Y2hCdWZmZXIpXG4gICAgICAgIHRoaXMuYmF0Y2hCdWZmZXIgPSBbXVxuICAgICAgfVxuICAgIH0pXG4gIH1cblxuICBjbG9zZShjb2RlLCByZWFzb24sIHdhc0NsZWFuKXtcbiAgICBmb3IobGV0IHJlcSBvZiB0aGlzLnJlcXMpeyByZXEuYWJvcnQoKSB9XG4gICAgdGhpcy5yZWFkeVN0YXRlID0gU09DS0VUX1NUQVRFUy5jbG9zZWRcbiAgICBsZXQgb3B0cyA9IE9iamVjdC5hc3NpZ24oe2NvZGU6IDEwMDAsIHJlYXNvbjogdW5kZWZpbmVkLCB3YXNDbGVhbjogdHJ1ZX0sIHtjb2RlLCByZWFzb24sIHdhc0NsZWFufSlcbiAgICB0aGlzLmJhdGNoQnVmZmVyID0gW11cbiAgICBjbGVhclRpbWVvdXQodGhpcy5jdXJyZW50QmF0Y2hUaW1lcilcbiAgICB0aGlzLmN1cnJlbnRCYXRjaFRpbWVyID0gbnVsbFxuICAgIGlmKHR5cGVvZihDbG9zZUV2ZW50KSAhPT0gXCJ1bmRlZmluZWRcIil7XG4gICAgICB0aGlzLm9uY2xvc2UobmV3IENsb3NlRXZlbnQoXCJjbG9zZVwiLCBvcHRzKSlcbiAgICB9IGVsc2Uge1xuICAgICAgdGhpcy5vbmNsb3NlKG9wdHMpXG4gICAgfVxuICB9XG5cbiAgYWpheChtZXRob2QsIGNvbnRlbnRUeXBlLCBib2R5LCBvbkNhbGxlclRpbWVvdXQsIGNhbGxiYWNrKXtcbiAgICBsZXQgcmVxXG4gICAgbGV0IG9udGltZW91dCA9ICgpID0+IHtcbiAgICAgIHRoaXMucmVxcy5kZWxldGUocmVxKVxuICAgICAgb25DYWxsZXJUaW1lb3V0KClcbiAgICB9XG4gICAgcmVxID0gQWpheC5yZXF1ZXN0KG1ldGhvZCwgdGhpcy5lbmRwb2ludFVSTCgpLCBjb250ZW50VHlwZSwgYm9keSwgdGhpcy50aW1lb3V0LCBvbnRpbWVvdXQsIHJlc3AgPT4ge1xuICAgICAgdGhpcy5yZXFzLmRlbGV0ZShyZXEpXG4gICAgICBpZih0aGlzLmlzQWN0aXZlKCkpeyBjYWxsYmFjayhyZXNwKSB9XG4gICAgfSlcbiAgICB0aGlzLnJlcXMuYWRkKHJlcSlcbiAgfVxufVxuIiwgIi8qKlxuICogSW5pdGlhbGl6ZXMgdGhlIFByZXNlbmNlXG4gKiBAcGFyYW0ge0NoYW5uZWx9IGNoYW5uZWwgLSBUaGUgQ2hhbm5lbFxuICogQHBhcmFtIHtPYmplY3R9IG9wdHMgLSBUaGUgb3B0aW9ucyxcbiAqICAgICAgICBmb3IgZXhhbXBsZSBge2V2ZW50czoge3N0YXRlOiBcInN0YXRlXCIsIGRpZmY6IFwiZGlmZlwifX1gXG4gKi9cbmV4cG9ydCBkZWZhdWx0IGNsYXNzIFByZXNlbmNlIHtcblxuICBjb25zdHJ1Y3RvcihjaGFubmVsLCBvcHRzID0ge30pe1xuICAgIGxldCBldmVudHMgPSBvcHRzLmV2ZW50cyB8fCB7c3RhdGU6IFwicHJlc2VuY2Vfc3RhdGVcIiwgZGlmZjogXCJwcmVzZW5jZV9kaWZmXCJ9XG4gICAgdGhpcy5zdGF0ZSA9IHt9XG4gICAgdGhpcy5wZW5kaW5nRGlmZnMgPSBbXVxuICAgIHRoaXMuY2hhbm5lbCA9IGNoYW5uZWxcbiAgICB0aGlzLmpvaW5SZWYgPSBudWxsXG4gICAgdGhpcy5jYWxsZXIgPSB7XG4gICAgICBvbkpvaW46IGZ1bmN0aW9uICgpeyB9LFxuICAgICAgb25MZWF2ZTogZnVuY3Rpb24gKCl7IH0sXG4gICAgICBvblN5bmM6IGZ1bmN0aW9uICgpeyB9XG4gICAgfVxuXG4gICAgdGhpcy5jaGFubmVsLm9uKGV2ZW50cy5zdGF0ZSwgbmV3U3RhdGUgPT4ge1xuICAgICAgbGV0IHtvbkpvaW4sIG9uTGVhdmUsIG9uU3luY30gPSB0aGlzLmNhbGxlclxuXG4gICAgICB0aGlzLmpvaW5SZWYgPSB0aGlzLmNoYW5uZWwuam9pblJlZigpXG4gICAgICB0aGlzLnN0YXRlID0gUHJlc2VuY2Uuc3luY1N0YXRlKHRoaXMuc3RhdGUsIG5ld1N0YXRlLCBvbkpvaW4sIG9uTGVhdmUpXG5cbiAgICAgIHRoaXMucGVuZGluZ0RpZmZzLmZvckVhY2goZGlmZiA9PiB7XG4gICAgICAgIHRoaXMuc3RhdGUgPSBQcmVzZW5jZS5zeW5jRGlmZih0aGlzLnN0YXRlLCBkaWZmLCBvbkpvaW4sIG9uTGVhdmUpXG4gICAgICB9KVxuICAgICAgdGhpcy5wZW5kaW5nRGlmZnMgPSBbXVxuICAgICAgb25TeW5jKClcbiAgICB9KVxuXG4gICAgdGhpcy5jaGFubmVsLm9uKGV2ZW50cy5kaWZmLCBkaWZmID0+IHtcbiAgICAgIGxldCB7b25Kb2luLCBvbkxlYXZlLCBvblN5bmN9ID0gdGhpcy5jYWxsZXJcblxuICAgICAgaWYodGhpcy5pblBlbmRpbmdTeW5jU3RhdGUoKSl7XG4gICAgICAgIHRoaXMucGVuZGluZ0RpZmZzLnB1c2goZGlmZilcbiAgICAgIH0gZWxzZSB7XG4gICAgICAgIHRoaXMuc3RhdGUgPSBQcmVzZW5jZS5zeW5jRGlmZih0aGlzLnN0YXRlLCBkaWZmLCBvbkpvaW4sIG9uTGVhdmUpXG4gICAgICAgIG9uU3luYygpXG4gICAgICB9XG4gICAgfSlcbiAgfVxuXG4gIG9uSm9pbihjYWxsYmFjayl7IHRoaXMuY2FsbGVyLm9uSm9pbiA9IGNhbGxiYWNrIH1cblxuICBvbkxlYXZlKGNhbGxiYWNrKXsgdGhpcy5jYWxsZXIub25MZWF2ZSA9IGNhbGxiYWNrIH1cblxuICBvblN5bmMoY2FsbGJhY2speyB0aGlzLmNhbGxlci5vblN5bmMgPSBjYWxsYmFjayB9XG5cbiAgbGlzdChieSl7IHJldHVybiBQcmVzZW5jZS5saXN0KHRoaXMuc3RhdGUsIGJ5KSB9XG5cbiAgaW5QZW5kaW5nU3luY1N0YXRlKCl7XG4gICAgcmV0dXJuICF0aGlzLmpvaW5SZWYgfHwgKHRoaXMuam9pblJlZiAhPT0gdGhpcy5jaGFubmVsLmpvaW5SZWYoKSlcbiAgfVxuXG4gIC8vIGxvd2VyLWxldmVsIHB1YmxpYyBzdGF0aWMgQVBJXG5cbiAgLyoqXG4gICAqIFVzZWQgdG8gc3luYyB0aGUgbGlzdCBvZiBwcmVzZW5jZXMgb24gdGhlIHNlcnZlclxuICAgKiB3aXRoIHRoZSBjbGllbnQncyBzdGF0ZS4gQW4gb3B0aW9uYWwgYG9uSm9pbmAgYW5kIGBvbkxlYXZlYCBjYWxsYmFjayBjYW5cbiAgICogYmUgcHJvdmlkZWQgdG8gcmVhY3QgdG8gY2hhbmdlcyBpbiB0aGUgY2xpZW50J3MgbG9jYWwgcHJlc2VuY2VzIGFjcm9zc1xuICAgKiBkaXNjb25uZWN0cyBhbmQgcmVjb25uZWN0cyB3aXRoIHRoZSBzZXJ2ZXIuXG4gICAqXG4gICAqIEByZXR1cm5zIHtQcmVzZW5jZX1cbiAgICovXG4gIHN0YXRpYyBzeW5jU3RhdGUoY3VycmVudFN0YXRlLCBuZXdTdGF0ZSwgb25Kb2luLCBvbkxlYXZlKXtcbiAgICBsZXQgc3RhdGUgPSB0aGlzLmNsb25lKGN1cnJlbnRTdGF0ZSlcbiAgICBsZXQgam9pbnMgPSB7fVxuICAgIGxldCBsZWF2ZXMgPSB7fVxuXG4gICAgdGhpcy5tYXAoc3RhdGUsIChrZXksIHByZXNlbmNlKSA9PiB7XG4gICAgICBpZighbmV3U3RhdGVba2V5XSl7XG4gICAgICAgIGxlYXZlc1trZXldID0gcHJlc2VuY2VcbiAgICAgIH1cbiAgICB9KVxuICAgIHRoaXMubWFwKG5ld1N0YXRlLCAoa2V5LCBuZXdQcmVzZW5jZSkgPT4ge1xuICAgICAgbGV0IGN1cnJlbnRQcmVzZW5jZSA9IHN0YXRlW2tleV1cbiAgICAgIGlmKGN1cnJlbnRQcmVzZW5jZSl7XG4gICAgICAgIGxldCBuZXdSZWZzID0gbmV3UHJlc2VuY2UubWV0YXMubWFwKG0gPT4gbS5waHhfcmVmKVxuICAgICAgICBsZXQgY3VyUmVmcyA9IGN1cnJlbnRQcmVzZW5jZS5tZXRhcy5tYXAobSA9PiBtLnBoeF9yZWYpXG4gICAgICAgIGxldCBqb2luZWRNZXRhcyA9IG5ld1ByZXNlbmNlLm1ldGFzLmZpbHRlcihtID0+IGN1clJlZnMuaW5kZXhPZihtLnBoeF9yZWYpIDwgMClcbiAgICAgICAgbGV0IGxlZnRNZXRhcyA9IGN1cnJlbnRQcmVzZW5jZS5tZXRhcy5maWx0ZXIobSA9PiBuZXdSZWZzLmluZGV4T2YobS5waHhfcmVmKSA8IDApXG4gICAgICAgIGlmKGpvaW5lZE1ldGFzLmxlbmd0aCA+IDApe1xuICAgICAgICAgIGpvaW5zW2tleV0gPSBuZXdQcmVzZW5jZVxuICAgICAgICAgIGpvaW5zW2tleV0ubWV0YXMgPSBqb2luZWRNZXRhc1xuICAgICAgICB9XG4gICAgICAgIGlmKGxlZnRNZXRhcy5sZW5ndGggPiAwKXtcbiAgICAgICAgICBsZWF2ZXNba2V5XSA9IHRoaXMuY2xvbmUoY3VycmVudFByZXNlbmNlKVxuICAgICAgICAgIGxlYXZlc1trZXldLm1ldGFzID0gbGVmdE1ldGFzXG4gICAgICAgIH1cbiAgICAgIH0gZWxzZSB7XG4gICAgICAgIGpvaW5zW2tleV0gPSBuZXdQcmVzZW5jZVxuICAgICAgfVxuICAgIH0pXG4gICAgcmV0dXJuIHRoaXMuc3luY0RpZmYoc3RhdGUsIHtqb2luczogam9pbnMsIGxlYXZlczogbGVhdmVzfSwgb25Kb2luLCBvbkxlYXZlKVxuICB9XG5cbiAgLyoqXG4gICAqXG4gICAqIFVzZWQgdG8gc3luYyBhIGRpZmYgb2YgcHJlc2VuY2Ugam9pbiBhbmQgbGVhdmVcbiAgICogZXZlbnRzIGZyb20gdGhlIHNlcnZlciwgYXMgdGhleSBoYXBwZW4uIExpa2UgYHN5bmNTdGF0ZWAsIGBzeW5jRGlmZmBcbiAgICogYWNjZXB0cyBvcHRpb25hbCBgb25Kb2luYCBhbmQgYG9uTGVhdmVgIGNhbGxiYWNrcyB0byByZWFjdCB0byBhIHVzZXJcbiAgICogam9pbmluZyBvciBsZWF2aW5nIGZyb20gYSBkZXZpY2UuXG4gICAqXG4gICAqIEByZXR1cm5zIHtQcmVzZW5jZX1cbiAgICovXG4gIHN0YXRpYyBzeW5jRGlmZihzdGF0ZSwgZGlmZiwgb25Kb2luLCBvbkxlYXZlKXtcbiAgICBsZXQge2pvaW5zLCBsZWF2ZXN9ID0gdGhpcy5jbG9uZShkaWZmKVxuICAgIGlmKCFvbkpvaW4peyBvbkpvaW4gPSBmdW5jdGlvbiAoKXsgfSB9XG4gICAgaWYoIW9uTGVhdmUpeyBvbkxlYXZlID0gZnVuY3Rpb24gKCl7IH0gfVxuXG4gICAgdGhpcy5tYXAoam9pbnMsIChrZXksIG5ld1ByZXNlbmNlKSA9PiB7XG4gICAgICBsZXQgY3VycmVudFByZXNlbmNlID0gc3RhdGVba2V5XVxuICAgICAgc3RhdGVba2V5XSA9IHRoaXMuY2xvbmUobmV3UHJlc2VuY2UpXG4gICAgICBpZihjdXJyZW50UHJlc2VuY2Upe1xuICAgICAgICBsZXQgam9pbmVkUmVmcyA9IHN0YXRlW2tleV0ubWV0YXMubWFwKG0gPT4gbS5waHhfcmVmKVxuICAgICAgICBsZXQgY3VyTWV0YXMgPSBjdXJyZW50UHJlc2VuY2UubWV0YXMuZmlsdGVyKG0gPT4gam9pbmVkUmVmcy5pbmRleE9mKG0ucGh4X3JlZikgPCAwKVxuICAgICAgICBzdGF0ZVtrZXldLm1ldGFzLnVuc2hpZnQoLi4uY3VyTWV0YXMpXG4gICAgICB9XG4gICAgICBvbkpvaW4oa2V5LCBjdXJyZW50UHJlc2VuY2UsIG5ld1ByZXNlbmNlKVxuICAgIH0pXG4gICAgdGhpcy5tYXAobGVhdmVzLCAoa2V5LCBsZWZ0UHJlc2VuY2UpID0+IHtcbiAgICAgIGxldCBjdXJyZW50UHJlc2VuY2UgPSBzdGF0ZVtrZXldXG4gICAgICBpZighY3VycmVudFByZXNlbmNlKXsgcmV0dXJuIH1cbiAgICAgIGxldCByZWZzVG9SZW1vdmUgPSBsZWZ0UHJlc2VuY2UubWV0YXMubWFwKG0gPT4gbS5waHhfcmVmKVxuICAgICAgY3VycmVudFByZXNlbmNlLm1ldGFzID0gY3VycmVudFByZXNlbmNlLm1ldGFzLmZpbHRlcihwID0+IHtcbiAgICAgICAgcmV0dXJuIHJlZnNUb1JlbW92ZS5pbmRleE9mKHAucGh4X3JlZikgPCAwXG4gICAgICB9KVxuICAgICAgb25MZWF2ZShrZXksIGN1cnJlbnRQcmVzZW5jZSwgbGVmdFByZXNlbmNlKVxuICAgICAgaWYoY3VycmVudFByZXNlbmNlLm1ldGFzLmxlbmd0aCA9PT0gMCl7XG4gICAgICAgIGRlbGV0ZSBzdGF0ZVtrZXldXG4gICAgICB9XG4gICAgfSlcbiAgICByZXR1cm4gc3RhdGVcbiAgfVxuXG4gIC8qKlxuICAgKiBSZXR1cm5zIHRoZSBhcnJheSBvZiBwcmVzZW5jZXMsIHdpdGggc2VsZWN0ZWQgbWV0YWRhdGEuXG4gICAqXG4gICAqIEBwYXJhbSB7T2JqZWN0fSBwcmVzZW5jZXNcbiAgICogQHBhcmFtIHtGdW5jdGlvbn0gY2hvb3NlclxuICAgKlxuICAgKiBAcmV0dXJucyB7UHJlc2VuY2V9XG4gICAqL1xuICBzdGF0aWMgbGlzdChwcmVzZW5jZXMsIGNob29zZXIpe1xuICAgIGlmKCFjaG9vc2VyKXsgY2hvb3NlciA9IGZ1bmN0aW9uIChrZXksIHByZXMpeyByZXR1cm4gcHJlcyB9IH1cblxuICAgIHJldHVybiB0aGlzLm1hcChwcmVzZW5jZXMsIChrZXksIHByZXNlbmNlKSA9PiB7XG4gICAgICByZXR1cm4gY2hvb3NlcihrZXksIHByZXNlbmNlKVxuICAgIH0pXG4gIH1cblxuICAvLyBwcml2YXRlXG5cbiAgc3RhdGljIG1hcChvYmosIGZ1bmMpe1xuICAgIHJldHVybiBPYmplY3QuZ2V0T3duUHJvcGVydHlOYW1lcyhvYmopLm1hcChrZXkgPT4gZnVuYyhrZXksIG9ialtrZXldKSlcbiAgfVxuXG4gIHN0YXRpYyBjbG9uZShvYmopeyByZXR1cm4gSlNPTi5wYXJzZShKU09OLnN0cmluZ2lmeShvYmopKSB9XG59XG4iLCAiLyogVGhlIGRlZmF1bHQgc2VyaWFsaXplciBmb3IgZW5jb2RpbmcgYW5kIGRlY29kaW5nIG1lc3NhZ2VzICovXG5pbXBvcnQge1xuICBDSEFOTkVMX0VWRU5UU1xufSBmcm9tIFwiLi9jb25zdGFudHNcIlxuXG5leHBvcnQgZGVmYXVsdCB7XG4gIEhFQURFUl9MRU5HVEg6IDEsXG4gIE1FVEFfTEVOR1RIOiA0LFxuICBLSU5EUzoge3B1c2g6IDAsIHJlcGx5OiAxLCBicm9hZGNhc3Q6IDJ9LFxuXG4gIGVuY29kZShtc2csIGNhbGxiYWNrKXtcbiAgICBpZihtc2cucGF5bG9hZC5jb25zdHJ1Y3RvciA9PT0gQXJyYXlCdWZmZXIpe1xuICAgICAgcmV0dXJuIGNhbGxiYWNrKHRoaXMuYmluYXJ5RW5jb2RlKG1zZykpXG4gICAgfSBlbHNlIHtcbiAgICAgIGxldCBwYXlsb2FkID0gW21zZy5qb2luX3JlZiwgbXNnLnJlZiwgbXNnLnRvcGljLCBtc2cuZXZlbnQsIG1zZy5wYXlsb2FkXVxuICAgICAgcmV0dXJuIGNhbGxiYWNrKEpTT04uc3RyaW5naWZ5KHBheWxvYWQpKVxuICAgIH1cbiAgfSxcblxuICBkZWNvZGUocmF3UGF5bG9hZCwgY2FsbGJhY2spe1xuICAgIGlmKHJhd1BheWxvYWQuY29uc3RydWN0b3IgPT09IEFycmF5QnVmZmVyKXtcbiAgICAgIHJldHVybiBjYWxsYmFjayh0aGlzLmJpbmFyeURlY29kZShyYXdQYXlsb2FkKSlcbiAgICB9IGVsc2Uge1xuICAgICAgbGV0IFtqb2luX3JlZiwgcmVmLCB0b3BpYywgZXZlbnQsIHBheWxvYWRdID0gSlNPTi5wYXJzZShyYXdQYXlsb2FkKVxuICAgICAgcmV0dXJuIGNhbGxiYWNrKHtqb2luX3JlZiwgcmVmLCB0b3BpYywgZXZlbnQsIHBheWxvYWR9KVxuICAgIH1cbiAgfSxcblxuICAvLyBwcml2YXRlXG5cbiAgYmluYXJ5RW5jb2RlKG1lc3NhZ2Upe1xuICAgIGxldCB7am9pbl9yZWYsIHJlZiwgZXZlbnQsIHRvcGljLCBwYXlsb2FkfSA9IG1lc3NhZ2VcbiAgICBsZXQgbWV0YUxlbmd0aCA9IHRoaXMuTUVUQV9MRU5HVEggKyBqb2luX3JlZi5sZW5ndGggKyByZWYubGVuZ3RoICsgdG9waWMubGVuZ3RoICsgZXZlbnQubGVuZ3RoXG4gICAgbGV0IGhlYWRlciA9IG5ldyBBcnJheUJ1ZmZlcih0aGlzLkhFQURFUl9MRU5HVEggKyBtZXRhTGVuZ3RoKVxuICAgIGxldCB2aWV3ID0gbmV3IERhdGFWaWV3KGhlYWRlcilcbiAgICBsZXQgb2Zmc2V0ID0gMFxuXG4gICAgdmlldy5zZXRVaW50OChvZmZzZXQrKywgdGhpcy5LSU5EUy5wdXNoKSAvLyBraW5kXG4gICAgdmlldy5zZXRVaW50OChvZmZzZXQrKywgam9pbl9yZWYubGVuZ3RoKVxuICAgIHZpZXcuc2V0VWludDgob2Zmc2V0KyssIHJlZi5sZW5ndGgpXG4gICAgdmlldy5zZXRVaW50OChvZmZzZXQrKywgdG9waWMubGVuZ3RoKVxuICAgIHZpZXcuc2V0VWludDgob2Zmc2V0KyssIGV2ZW50Lmxlbmd0aClcbiAgICBBcnJheS5mcm9tKGpvaW5fcmVmLCBjaGFyID0+IHZpZXcuc2V0VWludDgob2Zmc2V0KyssIGNoYXIuY2hhckNvZGVBdCgwKSkpXG4gICAgQXJyYXkuZnJvbShyZWYsIGNoYXIgPT4gdmlldy5zZXRVaW50OChvZmZzZXQrKywgY2hhci5jaGFyQ29kZUF0KDApKSlcbiAgICBBcnJheS5mcm9tKHRvcGljLCBjaGFyID0+IHZpZXcuc2V0VWludDgob2Zmc2V0KyssIGNoYXIuY2hhckNvZGVBdCgwKSkpXG4gICAgQXJyYXkuZnJvbShldmVudCwgY2hhciA9PiB2aWV3LnNldFVpbnQ4KG9mZnNldCsrLCBjaGFyLmNoYXJDb2RlQXQoMCkpKVxuXG4gICAgdmFyIGNvbWJpbmVkID0gbmV3IFVpbnQ4QXJyYXkoaGVhZGVyLmJ5dGVMZW5ndGggKyBwYXlsb2FkLmJ5dGVMZW5ndGgpXG4gICAgY29tYmluZWQuc2V0KG5ldyBVaW50OEFycmF5KGhlYWRlciksIDApXG4gICAgY29tYmluZWQuc2V0KG5ldyBVaW50OEFycmF5KHBheWxvYWQpLCBoZWFkZXIuYnl0ZUxlbmd0aClcblxuICAgIHJldHVybiBjb21iaW5lZC5idWZmZXJcbiAgfSxcblxuICBiaW5hcnlEZWNvZGUoYnVmZmVyKXtcbiAgICBsZXQgdmlldyA9IG5ldyBEYXRhVmlldyhidWZmZXIpXG4gICAgbGV0IGtpbmQgPSB2aWV3LmdldFVpbnQ4KDApXG4gICAgbGV0IGRlY29kZXIgPSBuZXcgVGV4dERlY29kZXIoKVxuICAgIHN3aXRjaChraW5kKXtcbiAgICAgIGNhc2UgdGhpcy5LSU5EUy5wdXNoOiByZXR1cm4gdGhpcy5kZWNvZGVQdXNoKGJ1ZmZlciwgdmlldywgZGVjb2RlcilcbiAgICAgIGNhc2UgdGhpcy5LSU5EUy5yZXBseTogcmV0dXJuIHRoaXMuZGVjb2RlUmVwbHkoYnVmZmVyLCB2aWV3LCBkZWNvZGVyKVxuICAgICAgY2FzZSB0aGlzLktJTkRTLmJyb2FkY2FzdDogcmV0dXJuIHRoaXMuZGVjb2RlQnJvYWRjYXN0KGJ1ZmZlciwgdmlldywgZGVjb2RlcilcbiAgICB9XG4gIH0sXG5cbiAgZGVjb2RlUHVzaChidWZmZXIsIHZpZXcsIGRlY29kZXIpe1xuICAgIGxldCBqb2luUmVmU2l6ZSA9IHZpZXcuZ2V0VWludDgoMSlcbiAgICBsZXQgdG9waWNTaXplID0gdmlldy5nZXRVaW50OCgyKVxuICAgIGxldCBldmVudFNpemUgPSB2aWV3LmdldFVpbnQ4KDMpXG4gICAgbGV0IG9mZnNldCA9IHRoaXMuSEVBREVSX0xFTkdUSCArIHRoaXMuTUVUQV9MRU5HVEggLSAxIC8vIHB1c2hlcyBoYXZlIG5vIHJlZlxuICAgIGxldCBqb2luUmVmID0gZGVjb2Rlci5kZWNvZGUoYnVmZmVyLnNsaWNlKG9mZnNldCwgb2Zmc2V0ICsgam9pblJlZlNpemUpKVxuICAgIG9mZnNldCA9IG9mZnNldCArIGpvaW5SZWZTaXplXG4gICAgbGV0IHRvcGljID0gZGVjb2Rlci5kZWNvZGUoYnVmZmVyLnNsaWNlKG9mZnNldCwgb2Zmc2V0ICsgdG9waWNTaXplKSlcbiAgICBvZmZzZXQgPSBvZmZzZXQgKyB0b3BpY1NpemVcbiAgICBsZXQgZXZlbnQgPSBkZWNvZGVyLmRlY29kZShidWZmZXIuc2xpY2Uob2Zmc2V0LCBvZmZzZXQgKyBldmVudFNpemUpKVxuICAgIG9mZnNldCA9IG9mZnNldCArIGV2ZW50U2l6ZVxuICAgIGxldCBkYXRhID0gYnVmZmVyLnNsaWNlKG9mZnNldCwgYnVmZmVyLmJ5dGVMZW5ndGgpXG4gICAgcmV0dXJuIHtqb2luX3JlZjogam9pblJlZiwgcmVmOiBudWxsLCB0b3BpYzogdG9waWMsIGV2ZW50OiBldmVudCwgcGF5bG9hZDogZGF0YX1cbiAgfSxcblxuICBkZWNvZGVSZXBseShidWZmZXIsIHZpZXcsIGRlY29kZXIpe1xuICAgIGxldCBqb2luUmVmU2l6ZSA9IHZpZXcuZ2V0VWludDgoMSlcbiAgICBsZXQgcmVmU2l6ZSA9IHZpZXcuZ2V0VWludDgoMilcbiAgICBsZXQgdG9waWNTaXplID0gdmlldy5nZXRVaW50OCgzKVxuICAgIGxldCBldmVudFNpemUgPSB2aWV3LmdldFVpbnQ4KDQpXG4gICAgbGV0IG9mZnNldCA9IHRoaXMuSEVBREVSX0xFTkdUSCArIHRoaXMuTUVUQV9MRU5HVEhcbiAgICBsZXQgam9pblJlZiA9IGRlY29kZXIuZGVjb2RlKGJ1ZmZlci5zbGljZShvZmZzZXQsIG9mZnNldCArIGpvaW5SZWZTaXplKSlcbiAgICBvZmZzZXQgPSBvZmZzZXQgKyBqb2luUmVmU2l6ZVxuICAgIGxldCByZWYgPSBkZWNvZGVyLmRlY29kZShidWZmZXIuc2xpY2Uob2Zmc2V0LCBvZmZzZXQgKyByZWZTaXplKSlcbiAgICBvZmZzZXQgPSBvZmZzZXQgKyByZWZTaXplXG4gICAgbGV0IHRvcGljID0gZGVjb2Rlci5kZWNvZGUoYnVmZmVyLnNsaWNlKG9mZnNldCwgb2Zmc2V0ICsgdG9waWNTaXplKSlcbiAgICBvZmZzZXQgPSBvZmZzZXQgKyB0b3BpY1NpemVcbiAgICBsZXQgZXZlbnQgPSBkZWNvZGVyLmRlY29kZShidWZmZXIuc2xpY2Uob2Zmc2V0LCBvZmZzZXQgKyBldmVudFNpemUpKVxuICAgIG9mZnNldCA9IG9mZnNldCArIGV2ZW50U2l6ZVxuICAgIGxldCBkYXRhID0gYnVmZmVyLnNsaWNlKG9mZnNldCwgYnVmZmVyLmJ5dGVMZW5ndGgpXG4gICAgbGV0IHBheWxvYWQgPSB7c3RhdHVzOiBldmVudCwgcmVzcG9uc2U6IGRhdGF9XG4gICAgcmV0dXJuIHtqb2luX3JlZjogam9pblJlZiwgcmVmOiByZWYsIHRvcGljOiB0b3BpYywgZXZlbnQ6IENIQU5ORUxfRVZFTlRTLnJlcGx5LCBwYXlsb2FkOiBwYXlsb2FkfVxuICB9LFxuXG4gIGRlY29kZUJyb2FkY2FzdChidWZmZXIsIHZpZXcsIGRlY29kZXIpe1xuICAgIGxldCB0b3BpY1NpemUgPSB2aWV3LmdldFVpbnQ4KDEpXG4gICAgbGV0IGV2ZW50U2l6ZSA9IHZpZXcuZ2V0VWludDgoMilcbiAgICBsZXQgb2Zmc2V0ID0gdGhpcy5IRUFERVJfTEVOR1RIICsgMlxuICAgIGxldCB0b3BpYyA9IGRlY29kZXIuZGVjb2RlKGJ1ZmZlci5zbGljZShvZmZzZXQsIG9mZnNldCArIHRvcGljU2l6ZSkpXG4gICAgb2Zmc2V0ID0gb2Zmc2V0ICsgdG9waWNTaXplXG4gICAgbGV0IGV2ZW50ID0gZGVjb2Rlci5kZWNvZGUoYnVmZmVyLnNsaWNlKG9mZnNldCwgb2Zmc2V0ICsgZXZlbnRTaXplKSlcbiAgICBvZmZzZXQgPSBvZmZzZXQgKyBldmVudFNpemVcbiAgICBsZXQgZGF0YSA9IGJ1ZmZlci5zbGljZShvZmZzZXQsIGJ1ZmZlci5ieXRlTGVuZ3RoKVxuXG4gICAgcmV0dXJuIHtqb2luX3JlZjogbnVsbCwgcmVmOiBudWxsLCB0b3BpYzogdG9waWMsIGV2ZW50OiBldmVudCwgcGF5bG9hZDogZGF0YX1cbiAgfVxufVxuIiwgImltcG9ydCB7XG4gIGdsb2JhbCxcbiAgcGh4V2luZG93LFxuICBDSEFOTkVMX0VWRU5UUyxcbiAgREVGQVVMVF9USU1FT1VULFxuICBERUZBVUxUX1ZTTixcbiAgU09DS0VUX1NUQVRFUyxcbiAgVFJBTlNQT1JUUyxcbiAgV1NfQ0xPU0VfTk9STUFMXG59IGZyb20gXCIuL2NvbnN0YW50c1wiXG5cbmltcG9ydCB7XG4gIGNsb3N1cmVcbn0gZnJvbSBcIi4vdXRpbHNcIlxuXG5pbXBvcnQgQWpheCBmcm9tIFwiLi9hamF4XCJcbmltcG9ydCBDaGFubmVsIGZyb20gXCIuL2NoYW5uZWxcIlxuaW1wb3J0IExvbmdQb2xsIGZyb20gXCIuL2xvbmdwb2xsXCJcbmltcG9ydCBTZXJpYWxpemVyIGZyb20gXCIuL3NlcmlhbGl6ZXJcIlxuaW1wb3J0IFRpbWVyIGZyb20gXCIuL3RpbWVyXCJcblxuLyoqIEluaXRpYWxpemVzIHRoZSBTb2NrZXQgKlxuICpcbiAqIEZvciBJRTggc3VwcG9ydCB1c2UgYW4gRVM1LXNoaW0gKGh0dHBzOi8vZ2l0aHViLmNvbS9lcy1zaGltcy9lczUtc2hpbSlcbiAqXG4gKiBAcGFyYW0ge3N0cmluZ30gZW5kUG9pbnQgLSBUaGUgc3RyaW5nIFdlYlNvY2tldCBlbmRwb2ludCwgaWUsIGBcIndzOi8vZXhhbXBsZS5jb20vc29ja2V0XCJgLFxuICogICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgIGBcIndzczovL2V4YW1wbGUuY29tXCJgXG4gKiAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgYFwiL3NvY2tldFwiYCAoaW5oZXJpdGVkIGhvc3QgJiBwcm90b2NvbClcbiAqIEBwYXJhbSB7T2JqZWN0fSBbb3B0c10gLSBPcHRpb25hbCBjb25maWd1cmF0aW9uXG4gKiBAcGFyYW0ge0Z1bmN0aW9ufSBbb3B0cy50cmFuc3BvcnRdIC0gVGhlIFdlYnNvY2tldCBUcmFuc3BvcnQsIGZvciBleGFtcGxlIFdlYlNvY2tldCBvciBQaG9lbml4LkxvbmdQb2xsLlxuICpcbiAqIERlZmF1bHRzIHRvIFdlYlNvY2tldCB3aXRoIGF1dG9tYXRpYyBMb25nUG9sbCBmYWxsYmFjayBpZiBXZWJTb2NrZXQgaXMgbm90IGRlZmluZWQuXG4gKiBUbyBmYWxsYmFjayB0byBMb25nUG9sbCB3aGVuIFdlYlNvY2tldCBhdHRlbXB0cyBmYWlsLCB1c2UgYGxvbmdQb2xsRmFsbGJhY2tNczogMjUwMGAuXG4gKlxuICogQHBhcmFtIHtGdW5jdGlvbn0gW29wdHMubG9uZ1BvbGxGYWxsYmFja01zXSAtIFRoZSBtaWxsaXNlY29uZCB0aW1lIHRvIGF0dGVtcHQgdGhlIHByaW1hcnkgdHJhbnNwb3J0XG4gKiBiZWZvcmUgZmFsbGluZyBiYWNrIHRvIHRoZSBMb25nUG9sbCB0cmFuc3BvcnQuIERpc2FibGVkIGJ5IGRlZmF1bHQuXG4gKlxuICogQHBhcmFtIHtGdW5jdGlvbn0gW29wdHMuZGVidWddIC0gV2hlbiB0cnVlLCBlbmFibGVzIGRlYnVnIGxvZ2dpbmcuIERlZmF1bHQgZmFsc2UuXG4gKlxuICogQHBhcmFtIHtGdW5jdGlvbn0gW29wdHMuZW5jb2RlXSAtIFRoZSBmdW5jdGlvbiB0byBlbmNvZGUgb3V0Z29pbmcgbWVzc2FnZXMuXG4gKlxuICogRGVmYXVsdHMgdG8gSlNPTiBlbmNvZGVyLlxuICpcbiAqIEBwYXJhbSB7RnVuY3Rpb259IFtvcHRzLmRlY29kZV0gLSBUaGUgZnVuY3Rpb24gdG8gZGVjb2RlIGluY29taW5nIG1lc3NhZ2VzLlxuICpcbiAqIERlZmF1bHRzIHRvIEpTT046XG4gKlxuICogYGBgamF2YXNjcmlwdFxuICogKHBheWxvYWQsIGNhbGxiYWNrKSA9PiBjYWxsYmFjayhKU09OLnBhcnNlKHBheWxvYWQpKVxuICogYGBgXG4gKlxuICogQHBhcmFtIHtudW1iZXJ9IFtvcHRzLnRpbWVvdXRdIC0gVGhlIGRlZmF1bHQgdGltZW91dCBpbiBtaWxsaXNlY29uZHMgdG8gdHJpZ2dlciBwdXNoIHRpbWVvdXRzLlxuICpcbiAqIERlZmF1bHRzIGBERUZBVUxUX1RJTUVPVVRgXG4gKiBAcGFyYW0ge251bWJlcn0gW29wdHMuaGVhcnRiZWF0SW50ZXJ2YWxNc10gLSBUaGUgbWlsbGlzZWMgaW50ZXJ2YWwgdG8gc2VuZCBhIGhlYXJ0YmVhdCBtZXNzYWdlXG4gKiBAcGFyYW0ge251bWJlcn0gW29wdHMucmVjb25uZWN0QWZ0ZXJNc10gLSBUaGUgb3B0aW9uYWwgZnVuY3Rpb24gdGhhdCByZXR1cm5zIHRoZSBtaWxsaXNlY1xuICogc29ja2V0IHJlY29ubmVjdCBpbnRlcnZhbC5cbiAqXG4gKiBEZWZhdWx0cyB0byBzdGVwcGVkIGJhY2tvZmYgb2Y6XG4gKlxuICogYGBgamF2YXNjcmlwdFxuICogZnVuY3Rpb24odHJpZXMpe1xuICogICByZXR1cm4gWzEwLCA1MCwgMTAwLCAxNTAsIDIwMCwgMjUwLCA1MDAsIDEwMDAsIDIwMDBdW3RyaWVzIC0gMV0gfHwgNTAwMFxuICogfVxuICogYGBgYFxuICpcbiAqIEBwYXJhbSB7bnVtYmVyfSBbb3B0cy5yZWpvaW5BZnRlck1zXSAtIFRoZSBvcHRpb25hbCBmdW5jdGlvbiB0aGF0IHJldHVybnMgdGhlIG1pbGxpc2VjXG4gKiByZWpvaW4gaW50ZXJ2YWwgZm9yIGluZGl2aWR1YWwgY2hhbm5lbHMuXG4gKlxuICogYGBgamF2YXNjcmlwdFxuICogZnVuY3Rpb24odHJpZXMpe1xuICogICByZXR1cm4gWzEwMDAsIDIwMDAsIDUwMDBdW3RyaWVzIC0gMV0gfHwgMTAwMDBcbiAqIH1cbiAqIGBgYGBcbiAqXG4gKiBAcGFyYW0ge0Z1bmN0aW9ufSBbb3B0cy5sb2dnZXJdIC0gVGhlIG9wdGlvbmFsIGZ1bmN0aW9uIGZvciBzcGVjaWFsaXplZCBsb2dnaW5nLCBpZTpcbiAqXG4gKiBgYGBqYXZhc2NyaXB0XG4gKiBmdW5jdGlvbihraW5kLCBtc2csIGRhdGEpIHtcbiAqICAgY29uc29sZS5sb2coYCR7a2luZH06ICR7bXNnfWAsIGRhdGEpXG4gKiB9XG4gKiBgYGBcbiAqXG4gKiBAcGFyYW0ge251bWJlcn0gW29wdHMubG9uZ3BvbGxlclRpbWVvdXRdIC0gVGhlIG1heGltdW0gdGltZW91dCBvZiBhIGxvbmcgcG9sbCBBSkFYIHJlcXVlc3QuXG4gKlxuICogRGVmYXVsdHMgdG8gMjBzIChkb3VibGUgdGhlIHNlcnZlciBsb25nIHBvbGwgdGltZXIpLlxuICpcbiAqIEBwYXJhbSB7KE9iamVjdHxmdW5jdGlvbil9IFtvcHRzLnBhcmFtc10gLSBUaGUgb3B0aW9uYWwgcGFyYW1zIHRvIHBhc3Mgd2hlbiBjb25uZWN0aW5nXG4gKiBAcGFyYW0ge3N0cmluZ30gW29wdHMuYmluYXJ5VHlwZV0gLSBUaGUgYmluYXJ5IHR5cGUgdG8gdXNlIGZvciBiaW5hcnkgV2ViU29ja2V0IGZyYW1lcy5cbiAqXG4gKiBEZWZhdWx0cyB0byBcImFycmF5YnVmZmVyXCJcbiAqXG4gKiBAcGFyYW0ge3Zzbn0gW29wdHMudnNuXSAtIFRoZSBzZXJpYWxpemVyJ3MgcHJvdG9jb2wgdmVyc2lvbiB0byBzZW5kIG9uIGNvbm5lY3QuXG4gKlxuICogRGVmYXVsdHMgdG8gREVGQVVMVF9WU04uXG4gKlxuICogQHBhcmFtIHtPYmplY3R9IFtvcHRzLnNlc3Npb25TdG9yYWdlXSAtIEFuIG9wdGlvbmFsIFN0b3JhZ2UgY29tcGF0aWJsZSBvYmplY3RcbiAqIFBob2VuaXggdXNlcyBzZXNzaW9uU3RvcmFnZSBmb3IgbG9uZ3BvbGwgZmFsbGJhY2sgaGlzdG9yeS4gT3ZlcnJpZGluZyB0aGUgc3RvcmUgaXNcbiAqIHVzZWZ1bCB3aGVuIFBob2VuaXggd29uJ3QgaGF2ZSBhY2Nlc3MgdG8gYHNlc3Npb25TdG9yYWdlYC4gRm9yIGV4YW1wbGUsIFRoaXMgY291bGRcbiAqIGhhcHBlbiBpZiBhIHNpdGUgbG9hZHMgYSBjcm9zcy1kb21haW4gY2hhbm5lbCBpbiBhbiBpZnJhbWUuIEV4YW1wbGUgdXNhZ2U6XG4gKlxuICogICAgIGNsYXNzIEluTWVtb3J5U3RvcmFnZSB7XG4gKiAgICAgICBjb25zdHJ1Y3RvcigpIHsgdGhpcy5zdG9yYWdlID0ge30gfVxuICogICAgICAgZ2V0SXRlbShrZXlOYW1lKSB7IHJldHVybiB0aGlzLnN0b3JhZ2Vba2V5TmFtZV0gfHwgbnVsbCB9XG4gKiAgICAgICByZW1vdmVJdGVtKGtleU5hbWUpIHsgZGVsZXRlIHRoaXMuc3RvcmFnZVtrZXlOYW1lXSB9XG4gKiAgICAgICBzZXRJdGVtKGtleU5hbWUsIGtleVZhbHVlKSB7IHRoaXMuc3RvcmFnZVtrZXlOYW1lXSA9IGtleVZhbHVlIH1cbiAqICAgICB9XG4gKlxuKi9cbmV4cG9ydCBkZWZhdWx0IGNsYXNzIFNvY2tldCB7XG4gIGNvbnN0cnVjdG9yKGVuZFBvaW50LCBvcHRzID0ge30pe1xuICAgIHRoaXMuc3RhdGVDaGFuZ2VDYWxsYmFja3MgPSB7b3BlbjogW10sIGNsb3NlOiBbXSwgZXJyb3I6IFtdLCBtZXNzYWdlOiBbXX1cbiAgICB0aGlzLmNoYW5uZWxzID0gW11cbiAgICB0aGlzLnNlbmRCdWZmZXIgPSBbXVxuICAgIHRoaXMucmVmID0gMFxuICAgIHRoaXMudGltZW91dCA9IG9wdHMudGltZW91dCB8fCBERUZBVUxUX1RJTUVPVVRcbiAgICB0aGlzLnRyYW5zcG9ydCA9IG9wdHMudHJhbnNwb3J0IHx8IGdsb2JhbC5XZWJTb2NrZXQgfHwgTG9uZ1BvbGxcbiAgICB0aGlzLnByaW1hcnlQYXNzZWRIZWFsdGhDaGVjayA9IGZhbHNlXG4gICAgdGhpcy5sb25nUG9sbEZhbGxiYWNrTXMgPSBvcHRzLmxvbmdQb2xsRmFsbGJhY2tNc1xuICAgIHRoaXMuZmFsbGJhY2tUaW1lciA9IG51bGxcbiAgICB0aGlzLnNlc3Npb25TdG9yZSA9IG9wdHMuc2Vzc2lvblN0b3JhZ2UgfHwgKGdsb2JhbCAmJiBnbG9iYWwuc2Vzc2lvblN0b3JhZ2UpXG4gICAgdGhpcy5lc3RhYmxpc2hlZENvbm5lY3Rpb25zID0gMFxuICAgIHRoaXMuZGVmYXVsdEVuY29kZXIgPSBTZXJpYWxpemVyLmVuY29kZS5iaW5kKFNlcmlhbGl6ZXIpXG4gICAgdGhpcy5kZWZhdWx0RGVjb2RlciA9IFNlcmlhbGl6ZXIuZGVjb2RlLmJpbmQoU2VyaWFsaXplcilcbiAgICB0aGlzLmNsb3NlV2FzQ2xlYW4gPSBmYWxzZVxuICAgIHRoaXMuZGlzY29ubmVjdGluZyA9IGZhbHNlXG4gICAgdGhpcy5iaW5hcnlUeXBlID0gb3B0cy5iaW5hcnlUeXBlIHx8IFwiYXJyYXlidWZmZXJcIlxuICAgIHRoaXMuY29ubmVjdENsb2NrID0gMVxuICAgIGlmKHRoaXMudHJhbnNwb3J0ICE9PSBMb25nUG9sbCl7XG4gICAgICB0aGlzLmVuY29kZSA9IG9wdHMuZW5jb2RlIHx8IHRoaXMuZGVmYXVsdEVuY29kZXJcbiAgICAgIHRoaXMuZGVjb2RlID0gb3B0cy5kZWNvZGUgfHwgdGhpcy5kZWZhdWx0RGVjb2RlclxuICAgIH0gZWxzZSB7XG4gICAgICB0aGlzLmVuY29kZSA9IHRoaXMuZGVmYXVsdEVuY29kZXJcbiAgICAgIHRoaXMuZGVjb2RlID0gdGhpcy5kZWZhdWx0RGVjb2RlclxuICAgIH1cbiAgICBsZXQgYXdhaXRpbmdDb25uZWN0aW9uT25QYWdlU2hvdyA9IG51bGxcbiAgICBpZihwaHhXaW5kb3cgJiYgcGh4V2luZG93LmFkZEV2ZW50TGlzdGVuZXIpe1xuICAgICAgcGh4V2luZG93LmFkZEV2ZW50TGlzdGVuZXIoXCJwYWdlaGlkZVwiLCBfZSA9PiB7XG4gICAgICAgIGlmKHRoaXMuY29ubil7XG4gICAgICAgICAgdGhpcy5kaXNjb25uZWN0KClcbiAgICAgICAgICBhd2FpdGluZ0Nvbm5lY3Rpb25PblBhZ2VTaG93ID0gdGhpcy5jb25uZWN0Q2xvY2tcbiAgICAgICAgfVxuICAgICAgfSlcbiAgICAgIHBoeFdpbmRvdy5hZGRFdmVudExpc3RlbmVyKFwicGFnZXNob3dcIiwgX2UgPT4ge1xuICAgICAgICBpZihhd2FpdGluZ0Nvbm5lY3Rpb25PblBhZ2VTaG93ID09PSB0aGlzLmNvbm5lY3RDbG9jayl7XG4gICAgICAgICAgYXdhaXRpbmdDb25uZWN0aW9uT25QYWdlU2hvdyA9IG51bGxcbiAgICAgICAgICB0aGlzLmNvbm5lY3QoKVxuICAgICAgICB9XG4gICAgICB9KVxuICAgIH1cbiAgICB0aGlzLmhlYXJ0YmVhdEludGVydmFsTXMgPSBvcHRzLmhlYXJ0YmVhdEludGVydmFsTXMgfHwgMzAwMDBcbiAgICB0aGlzLnJlam9pbkFmdGVyTXMgPSAodHJpZXMpID0+IHtcbiAgICAgIGlmKG9wdHMucmVqb2luQWZ0ZXJNcyl7XG4gICAgICAgIHJldHVybiBvcHRzLnJlam9pbkFmdGVyTXModHJpZXMpXG4gICAgICB9IGVsc2Uge1xuICAgICAgICByZXR1cm4gWzEwMDAsIDIwMDAsIDUwMDBdW3RyaWVzIC0gMV0gfHwgMTAwMDBcbiAgICAgIH1cbiAgICB9XG4gICAgdGhpcy5yZWNvbm5lY3RBZnRlck1zID0gKHRyaWVzKSA9PiB7XG4gICAgICBpZihvcHRzLnJlY29ubmVjdEFmdGVyTXMpe1xuICAgICAgICByZXR1cm4gb3B0cy5yZWNvbm5lY3RBZnRlck1zKHRyaWVzKVxuICAgICAgfSBlbHNlIHtcbiAgICAgICAgcmV0dXJuIFsxMCwgNTAsIDEwMCwgMTUwLCAyMDAsIDI1MCwgNTAwLCAxMDAwLCAyMDAwXVt0cmllcyAtIDFdIHx8IDUwMDBcbiAgICAgIH1cbiAgICB9XG4gICAgdGhpcy5sb2dnZXIgPSBvcHRzLmxvZ2dlciB8fCBudWxsXG4gICAgaWYoIXRoaXMubG9nZ2VyICYmIG9wdHMuZGVidWcpe1xuICAgICAgdGhpcy5sb2dnZXIgPSAoa2luZCwgbXNnLCBkYXRhKSA9PiB7IGNvbnNvbGUubG9nKGAke2tpbmR9OiAke21zZ31gLCBkYXRhKSB9XG4gICAgfVxuICAgIHRoaXMubG9uZ3BvbGxlclRpbWVvdXQgPSBvcHRzLmxvbmdwb2xsZXJUaW1lb3V0IHx8IDIwMDAwXG4gICAgdGhpcy5wYXJhbXMgPSBjbG9zdXJlKG9wdHMucGFyYW1zIHx8IHt9KVxuICAgIHRoaXMuZW5kUG9pbnQgPSBgJHtlbmRQb2ludH0vJHtUUkFOU1BPUlRTLndlYnNvY2tldH1gXG4gICAgdGhpcy52c24gPSBvcHRzLnZzbiB8fCBERUZBVUxUX1ZTTlxuICAgIHRoaXMuaGVhcnRiZWF0VGltZW91dFRpbWVyID0gbnVsbFxuICAgIHRoaXMuaGVhcnRiZWF0VGltZXIgPSBudWxsXG4gICAgdGhpcy5wZW5kaW5nSGVhcnRiZWF0UmVmID0gbnVsbFxuICAgIHRoaXMucmVjb25uZWN0VGltZXIgPSBuZXcgVGltZXIoKCkgPT4ge1xuICAgICAgdGhpcy50ZWFyZG93bigoKSA9PiB0aGlzLmNvbm5lY3QoKSlcbiAgICB9LCB0aGlzLnJlY29ubmVjdEFmdGVyTXMpXG4gIH1cblxuICAvKipcbiAgICogUmV0dXJucyB0aGUgTG9uZ1BvbGwgdHJhbnNwb3J0IHJlZmVyZW5jZVxuICAgKi9cbiAgZ2V0TG9uZ1BvbGxUcmFuc3BvcnQoKXsgcmV0dXJuIExvbmdQb2xsIH1cblxuICAvKipcbiAgICogRGlzY29ubmVjdHMgYW5kIHJlcGxhY2VzIHRoZSBhY3RpdmUgdHJhbnNwb3J0XG4gICAqXG4gICAqIEBwYXJhbSB7RnVuY3Rpb259IG5ld1RyYW5zcG9ydCAtIFRoZSBuZXcgdHJhbnNwb3J0IGNsYXNzIHRvIGluc3RhbnRpYXRlXG4gICAqXG4gICAqL1xuICByZXBsYWNlVHJhbnNwb3J0KG5ld1RyYW5zcG9ydCl7XG4gICAgdGhpcy5jb25uZWN0Q2xvY2srK1xuICAgIHRoaXMuY2xvc2VXYXNDbGVhbiA9IHRydWVcbiAgICBjbGVhclRpbWVvdXQodGhpcy5mYWxsYmFja1RpbWVyKVxuICAgIHRoaXMucmVjb25uZWN0VGltZXIucmVzZXQoKVxuICAgIGlmKHRoaXMuY29ubil7XG4gICAgICB0aGlzLmNvbm4uY2xvc2UoKVxuICAgICAgdGhpcy5jb25uID0gbnVsbFxuICAgIH1cbiAgICB0aGlzLnRyYW5zcG9ydCA9IG5ld1RyYW5zcG9ydFxuICB9XG5cbiAgLyoqXG4gICAqIFJldHVybnMgdGhlIHNvY2tldCBwcm90b2NvbFxuICAgKlxuICAgKiBAcmV0dXJucyB7c3RyaW5nfVxuICAgKi9cbiAgcHJvdG9jb2woKXsgcmV0dXJuIGxvY2F0aW9uLnByb3RvY29sLm1hdGNoKC9eaHR0cHMvKSA/IFwid3NzXCIgOiBcIndzXCIgfVxuXG4gIC8qKlxuICAgKiBUaGUgZnVsbHkgcXVhbGlmaWVkIHNvY2tldCB1cmxcbiAgICpcbiAgICogQHJldHVybnMge3N0cmluZ31cbiAgICovXG4gIGVuZFBvaW50VVJMKCl7XG4gICAgbGV0IHVyaSA9IEFqYXguYXBwZW5kUGFyYW1zKFxuICAgICAgQWpheC5hcHBlbmRQYXJhbXModGhpcy5lbmRQb2ludCwgdGhpcy5wYXJhbXMoKSksIHt2c246IHRoaXMudnNufSlcbiAgICBpZih1cmkuY2hhckF0KDApICE9PSBcIi9cIil7IHJldHVybiB1cmkgfVxuICAgIGlmKHVyaS5jaGFyQXQoMSkgPT09IFwiL1wiKXsgcmV0dXJuIGAke3RoaXMucHJvdG9jb2woKX06JHt1cml9YCB9XG5cbiAgICByZXR1cm4gYCR7dGhpcy5wcm90b2NvbCgpfTovLyR7bG9jYXRpb24uaG9zdH0ke3VyaX1gXG4gIH1cblxuICAvKipcbiAgICogRGlzY29ubmVjdHMgdGhlIHNvY2tldFxuICAgKlxuICAgKiBTZWUgaHR0cHM6Ly9kZXZlbG9wZXIubW96aWxsYS5vcmcvZW4tVVMvZG9jcy9XZWIvQVBJL0Nsb3NlRXZlbnQjU3RhdHVzX2NvZGVzIGZvciB2YWxpZCBzdGF0dXMgY29kZXMuXG4gICAqXG4gICAqIEBwYXJhbSB7RnVuY3Rpb259IGNhbGxiYWNrIC0gT3B0aW9uYWwgY2FsbGJhY2sgd2hpY2ggaXMgY2FsbGVkIGFmdGVyIHNvY2tldCBpcyBkaXNjb25uZWN0ZWQuXG4gICAqIEBwYXJhbSB7aW50ZWdlcn0gY29kZSAtIEEgc3RhdHVzIGNvZGUgZm9yIGRpc2Nvbm5lY3Rpb24gKE9wdGlvbmFsKS5cbiAgICogQHBhcmFtIHtzdHJpbmd9IHJlYXNvbiAtIEEgdGV4dHVhbCBkZXNjcmlwdGlvbiBvZiB0aGUgcmVhc29uIHRvIGRpc2Nvbm5lY3QuIChPcHRpb25hbClcbiAgICovXG4gIGRpc2Nvbm5lY3QoY2FsbGJhY2ssIGNvZGUsIHJlYXNvbil7XG4gICAgdGhpcy5jb25uZWN0Q2xvY2srK1xuICAgIHRoaXMuZGlzY29ubmVjdGluZyA9IHRydWVcbiAgICB0aGlzLmNsb3NlV2FzQ2xlYW4gPSB0cnVlXG4gICAgY2xlYXJUaW1lb3V0KHRoaXMuZmFsbGJhY2tUaW1lcilcbiAgICB0aGlzLnJlY29ubmVjdFRpbWVyLnJlc2V0KClcbiAgICB0aGlzLnRlYXJkb3duKCgpID0+IHtcbiAgICAgIHRoaXMuZGlzY29ubmVjdGluZyA9IGZhbHNlXG4gICAgICBjYWxsYmFjayAmJiBjYWxsYmFjaygpXG4gICAgfSwgY29kZSwgcmVhc29uKVxuICB9XG5cbiAgLyoqXG4gICAqXG4gICAqIEBwYXJhbSB7T2JqZWN0fSBwYXJhbXMgLSBUaGUgcGFyYW1zIHRvIHNlbmQgd2hlbiBjb25uZWN0aW5nLCBmb3IgZXhhbXBsZSBge3VzZXJfaWQ6IHVzZXJUb2tlbn1gXG4gICAqXG4gICAqIFBhc3NpbmcgcGFyYW1zIHRvIGNvbm5lY3QgaXMgZGVwcmVjYXRlZDsgcGFzcyB0aGVtIGluIHRoZSBTb2NrZXQgY29uc3RydWN0b3IgaW5zdGVhZDpcbiAgICogYG5ldyBTb2NrZXQoXCIvc29ja2V0XCIsIHtwYXJhbXM6IHt1c2VyX2lkOiB1c2VyVG9rZW59fSlgLlxuICAgKi9cbiAgY29ubmVjdChwYXJhbXMpe1xuICAgIGlmKHBhcmFtcyl7XG4gICAgICBjb25zb2xlICYmIGNvbnNvbGUubG9nKFwicGFzc2luZyBwYXJhbXMgdG8gY29ubmVjdCBpcyBkZXByZWNhdGVkLiBJbnN0ZWFkIHBhc3MgOnBhcmFtcyB0byB0aGUgU29ja2V0IGNvbnN0cnVjdG9yXCIpXG4gICAgICB0aGlzLnBhcmFtcyA9IGNsb3N1cmUocGFyYW1zKVxuICAgIH1cbiAgICBpZih0aGlzLmNvbm4gJiYgIXRoaXMuZGlzY29ubmVjdGluZyl7IHJldHVybiB9XG4gICAgaWYodGhpcy5sb25nUG9sbEZhbGxiYWNrTXMgJiYgdGhpcy50cmFuc3BvcnQgIT09IExvbmdQb2xsKXtcbiAgICAgIHRoaXMuY29ubmVjdFdpdGhGYWxsYmFjayhMb25nUG9sbCwgdGhpcy5sb25nUG9sbEZhbGxiYWNrTXMpXG4gICAgfSBlbHNlIHtcbiAgICAgIHRoaXMudHJhbnNwb3J0Q29ubmVjdCgpXG4gICAgfVxuICB9XG5cbiAgLyoqXG4gICAqIExvZ3MgdGhlIG1lc3NhZ2UuIE92ZXJyaWRlIGB0aGlzLmxvZ2dlcmAgZm9yIHNwZWNpYWxpemVkIGxvZ2dpbmcuIG5vb3BzIGJ5IGRlZmF1bHRcbiAgICogQHBhcmFtIHtzdHJpbmd9IGtpbmRcbiAgICogQHBhcmFtIHtzdHJpbmd9IG1zZ1xuICAgKiBAcGFyYW0ge09iamVjdH0gZGF0YVxuICAgKi9cbiAgbG9nKGtpbmQsIG1zZywgZGF0YSl7IHRoaXMubG9nZ2VyICYmIHRoaXMubG9nZ2VyKGtpbmQsIG1zZywgZGF0YSkgfVxuXG4gIC8qKlxuICAgKiBSZXR1cm5zIHRydWUgaWYgYSBsb2dnZXIgaGFzIGJlZW4gc2V0IG9uIHRoaXMgc29ja2V0LlxuICAgKi9cbiAgaGFzTG9nZ2VyKCl7IHJldHVybiB0aGlzLmxvZ2dlciAhPT0gbnVsbCB9XG5cbiAgLyoqXG4gICAqIFJlZ2lzdGVycyBjYWxsYmFja3MgZm9yIGNvbm5lY3Rpb24gb3BlbiBldmVudHNcbiAgICpcbiAgICogQGV4YW1wbGUgc29ja2V0Lm9uT3BlbihmdW5jdGlvbigpeyBjb25zb2xlLmluZm8oXCJ0aGUgc29ja2V0IHdhcyBvcGVuZWRcIikgfSlcbiAgICpcbiAgICogQHBhcmFtIHtGdW5jdGlvbn0gY2FsbGJhY2tcbiAgICovXG4gIG9uT3BlbihjYWxsYmFjayl7XG4gICAgbGV0IHJlZiA9IHRoaXMubWFrZVJlZigpXG4gICAgdGhpcy5zdGF0ZUNoYW5nZUNhbGxiYWNrcy5vcGVuLnB1c2goW3JlZiwgY2FsbGJhY2tdKVxuICAgIHJldHVybiByZWZcbiAgfVxuXG4gIC8qKlxuICAgKiBSZWdpc3RlcnMgY2FsbGJhY2tzIGZvciBjb25uZWN0aW9uIGNsb3NlIGV2ZW50c1xuICAgKiBAcGFyYW0ge0Z1bmN0aW9ufSBjYWxsYmFja1xuICAgKi9cbiAgb25DbG9zZShjYWxsYmFjayl7XG4gICAgbGV0IHJlZiA9IHRoaXMubWFrZVJlZigpXG4gICAgdGhpcy5zdGF0ZUNoYW5nZUNhbGxiYWNrcy5jbG9zZS5wdXNoKFtyZWYsIGNhbGxiYWNrXSlcbiAgICByZXR1cm4gcmVmXG4gIH1cblxuICAvKipcbiAgICogUmVnaXN0ZXJzIGNhbGxiYWNrcyBmb3IgY29ubmVjdGlvbiBlcnJvciBldmVudHNcbiAgICpcbiAgICogQGV4YW1wbGUgc29ja2V0Lm9uRXJyb3IoZnVuY3Rpb24oZXJyb3IpeyBhbGVydChcIkFuIGVycm9yIG9jY3VycmVkXCIpIH0pXG4gICAqXG4gICAqIEBwYXJhbSB7RnVuY3Rpb259IGNhbGxiYWNrXG4gICAqL1xuICBvbkVycm9yKGNhbGxiYWNrKXtcbiAgICBsZXQgcmVmID0gdGhpcy5tYWtlUmVmKClcbiAgICB0aGlzLnN0YXRlQ2hhbmdlQ2FsbGJhY2tzLmVycm9yLnB1c2goW3JlZiwgY2FsbGJhY2tdKVxuICAgIHJldHVybiByZWZcbiAgfVxuXG4gIC8qKlxuICAgKiBSZWdpc3RlcnMgY2FsbGJhY2tzIGZvciBjb25uZWN0aW9uIG1lc3NhZ2UgZXZlbnRzXG4gICAqIEBwYXJhbSB7RnVuY3Rpb259IGNhbGxiYWNrXG4gICAqL1xuICBvbk1lc3NhZ2UoY2FsbGJhY2spe1xuICAgIGxldCByZWYgPSB0aGlzLm1ha2VSZWYoKVxuICAgIHRoaXMuc3RhdGVDaGFuZ2VDYWxsYmFja3MubWVzc2FnZS5wdXNoKFtyZWYsIGNhbGxiYWNrXSlcbiAgICByZXR1cm4gcmVmXG4gIH1cblxuICAvKipcbiAgICogUGluZ3MgdGhlIHNlcnZlciBhbmQgaW52b2tlcyB0aGUgY2FsbGJhY2sgd2l0aCB0aGUgUlRUIGluIG1pbGxpc2Vjb25kc1xuICAgKiBAcGFyYW0ge0Z1bmN0aW9ufSBjYWxsYmFja1xuICAgKlxuICAgKiBSZXR1cm5zIHRydWUgaWYgdGhlIHBpbmcgd2FzIHB1c2hlZCBvciBmYWxzZSBpZiB1bmFibGUgdG8gYmUgcHVzaGVkLlxuICAgKi9cbiAgcGluZyhjYWxsYmFjayl7XG4gICAgaWYoIXRoaXMuaXNDb25uZWN0ZWQoKSl7IHJldHVybiBmYWxzZSB9XG4gICAgbGV0IHJlZiA9IHRoaXMubWFrZVJlZigpXG4gICAgbGV0IHN0YXJ0VGltZSA9IERhdGUubm93KClcbiAgICB0aGlzLnB1c2goe3RvcGljOiBcInBob2VuaXhcIiwgZXZlbnQ6IFwiaGVhcnRiZWF0XCIsIHBheWxvYWQ6IHt9LCByZWY6IHJlZn0pXG4gICAgbGV0IG9uTXNnUmVmID0gdGhpcy5vbk1lc3NhZ2UobXNnID0+IHtcbiAgICAgIGlmKG1zZy5yZWYgPT09IHJlZil7XG4gICAgICAgIHRoaXMub2ZmKFtvbk1zZ1JlZl0pXG4gICAgICAgIGNhbGxiYWNrKERhdGUubm93KCkgLSBzdGFydFRpbWUpXG4gICAgICB9XG4gICAgfSlcbiAgICByZXR1cm4gdHJ1ZVxuICB9XG5cbiAgLyoqXG4gICAqIEBwcml2YXRlXG4gICAqL1xuXG4gIHRyYW5zcG9ydENvbm5lY3QoKXtcbiAgICB0aGlzLmNvbm5lY3RDbG9jaysrXG4gICAgdGhpcy5jbG9zZVdhc0NsZWFuID0gZmFsc2VcbiAgICB0aGlzLmNvbm4gPSBuZXcgdGhpcy50cmFuc3BvcnQodGhpcy5lbmRQb2ludFVSTCgpKVxuICAgIHRoaXMuY29ubi5iaW5hcnlUeXBlID0gdGhpcy5iaW5hcnlUeXBlXG4gICAgdGhpcy5jb25uLnRpbWVvdXQgPSB0aGlzLmxvbmdwb2xsZXJUaW1lb3V0XG4gICAgdGhpcy5jb25uLm9ub3BlbiA9ICgpID0+IHRoaXMub25Db25uT3BlbigpXG4gICAgdGhpcy5jb25uLm9uZXJyb3IgPSBlcnJvciA9PiB0aGlzLm9uQ29ubkVycm9yKGVycm9yKVxuICAgIHRoaXMuY29ubi5vbm1lc3NhZ2UgPSBldmVudCA9PiB0aGlzLm9uQ29ubk1lc3NhZ2UoZXZlbnQpXG4gICAgdGhpcy5jb25uLm9uY2xvc2UgPSBldmVudCA9PiB0aGlzLm9uQ29ubkNsb3NlKGV2ZW50KVxuICB9XG5cbiAgZ2V0U2Vzc2lvbihrZXkpeyByZXR1cm4gdGhpcy5zZXNzaW9uU3RvcmUgJiYgdGhpcy5zZXNzaW9uU3RvcmUuZ2V0SXRlbShrZXkpIH1cblxuICBzdG9yZVNlc3Npb24oa2V5LCB2YWwpeyB0aGlzLnNlc3Npb25TdG9yZSAmJiB0aGlzLnNlc3Npb25TdG9yZS5zZXRJdGVtKGtleSwgdmFsKSB9XG5cbiAgY29ubmVjdFdpdGhGYWxsYmFjayhmYWxsYmFja1RyYW5zcG9ydCwgZmFsbGJhY2tUaHJlc2hvbGQgPSAyNTAwKXtcbiAgICBjbGVhclRpbWVvdXQodGhpcy5mYWxsYmFja1RpbWVyKVxuICAgIGxldCBlc3RhYmxpc2hlZCA9IGZhbHNlXG4gICAgbGV0IHByaW1hcnlUcmFuc3BvcnQgPSB0cnVlXG4gICAgbGV0IG9wZW5SZWYsIGVycm9yUmVmXG4gICAgbGV0IGZhbGxiYWNrID0gKHJlYXNvbikgPT4ge1xuICAgICAgdGhpcy5sb2coXCJ0cmFuc3BvcnRcIiwgYGZhbGxpbmcgYmFjayB0byAke2ZhbGxiYWNrVHJhbnNwb3J0Lm5hbWV9Li4uYCwgcmVhc29uKVxuICAgICAgdGhpcy5vZmYoW29wZW5SZWYsIGVycm9yUmVmXSlcbiAgICAgIHByaW1hcnlUcmFuc3BvcnQgPSBmYWxzZVxuICAgICAgdGhpcy5yZXBsYWNlVHJhbnNwb3J0KGZhbGxiYWNrVHJhbnNwb3J0KVxuICAgICAgdGhpcy50cmFuc3BvcnRDb25uZWN0KClcbiAgICB9XG4gICAgaWYodGhpcy5nZXRTZXNzaW9uKGBwaHg6ZmFsbGJhY2s6JHtmYWxsYmFja1RyYW5zcG9ydC5uYW1lfWApKXsgcmV0dXJuIGZhbGxiYWNrKFwibWVtb3JpemVkXCIpIH1cblxuICAgIHRoaXMuZmFsbGJhY2tUaW1lciA9IHNldFRpbWVvdXQoZmFsbGJhY2ssIGZhbGxiYWNrVGhyZXNob2xkKVxuXG4gICAgZXJyb3JSZWYgPSB0aGlzLm9uRXJyb3IocmVhc29uID0+IHtcbiAgICAgIHRoaXMubG9nKFwidHJhbnNwb3J0XCIsIFwiZXJyb3JcIiwgcmVhc29uKVxuICAgICAgaWYocHJpbWFyeVRyYW5zcG9ydCAmJiAhZXN0YWJsaXNoZWQpe1xuICAgICAgICBjbGVhclRpbWVvdXQodGhpcy5mYWxsYmFja1RpbWVyKVxuICAgICAgICBmYWxsYmFjayhyZWFzb24pXG4gICAgICB9XG4gICAgfSlcbiAgICB0aGlzLm9uT3BlbigoKSA9PiB7XG4gICAgICBlc3RhYmxpc2hlZCA9IHRydWVcbiAgICAgIGlmKCFwcmltYXJ5VHJhbnNwb3J0KXtcbiAgICAgICAgLy8gb25seSBtZW1vcml6ZSBMUCBpZiB3ZSBuZXZlciBjb25uZWN0ZWQgdG8gcHJpbWFyeVxuICAgICAgICBpZighdGhpcy5wcmltYXJ5UGFzc2VkSGVhbHRoQ2hlY2speyB0aGlzLnN0b3JlU2Vzc2lvbihgcGh4OmZhbGxiYWNrOiR7ZmFsbGJhY2tUcmFuc3BvcnQubmFtZX1gLCBcInRydWVcIikgfVxuICAgICAgICByZXR1cm4gdGhpcy5sb2coXCJ0cmFuc3BvcnRcIiwgYGVzdGFibGlzaGVkICR7ZmFsbGJhY2tUcmFuc3BvcnQubmFtZX0gZmFsbGJhY2tgKVxuICAgICAgfVxuICAgICAgLy8gaWYgd2UndmUgZXN0YWJsaXNoZWQgcHJpbWFyeSwgZ2l2ZSB0aGUgZmFsbGJhY2sgYSBuZXcgcGVyaW9kIHRvIGF0dGVtcHQgcGluZ1xuICAgICAgY2xlYXJUaW1lb3V0KHRoaXMuZmFsbGJhY2tUaW1lcilcbiAgICAgIHRoaXMuZmFsbGJhY2tUaW1lciA9IHNldFRpbWVvdXQoZmFsbGJhY2ssIGZhbGxiYWNrVGhyZXNob2xkKVxuICAgICAgdGhpcy5waW5nKHJ0dCA9PiB7XG4gICAgICAgIHRoaXMubG9nKFwidHJhbnNwb3J0XCIsIFwiY29ubmVjdGVkIHRvIHByaW1hcnkgYWZ0ZXJcIiwgcnR0KVxuICAgICAgICB0aGlzLnByaW1hcnlQYXNzZWRIZWFsdGhDaGVjayA9IHRydWVcbiAgICAgICAgY2xlYXJUaW1lb3V0KHRoaXMuZmFsbGJhY2tUaW1lcilcbiAgICAgIH0pXG4gICAgfSlcbiAgICB0aGlzLnRyYW5zcG9ydENvbm5lY3QoKVxuICB9XG5cbiAgY2xlYXJIZWFydGJlYXRzKCl7XG4gICAgY2xlYXJUaW1lb3V0KHRoaXMuaGVhcnRiZWF0VGltZXIpXG4gICAgY2xlYXJUaW1lb3V0KHRoaXMuaGVhcnRiZWF0VGltZW91dFRpbWVyKVxuICB9XG5cbiAgb25Db25uT3Blbigpe1xuICAgIGlmKHRoaXMuaGFzTG9nZ2VyKCkpIHRoaXMubG9nKFwidHJhbnNwb3J0XCIsIGAke3RoaXMudHJhbnNwb3J0Lm5hbWV9IGNvbm5lY3RlZCB0byAke3RoaXMuZW5kUG9pbnRVUkwoKX1gKVxuICAgIHRoaXMuY2xvc2VXYXNDbGVhbiA9IGZhbHNlXG4gICAgdGhpcy5kaXNjb25uZWN0aW5nID0gZmFsc2VcbiAgICB0aGlzLmVzdGFibGlzaGVkQ29ubmVjdGlvbnMrK1xuICAgIHRoaXMuZmx1c2hTZW5kQnVmZmVyKClcbiAgICB0aGlzLnJlY29ubmVjdFRpbWVyLnJlc2V0KClcbiAgICB0aGlzLnJlc2V0SGVhcnRiZWF0KClcbiAgICB0aGlzLnN0YXRlQ2hhbmdlQ2FsbGJhY2tzLm9wZW4uZm9yRWFjaCgoWywgY2FsbGJhY2tdKSA9PiBjYWxsYmFjaygpKVxuICB9XG5cbiAgLyoqXG4gICAqIEBwcml2YXRlXG4gICAqL1xuXG4gIGhlYXJ0YmVhdFRpbWVvdXQoKXtcbiAgICBpZih0aGlzLnBlbmRpbmdIZWFydGJlYXRSZWYpe1xuICAgICAgdGhpcy5wZW5kaW5nSGVhcnRiZWF0UmVmID0gbnVsbFxuICAgICAgaWYodGhpcy5oYXNMb2dnZXIoKSl7IHRoaXMubG9nKFwidHJhbnNwb3J0XCIsIFwiaGVhcnRiZWF0IHRpbWVvdXQuIEF0dGVtcHRpbmcgdG8gcmUtZXN0YWJsaXNoIGNvbm5lY3Rpb25cIikgfVxuICAgICAgdGhpcy50cmlnZ2VyQ2hhbkVycm9yKClcbiAgICAgIHRoaXMuY2xvc2VXYXNDbGVhbiA9IGZhbHNlXG4gICAgICB0aGlzLnRlYXJkb3duKCgpID0+IHRoaXMucmVjb25uZWN0VGltZXIuc2NoZWR1bGVUaW1lb3V0KCksIFdTX0NMT1NFX05PUk1BTCwgXCJoZWFydGJlYXQgdGltZW91dFwiKVxuICAgIH1cbiAgfVxuXG4gIHJlc2V0SGVhcnRiZWF0KCl7XG4gICAgaWYodGhpcy5jb25uICYmIHRoaXMuY29ubi5za2lwSGVhcnRiZWF0KXsgcmV0dXJuIH1cbiAgICB0aGlzLnBlbmRpbmdIZWFydGJlYXRSZWYgPSBudWxsXG4gICAgdGhpcy5jbGVhckhlYXJ0YmVhdHMoKVxuICAgIHRoaXMuaGVhcnRiZWF0VGltZXIgPSBzZXRUaW1lb3V0KCgpID0+IHRoaXMuc2VuZEhlYXJ0YmVhdCgpLCB0aGlzLmhlYXJ0YmVhdEludGVydmFsTXMpXG4gIH1cblxuICB0ZWFyZG93bihjYWxsYmFjaywgY29kZSwgcmVhc29uKXtcbiAgICBpZighdGhpcy5jb25uKXtcbiAgICAgIHJldHVybiBjYWxsYmFjayAmJiBjYWxsYmFjaygpXG4gICAgfVxuICAgIGxldCBjb25uZWN0Q2xvY2sgPSB0aGlzLmNvbm5lY3RDbG9ja1xuXG4gICAgdGhpcy53YWl0Rm9yQnVmZmVyRG9uZSgoKSA9PiB7XG4gICAgICBpZihjb25uZWN0Q2xvY2sgIT09IHRoaXMuY29ubmVjdENsb2NrKXsgcmV0dXJuIH1cbiAgICAgIGlmKHRoaXMuY29ubil7XG4gICAgICAgIGlmKGNvZGUpeyB0aGlzLmNvbm4uY2xvc2UoY29kZSwgcmVhc29uIHx8IFwiXCIpIH0gZWxzZSB7IHRoaXMuY29ubi5jbG9zZSgpIH1cbiAgICAgIH1cblxuICAgICAgdGhpcy53YWl0Rm9yU29ja2V0Q2xvc2VkKCgpID0+IHtcbiAgICAgICAgaWYoY29ubmVjdENsb2NrICE9PSB0aGlzLmNvbm5lY3RDbG9jayl7IHJldHVybiB9XG4gICAgICAgIGlmKHRoaXMuY29ubil7XG4gICAgICAgICAgdGhpcy5jb25uLm9ub3BlbiA9IGZ1bmN0aW9uICgpeyB9IC8vIG5vb3BcbiAgICAgICAgICB0aGlzLmNvbm4ub25lcnJvciA9IGZ1bmN0aW9uICgpeyB9IC8vIG5vb3BcbiAgICAgICAgICB0aGlzLmNvbm4ub25tZXNzYWdlID0gZnVuY3Rpb24gKCl7IH0gLy8gbm9vcFxuICAgICAgICAgIHRoaXMuY29ubi5vbmNsb3NlID0gZnVuY3Rpb24gKCl7IH0gLy8gbm9vcFxuICAgICAgICAgIHRoaXMuY29ubiA9IG51bGxcbiAgICAgICAgfVxuXG4gICAgICAgIGNhbGxiYWNrICYmIGNhbGxiYWNrKClcbiAgICAgIH0pXG4gICAgfSlcbiAgfVxuXG4gIHdhaXRGb3JCdWZmZXJEb25lKGNhbGxiYWNrLCB0cmllcyA9IDEpe1xuICAgIGlmKHRyaWVzID09PSA1IHx8ICF0aGlzLmNvbm4gfHwgIXRoaXMuY29ubi5idWZmZXJlZEFtb3VudCl7XG4gICAgICBjYWxsYmFjaygpXG4gICAgICByZXR1cm5cbiAgICB9XG5cbiAgICBzZXRUaW1lb3V0KCgpID0+IHtcbiAgICAgIHRoaXMud2FpdEZvckJ1ZmZlckRvbmUoY2FsbGJhY2ssIHRyaWVzICsgMSlcbiAgICB9LCAxNTAgKiB0cmllcylcbiAgfVxuXG4gIHdhaXRGb3JTb2NrZXRDbG9zZWQoY2FsbGJhY2ssIHRyaWVzID0gMSl7XG4gICAgaWYodHJpZXMgPT09IDUgfHwgIXRoaXMuY29ubiB8fCB0aGlzLmNvbm4ucmVhZHlTdGF0ZSA9PT0gU09DS0VUX1NUQVRFUy5jbG9zZWQpe1xuICAgICAgY2FsbGJhY2soKVxuICAgICAgcmV0dXJuXG4gICAgfVxuXG4gICAgc2V0VGltZW91dCgoKSA9PiB7XG4gICAgICB0aGlzLndhaXRGb3JTb2NrZXRDbG9zZWQoY2FsbGJhY2ssIHRyaWVzICsgMSlcbiAgICB9LCAxNTAgKiB0cmllcylcbiAgfVxuXG4gIG9uQ29ubkNsb3NlKGV2ZW50KXtcbiAgICBsZXQgY2xvc2VDb2RlID0gZXZlbnQgJiYgZXZlbnQuY29kZVxuICAgIGlmKHRoaXMuaGFzTG9nZ2VyKCkpIHRoaXMubG9nKFwidHJhbnNwb3J0XCIsIFwiY2xvc2VcIiwgZXZlbnQpXG4gICAgdGhpcy50cmlnZ2VyQ2hhbkVycm9yKClcbiAgICB0aGlzLmNsZWFySGVhcnRiZWF0cygpXG4gICAgaWYoIXRoaXMuY2xvc2VXYXNDbGVhbiAmJiBjbG9zZUNvZGUgIT09IDEwMDApe1xuICAgICAgdGhpcy5yZWNvbm5lY3RUaW1lci5zY2hlZHVsZVRpbWVvdXQoKVxuICAgIH1cbiAgICB0aGlzLnN0YXRlQ2hhbmdlQ2FsbGJhY2tzLmNsb3NlLmZvckVhY2goKFssIGNhbGxiYWNrXSkgPT4gY2FsbGJhY2soZXZlbnQpKVxuICB9XG5cbiAgLyoqXG4gICAqIEBwcml2YXRlXG4gICAqL1xuICBvbkNvbm5FcnJvcihlcnJvcil7XG4gICAgaWYodGhpcy5oYXNMb2dnZXIoKSkgdGhpcy5sb2coXCJ0cmFuc3BvcnRcIiwgZXJyb3IpXG4gICAgbGV0IHRyYW5zcG9ydEJlZm9yZSA9IHRoaXMudHJhbnNwb3J0XG4gICAgbGV0IGVzdGFibGlzaGVkQmVmb3JlID0gdGhpcy5lc3RhYmxpc2hlZENvbm5lY3Rpb25zXG4gICAgdGhpcy5zdGF0ZUNoYW5nZUNhbGxiYWNrcy5lcnJvci5mb3JFYWNoKChbLCBjYWxsYmFja10pID0+IHtcbiAgICAgIGNhbGxiYWNrKGVycm9yLCB0cmFuc3BvcnRCZWZvcmUsIGVzdGFibGlzaGVkQmVmb3JlKVxuICAgIH0pXG4gICAgaWYodHJhbnNwb3J0QmVmb3JlID09PSB0aGlzLnRyYW5zcG9ydCB8fCBlc3RhYmxpc2hlZEJlZm9yZSA+IDApe1xuICAgICAgdGhpcy50cmlnZ2VyQ2hhbkVycm9yKClcbiAgICB9XG4gIH1cblxuICAvKipcbiAgICogQHByaXZhdGVcbiAgICovXG4gIHRyaWdnZXJDaGFuRXJyb3IoKXtcbiAgICB0aGlzLmNoYW5uZWxzLmZvckVhY2goY2hhbm5lbCA9PiB7XG4gICAgICBpZighKGNoYW5uZWwuaXNFcnJvcmVkKCkgfHwgY2hhbm5lbC5pc0xlYXZpbmcoKSB8fCBjaGFubmVsLmlzQ2xvc2VkKCkpKXtcbiAgICAgICAgY2hhbm5lbC50cmlnZ2VyKENIQU5ORUxfRVZFTlRTLmVycm9yKVxuICAgICAgfVxuICAgIH0pXG4gIH1cblxuICAvKipcbiAgICogQHJldHVybnMge3N0cmluZ31cbiAgICovXG4gIGNvbm5lY3Rpb25TdGF0ZSgpe1xuICAgIHN3aXRjaCh0aGlzLmNvbm4gJiYgdGhpcy5jb25uLnJlYWR5U3RhdGUpe1xuICAgICAgY2FzZSBTT0NLRVRfU1RBVEVTLmNvbm5lY3Rpbmc6IHJldHVybiBcImNvbm5lY3RpbmdcIlxuICAgICAgY2FzZSBTT0NLRVRfU1RBVEVTLm9wZW46IHJldHVybiBcIm9wZW5cIlxuICAgICAgY2FzZSBTT0NLRVRfU1RBVEVTLmNsb3Npbmc6IHJldHVybiBcImNsb3NpbmdcIlxuICAgICAgZGVmYXVsdDogcmV0dXJuIFwiY2xvc2VkXCJcbiAgICB9XG4gIH1cblxuICAvKipcbiAgICogQHJldHVybnMge2Jvb2xlYW59XG4gICAqL1xuICBpc0Nvbm5lY3RlZCgpeyByZXR1cm4gdGhpcy5jb25uZWN0aW9uU3RhdGUoKSA9PT0gXCJvcGVuXCIgfVxuXG4gIC8qKlxuICAgKiBAcHJpdmF0ZVxuICAgKlxuICAgKiBAcGFyYW0ge0NoYW5uZWx9XG4gICAqL1xuICByZW1vdmUoY2hhbm5lbCl7XG4gICAgdGhpcy5vZmYoY2hhbm5lbC5zdGF0ZUNoYW5nZVJlZnMpXG4gICAgdGhpcy5jaGFubmVscyA9IHRoaXMuY2hhbm5lbHMuZmlsdGVyKGMgPT4gYyAhPT0gY2hhbm5lbClcbiAgfVxuXG4gIC8qKlxuICAgKiBSZW1vdmVzIGBvbk9wZW5gLCBgb25DbG9zZWAsIGBvbkVycm9yLGAgYW5kIGBvbk1lc3NhZ2VgIHJlZ2lzdHJhdGlvbnMuXG4gICAqXG4gICAqIEBwYXJhbSB7cmVmc30gLSBsaXN0IG9mIHJlZnMgcmV0dXJuZWQgYnkgY2FsbHMgdG9cbiAgICogICAgICAgICAgICAgICAgIGBvbk9wZW5gLCBgb25DbG9zZWAsIGBvbkVycm9yLGAgYW5kIGBvbk1lc3NhZ2VgXG4gICAqL1xuICBvZmYocmVmcyl7XG4gICAgZm9yKGxldCBrZXkgaW4gdGhpcy5zdGF0ZUNoYW5nZUNhbGxiYWNrcyl7XG4gICAgICB0aGlzLnN0YXRlQ2hhbmdlQ2FsbGJhY2tzW2tleV0gPSB0aGlzLnN0YXRlQ2hhbmdlQ2FsbGJhY2tzW2tleV0uZmlsdGVyKChbcmVmXSkgPT4ge1xuICAgICAgICByZXR1cm4gcmVmcy5pbmRleE9mKHJlZikgPT09IC0xXG4gICAgICB9KVxuICAgIH1cbiAgfVxuXG4gIC8qKlxuICAgKiBJbml0aWF0ZXMgYSBuZXcgY2hhbm5lbCBmb3IgdGhlIGdpdmVuIHRvcGljXG4gICAqXG4gICAqIEBwYXJhbSB7c3RyaW5nfSB0b3BpY1xuICAgKiBAcGFyYW0ge09iamVjdH0gY2hhblBhcmFtcyAtIFBhcmFtZXRlcnMgZm9yIHRoZSBjaGFubmVsXG4gICAqIEByZXR1cm5zIHtDaGFubmVsfVxuICAgKi9cbiAgY2hhbm5lbCh0b3BpYywgY2hhblBhcmFtcyA9IHt9KXtcbiAgICBsZXQgY2hhbiA9IG5ldyBDaGFubmVsKHRvcGljLCBjaGFuUGFyYW1zLCB0aGlzKVxuICAgIHRoaXMuY2hhbm5lbHMucHVzaChjaGFuKVxuICAgIHJldHVybiBjaGFuXG4gIH1cblxuICAvKipcbiAgICogQHBhcmFtIHtPYmplY3R9IGRhdGFcbiAgICovXG4gIHB1c2goZGF0YSl7XG4gICAgaWYodGhpcy5oYXNMb2dnZXIoKSl7XG4gICAgICBsZXQge3RvcGljLCBldmVudCwgcGF5bG9hZCwgcmVmLCBqb2luX3JlZn0gPSBkYXRhXG4gICAgICB0aGlzLmxvZyhcInB1c2hcIiwgYCR7dG9waWN9ICR7ZXZlbnR9ICgke2pvaW5fcmVmfSwgJHtyZWZ9KWAsIHBheWxvYWQpXG4gICAgfVxuXG4gICAgaWYodGhpcy5pc0Nvbm5lY3RlZCgpKXtcbiAgICAgIHRoaXMuZW5jb2RlKGRhdGEsIHJlc3VsdCA9PiB0aGlzLmNvbm4uc2VuZChyZXN1bHQpKVxuICAgIH0gZWxzZSB7XG4gICAgICB0aGlzLnNlbmRCdWZmZXIucHVzaCgoKSA9PiB0aGlzLmVuY29kZShkYXRhLCByZXN1bHQgPT4gdGhpcy5jb25uLnNlbmQocmVzdWx0KSkpXG4gICAgfVxuICB9XG5cbiAgLyoqXG4gICAqIFJldHVybiB0aGUgbmV4dCBtZXNzYWdlIHJlZiwgYWNjb3VudGluZyBmb3Igb3ZlcmZsb3dzXG4gICAqIEByZXR1cm5zIHtzdHJpbmd9XG4gICAqL1xuICBtYWtlUmVmKCl7XG4gICAgbGV0IG5ld1JlZiA9IHRoaXMucmVmICsgMVxuICAgIGlmKG5ld1JlZiA9PT0gdGhpcy5yZWYpeyB0aGlzLnJlZiA9IDAgfSBlbHNlIHsgdGhpcy5yZWYgPSBuZXdSZWYgfVxuXG4gICAgcmV0dXJuIHRoaXMucmVmLnRvU3RyaW5nKClcbiAgfVxuXG4gIHNlbmRIZWFydGJlYXQoKXtcbiAgICBpZih0aGlzLnBlbmRpbmdIZWFydGJlYXRSZWYgJiYgIXRoaXMuaXNDb25uZWN0ZWQoKSl7IHJldHVybiB9XG4gICAgdGhpcy5wZW5kaW5nSGVhcnRiZWF0UmVmID0gdGhpcy5tYWtlUmVmKClcbiAgICB0aGlzLnB1c2goe3RvcGljOiBcInBob2VuaXhcIiwgZXZlbnQ6IFwiaGVhcnRiZWF0XCIsIHBheWxvYWQ6IHt9LCByZWY6IHRoaXMucGVuZGluZ0hlYXJ0YmVhdFJlZn0pXG4gICAgdGhpcy5oZWFydGJlYXRUaW1lb3V0VGltZXIgPSBzZXRUaW1lb3V0KCgpID0+IHRoaXMuaGVhcnRiZWF0VGltZW91dCgpLCB0aGlzLmhlYXJ0YmVhdEludGVydmFsTXMpXG4gIH1cblxuICBmbHVzaFNlbmRCdWZmZXIoKXtcbiAgICBpZih0aGlzLmlzQ29ubmVjdGVkKCkgJiYgdGhpcy5zZW5kQnVmZmVyLmxlbmd0aCA+IDApe1xuICAgICAgdGhpcy5zZW5kQnVmZmVyLmZvckVhY2goY2FsbGJhY2sgPT4gY2FsbGJhY2soKSlcbiAgICAgIHRoaXMuc2VuZEJ1ZmZlciA9IFtdXG4gICAgfVxuICB9XG5cbiAgb25Db25uTWVzc2FnZShyYXdNZXNzYWdlKXtcbiAgICB0aGlzLmRlY29kZShyYXdNZXNzYWdlLmRhdGEsIG1zZyA9PiB7XG4gICAgICBsZXQge3RvcGljLCBldmVudCwgcGF5bG9hZCwgcmVmLCBqb2luX3JlZn0gPSBtc2dcbiAgICAgIGlmKHJlZiAmJiByZWYgPT09IHRoaXMucGVuZGluZ0hlYXJ0YmVhdFJlZil7XG4gICAgICAgIHRoaXMuY2xlYXJIZWFydGJlYXRzKClcbiAgICAgICAgdGhpcy5wZW5kaW5nSGVhcnRiZWF0UmVmID0gbnVsbFxuICAgICAgICB0aGlzLmhlYXJ0YmVhdFRpbWVyID0gc2V0VGltZW91dCgoKSA9PiB0aGlzLnNlbmRIZWFydGJlYXQoKSwgdGhpcy5oZWFydGJlYXRJbnRlcnZhbE1zKVxuICAgICAgfVxuXG4gICAgICBpZih0aGlzLmhhc0xvZ2dlcigpKSB0aGlzLmxvZyhcInJlY2VpdmVcIiwgYCR7cGF5bG9hZC5zdGF0dXMgfHwgXCJcIn0gJHt0b3BpY30gJHtldmVudH0gJHtyZWYgJiYgXCIoXCIgKyByZWYgKyBcIilcIiB8fCBcIlwifWAsIHBheWxvYWQpXG5cbiAgICAgIGZvcihsZXQgaSA9IDA7IGkgPCB0aGlzLmNoYW5uZWxzLmxlbmd0aDsgaSsrKXtcbiAgICAgICAgY29uc3QgY2hhbm5lbCA9IHRoaXMuY2hhbm5lbHNbaV1cbiAgICAgICAgaWYoIWNoYW5uZWwuaXNNZW1iZXIodG9waWMsIGV2ZW50LCBwYXlsb2FkLCBqb2luX3JlZikpeyBjb250aW51ZSB9XG4gICAgICAgIGNoYW5uZWwudHJpZ2dlcihldmVudCwgcGF5bG9hZCwgcmVmLCBqb2luX3JlZilcbiAgICAgIH1cblxuICAgICAgZm9yKGxldCBpID0gMDsgaSA8IHRoaXMuc3RhdGVDaGFuZ2VDYWxsYmFja3MubWVzc2FnZS5sZW5ndGg7IGkrKyl7XG4gICAgICAgIGxldCBbLCBjYWxsYmFja10gPSB0aGlzLnN0YXRlQ2hhbmdlQ2FsbGJhY2tzLm1lc3NhZ2VbaV1cbiAgICAgICAgY2FsbGJhY2sobXNnKVxuICAgICAgfVxuICAgIH0pXG4gIH1cblxuICBsZWF2ZU9wZW5Ub3BpYyh0b3BpYyl7XG4gICAgbGV0IGR1cENoYW5uZWwgPSB0aGlzLmNoYW5uZWxzLmZpbmQoYyA9PiBjLnRvcGljID09PSB0b3BpYyAmJiAoYy5pc0pvaW5lZCgpIHx8IGMuaXNKb2luaW5nKCkpKVxuICAgIGlmKGR1cENoYW5uZWwpe1xuICAgICAgaWYodGhpcy5oYXNMb2dnZXIoKSkgdGhpcy5sb2coXCJ0cmFuc3BvcnRcIiwgYGxlYXZpbmcgZHVwbGljYXRlIHRvcGljIFwiJHt0b3BpY31cImApXG4gICAgICBkdXBDaGFubmVsLmxlYXZlKClcbiAgICB9XG4gIH1cbn1cbiIsICIvLyBEYXJrIFBvb2wgXHUyMDE0IHJlYWx0aW1lIGNsaWVudFxuaW1wb3J0IHsgU29ja2V0IH0gZnJvbSBcInBob2VuaXhcIjtcblxuLy8gXHUyNTAwXHUyNTAwXHUyNTAwIEJvb3QgXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXG5jb25zdCBjb25maWcgPSB3aW5kb3cuRGFya1Bvb2wgfHwge307XG5cbmZ1bmN0aW9uIGJvb3QoY2ZnKSB7XG4gIGlmICghY2ZnLnNvY2tldFRva2VuKSB7XG4gICAgY29uc29sZS5kZWJ1ZyhcIltEYXJrUG9vbF0gTm8gc29ja2V0IHRva2VuIGZvdW5kIFx1MjAxNCBXZWJTb2NrZXQgbm90IHN0YXJ0ZWQuXCIpO1xuICAgIHJldHVybjtcbiAgfVxuXG4gIGNvbnN0IHNvY2tldCA9IG5ldyBTb2NrZXQoXCIvc29ja2V0XCIsIHtcbiAgICBwYXJhbXM6IHsgdG9rZW46IGNmZy5zb2NrZXRUb2tlbiB9LFxuICB9KTtcblxuICBzb2NrZXQuY29ubmVjdCgpO1xuXG4gIGNvbnN0IGxvYmJ5TWdyID0gbmV3IExvYmJ5TWFuYWdlcihzb2NrZXQsIGNmZyk7XG4gIGxvYmJ5TWdyLmluaXQoKTtcblxuICBpZiAoY2ZnLm1hdGNoSWQpIHtcbiAgICBjb25zdCBtYXRjaE1nciA9IG5ldyBNYXRjaE1hbmFnZXIoc29ja2V0LCBjZmcsIGxvYmJ5TWdyKTtcbiAgICBtYXRjaE1nci5pbml0KCk7XG4gIH1cbn1cblxuLy8gXHUyNTAwXHUyNTAwXHUyNTAwIFRvYXN0IFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFxuY29uc3QgVG9hc3QgPSB7XG4gIHNob3cobXNnLCB0eXBlID0gXCJpbmZvXCIsIGR1cmF0aW9uID0gNDAwMCkge1xuICAgIGNvbnN0IGljb25zID0ge1xuICAgICAgaW5mbzogXCJcdTIxMzlcIixcbiAgICAgIHN1Y2Nlc3M6IFwiXHUyNzEzXCIsXG4gICAgICB3YXJuaW5nOiBcIlx1MjZBMFwiLFxuICAgICAgZXJyb3I6IFwiXHUyNzE1XCIsXG4gICAgfTtcblxuICAgIGxldCBjb250YWluZXIgPSBkb2N1bWVudC5nZXRFbGVtZW50QnlJZChcInRvYXN0LWNvbnRhaW5lclwiKTtcbiAgICBpZiAoIWNvbnRhaW5lcikge1xuICAgICAgY29udGFpbmVyID0gZG9jdW1lbnQuY3JlYXRlRWxlbWVudChcImRpdlwiKTtcbiAgICAgIGNvbnRhaW5lci5pZCA9IFwidG9hc3QtY29udGFpbmVyXCI7XG4gICAgICBjb250YWluZXIuc3R5bGUucG9zaXRpb24gPSBcImZpeGVkXCI7XG4gICAgICBjb250YWluZXIuc3R5bGUudG9wID0gXCIxNnB4XCI7XG4gICAgICBjb250YWluZXIuc3R5bGUucmlnaHQgPSBcIjE2cHhcIjtcbiAgICAgIGNvbnRhaW5lci5zdHlsZS56SW5kZXggPSBcIjk5OTlcIjtcbiAgICAgIGNvbnRhaW5lci5zdHlsZS5kaXNwbGF5ID0gXCJmbGV4XCI7XG4gICAgICBjb250YWluZXIuc3R5bGUuZmxleERpcmVjdGlvbiA9IFwiY29sdW1uXCI7XG4gICAgICBjb250YWluZXIuc3R5bGUuZ2FwID0gXCI4cHhcIjtcbiAgICAgIGRvY3VtZW50LmJvZHkuYXBwZW5kQ2hpbGQoY29udGFpbmVyKTtcbiAgICB9XG5cbiAgICBjb25zdCBlbCA9IGRvY3VtZW50LmNyZWF0ZUVsZW1lbnQoXCJkaXZcIik7XG4gICAgZWwuY2xhc3NOYW1lID0gYHRvYXN0ICR7dHlwZX1gO1xuICAgIGVsLnN0eWxlLnBhZGRpbmcgPSBcIjEwcHggMTJweFwiO1xuICAgIGVsLnN0eWxlLmJvcmRlclJhZGl1cyA9IFwiMTBweFwiO1xuICAgIGVsLnN0eWxlLmJvcmRlciA9IFwiMXB4IHNvbGlkIHJnYmEoMjU1LDI1NSwyNTUsLjEyKVwiO1xuICAgIGVsLnN0eWxlLmJhY2tncm91bmQgPSBcIiMxMTE4MjdcIjtcbiAgICBlbC5zdHlsZS5jb2xvciA9IFwiI2U1ZTdlYlwiO1xuICAgIGVsLnN0eWxlLmZvbnRTaXplID0gXCIxM3B4XCI7XG4gICAgZWwuc3R5bGUuZm9udEZhbWlseSA9IFwidWktbW9ub3NwYWNlLCBTRk1vbm8tUmVndWxhciwgTWVubG8sIG1vbm9zcGFjZVwiO1xuICAgIGVsLnN0eWxlLmRpc3BsYXkgPSBcImZsZXhcIjtcbiAgICBlbC5zdHlsZS5hbGlnbkl0ZW1zID0gXCJjZW50ZXJcIjtcbiAgICBlbC5zdHlsZS5nYXAgPSBcIjhweFwiO1xuICAgIGVsLnN0eWxlLmN1cnNvciA9IFwicG9pbnRlclwiO1xuICAgIGVsLmlubmVySFRNTCA9IGBcbiAgICAgIDxzcGFuIHN0eWxlPVwiZmxleC1zaHJpbms6MFwiPiR7aWNvbnNbdHlwZV0gfHwgXCJcdTIwMjJcIn08L3NwYW4+XG4gICAgICA8c3Bhbj4ke2VzY2FwZUh0bWwobXNnKX08L3NwYW4+XG4gICAgYDtcblxuICAgIGNvbnRhaW5lci5hcHBlbmRDaGlsZChlbCk7XG5cbiAgICBjb25zdCB0aW1lciA9IHNldFRpbWVvdXQoKCkgPT4gZWwucmVtb3ZlKCksIGR1cmF0aW9uKTtcbiAgICBlbC5hZGRFdmVudExpc3RlbmVyKFwiY2xpY2tcIiwgKCkgPT4ge1xuICAgICAgY2xlYXJUaW1lb3V0KHRpbWVyKTtcbiAgICAgIGVsLnJlbW92ZSgpO1xuICAgIH0pO1xuICB9LFxufTtcblxuLy8gXHUyNTAwXHUyNTAwXHUyNTAwIFNwYXJrbGluZSBDaGFydCBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcbmNsYXNzIFNwYXJrbGluZUNoYXJ0IHtcbiAgY29uc3RydWN0b3IoY29udGFpbmVySWQsIG9wdGlvbnMgPSB7fSkge1xuICAgIHRoaXMuY29udGFpbmVyID0gZG9jdW1lbnQuZ2V0RWxlbWVudEJ5SWQoY29udGFpbmVySWQpO1xuICAgIHRoaXMuaGlzdG9yeSA9IFtdO1xuICAgIHRoaXMubWF4UG9pbnRzID0gb3B0aW9ucy5tYXhQb2ludHMgfHwgMjA7XG4gICAgdGhpcy53ID0gb3B0aW9ucy53aWR0aCB8fCA4MDtcbiAgICB0aGlzLmggPSBvcHRpb25zLmhlaWdodCB8fCAyODtcbiAgICBpZiAodGhpcy5jb250YWluZXIpIHRoaXMuX2NyZWF0ZVN2ZygpO1xuICB9XG5cbiAgX2NyZWF0ZVN2ZygpIHtcbiAgICB0aGlzLnN2ZyA9IGRvY3VtZW50LmNyZWF0ZUVsZW1lbnROUyhcImh0dHA6Ly93d3cudzMub3JnLzIwMDAvc3ZnXCIsIFwic3ZnXCIpO1xuICAgIHRoaXMuc3ZnLnNldEF0dHJpYnV0ZShcInZpZXdCb3hcIiwgYDAgMCAke3RoaXMud30gJHt0aGlzLmh9YCk7XG4gICAgdGhpcy5zdmcuc2V0QXR0cmlidXRlKFwid2lkdGhcIiwgdGhpcy53KTtcbiAgICB0aGlzLnN2Zy5zZXRBdHRyaWJ1dGUoXCJoZWlnaHRcIiwgdGhpcy5oKTtcblxuICAgIGNvbnN0IGdyYWRJZCA9IGBzZy0ke01hdGgucmFuZG9tKCkudG9TdHJpbmcoMzYpLnNsaWNlKDIpfWA7XG4gICAgdGhpcy5zdmcuaW5uZXJIVE1MID0gYFxuICAgICAgPGRlZnM+XG4gICAgICAgIDxsaW5lYXJHcmFkaWVudCBpZD1cIiR7Z3JhZElkfVwiIHgxPVwiMFwiIHkxPVwiMFwiIHgyPVwiMFwiIHkyPVwiMVwiPlxuICAgICAgICAgIDxzdG9wIG9mZnNldD1cIjAlXCIgc3RvcC1jb2xvcj1cInZhcigtLXNwYXJrLWNvbG9yLCMxMGI5ODEpXCIgc3RvcC1vcGFjaXR5PVwiMC4zNVwiLz5cbiAgICAgICAgICA8c3RvcCBvZmZzZXQ9XCIxMDAlXCIgc3RvcC1jb2xvcj1cInZhcigtLXNwYXJrLWNvbG9yLCMxMGI5ODEpXCIgc3RvcC1vcGFjaXR5PVwiMFwiLz5cbiAgICAgICAgPC9saW5lYXJHcmFkaWVudD5cbiAgICAgIDwvZGVmcz5cbiAgICAgIDxwYXRoIGNsYXNzPVwiYXJlYVwiIGZpbGw9XCJ1cmwoIyR7Z3JhZElkfSlcIj48L3BhdGg+XG4gICAgICA8cG9seWxpbmUgY2xhc3M9XCJsaW5lXCIgZmlsbD1cIm5vbmVcIiBzdHJva2U9XCJ2YXIoLS1zcGFyay1jb2xvciwjMTBiOTgxKVwiIHN0cm9rZS13aWR0aD1cIjJcIj48L3BvbHlsaW5lPlxuICAgIGA7XG4gICAgdGhpcy5ncmFkSWQgPSBncmFkSWQ7XG4gICAgdGhpcy5jb250YWluZXIuaW5uZXJIVE1MID0gXCJcIjtcbiAgICB0aGlzLmNvbnRhaW5lci5hcHBlbmRDaGlsZCh0aGlzLnN2Zyk7XG4gIH1cblxuICByZXNldCh2YWx1ZXMgPSBbXSkge1xuICAgIHRoaXMuaGlzdG9yeSA9IFtdO1xuICAgIHZhbHVlcy5mb3JFYWNoKCh2KSA9PiB0aGlzLnB1c2godikpO1xuICB9XG5cbiAgcHVzaChwcmljZSkge1xuICAgIGNvbnN0IHAgPSBwYXJzZUZsb2F0KHByaWNlKTtcbiAgICBpZiAoTnVtYmVyLmlzTmFOKHApKSByZXR1cm47XG4gICAgdGhpcy5oaXN0b3J5LnB1c2gocCk7XG4gICAgaWYgKHRoaXMuaGlzdG9yeS5sZW5ndGggPiB0aGlzLm1heFBvaW50cykgdGhpcy5oaXN0b3J5LnNoaWZ0KCk7XG4gICAgdGhpcy5fcmVuZGVyKCk7XG4gIH1cblxuICBzZXRDb2xvcihpc1VwKSB7XG4gICAgaWYgKCF0aGlzLnN2ZykgcmV0dXJuO1xuICAgIGNvbnN0IGMgPSBpc1VwID8gXCIjMTBiOTgxXCIgOiBcIiNlZjQ0NDRcIjtcbiAgICB0aGlzLnN2Zy5zdHlsZS5zZXRQcm9wZXJ0eShcIi0tc3BhcmstY29sb3JcIiwgYyk7XG4gICAgY29uc3QgZmlyc3RTdG9wID0gdGhpcy5zdmcucXVlcnlTZWxlY3RvcihcInN0b3BcIik7XG4gICAgaWYgKGZpcnN0U3RvcCkgZmlyc3RTdG9wLnNldEF0dHJpYnV0ZShcInN0b3AtY29sb3JcIiwgYyk7XG4gIH1cblxuICBfcmVuZGVyKCkge1xuICAgIGlmICghdGhpcy5zdmcgfHwgdGhpcy5oaXN0b3J5Lmxlbmd0aCA9PT0gMCkgcmV0dXJuO1xuXG4gICAgY29uc3QgbWluID0gTWF0aC5taW4oLi4udGhpcy5oaXN0b3J5KTtcbiAgICBjb25zdCBtYXggPSBNYXRoLm1heCguLi50aGlzLmhpc3RvcnkpO1xuICAgIGNvbnN0IHJhbmdlID0gbWF4IC0gbWluIHx8IDE7XG4gICAgY29uc3QgcGFkID0gMjtcbiAgICBjb25zdCBzdGVwID1cbiAgICAgIHRoaXMuaGlzdG9yeS5sZW5ndGggPiAxXG4gICAgICAgID8gKHRoaXMudyAtIHBhZCAqIDIpIC8gKHRoaXMuaGlzdG9yeS5sZW5ndGggLSAxKVxuICAgICAgICA6IDA7XG5cbiAgICBjb25zdCBwb2ludHMgPSB0aGlzLmhpc3RvcnkubWFwKCh2LCBpKSA9PiB7XG4gICAgICBjb25zdCB4ID0gcGFkICsgaSAqIHN0ZXA7XG4gICAgICBjb25zdCB5ID0gcGFkICsgKDEgLSAodiAtIG1pbikgLyByYW5nZSkgKiAodGhpcy5oIC0gcGFkICogMik7XG4gICAgICByZXR1cm4gYCR7eC50b0ZpeGVkKDIpfSwke3kudG9GaXhlZCgyKX1gO1xuICAgIH0pO1xuXG4gICAgY29uc3QgbGluZSA9IHRoaXMuc3ZnLnF1ZXJ5U2VsZWN0b3IoXCIubGluZVwiKTtcbiAgICBjb25zdCBhcmVhID0gdGhpcy5zdmcucXVlcnlTZWxlY3RvcihcIi5hcmVhXCIpO1xuXG4gICAgaWYgKGxpbmUpIGxpbmUuc2V0QXR0cmlidXRlKFwicG9pbnRzXCIsIHBvaW50cy5qb2luKFwiIFwiKSk7XG4gICAgaWYgKGFyZWEgJiYgcG9pbnRzLmxlbmd0aCA+IDApIHtcbiAgICAgIGNvbnN0IGZpcnN0ID0gcG9pbnRzWzBdLnNwbGl0KFwiLFwiKTtcbiAgICAgIGNvbnN0IGxhc3QgPSBwb2ludHNbcG9pbnRzLmxlbmd0aCAtIDFdLnNwbGl0KFwiLFwiKTtcbiAgICAgIGFyZWEuc2V0QXR0cmlidXRlKFxuICAgICAgICBcImRcIixcbiAgICAgICAgYE0ke2ZpcnN0WzBdfSwke3RoaXMuaCAtIHBhZH0gTCR7cG9pbnRzLmpvaW4oXCIgTFwiKX0gTCR7bGFzdFswXX0sJHt0aGlzLmggLSBwYWR9IFpgLFxuICAgICAgKTtcbiAgICB9XG4gIH1cbn1cblxuLy8gXHUyNTAwXHUyNTAwXHUyNTAwIExhcmdlIERldGFpbCBDaGFydCBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcbmNsYXNzIERldGFpbENoYXJ0IHtcbiAgY29uc3RydWN0b3IoY2FudmFzSWQpIHtcbiAgICB0aGlzLmNhbnZhcyA9IGRvY3VtZW50LmdldEVsZW1lbnRCeUlkKGNhbnZhc0lkKTtcbiAgICB0aGlzLmN0eCA9IHRoaXMuY2FudmFzPy5nZXRDb250ZXh0KFwiMmRcIik7XG4gIH1cblxuICByZW5kZXIocG9pbnRzKSB7XG4gICAgaWYgKCF0aGlzLmNhbnZhcyB8fCAhdGhpcy5jdHgpIHJldHVybjtcblxuICAgIGNvbnN0IHJlY3QgPSB0aGlzLmNhbnZhcy5nZXRCb3VuZGluZ0NsaWVudFJlY3QoKTtcbiAgICBjb25zdCB3ID0gTWF0aC5tYXgoNDIwLCBNYXRoLmZsb29yKHJlY3Qud2lkdGggfHwgNDIwKSk7XG4gICAgY29uc3QgaCA9IE1hdGgubWF4KDI2MCwgTWF0aC5mbG9vcihyZWN0LmhlaWdodCB8fCAyNjApKTtcbiAgICB0aGlzLmNhbnZhcy53aWR0aCA9IHc7XG4gICAgdGhpcy5jYW52YXMuaGVpZ2h0ID0gaDtcblxuICAgIGNvbnN0IGN0eCA9IHRoaXMuY3R4O1xuICAgIGN0eC5jbGVhclJlY3QoMCwgMCwgdywgaCk7XG5cbiAgICBpZiAoIXBvaW50cyB8fCBwb2ludHMubGVuZ3RoID09PSAwKSByZXR1cm47XG5cbiAgICBjb25zdCB2YWx1ZXMgPSBwb2ludHMubWFwKChwKSA9PiBwLnByaWNlKTtcbiAgICBjb25zdCBtaW4gPSBNYXRoLm1pbiguLi52YWx1ZXMpO1xuICAgIGNvbnN0IG1heCA9IE1hdGgubWF4KC4uLnZhbHVlcyk7XG4gICAgY29uc3QgcmFuZ2UgPSBtYXggLSBtaW4gfHwgMTtcbiAgICBjb25zdCBwYWRMID0gNDQ7XG4gICAgY29uc3QgcGFkUiA9IDE4O1xuICAgIGNvbnN0IHBhZFQgPSAxODtcbiAgICBjb25zdCBwYWRCID0gMzA7XG5cbiAgICBjdHguc3Ryb2tlU3R5bGUgPSBcInJnYmEoMjU1LDI1NSwyNTUsMC4wOClcIjtcbiAgICBjdHgubGluZVdpZHRoID0gMTtcblxuICAgIGZvciAobGV0IGkgPSAwOyBpIDwgNDsgaSsrKSB7XG4gICAgICBjb25zdCB5ID0gcGFkVCArICgoaCAtIHBhZFQgLSBwYWRCKSAvIDMpICogaTtcbiAgICAgIGN0eC5iZWdpblBhdGgoKTtcbiAgICAgIGN0eC5tb3ZlVG8ocGFkTCwgeSk7XG4gICAgICBjdHgubGluZVRvKHcgLSBwYWRSLCB5KTtcbiAgICAgIGN0eC5zdHJva2UoKTtcbiAgICB9XG5cbiAgICBjdHguYmVnaW5QYXRoKCk7XG4gICAgcG9pbnRzLmZvckVhY2goKHAsIGkpID0+IHtcbiAgICAgIGNvbnN0IHggPSBwYWRMICsgKGkgLyBNYXRoLm1heCgxLCBwb2ludHMubGVuZ3RoIC0gMSkpICogKHcgLSBwYWRMIC0gcGFkUik7XG4gICAgICBjb25zdCB5ID0gaCAtIHBhZEIgLSAoKHAucHJpY2UgLSBtaW4pIC8gcmFuZ2UpICogKGggLSBwYWRUIC0gcGFkQik7XG4gICAgICBpZiAoaSA9PT0gMCkgY3R4Lm1vdmVUbyh4LCB5KTtcbiAgICAgIGVsc2UgY3R4LmxpbmVUbyh4LCB5KTtcbiAgICB9KTtcbiAgICBjdHguc3Ryb2tlU3R5bGUgPSBcIiMxMGI5ODFcIjtcbiAgICBjdHgubGluZVdpZHRoID0gMztcbiAgICBjdHguc3Ryb2tlKCk7XG5cbiAgICBwb2ludHMuZm9yRWFjaCgocCwgaSkgPT4ge1xuICAgICAgY29uc3QgeCA9IHBhZEwgKyAoaSAvIE1hdGgubWF4KDEsIHBvaW50cy5sZW5ndGggLSAxKSkgKiAodyAtIHBhZEwgLSBwYWRSKTtcbiAgICAgIGNvbnN0IHkgPSBoIC0gcGFkQiAtICgocC5wcmljZSAtIG1pbikgLyByYW5nZSkgKiAoaCAtIHBhZFQgLSBwYWRCKTtcbiAgICAgIGN0eC5iZWdpblBhdGgoKTtcbiAgICAgIGN0eC5hcmMoeCwgeSwgMywgMCwgTWF0aC5QSSAqIDIpO1xuICAgICAgY3R4LmZpbGxTdHlsZSA9IFwiIzEwYjk4MVwiO1xuICAgICAgY3R4LmZpbGwoKTtcbiAgICB9KTtcblxuICAgIGN0eC5maWxsU3R5bGUgPSBcIiM5Y2EzYWZcIjtcbiAgICBjdHguZm9udCA9IFwiMTJweCB1aS1tb25vc3BhY2UsIG1vbm9zcGFjZVwiO1xuICAgIGN0eC5maWxsVGV4dChgJCR7bWF4LnRvRml4ZWQoMil9YCwgNiwgMTQpO1xuICAgIGN0eC5maWxsVGV4dChgJCR7bWluLnRvRml4ZWQoMil9YCwgNiwgaCAtIDEwKTtcblxuICAgIHBvaW50cy5mb3JFYWNoKChwLCBpKSA9PiB7XG4gICAgICBjb25zdCB4ID0gcGFkTCArIChpIC8gTWF0aC5tYXgoMSwgcG9pbnRzLmxlbmd0aCAtIDEpKSAqICh3IC0gcGFkTCAtIHBhZFIpO1xuICAgICAgY3R4LmZpbGxUZXh0KFN0cmluZyhwLnJvdW5kKSwgeCAtIDMsIGggLSA4KTtcbiAgICB9KTtcbiAgfVxufVxuXG4vLyBcdTI1MDBcdTI1MDBcdTI1MDAgTG9iYnlNYW5hZ2VyIFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFxuY2xhc3MgTG9iYnlNYW5hZ2VyIHtcbiAgY29uc3RydWN0b3Ioc29ja2V0LCBjZmcpIHtcbiAgICB0aGlzLnNvY2tldCA9IHNvY2tldDtcbiAgICB0aGlzLmNmZyA9IGNmZztcbiAgICB0aGlzLmNoYW5uZWwgPSBudWxsO1xuICAgIHRoaXMuam9pbmVkID0gZmFsc2U7XG4gIH1cblxuICBpbml0KCkge1xuICAgIHRoaXMuY2hhbm5lbCA9IHRoaXMuc29ja2V0LmNoYW5uZWwoXCJsb2JieTpnZW5lcmFsXCIsIHt9KTtcblxuICAgIHRoaXMuY2hhbm5lbC5vbihcIm9wZW5fbWF0Y2hlc1wiLCAocCkgPT5cbiAgICAgIHRoaXMuX3VwZGF0ZU1hdGNoTGlzdChwLm1hdGNoZXMgfHwgW10pLFxuICAgICk7XG4gICAgdGhpcy5jaGFubmVsLm9uKFwibWF0Y2hfY3JlYXRlZFwiLCAobWF0Y2gpID0+IHRoaXMuX2FkZE9yVXBkYXRlTWF0Y2gobWF0Y2gpKTtcbiAgICB0aGlzLmNoYW5uZWwub24oXCJtYXRjaF91cGRhdGVkXCIsIChtYXRjaCkgPT4gdGhpcy5fYWRkT3JVcGRhdGVNYXRjaChtYXRjaCkpO1xuICAgIHRoaXMuY2hhbm5lbC5vbihcIm1hdGNoX3N0YXJ0ZWRcIiwgKHApID0+IHRoaXMuX29uTWF0Y2hTdGFydGVkKHAubWF0Y2hfaWQpKTtcbiAgICB0aGlzLmNoYW5uZWwub24oXCJwcmVzZW5jZV9zdGF0ZVwiLCAoc3RhdGUpID0+IHRoaXMuX29uUHJlc2VuY2VTdGF0ZShzdGF0ZSkpO1xuICAgIHRoaXMuY2hhbm5lbC5vbihcInByZXNlbmNlX2RpZmZcIiwgKGRpZmYpID0+IHRoaXMuX29uUHJlc2VuY2VEaWZmKGRpZmYpKTtcblxuICAgIHRoaXMuY2hhbm5lbFxuICAgICAgLmpvaW4oKVxuICAgICAgLnJlY2VpdmUoXCJva1wiLCAoKSA9PiB7XG4gICAgICAgIHRoaXMuam9pbmVkID0gdHJ1ZTtcbiAgICAgICAgdGhpcy5fd2lyZUpvaW5CdXR0b25zKGRvY3VtZW50KTtcbiAgICAgIH0pXG4gICAgICAucmVjZWl2ZShcImVycm9yXCIsICgpID0+IHtcbiAgICAgICAgdGhpcy5fd2lyZUpvaW5CdXR0b25zKGRvY3VtZW50KTtcbiAgICAgIH0pO1xuICB9XG5cbiAgcHVzaChldmVudCwgcGF5bG9hZCkge1xuICAgIGlmICh0aGlzLmNoYW5uZWwpIHJldHVybiB0aGlzLmNoYW5uZWwucHVzaChldmVudCwgcGF5bG9hZCk7XG4gIH1cblxuICBfd2lyZUpvaW5CdXR0b25zKHJvb3QgPSBkb2N1bWVudCkge1xuICAgIHJvb3RcbiAgICAgIC5xdWVyeVNlbGVjdG9yQWxsKFwiLmpvaW4tbWF0Y2gtYnRuOm5vdChbZGF0YS13aXJlZF0pXCIpXG4gICAgICAuZm9yRWFjaCgoYnRuKSA9PiB7XG4gICAgICAgIGJ0bi5kYXRhc2V0LndpcmVkID0gXCIxXCI7XG4gICAgICAgIGJ0bi5hZGRFdmVudExpc3RlbmVyKFwiY2xpY2tcIiwgKGUpID0+IHtcbiAgICAgICAgICBlLnByZXZlbnREZWZhdWx0KCk7XG4gICAgICAgICAgY29uc3QgbWF0Y2hJZCA9XG4gICAgICAgICAgICBidG4uZGF0YXNldC5tYXRjaElkIHx8XG4gICAgICAgICAgICBidG4uY2xvc2VzdChcIltkYXRhLW1hdGNoLWlkXVwiKT8uZGF0YXNldC5tYXRjaElkO1xuICAgICAgICAgIGlmICghbWF0Y2hJZCkgcmV0dXJuO1xuICAgICAgICAgIHRoaXMuX2pvaW5NYXRjaChtYXRjaElkLCBidG4pO1xuICAgICAgICB9KTtcbiAgICAgIH0pO1xuICB9XG5cbiAgX2pvaW5NYXRjaChtYXRjaElkLCBidG4pIHtcbiAgICBjb25zdCBvcmlnaW5hbCA9IGJ0bi50ZXh0Q29udGVudDtcbiAgICBidG4uZGlzYWJsZWQgPSB0cnVlO1xuICAgIGJ0bi50ZXh0Q29udGVudCA9IFwiSm9pbmluZy4uLlwiO1xuXG4gICAgdGhpcy5jaGFubmVsXG4gICAgICAucHVzaChcImpvaW5fbWF0Y2hcIiwgeyBtYXRjaF9pZDogbWF0Y2hJZCB9KVxuICAgICAgLnJlY2VpdmUoXCJva1wiLCAoKSA9PiB7XG4gICAgICAgIHdpbmRvdy5sb2NhdGlvbi5ocmVmID0gYC9tYXRjaGVzLyR7bWF0Y2hJZH1gO1xuICAgICAgfSlcbiAgICAgIC5yZWNlaXZlKFwiZXJyb3JcIiwgKGVycikgPT4ge1xuICAgICAgICBidG4uZGlzYWJsZWQgPSBmYWxzZTtcbiAgICAgICAgYnRuLnRleHRDb250ZW50ID0gb3JpZ2luYWw7XG4gICAgICAgIFRvYXN0LnNob3coXG4gICAgICAgICAgYENvdWxkIG5vdCBqb2luOiAke2Vycj8ucmVhc29uIHx8IFwidW5rbm93biBlcnJvclwifWAsXG4gICAgICAgICAgXCJlcnJvclwiLFxuICAgICAgICApO1xuICAgICAgfSlcbiAgICAgIC5yZWNlaXZlKFwidGltZW91dFwiLCAoKSA9PiB7XG4gICAgICAgIGJ0bi5kaXNhYmxlZCA9IGZhbHNlO1xuICAgICAgICBidG4udGV4dENvbnRlbnQgPSBvcmlnaW5hbDtcbiAgICAgICAgVG9hc3Quc2hvdyhcIkpvaW4gdGltZWQgb3V0LiBQbGVhc2UgdHJ5IGFnYWluLlwiLCBcIndhcm5pbmdcIik7XG4gICAgICB9KTtcbiAgfVxuXG4gIF9hZGRPclVwZGF0ZU1hdGNoKG1hdGNoKSB7XG4gICAgY29uc3QgbGlzdCA9IGRvY3VtZW50LmdldEVsZW1lbnRCeUlkKFwib3Blbi1tYXRjaGVzLWxpc3RcIik7XG4gICAgaWYgKCFsaXN0IHx8ICFtYXRjaCkgcmV0dXJuO1xuXG4gICAgZG9jdW1lbnQuZ2V0RWxlbWVudEJ5SWQoXCJuby1tYXRjaGVzLXBsYWNlaG9sZGVyXCIpPy5yZW1vdmUoKTtcblxuICAgIGNvbnN0IGV4aXN0aW5nID0gbGlzdC5xdWVyeVNlbGVjdG9yKGBbZGF0YS1tYXRjaC1pZD1cIiR7bWF0Y2guaWR9XCJdYCk7XG4gICAgY29uc3QgY2FyZCA9IHRoaXMuX2J1aWxkTWF0Y2hDYXJkKG1hdGNoKTtcblxuICAgIGlmIChleGlzdGluZykgZXhpc3RpbmcucmVwbGFjZVdpdGgoY2FyZCk7XG4gICAgZWxzZSBsaXN0Lmluc2VydEFkamFjZW50RWxlbWVudChcImFmdGVyYmVnaW5cIiwgY2FyZCk7XG5cbiAgICB0aGlzLl93aXJlSm9pbkJ1dHRvbnMobGlzdCk7XG4gIH1cblxuICBfYnVpbGRNYXRjaENhcmQobWF0Y2gpIHtcbiAgICBjb25zdCBkaXYgPSBkb2N1bWVudC5jcmVhdGVFbGVtZW50KFwiZGl2XCIpO1xuICAgIGRpdi5jbGFzc05hbWUgPSBcImNhcmQgY2FyZC1ob3ZlciBwLTQgbWF0Y2gtY2FyZCBmYWRlLWluLXVwXCI7XG4gICAgZGl2LmRhdGFzZXQubWF0Y2hJZCA9IG1hdGNoLmlkO1xuXG4gICAgY29uc3QgaXNGdWxsID0gKG1hdGNoLnBsYXllcl9jb3VudCB8fCAwKSA+PSBtYXRjaC5tYXhfcGxheWVycztcblxuICAgIGRpdi5pbm5lckhUTUwgPSBgXG4gICAgICA8ZGl2IGNsYXNzPVwiZmxleCBpdGVtcy1jZW50ZXIganVzdGlmeS1iZXR3ZWVuXCI+XG4gICAgICAgIDxkaXYgY2xhc3M9XCJmbGV4IGl0ZW1zLWNlbnRlciBnYXAtM1wiPlxuICAgICAgICAgIDxkaXYgY2xhc3M9XCJ3LTkgaC05IHJvdW5kZWQtbGcgYmctZW1lcmFsZC01MDAvMTAgYm9yZGVyIGJvcmRlci1lbWVyYWxkLTUwMC8yMCBmbGV4IGl0ZW1zLWNlbnRlciBqdXN0aWZ5LWNlbnRlciBzaHJpbmstMFwiPlxuICAgICAgICAgICAgPHN2ZyBjbGFzcz1cInctNCBoLTQgdGV4dC1lbWVyYWxkLTQwMFwiIGZpbGw9XCJub25lXCIgdmlld0JveD1cIjAgMCAyNCAyNFwiIHN0cm9rZT1cImN1cnJlbnRDb2xvclwiIHN0cm9rZS13aWR0aD1cIjJcIj5cbiAgICAgICAgICAgICAgPHBvbHlsaW5lIHBvaW50cz1cIjIyIDcgMTMuNSAxNS41IDguNSAxMC41IDIgMTdcIj48L3BvbHlsaW5lPlxuICAgICAgICAgICAgPC9zdmc+XG4gICAgICAgICAgPC9kaXY+XG4gICAgICAgICAgPGRpdj5cbiAgICAgICAgICAgIDxkaXYgY2xhc3M9XCJmb250LXNlbWlib2xkIHRleHQtc20gdGV4dC13aGl0ZSBmb250LW1vbm9cIj4ke2VzY2FwZUh0bWwobWF0Y2gubmFtZSl9PC9kaXY+XG4gICAgICAgICAgICA8ZGl2IGNsYXNzPVwidGV4dC14cyB0ZXh0LWdyYXktNTAwIG10LTAuNVwiPlxuICAgICAgICAgICAgICBob3N0ZWQgYnkgPHNwYW4gY2xhc3M9XCJ0ZXh0LWdyYXktNDAwXCI+JHtlc2NhcGVIdG1sKG1hdGNoLmhvc3RfdXNlcm5hbWUgfHwgXCJcdTIwMTRcIil9PC9zcGFuPlxuICAgICAgICAgICAgPC9kaXY+XG4gICAgICAgICAgPC9kaXY+XG4gICAgICAgIDwvZGl2PlxuICAgICAgICA8ZGl2IGNsYXNzPVwiZmxleCBpdGVtcy1jZW50ZXIgZ2FwLTNcIj5cbiAgICAgICAgICA8ZGl2IGNsYXNzPVwidGV4dC1yaWdodFwiPlxuICAgICAgICAgICAgPGRpdiBjbGFzcz1cInRleHQteHMgZm9udC1tb25vIHRleHQtZ3JheS00MDBcIj5cbiAgICAgICAgICAgICAgPHNwYW4gY2xhc3M9XCJ0ZXh0LXdoaXRlIGZvbnQtc2VtaWJvbGRcIj4ke21hdGNoLnBsYXllcl9jb3VudCB8fCAwfTwvc3Bhbj4vJHttYXRjaC5tYXhfcGxheWVycyB8fCA0fVxuICAgICAgICAgICAgPC9kaXY+XG4gICAgICAgICAgPC9kaXY+XG4gICAgICAgICAgJHtcbiAgICAgICAgICAgIGlzRnVsbFxuICAgICAgICAgICAgICA/IGA8c3BhbiBjbGFzcz1cImJ0bi1naG9zdCB0ZXh0LXhzIHB5LTEuNSBweC0zIG9wYWNpdHktNDAgcG9pbnRlci1ldmVudHMtbm9uZVwiPkZ1bGw8L3NwYW4+YFxuICAgICAgICAgICAgICA6IGA8YnV0dG9uIHR5cGU9XCJidXR0b25cIiBkYXRhLW1hdGNoLWlkPVwiJHtlc2NhcGVIdG1sKG1hdGNoLmlkKX1cIiBjbGFzcz1cImpvaW4tbWF0Y2gtYnRuIGJ0bi1wcmltYXJ5IHRleHQteHMgcHktMS41IHB4LTRcIj5Kb2luPC9idXR0b24+YFxuICAgICAgICAgIH1cbiAgICAgICAgPC9kaXY+XG4gICAgICA8L2Rpdj5cbiAgICBgO1xuXG4gICAgcmV0dXJuIGRpdjtcbiAgfVxuXG4gIF91cGRhdGVNYXRjaExpc3QobWF0Y2hlcykge1xuICAgIGNvbnN0IGxpc3QgPSBkb2N1bWVudC5nZXRFbGVtZW50QnlJZChcIm9wZW4tbWF0Y2hlcy1saXN0XCIpO1xuICAgIGlmICghbGlzdCkgcmV0dXJuO1xuXG4gICAgaWYgKCFtYXRjaGVzLmxlbmd0aCkgcmV0dXJuO1xuXG4gICAgbGlzdC5pbm5lckhUTUwgPSBcIlwiO1xuICAgIG1hdGNoZXMuZm9yRWFjaCgobSkgPT4gbGlzdC5hcHBlbmRDaGlsZCh0aGlzLl9idWlsZE1hdGNoQ2FyZChtKSkpO1xuICAgIHRoaXMuX3dpcmVKb2luQnV0dG9ucyhsaXN0KTtcbiAgfVxuXG4gIF9vbk1hdGNoU3RhcnRlZChtYXRjaElkKSB7XG4gICAgY29uc3QgY2FyZCA9IGRvY3VtZW50LnF1ZXJ5U2VsZWN0b3IoXG4gICAgICBgI29wZW4tbWF0Y2hlcy1saXN0IFtkYXRhLW1hdGNoLWlkPVwiJHttYXRjaElkfVwiXWAsXG4gICAgKTtcbiAgICBpZiAoY2FyZCkgY2FyZC5yZW1vdmUoKTtcbiAgfVxuXG4gIF9vblByZXNlbmNlU3RhdGUoc3RhdGUpIHtcbiAgICBjb25zdCBsaXN0ID0gZG9jdW1lbnQuZ2V0RWxlbWVudEJ5SWQoXCJsb2JieS1wcmVzZW5jZS1saXN0XCIpO1xuICAgIGNvbnN0IGNvdW50ZXIgPSBkb2N1bWVudC5nZXRFbGVtZW50QnlJZChcImxvYmJ5LW9ubGluZS1jb3VudFwiKTtcbiAgICBpZiAoIWxpc3QpIHJldHVybjtcblxuICAgIGNvbnN0IHVzZXJzID0gW107XG4gICAgT2JqZWN0LnZhbHVlcyhzdGF0ZSB8fCB7fSkuZm9yRWFjaCgoZW50cnkpID0+IHtcbiAgICAgIGNvbnN0IG1ldGEgPSBlbnRyeS5tZXRhcz8uWzBdO1xuICAgICAgaWYgKG1ldGE/LnVzZXJuYW1lKSB1c2Vycy5wdXNoKG1ldGEudXNlcm5hbWUpO1xuICAgIH0pO1xuXG4gICAgaWYgKGNvdW50ZXIpIGNvdW50ZXIudGV4dENvbnRlbnQgPSBTdHJpbmcodXNlcnMubGVuZ3RoKTtcbiAgICB0aGlzLl9yZW5kZXJQcmVzZW5jZUxpc3QobGlzdCwgdXNlcnMpO1xuICB9XG5cbiAgX29uUHJlc2VuY2VEaWZmKGRpZmYpIHtcbiAgICBjb25zdCBsaXN0ID0gZG9jdW1lbnQuZ2V0RWxlbWVudEJ5SWQoXCJsb2JieS1wcmVzZW5jZS1saXN0XCIpO1xuICAgIGlmICghbGlzdCkgcmV0dXJuO1xuXG4gICAgY29uc3QgY3VycmVudCA9IG5ldyBNYXAoKTtcbiAgICBsaXN0LnF1ZXJ5U2VsZWN0b3JBbGwoXCJbZGF0YS1wcmVzZW5jZS11c2VyXVwiKS5mb3JFYWNoKChlbCkgPT4ge1xuICAgICAgY3VycmVudC5zZXQoZWwuZGF0YXNldC5wcmVzZW5jZVVzZXIsIGVsKTtcbiAgICB9KTtcblxuICAgIE9iamVjdC52YWx1ZXMoZGlmZj8uam9pbnMgfHwge30pLmZvckVhY2goKGVudHJ5KSA9PiB7XG4gICAgICBjb25zdCBtZXRhID0gZW50cnkubWV0YXM/LlswXTtcbiAgICAgIGlmICghbWV0YT8udXNlcm5hbWUgfHwgY3VycmVudC5oYXMobWV0YS51c2VybmFtZSkpIHJldHVybjtcbiAgICAgIGNvbnN0IGVsID0gdGhpcy5fYnVpbGRQcmVzZW5jZVJvdyhtZXRhLnVzZXJuYW1lKTtcbiAgICAgIGxpc3QuYXBwZW5kQ2hpbGQoZWwpO1xuICAgICAgY3VycmVudC5zZXQobWV0YS51c2VybmFtZSwgZWwpO1xuICAgIH0pO1xuXG4gICAgT2JqZWN0LnZhbHVlcyhkaWZmPy5sZWF2ZXMgfHwge30pLmZvckVhY2goKGVudHJ5KSA9PiB7XG4gICAgICBjb25zdCBtZXRhID0gZW50cnkubWV0YXM/LlswXTtcbiAgICAgIGlmICghbWV0YT8udXNlcm5hbWUpIHJldHVybjtcbiAgICAgIGNvbnN0IGVsID0gY3VycmVudC5nZXQobWV0YS51c2VybmFtZSk7XG4gICAgICBpZiAoZWwpIHtcbiAgICAgICAgZWwucmVtb3ZlKCk7XG4gICAgICAgIGN1cnJlbnQuZGVsZXRlKG1ldGEudXNlcm5hbWUpO1xuICAgICAgfVxuICAgIH0pO1xuXG4gICAgY29uc3QgY291bnRlciA9IGRvY3VtZW50LmdldEVsZW1lbnRCeUlkKFwibG9iYnktb25saW5lLWNvdW50XCIpO1xuICAgIGlmIChjb3VudGVyKSBjb3VudGVyLnRleHRDb250ZW50ID0gU3RyaW5nKGN1cnJlbnQuc2l6ZSk7XG5cbiAgICBpZiAoY3VycmVudC5zaXplID09PSAwKSB7XG4gICAgICBsaXN0LmlubmVySFRNTCA9IGA8cCBjbGFzcz1cInRleHQteHMgdGV4dC1ncmF5LTYwMCBmb250LW1vbm9cIj5Ob2JvZHkgZWxzZSBvbmxpbmUuPC9wPmA7XG4gICAgfVxuICB9XG5cbiAgX3JlbmRlclByZXNlbmNlTGlzdChsaXN0LCB1c2Vycykge1xuICAgIGlmICghdXNlcnMubGVuZ3RoKSB7XG4gICAgICBsaXN0LmlubmVySFRNTCA9IGA8cCBjbGFzcz1cInRleHQteHMgdGV4dC1ncmF5LTYwMCBmb250LW1vbm9cIj5Ob2JvZHkgZWxzZSBvbmxpbmUuPC9wPmA7XG4gICAgICByZXR1cm47XG4gICAgfVxuXG4gICAgbGlzdC5pbm5lckhUTUwgPSBcIlwiO1xuICAgIHVzZXJzLmZvckVhY2goKHUpID0+IGxpc3QuYXBwZW5kQ2hpbGQodGhpcy5fYnVpbGRQcmVzZW5jZVJvdyh1KSkpO1xuICB9XG5cbiAgX2J1aWxkUHJlc2VuY2VSb3codXNlcm5hbWUpIHtcbiAgICBjb25zdCBlbCA9IGRvY3VtZW50LmNyZWF0ZUVsZW1lbnQoXCJkaXZcIik7XG4gICAgZWwuY2xhc3NOYW1lID0gXCJmbGV4IGl0ZW1zLWNlbnRlciBnYXAtMiBmYWRlLWluLXVwXCI7XG4gICAgZWwuZGF0YXNldC5wcmVzZW5jZVVzZXIgPSB1c2VybmFtZTtcbiAgICBlbC5pbm5lckhUTUwgPSBgXG4gICAgICA8ZGl2IGNsYXNzPVwicHJlc2VuY2UtZG90XCI+PC9kaXY+XG4gICAgICA8c3BhbiBjbGFzcz1cInRleHQteHMgZm9udC1tb25vIHRleHQtZ3JheS0zMDBcIj4ke2VzY2FwZUh0bWwodXNlcm5hbWUpfTwvc3Bhbj5cbiAgICBgO1xuICAgIHJldHVybiBlbDtcbiAgfVxufVxuXG4vLyBcdTI1MDBcdTI1MDBcdTI1MDAgTWF0Y2hNYW5hZ2VyIFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFxuY2xhc3MgTWF0Y2hNYW5hZ2VyIHtcbiAgY29uc3RydWN0b3Ioc29ja2V0LCBjZmcsIGxvYmJ5TWdyKSB7XG4gICAgdGhpcy5zb2NrZXQgPSBzb2NrZXQ7XG4gICAgdGhpcy5jZmcgPSBjZmc7XG4gICAgdGhpcy5sb2JieU1nciA9IGxvYmJ5TWdyO1xuICAgIHRoaXMuY2hhbm5lbCA9IG51bGw7XG4gICAgdGhpcy5wbGF5ZXJzID0gW107XG4gICAgdGhpcy5jb21wYW5pZXMgPSBbXTtcbiAgICB0aGlzLm15U3RhdGUgPSBudWxsO1xuICAgIHRoaXMucGhhc2UgPSBudWxsO1xuICAgIHRoaXMuY291bnRkb3duID0gbnVsbDtcbiAgICB0aGlzLnBoYXNlT3ZlcmxheVRpbWVyID0gbnVsbDtcblxuICAgIHRoaXMucHJldlByaWNlcyA9IHt9O1xuICAgIHRoaXMuc3BhcmtsaW5lQ2hhcnRzID0ge307XG4gICAgdGhpcy5wcmljZUhpc3RvcnkgPSB7fTtcbiAgICB0aGlzLnNlbGVjdGVkVGlja2VyID0gbnVsbDtcbiAgICB0aGlzLmRldGFpbENoYXJ0ID0gbmV3IERldGFpbENoYXJ0KFwic3RvY2stZGV0YWlsLWNoYXJ0XCIpO1xuXG4gICAgdGhpcy5vbmxpbmVVc2VycyA9IG5ldyBTZXQoKTtcbiAgfVxuXG4gIGluaXQoKSB7XG4gICAgdGhpcy5jaGFubmVsID0gdGhpcy5zb2NrZXQuY2hhbm5lbChgbWF0Y2g6JHt0aGlzLmNmZy5tYXRjaElkfWAsIHt9KTtcblxuICAgIHRoaXMuY2hhbm5lbC5vbihcInN0YXRlX3VwZGF0ZWRcIiwgKHApID0+IHRoaXMuX29uU3RhdGVVcGRhdGVkKHApKTtcbiAgICB0aGlzLmNoYW5uZWwub24oXCJwaGFzZV9jaGFuZ2VkXCIsIChwKSA9PiB0aGlzLl9vblBoYXNlQ2hhbmdlZChwKSk7XG4gICAgdGhpcy5jaGFubmVsLm9uKFwicHJpdmF0ZV9zdGF0ZVwiLCAocCkgPT4gdGhpcy5fb25Qcml2YXRlU3RhdGUocCkpO1xuICAgIHRoaXMuY2hhbm5lbC5vbihcInByaXZhdGVfZXZlbnRzXCIsIChwKSA9PiB0aGlzLl9vblByaXZhdGVFdmVudHMocCkpO1xuICAgIHRoaXMuY2hhbm5lbC5vbihcIm5ld19tZXNzYWdlXCIsIChtKSA9PiB0aGlzLl9hcHBlbmRDaGF0KG0pKTtcbiAgICB0aGlzLmNoYW5uZWwub24oXCJtYXRjaF9maW5pc2hlZFwiLCAocCkgPT4gdGhpcy5fb25NYXRjaEZpbmlzaGVkKHApKTtcbiAgICB0aGlzLmNoYW5uZWwub24oXCJwcmVzZW5jZV9zdGF0ZVwiLCAocykgPT4gdGhpcy5fb25QcmVzZW5jZVN0YXRlKHMpKTtcbiAgICB0aGlzLmNoYW5uZWwub24oXCJwcmVzZW5jZV9kaWZmXCIsIChkKSA9PiB0aGlzLl9vblByZXNlbmNlRGlmZihkKSk7XG4gICAgdGhpcy5jaGFubmVsLm9uKFwid2FpdGluZ19yb29tX3VwZGF0ZWRcIiwgKHApID0+XG4gICAgICB0aGlzLl9vbldhaXRpbmdSb29tVXBkYXRlZChwKSxcbiAgICApO1xuXG4gICAgdGhpcy5jaGFubmVsXG4gICAgICAuam9pbigpXG4gICAgICAucmVjZWl2ZShcIm9rXCIsICgpID0+IHt9KVxuICAgICAgLnJlY2VpdmUoXCJlcnJvclwiLCAoeyByZWFzb24gfSkgPT4ge1xuICAgICAgICBUb2FzdC5zaG93KGBDb3VsZCBub3Qgam9pbiBtYXRjaDogJHtyZWFzb259YCwgXCJlcnJvclwiKTtcbiAgICAgIH0pO1xuXG4gICAgdGhpcy5fd2lyZUNvbnRyb2xzKCk7XG4gIH1cblxuICBfd2lyZUNvbnRyb2xzKCkge1xuICAgIGNvbnN0IHN0YXJ0QnRuID0gZG9jdW1lbnQuZ2V0RWxlbWVudEJ5SWQoXCJzdGFydC1tYXRjaC1idG5cIik7XG4gICAgaWYgKHN0YXJ0QnRuKSB7XG4gICAgICBzdGFydEJ0bi5hZGRFdmVudExpc3RlbmVyKFwiY2xpY2tcIiwgKGUpID0+IHtcbiAgICAgICAgZS5wcmV2ZW50RGVmYXVsdCgpO1xuICAgICAgICBzdGFydEJ0bi5kaXNhYmxlZCA9IHRydWU7XG4gICAgICAgIHN0YXJ0QnRuLnRleHRDb250ZW50ID0gXCJTdGFydGluZy4uLlwiO1xuXG4gICAgICAgIHRoaXMubG9iYnlNZ3JcbiAgICAgICAgICAucHVzaChcInN0YXJ0X21hdGNoXCIsIHsgbWF0Y2hfaWQ6IHRoaXMuY2ZnLm1hdGNoSWQgfSlcbiAgICAgICAgICA/LnJlY2VpdmUoXCJva1wiLCAoKSA9PiB7XG4gICAgICAgICAgICBzdGFydEJ0bi5yZW1vdmUoKTtcbiAgICAgICAgICB9KVxuICAgICAgICAgID8ucmVjZWl2ZShcImVycm9yXCIsIChlcnIpID0+IHtcbiAgICAgICAgICAgIHN0YXJ0QnRuLmRpc2FibGVkID0gZmFsc2U7XG4gICAgICAgICAgICBzdGFydEJ0bi50ZXh0Q29udGVudCA9IFwiU3RhcnQgTWF0Y2hcIjtcbiAgICAgICAgICAgIFRvYXN0LnNob3coXG4gICAgICAgICAgICAgIGBDb3VsZCBub3Qgc3RhcnQ6ICR7ZXJyPy5yZWFzb24gfHwgXCJ1bmtub3duIGVycm9yXCJ9YCxcbiAgICAgICAgICAgICAgXCJlcnJvclwiLFxuICAgICAgICAgICAgKTtcbiAgICAgICAgICB9KTtcbiAgICAgIH0pO1xuICAgIH1cblxuICAgIGRvY3VtZW50LnF1ZXJ5U2VsZWN0b3JBbGwoXCIuYWN0aW9uLWJ0blwiKS5mb3JFYWNoKChidG4pID0+IHtcbiAgICAgIGJ0bi5hZGRFdmVudExpc3RlbmVyKFwiY2xpY2tcIiwgKGUpID0+IHtcbiAgICAgICAgZS5wcmV2ZW50RGVmYXVsdCgpO1xuICAgICAgICBpZiAoYnRuLmRpc2FibGVkKSByZXR1cm47XG4gICAgICAgIHRoaXMuX29wZW5BY3Rpb25Nb2RhbChidG4uZGF0YXNldCk7XG4gICAgICB9KTtcbiAgICB9KTtcblxuICAgIGRvY3VtZW50XG4gICAgICAuZ2V0RWxlbWVudEJ5SWQoXCJjb3B5LWludml0ZS1idG5cIilcbiAgICAgID8uYWRkRXZlbnRMaXN0ZW5lcihcImNsaWNrXCIsIChlKSA9PiB7XG4gICAgICAgIGUucHJldmVudERlZmF1bHQoKTtcbiAgICAgICAgY29uc3QgdXJsID0gd2luZG93LmxvY2F0aW9uLmhyZWY7XG4gICAgICAgIG5hdmlnYXRvci5jbGlwYm9hcmRcbiAgICAgICAgICA/LndyaXRlVGV4dCh1cmwpXG4gICAgICAgICAgLnRoZW4oKCkgPT4gVG9hc3Quc2hvdyhcIkludml0ZSBsaW5rIGNvcGllZFwiLCBcInN1Y2Nlc3NcIikpXG4gICAgICAgICAgLmNhdGNoKCgpID0+IHdpbmRvdy5wcm9tcHQoXCJDb3B5IHRoaXMgaW52aXRlIGxpbms6XCIsIHVybCkpO1xuICAgICAgfSk7XG5cbiAgICBkb2N1bWVudC5nZXRFbGVtZW50QnlJZChcIm1vZGFsLWNsb3NlXCIpPy5hZGRFdmVudExpc3RlbmVyKFwiY2xpY2tcIiwgKGUpID0+IHtcbiAgICAgIGUucHJldmVudERlZmF1bHQoKTtcbiAgICAgIHRoaXMuX2Nsb3NlTW9kYWwoKTtcbiAgICB9KTtcblxuICAgIGRvY3VtZW50LmdldEVsZW1lbnRCeUlkKFwiYWN0aW9uLW1vZGFsXCIpPy5hZGRFdmVudExpc3RlbmVyKFwiY2xpY2tcIiwgKGUpID0+IHtcbiAgICAgIGlmIChlLnRhcmdldCA9PT0gZS5jdXJyZW50VGFyZ2V0KSB0aGlzLl9jbG9zZU1vZGFsKCk7XG4gICAgfSk7XG5cbiAgICBkb2N1bWVudC5nZXRFbGVtZW50QnlJZChcIm1vZGFsLXN1Ym1pdFwiKT8uYWRkRXZlbnRMaXN0ZW5lcihcImNsaWNrXCIsIChlKSA9PiB7XG4gICAgICBlLnByZXZlbnREZWZhdWx0KCk7XG4gICAgICB0aGlzLl9zdWJtaXRBY3Rpb24oKTtcbiAgICB9KTtcblxuICAgIGRvY3VtZW50XG4gICAgICAuZ2V0RWxlbWVudEJ5SWQoXCJpbnB1dC1xdWFudGl0eVwiKVxuICAgICAgPy5hZGRFdmVudExpc3RlbmVyKFwia2V5ZG93blwiLCAoZSkgPT4ge1xuICAgICAgICBpZiAoZS5rZXkgPT09IFwiRW50ZXJcIikge1xuICAgICAgICAgIGUucHJldmVudERlZmF1bHQoKTtcbiAgICAgICAgICB0aGlzLl9zdWJtaXRBY3Rpb24oKTtcbiAgICAgICAgfVxuICAgICAgfSk7XG5cbiAgICBjb25zdCBzZW5kQnRuID0gZG9jdW1lbnQuZ2V0RWxlbWVudEJ5SWQoXCJjaGF0LXNlbmRcIik7XG4gICAgY29uc3QgY2hhdElucHV0ID0gZG9jdW1lbnQuZ2V0RWxlbWVudEJ5SWQoXCJjaGF0LWlucHV0XCIpO1xuXG4gICAgaWYgKHNlbmRCdG4gJiYgY2hhdElucHV0KSB7XG4gICAgICBzZW5kQnRuLmFkZEV2ZW50TGlzdGVuZXIoXCJjbGlja1wiLCAoZSkgPT4ge1xuICAgICAgICBlLnByZXZlbnREZWZhdWx0KCk7XG4gICAgICAgIHRoaXMuX3NlbmRDaGF0TWVzc2FnZSgpO1xuICAgICAgfSk7XG5cbiAgICAgIGNoYXRJbnB1dC5hZGRFdmVudExpc3RlbmVyKFwia2V5ZG93blwiLCAoZSkgPT4ge1xuICAgICAgICBpZiAoZS5rZXkgPT09IFwiRW50ZXJcIiAmJiAhZS5zaGlmdEtleSkge1xuICAgICAgICAgIGUucHJldmVudERlZmF1bHQoKTtcbiAgICAgICAgICB0aGlzLl9zZW5kQ2hhdE1lc3NhZ2UoKTtcbiAgICAgICAgfVxuICAgICAgfSk7XG4gICAgfVxuICB9XG5cbiAgLy8gXHUyNTAwXHUyNTAwIHN0YXRlIHVwZGF0ZXMgXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXG5cbiAgX29uU3RhdGVVcGRhdGVkKHBheWxvYWQpIHtcbiAgICB0aGlzLnBsYXllcnMgPSBwYXlsb2FkLnBsYXllcnMgfHwgW107XG4gICAgdGhpcy5jb21wYW5pZXMgPSBwYXlsb2FkLmNvbXBhbmllcyB8fCBbXTtcbiAgICB0aGlzLnBoYXNlID0gcGF5bG9hZC5waGFzZTtcblxuICAgIHRoaXMuX3JlbmRlclJvdW5kKHBheWxvYWQucm91bmQsIHBheWxvYWQudG90YWxfcm91bmRzKTtcbiAgICB0aGlzLl9yZW5kZXJQaGFzZShwYXlsb2FkLnBoYXNlKTtcbiAgICB0aGlzLl9yZW5kZXJQbGF5ZXJzKHBheWxvYWQucGxheWVycyB8fCBbXSk7XG4gICAgdGhpcy5fcmVuZGVyTWFya2V0KHBheWxvYWQuY29tcGFuaWVzIHx8IFtdLCBwYXlsb2FkLnJvdW5kKTtcbiAgICB0aGlzLl9yZW5kZXJBY3Rpb25QYW5lbChwYXlsb2FkLnBoYXNlKTtcbiAgICBjb25zdCBldmVudHMgPSBwYXlsb2FkLnB1YmxpY19ldmVudHMgfHwgW107XG4gICAgaWYgKGV2ZW50cy5sZW5ndGggPiAwKSB7XG4gICAgICB0aGlzLl9yZW5kZXJFdmVudHMoZXZlbnRzKTtcbiAgICAgIHRoaXMuX3JlbmRlck5ld3MoZXZlbnRzKTtcbiAgICB9XG4gICAgdGhpcy5fdXBkYXRlQ2hhdEluZGljYXRvcihwYXlsb2FkLnBoYXNlKTtcblxuICAgIGlmIChwYXlsb2FkLnBoYXNlID09PSBcIm5lZ290aWF0aW9uXCIgJiYgcGF5bG9hZC5uZWdvdGlhdGlvbl9kZWFkbGluZSkge1xuICAgICAgdGhpcy5fc3RhcnRDb3VudGRvd24obmV3IERhdGUocGF5bG9hZC5uZWdvdGlhdGlvbl9kZWFkbGluZSkpO1xuICAgIH0gZWxzZSBpZiAocGF5bG9hZC5waGFzZSAhPT0gXCJuZWdvdGlhdGlvblwiKSB7XG4gICAgICB0aGlzLl9jbGVhckNvdW50ZG93bigpO1xuICAgIH1cbiAgfVxuXG4gIF9vblBoYXNlQ2hhbmdlZChwYXlsb2FkKSB7XG4gICAgdGhpcy5waGFzZSA9IHBheWxvYWQucGhhc2U7XG5cbiAgICB0aGlzLl9yZW5kZXJSb3VuZChwYXlsb2FkLnJvdW5kLCBwYXlsb2FkLnRvdGFsX3JvdW5kcyk7XG4gICAgdGhpcy5fcmVuZGVyUGhhc2UocGF5bG9hZC5waGFzZSk7XG4gICAgdGhpcy5fcmVuZGVyQWN0aW9uUGFuZWwocGF5bG9hZC5waGFzZSk7XG4gICAgdGhpcy5fdXBkYXRlQ2hhdEluZGljYXRvcihwYXlsb2FkLnBoYXNlKTtcbiAgICB0aGlzLl9zaG93UGhhc2VPdmVybGF5KHBheWxvYWQucGhhc2UpO1xuXG4gICAgaWYgKHBheWxvYWQucGhhc2UgPT09IFwiYWN0aW9uX3N1Ym1pc3Npb25cIikge1xuICAgICAgdGhpcy5fY2xlYXJDb3VudGRvd24oKTtcbiAgICAgIHRoaXMuX2hpZGUoZG9jdW1lbnQuZ2V0RWxlbWVudEJ5SWQoXCJhY3Rpb24tc3VibWl0dGVkLWJhZGdlXCIpKTtcbiAgICAgIGRvY3VtZW50LnF1ZXJ5U2VsZWN0b3JBbGwoXCIuYWN0aW9uLWJ0blwiKS5mb3JFYWNoKChiKSA9PiB7XG4gICAgICAgIGIuZGlzYWJsZWQgPSBmYWxzZTtcbiAgICAgICAgYi5jbGFzc0xpc3QucmVtb3ZlKFwic3VibWl0dGVkXCIpO1xuICAgICAgfSk7XG4gICAgfVxuXG4gICAgaWYgKHBheWxvYWQucGhhc2UgPT09IFwibmVnb3RpYXRpb25cIiAmJiBwYXlsb2FkLm5lZ290aWF0aW9uX2RlYWRsaW5lKSB7XG4gICAgICB0aGlzLl9zdGFydENvdW50ZG93bihuZXcgRGF0ZShwYXlsb2FkLm5lZ290aWF0aW9uX2RlYWRsaW5lKSk7XG4gICAgfSBlbHNlIGlmIChwYXlsb2FkLnBoYXNlICE9PSBcIm5lZ290aWF0aW9uXCIpIHtcbiAgICAgIHRoaXMuX2NsZWFyQ291bnRkb3duKCk7XG4gICAgfVxuICB9XG5cbiAgX29uUHJpdmF0ZVN0YXRlKHN0YXRlKSB7XG4gICAgdGhpcy5teVN0YXRlID0gc3RhdGU7XG4gICAgdGhpcy5fcmVuZGVyTXlTdGF0cyhzdGF0ZSk7XG4gIH1cblxuICBfb25Qcml2YXRlRXZlbnRzKHBheWxvYWQpIHtcbiAgICAocGF5bG9hZC5ldmVudHMgfHwgW10pLmZvckVhY2goKGV2KSA9PiB7XG4gICAgICBjb25zdCBtc2cgPSBldi5tZXNzYWdlIHx8IGV2LmhlYWRsaW5lIHx8IGV2LnR5cGUgfHwgXCJFdmVudFwiO1xuICAgICAgVG9hc3Quc2hvdyhtc2csIGV2LnBvc2l0aXZlID8gXCJzdWNjZXNzXCIgOiBcImluZm9cIik7XG4gICAgfSk7XG4gIH1cblxuICBfb25NYXRjaEZpbmlzaGVkKHBheWxvYWQpIHtcbiAgICB0aGlzLl9jbGVhckNvdW50ZG93bigpO1xuICAgIHRoaXMuX3JlbmRlckxlYWRlcmJvYXJkKHBheWxvYWQubGVhZGVyYm9hcmQgfHwgW10pO1xuICAgIHRoaXMuX3Nob3dQaGFzZU92ZXJsYXkoXCJmaW5pc2hlZFwiKTtcbiAgICB0aGlzLl9oaWRlKGRvY3VtZW50LmdldEVsZW1lbnRCeUlkKFwiYWN0aW9uLXBhbmVsXCIpKTtcbiAgICB0aGlzLl9zaG93KGRvY3VtZW50LmdldEVsZW1lbnRCeUlkKFwibGVhZGVyYm9hcmQtcGFuZWxcIiksIFwiYmxvY2tcIik7XG4gICAgVG9hc3Quc2hvdyhcIk1hdGNoIG92ZXIhIEZpbmFsIHJhbmtpbmdzIGFyZSBpbi5cIiwgXCJpbmZvXCIsIDgwMDApO1xuICB9XG5cbiAgLy8gXHUyNTAwXHUyNTAwIHByZXNlbmNlIFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFxuXG4gIF9vblByZXNlbmNlU3RhdGUoc3RhdGUpIHtcbiAgICBPYmplY3QuZW50cmllcyhzdGF0ZSB8fCB7fSkuZm9yRWFjaCgoW3VzZXJJZF0pID0+IHtcbiAgICAgIHRoaXMuX3NldFBsYXllck9ubGluZSh1c2VySWQsIHRydWUpO1xuICAgIH0pO1xuICB9XG5cbiAgX29uUHJlc2VuY2VEaWZmKGRpZmYpIHtcbiAgICBPYmplY3QuZW50cmllcyhkaWZmPy5qb2lucyB8fCB7fSkuZm9yRWFjaCgoW3VzZXJJZF0pID0+XG4gICAgICB0aGlzLl9zZXRQbGF5ZXJPbmxpbmUodXNlcklkLCB0cnVlKSxcbiAgICApO1xuICAgIE9iamVjdC5lbnRyaWVzKGRpZmY/LmxlYXZlcyB8fCB7fSkuZm9yRWFjaCgoW3VzZXJJZF0pID0+XG4gICAgICB0aGlzLl9zZXRQbGF5ZXJPbmxpbmUodXNlcklkLCBmYWxzZSksXG4gICAgKTtcbiAgfVxuXG4gIF9vbldhaXRpbmdSb29tVXBkYXRlZChwYXlsb2FkKSB7XG4gICAgaWYgKHBheWxvYWQucGxheWVycykge1xuICAgICAgdGhpcy5wbGF5ZXJzID0gcGF5bG9hZC5wbGF5ZXJzO1xuICAgICAgdGhpcy5fcmVuZGVyUGxheWVycyhwYXlsb2FkLnBsYXllcnMpO1xuICAgIH1cbiAgfVxuXG4gIF9zZXRQbGF5ZXJPbmxpbmUodXNlcklkLCBvbmxpbmUpIHtcbiAgICBpZiAob25saW5lKSB0aGlzLm9ubGluZVVzZXJzLmFkZChTdHJpbmcodXNlcklkKSk7XG4gICAgZWxzZSB0aGlzLm9ubGluZVVzZXJzLmRlbGV0ZShTdHJpbmcodXNlcklkKSk7XG4gICAgdGhpcy5fYXBwbHlPbmxpbmVTdGF0ZSgpO1xuICB9XG5cbiAgX2FwcGx5T25saW5lU3RhdGUoKSB7XG4gICAgZG9jdW1lbnQucXVlcnlTZWxlY3RvckFsbChcIltkYXRhLXBsYXllci1pZF1cIikuZm9yRWFjaCgocm93KSA9PiB7XG4gICAgICBjb25zdCBkb3QgPSByb3cucXVlcnlTZWxlY3RvcihcIi5wcmVzZW5jZS1kb3RcIik7XG4gICAgICBpZiAoZG90KSBkb3QuY2xhc3NMaXN0LnRvZ2dsZShcIm9mZmxpbmVcIiwgIXRoaXMub25saW5lVXNlcnMuaGFzKHJvdy5kYXRhc2V0LnBsYXllcklkKSk7XG4gICAgfSk7XG4gIH1cblxuICAvLyBcdTI1MDBcdTI1MDAgcmVuZGVyaW5nIFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFxuXG4gIF9yZW5kZXJSb3VuZChyb3VuZCwgdG90YWwpIHtcbiAgICBjb25zdCBlbCA9IGRvY3VtZW50LmdldEVsZW1lbnRCeUlkKFwiY3VycmVudC1yb3VuZFwiKTtcbiAgICBpZiAoZWwpIGVsLnRleHRDb250ZW50ID0gcm91bmQgfHwgXCJcdTIwMTRcIjtcblxuICAgIGNvbnN0IGxhc3RSb3VuZCA9IGRvY3VtZW50LmdldEVsZW1lbnRCeUlkKFwiZGV0YWlsLWxhc3Qtcm91bmRcIik7XG4gICAgaWYgKGxhc3RSb3VuZCkgbGFzdFJvdW5kLnRleHRDb250ZW50ID0gcm91bmQgfHwgXCIxXCI7XG4gIH1cblxuICBfcmVuZGVyUGhhc2UocGhhc2UpIHtcbiAgICBjb25zdCBjb250YWluZXIgPSBkb2N1bWVudC5nZXRFbGVtZW50QnlJZChcInBoYXNlLWJhZGdlLWNvbnRhaW5lclwiKTtcbiAgICBpZiAoIWNvbnRhaW5lcikgcmV0dXJuO1xuICAgIGNvbnRhaW5lci5pbm5lckhUTUwgPSBgPHNwYW4gY2xhc3M9XCJwaGFzZS1iYWRnZSBwaGFzZS0ke3BoYXNlIHx8IFwid2FpdGluZ1wifVwiPiR7cGhhc2VMYWJlbChwaGFzZSl9PC9zcGFuPmA7XG4gIH1cblxuICBfcmVuZGVyQWN0aW9uUGFuZWwocGhhc2UpIHtcbiAgICBjb25zdCBwYW5lbCA9IGRvY3VtZW50LmdldEVsZW1lbnRCeUlkKFwiYWN0aW9uLXBhbmVsXCIpO1xuICAgIGlmICghcGFuZWwpIHJldHVybjtcblxuICAgIGlmIChwaGFzZSA9PT0gXCJhY3Rpb25fc3VibWlzc2lvblwiKSB0aGlzLl9zaG93KHBhbmVsLCBcImJsb2NrXCIpO1xuICAgIGVsc2UgdGhpcy5faGlkZShwYW5lbCk7XG4gIH1cblxuICBfcmVuZGVyUGxheWVycyhwbGF5ZXJzKSB7XG4gICAgY29uc3QgbGlzdCA9IGRvY3VtZW50LmdldEVsZW1lbnRCeUlkKFwicGxheWVycy1saXN0XCIpO1xuICAgIGlmICghbGlzdCB8fCAhcGxheWVycykgcmV0dXJuO1xuXG4gICAgbGlzdC5pbm5lckhUTUwgPSBcIlwiO1xuXG4gICAgcGxheWVyc1xuICAgICAgLnNsaWNlKClcbiAgICAgIC5zb3J0KChhLCBiKSA9PiAoYS5zZWF0X251bWJlciB8fCAwKSAtIChiLnNlYXRfbnVtYmVyIHx8IDApKVxuICAgICAgLmZvckVhY2goKHApID0+IHtcbiAgICAgICAgY29uc3Qgcm93ID0gZG9jdW1lbnQuY3JlYXRlRWxlbWVudChcImRpdlwiKTtcbiAgICAgICAgcm93LmNsYXNzTmFtZSA9IFwiZmxleCBpdGVtcy1jZW50ZXIgZ2FwLTIuNVwiO1xuICAgICAgICByb3cuZGF0YXNldC5wbGF5ZXJJZCA9IHAudXNlcl9pZDtcblxuICAgICAgICByb3cuaW5uZXJIVE1MID0gYFxuICAgICAgICAgIDxkaXYgY2xhc3M9XCJwcmVzZW5jZS1kb3Qgb2ZmbGluZVwiPjwvZGl2PlxuICAgICAgICAgIDxkaXYgY2xhc3M9XCJmbGV4LTEgbWluLXctMFwiPlxuICAgICAgICAgICAgPHNwYW4gY2xhc3M9XCJ0ZXh0LXNtIHRleHQtZ3JheS0zMDAgZm9udC1tb25vIHRydW5jYXRlXCI+JHtlc2NhcGVIdG1sKHAudXNlcm5hbWUpfTwvc3Bhbj5cbiAgICAgICAgICA8L2Rpdj5cbiAgICAgICAgICA8c3BhbiBjbGFzcz1cInRleHQteHMgZm9udC1tb25vIHRleHQtYW1iZXItNDAwXCI+JHsocC5yZWd1bGF0b3J5X2hlYXQgfHwgMCkgPiAwID8gYFx1MjY5NiR7cC5yZWd1bGF0b3J5X2hlYXR9YCA6IFwiXCJ9PC9zcGFuPlxuICAgICAgICAgIDxzcGFuIGNsYXNzPVwidGV4dC14cyBmb250LW1vbm8gdGV4dC1ibHVlLTQwMFwiPiR7cC5saXF1aWRpdHlfZnJvemVuID8gXCJcdTI3NDRcIiA6IFwiXCJ9PC9zcGFuPlxuICAgICAgICAgIDxzcGFuIGNsYXNzPVwidGV4dC14cyBmb250LW1vbm8gdGV4dC1lbWVyYWxkLTQwMFwiPiR7cC5oYXNfc3VibWl0dGVkID8gXCJcdTI3MTNcIiA6IFwiXCJ9PC9zcGFuPlxuICAgICAgICBgO1xuXG4gICAgICAgIGxpc3QuYXBwZW5kQ2hpbGQocm93KTtcbiAgICAgIH0pO1xuXG4gICAgdGhpcy5fYXBwbHlPbmxpbmVTdGF0ZSgpO1xuICB9XG5cbiAgX3JlbmRlck15U3RhdHMoc3RhdGUpIHtcbiAgICB0aGlzLl9zaG93KGRvY3VtZW50LmdldEVsZW1lbnRCeUlkKFwibXktc3RhdHNcIiksIFwiYmxvY2tcIik7XG5cbiAgICBjb25zdCBjYXNoID0gcGFyc2VGbG9hdChzdGF0ZS5jYXNoIHx8IDApO1xuICAgIGNvbnN0IG5ldFdvcnRoID0gcGFyc2VGbG9hdChzdGF0ZS5uZXRfd29ydGggfHwgMCk7XG5cbiAgICBjb25zdCBjYXNoRWwgPSBkb2N1bWVudC5nZXRFbGVtZW50QnlJZChcIm15LWNhc2hcIik7XG4gICAgY29uc3QgbndFbCA9IGRvY3VtZW50LmdldEVsZW1lbnRCeUlkKFwibXktbmV0d29ydGhcIik7XG4gICAgY29uc3QgaGVhdEVsID0gZG9jdW1lbnQuZ2V0RWxlbWVudEJ5SWQoXCJteS1oZWF0XCIpO1xuXG4gICAgaWYgKGNhc2hFbCkgY2FzaEVsLnRleHRDb250ZW50ID0gYCQke2Nhc2gudG9GaXhlZCgyKX1gO1xuICAgIGlmIChud0VsKSBud0VsLnRleHRDb250ZW50ID0gYCQke25ldFdvcnRoLnRvRml4ZWQoMil9YDtcbiAgICBpZiAoaGVhdEVsKSBoZWF0RWwudGV4dENvbnRlbnQgPSBzdGF0ZS5yZWd1bGF0b3J5X2hlYXQgfHwgMDtcblxuICAgIGlmIChzdGF0ZS5saXF1aWRpdHlfZnJvemVuKVxuICAgICAgdGhpcy5fc2hvdyhkb2N1bWVudC5nZXRFbGVtZW50QnlJZChcIm15LWZyb3plbi1iYWRnZVwiKSwgXCJibG9ja1wiKTtcbiAgICBlbHNlIHRoaXMuX2hpZGUoZG9jdW1lbnQuZ2V0RWxlbWVudEJ5SWQoXCJteS1mcm96ZW4tYmFkZ2VcIikpO1xuXG4gICAgY29uc3QgaG9sZGluZ3NFbCA9IGRvY3VtZW50LmdldEVsZW1lbnRCeUlkKFwibXktaG9sZGluZ3NcIik7XG4gICAgaWYgKGhvbGRpbmdzRWwpIHtcbiAgICAgIGhvbGRpbmdzRWwuaW5uZXJIVE1MID0gXCJcIjtcbiAgICAgIE9iamVjdC5lbnRyaWVzKHN0YXRlLnBvcnRmb2xpbyB8fCB7fSkuZm9yRWFjaCgoW3RpY2tlciwgcXR5XSkgPT4ge1xuICAgICAgICBjb25zdCByb3cgPSBkb2N1bWVudC5jcmVhdGVFbGVtZW50KFwiZGl2XCIpO1xuICAgICAgICByb3cuY2xhc3NOYW1lID0gXCJmbGV4IGp1c3RpZnktYmV0d2VlbiB0ZXh0LXhzIGZvbnQtbW9ub1wiO1xuICAgICAgICByb3cuaW5uZXJIVE1MID0gYDxzcGFuIGNsYXNzPVwidGV4dC1ncmF5LTUwMFwiPiR7ZXNjYXBlSHRtbCh0aWNrZXIpfTwvc3Bhbj48c3BhbiBjbGFzcz1cInRleHQtZ3JheS0zMDBcIj4ke3F0eX0gc2g8L3NwYW4+YDtcbiAgICAgICAgaG9sZGluZ3NFbC5hcHBlbmRDaGlsZChyb3cpO1xuICAgICAgfSk7XG4gICAgfVxuICB9XG5cbiAgX3JlbmRlck1hcmtldChjb21wYW5pZXMsIHJvdW5kKSB7XG4gICAgY29uc3QgbGlzdCA9IGRvY3VtZW50LmdldEVsZW1lbnRCeUlkKFwiY29tcGFuaWVzLWxpc3RcIik7XG4gICAgaWYgKCFsaXN0KSByZXR1cm47XG5cbiAgICBsaXN0LmlubmVySFRNTCA9IFwiXCI7XG5cbiAgICBjb21wYW5pZXMuZm9yRWFjaCgoYykgPT4ge1xuICAgICAgY29uc3QgcHJpY2UgPSBwYXJzZUZsb2F0KGMucHJpY2UgfHwgMCk7XG4gICAgICBjb25zdCBwcmV2ID0gdGhpcy5wcmV2UHJpY2VzW2MudGlja2VyXTtcbiAgICAgIGNvbnN0IGlzVXAgPSBwcmV2ID09PSB1bmRlZmluZWQgPyB0cnVlIDogcHJpY2UgPj0gcHJldjtcbiAgICAgIHRoaXMucHJldlByaWNlc1tjLnRpY2tlcl0gPSBwcmljZTtcblxuICAgICAgaWYgKCF0aGlzLnByaWNlSGlzdG9yeVtjLnRpY2tlcl0pIHRoaXMucHJpY2VIaXN0b3J5W2MudGlja2VyXSA9IFtdO1xuICAgICAgY29uc3QgaGlzdG9yeSA9IHRoaXMucHJpY2VIaXN0b3J5W2MudGlja2VyXTtcbiAgICAgIGNvbnN0IGxhc3QgPSBoaXN0b3J5W2hpc3RvcnkubGVuZ3RoIC0gMV07XG4gICAgICBpZiAoIWxhc3QgfHwgbGFzdC5yb3VuZCAhPT0gcm91bmQgfHwgbGFzdC5wcmljZSAhPT0gcHJpY2UpIHtcbiAgICAgICAgaGlzdG9yeS5wdXNoKHtcbiAgICAgICAgICByb3VuZDogcm91bmQgfHwgMSxcbiAgICAgICAgICBwcmljZSxcbiAgICAgICAgICByZWd1bGF0b3J5X2hlYXQ6IGMucmVndWxhdG9yeV9oZWF0IHx8IDAsXG4gICAgICAgIH0pO1xuICAgICAgfVxuXG4gICAgICBjb25zdCByb3cgPSBkb2N1bWVudC5jcmVhdGVFbGVtZW50KFwiZGl2XCIpO1xuICAgICAgcm93LmNsYXNzTmFtZSA9IGB0aWNrZXItcm93ICR7dGhpcy5zZWxlY3RlZFRpY2tlciA9PT0gYy50aWNrZXIgPyBcInNlbGVjdGVkXCIgOiBcIlwifWA7XG4gICAgICByb3cuZGF0YXNldC50aWNrZXIgPSBjLnRpY2tlcjtcblxuICAgICAgY29uc3QgY2hhcnRJZCA9IGBjaGFydC0ke2MudGlja2VyfWA7XG4gICAgICBjb25zdCBwY3QgPVxuICAgICAgICBwcmV2ID09PSB1bmRlZmluZWQgfHwgcHJldiA9PT0gMFxuICAgICAgICAgID8gXCJcIlxuICAgICAgICAgIDogYCR7cHJpY2UgPj0gcHJldiA/IFwiK1wiIDogXCJcIn0keygoKHByaWNlIC0gcHJldikgLyBwcmV2KSAqIDEwMCkudG9GaXhlZCgyKX0lYDtcblxuICAgICAgcm93LmlubmVySFRNTCA9IGBcbiAgICAgICAgPHNwYW4gY2xhc3M9XCJmb250LW1vbm8gZm9udC1ib2xkIHRleHQtc20gdGlja2VyLXN5bWJvbFwiPiR7ZXNjYXBlSHRtbChjLnRpY2tlcil9PC9zcGFuPlxuICAgICAgICA8c3BhbiBjbGFzcz1cInRleHQteHMgdGV4dC1ncmF5LTUwMCB0cnVuY2F0ZSB0aWNrZXItbmFtZVwiPiR7ZXNjYXBlSHRtbChjLm5hbWUgfHwgXCJcIil9PC9zcGFuPlxuICAgICAgICA8ZGl2IGNsYXNzPVwidGV4dC1yaWdodFwiPlxuICAgICAgICAgIDxkaXYgY2xhc3M9XCJmb250LW1vbm8gZm9udC1zZW1pYm9sZCB0ZXh0LXNtIHRpY2tlci1wcmljZVwiIHN0eWxlPVwiY29sb3I6JHtpc1VwID8gXCIjMTBiOTgxXCIgOiBcIiNlZjQ0NDRcIn1cIj4kJHtwcmljZS50b0ZpeGVkKDIpfTwvZGl2PlxuICAgICAgICAgIDxkaXYgY2xhc3M9XCJ0ZXh0LXhzIGZvbnQtbW9ubyB0aWNrZXItY2hhbmdlIG10LTAuNVwiIHN0eWxlPVwiY29sb3I6JHtpc1VwID8gXCIjMTBiOTgxXCIgOiBcIiNlZjQ0NDRcIn1cIj4ke3BjdH08L2Rpdj5cbiAgICAgICAgPC9kaXY+XG4gICAgICAgIDxkaXYgaWQ9XCIke2NoYXJ0SWR9XCIgY2xhc3M9XCJmbGV4IGl0ZW1zLWNlbnRlciBqdXN0aWZ5LWVuZFwiIHN0eWxlPVwiaGVpZ2h0OjI4cHg7d2lkdGg6ODBweFwiPjwvZGl2PlxuICAgICAgICA8ZGl2PjwvZGl2PlxuICAgICAgYDtcblxuICAgICAgcm93LmFkZEV2ZW50TGlzdGVuZXIoXCJjbGlja1wiLCAoKSA9PiB7XG4gICAgICAgIHRoaXMuc2VsZWN0ZWRUaWNrZXIgPSBjLnRpY2tlcjtcbiAgICAgICAgdGhpcy5fcmVuZGVyTWFya2V0KHRoaXMuY29tcGFuaWVzLCByb3VuZCk7XG4gICAgICAgIHRoaXMuX3JlbmRlclN0b2NrRGV0YWlsKGMudGlja2VyKTtcbiAgICAgIH0pO1xuXG4gICAgICBsaXN0LmFwcGVuZENoaWxkKHJvdyk7XG5cbiAgICAgIHJlcXVlc3RBbmltYXRpb25GcmFtZSgoKSA9PiB7XG4gICAgICAgIHRoaXMuc3BhcmtsaW5lQ2hhcnRzW2MudGlja2VyXSA9IG5ldyBTcGFya2xpbmVDaGFydChjaGFydElkLCB7XG4gICAgICAgICAgd2lkdGg6IDgwLFxuICAgICAgICAgIGhlaWdodDogMjgsXG4gICAgICAgIH0pO1xuICAgICAgICB0aGlzLnNwYXJrbGluZUNoYXJ0c1tjLnRpY2tlcl0ucmVzZXQoXG4gICAgICAgICAgdGhpcy5wcmljZUhpc3RvcnlbYy50aWNrZXJdLm1hcCgocCkgPT4gcC5wcmljZSksXG4gICAgICAgICk7XG4gICAgICAgIHRoaXMuc3BhcmtsaW5lQ2hhcnRzW2MudGlja2VyXS5zZXRDb2xvcihpc1VwKTtcbiAgICAgIH0pO1xuICAgIH0pO1xuXG4gICAgaWYgKCF0aGlzLnNlbGVjdGVkVGlja2VyICYmIGNvbXBhbmllc1swXSkge1xuICAgICAgdGhpcy5zZWxlY3RlZFRpY2tlciA9IGNvbXBhbmllc1swXS50aWNrZXI7XG4gICAgfVxuXG4gICAgaWYgKHRoaXMuc2VsZWN0ZWRUaWNrZXIpIHtcbiAgICAgIHRoaXMuX3JlbmRlclN0b2NrRGV0YWlsKHRoaXMuc2VsZWN0ZWRUaWNrZXIpO1xuICAgIH1cbiAgfVxuXG4gIF9yZW5kZXJTdG9ja0RldGFpbCh0aWNrZXIpIHtcbiAgICBjb25zdCBjb21wYW55ID0gdGhpcy5jb21wYW5pZXMuZmluZCgoYykgPT4gYy50aWNrZXIgPT09IHRpY2tlcik7XG4gICAgY29uc3QgaGlzdG9yeSA9IHRoaXMucHJpY2VIaXN0b3J5W3RpY2tlcl0gfHwgW107XG4gICAgaWYgKCFjb21wYW55KSByZXR1cm47XG5cbiAgICB0aGlzLl9oaWRlKGRvY3VtZW50LmdldEVsZW1lbnRCeUlkKFwic3RvY2stZGV0YWlsLWVtcHR5XCIpKTtcbiAgICB0aGlzLl9zaG93KGRvY3VtZW50LmdldEVsZW1lbnRCeUlkKFwic3RvY2stZGV0YWlsLXBhbmVsXCIpLCBcImJsb2NrXCIpO1xuXG4gICAgY29uc3QgY3VycmVudFByaWNlID0gcGFyc2VGbG9hdChjb21wYW55LnByaWNlIHx8IDApO1xuICAgIGNvbnN0IGZpcnN0UHJpY2UgPSBoaXN0b3J5WzBdPy5wcmljZSA/PyBjdXJyZW50UHJpY2U7XG4gICAgY29uc3QgbGFzdFByaWNlID0gaGlzdG9yeVtoaXN0b3J5Lmxlbmd0aCAtIDFdPy5wcmljZSA/PyBjdXJyZW50UHJpY2U7XG4gICAgY29uc3QgcGN0ID0gKChsYXN0UHJpY2UgLSBmaXJzdFByaWNlKSAvIE1hdGgubWF4KGZpcnN0UHJpY2UsIDAuMDAwMSkpICogMTAwO1xuXG4gICAgY29uc3QgdGl0bGUgPSBkb2N1bWVudC5nZXRFbGVtZW50QnlJZChcInNlbGVjdGVkLXN0b2NrLXRpdGxlXCIpO1xuICAgIGNvbnN0IG1ldGEgPSBkb2N1bWVudC5nZXRFbGVtZW50QnlJZChcInNlbGVjdGVkLXN0b2NrLW1ldGFcIik7XG4gICAgY29uc3QgY3VycmVudCA9IGRvY3VtZW50LmdldEVsZW1lbnRCeUlkKFwiZGV0YWlsLWN1cnJlbnQtcHJpY2VcIik7XG4gICAgY29uc3QgY2hhbmdlID0gZG9jdW1lbnQuZ2V0RWxlbWVudEJ5SWQoXCJkZXRhaWwtcm91bmQtY2hhbmdlXCIpO1xuICAgIGNvbnN0IGhlYXQgPSBkb2N1bWVudC5nZXRFbGVtZW50QnlJZChcImRldGFpbC1yZWctaGVhdFwiKTtcbiAgICBjb25zdCBjb3VudCA9IGRvY3VtZW50LmdldEVsZW1lbnRCeUlkKFwiZGV0YWlsLWhpc3RvcnktY291bnRcIik7XG4gICAgY29uc3QgbGFzdFJvdW5kID0gZG9jdW1lbnQuZ2V0RWxlbWVudEJ5SWQoXCJkZXRhaWwtbGFzdC1yb3VuZFwiKTtcbiAgICBjb25zdCBzdW1tYXJ5ID0gZG9jdW1lbnQuZ2V0RWxlbWVudEJ5SWQoXCJkZXRhaWwtaGlzdG9yeS1zdW1tYXJ5XCIpO1xuXG4gICAgaWYgKHRpdGxlKSB0aXRsZS50ZXh0Q29udGVudCA9IGAke3RpY2tlcn0gXHUwMEI3ICR7Y29tcGFueS5uYW1lfWA7XG4gICAgaWYgKG1ldGEpIG1ldGEudGV4dENvbnRlbnQgPSBgUm91bmQtYnktcm91bmQgbW92ZW1lbnQgZm9yICR7dGlja2VyfWA7XG4gICAgaWYgKGN1cnJlbnQpIGN1cnJlbnQudGV4dENvbnRlbnQgPSBgJCR7Y3VycmVudFByaWNlLnRvRml4ZWQoMil9YDtcbiAgICBpZiAoY2hhbmdlKSB7XG4gICAgICBjaGFuZ2UudGV4dENvbnRlbnQgPSBgJHtwY3QgPj0gMCA/IFwiK1wiIDogXCJcIn0ke3BjdC50b0ZpeGVkKDIpfSVgO1xuICAgICAgY2hhbmdlLnN0eWxlLmNvbG9yID0gcGN0ID49IDAgPyBcIiMxMGI5ODFcIiA6IFwiI2VmNDQ0NFwiO1xuICAgIH1cbiAgICBpZiAoaGVhdCkgaGVhdC50ZXh0Q29udGVudCA9IFN0cmluZyhjb21wYW55LnJlZ3VsYXRvcnlfaGVhdCB8fCAwKTtcbiAgICBpZiAoY291bnQpIGNvdW50LnRleHRDb250ZW50ID0gU3RyaW5nKGhpc3RvcnkubGVuZ3RoKTtcbiAgICBpZiAobGFzdFJvdW5kKVxuICAgICAgbGFzdFJvdW5kLnRleHRDb250ZW50ID0gU3RyaW5nKGhpc3RvcnlbaGlzdG9yeS5sZW5ndGggLSAxXT8ucm91bmQgfHwgMSk7XG4gICAgaWYgKHN1bW1hcnkpIHN1bW1hcnkudGV4dENvbnRlbnQgPSBgJHtoaXN0b3J5Lmxlbmd0aH0gcHJpY2UgcG9pbnRzIHRyYWNrZWRgO1xuXG4gICAgY29uc3QgaGlzdG9yeUxpc3QgPSBkb2N1bWVudC5nZXRFbGVtZW50QnlJZChcInN0b2NrLWhpc3RvcnktbGlzdFwiKTtcbiAgICBpZiAoaGlzdG9yeUxpc3QpIHtcbiAgICAgIGhpc3RvcnlMaXN0LmlubmVySFRNTCA9IFwiXCI7XG5cbiAgICAgIGlmICghaGlzdG9yeS5sZW5ndGgpIHtcbiAgICAgICAgaGlzdG9yeUxpc3QuaW5uZXJIVE1MID0gYDxwIGNsYXNzPVwidGV4dC14cyB0ZXh0LWdyYXktNjAwIGZvbnQtbW9ubyB0ZXh0LWNlbnRlciBweS00XCI+V2FpdGluZyBmb3IgcHJpY2UgaGlzdG9yeVx1MjAyNjwvcD5gO1xuICAgICAgfSBlbHNlIHtcbiAgICAgICAgaGlzdG9yeS5mb3JFYWNoKChpdGVtLCBpZHgpID0+IHtcbiAgICAgICAgICBjb25zdCBwcmV2ID0gaWR4ID4gMCA/IGhpc3RvcnlbaWR4IC0gMV0ucHJpY2UgOiBpdGVtLnByaWNlO1xuICAgICAgICAgIGNvbnN0IGRpciA9XG4gICAgICAgICAgICBpdGVtLnByaWNlID4gcHJldiA/IFwidXBcIiA6IGl0ZW0ucHJpY2UgPCBwcmV2ID8gXCJkb3duXCIgOiBcImZsYXRcIjtcbiAgICAgICAgICBjb25zdCByb3cgPSBkb2N1bWVudC5jcmVhdGVFbGVtZW50KFwiZGl2XCIpO1xuICAgICAgICAgIHJvdy5jbGFzc05hbWUgPSBgaGlzdG9yeS1pdGVtICR7ZGlyfWA7XG4gICAgICAgICAgcm93LmlubmVySFRNTCA9IGBcbiAgICAgICAgICAgIDxkaXYgY2xhc3M9XCJmbGV4IGl0ZW1zLWNlbnRlciBqdXN0aWZ5LWJldHdlZW5cIj5cbiAgICAgICAgICAgICAgPHNwYW4gY2xhc3M9XCJ0ZXh0LXhzIGZvbnQtbW9ubyB0ZXh0LWdyYXktNDAwXCI+Um91bmQgJHtpdGVtLnJvdW5kfTwvc3Bhbj5cbiAgICAgICAgICAgICAgPHNwYW4gY2xhc3M9XCJ0ZXh0LXhzIGZvbnQtbW9ubyAke1xuICAgICAgICAgICAgICAgIGRpciA9PT0gXCJ1cFwiXG4gICAgICAgICAgICAgICAgICA/IFwidGV4dC1lbWVyYWxkLTQwMFwiXG4gICAgICAgICAgICAgICAgICA6IGRpciA9PT0gXCJkb3duXCJcbiAgICAgICAgICAgICAgICAgICAgPyBcInRleHQtcmVkLTQwMFwiXG4gICAgICAgICAgICAgICAgICAgIDogXCJ0ZXh0LWdyYXktNDAwXCJcbiAgICAgICAgICAgICAgfVwiPiQke2l0ZW0ucHJpY2UudG9GaXhlZCgyKX08L3NwYW4+XG4gICAgICAgICAgICA8L2Rpdj5cbiAgICAgICAgICBgO1xuICAgICAgICAgIGhpc3RvcnlMaXN0LmFwcGVuZENoaWxkKHJvdyk7XG4gICAgICAgIH0pO1xuICAgICAgfVxuICAgIH1cblxuICAgIHRoaXMuZGV0YWlsQ2hhcnQucmVuZGVyKGhpc3RvcnkpO1xuICB9XG5cbiAgX3JlbmRlckV2ZW50cyhldmVudHMpIHtcbiAgICBjb25zdCBib3ggPSBkb2N1bWVudC5nZXRFbGVtZW50QnlJZChcImV2ZW50cy1mZWVkXCIpO1xuICAgIGlmICghYm94KSByZXR1cm47XG5cbiAgICBib3guaW5uZXJIVE1MID0gXCJcIjtcblxuICAgIGlmICghZXZlbnRzLmxlbmd0aCkge1xuICAgICAgYm94LmlubmVySFRNTCA9IGA8cCBjbGFzcz1cInRleHQteHMgdGV4dC1ncmF5LTYwMCBmb250LW1vbm8gdGV4dC1jZW50ZXIgcHktMlwiPk5vIGV2ZW50cyB5ZXQuPC9wPmA7XG4gICAgICByZXR1cm47XG4gICAgfVxuXG4gICAgZXZlbnRzLmZvckVhY2goKGV2KSA9PiB7XG4gICAgICBjb25zdCBpdGVtID0gZG9jdW1lbnQuY3JlYXRlRWxlbWVudChcImRpdlwiKTtcbiAgICAgIGl0ZW0uY2xhc3NOYW1lID0gXCJoaXN0b3J5LWl0ZW1cIjtcblxuICAgICAgaWYgKGV2LnR5cGUgPT09IFwibmV3c19ldmVudFwiKSB7XG4gICAgICAgIGl0ZW0uaW5uZXJIVE1MID0gYFxuICAgICAgICAgIDxkaXYgY2xhc3M9XCJ0ZXh0LXhzIGZvbnQtbW9ubyB0ZXh0LWVtZXJhbGQtNDAwIG1iLTFcIj5ORVdTPC9kaXY+XG4gICAgICAgICAgPGRpdiBjbGFzcz1cInRleHQteHMgdGV4dC1ncmF5LTMwMFwiPiR7ZXNjYXBlSHRtbChldi5oZWFkbGluZSB8fCBcIlwiKX08L2Rpdj5cbiAgICAgICAgYDtcbiAgICAgIH0gZWxzZSBpZiAoZXYudHlwZSA9PT0gXCJwcmljZV91cGRhdGVkXCIpIHtcbiAgICAgICAgaXRlbS5pbm5lckhUTUwgPSBgXG4gICAgICAgICAgPGRpdiBjbGFzcz1cInRleHQteHMgZm9udC1tb25vIHRleHQtY3lhbi00MDAgbWItMVwiPiR7ZXNjYXBlSHRtbChldi50aWNrZXIgfHwgXCJcIil9PC9kaXY+XG4gICAgICAgICAgPGRpdiBjbGFzcz1cInRleHQteHMgdGV4dC1ncmF5LTMwMFwiPlByaWNlIG5vdyAkJHtwYXJzZUZsb2F0KGV2LnByaWNlIHx8IDApLnRvRml4ZWQoMil9PC9kaXY+XG4gICAgICAgIGA7XG4gICAgICB9IGVsc2Uge1xuICAgICAgICBpdGVtLmlubmVySFRNTCA9IGA8ZGl2IGNsYXNzPVwidGV4dC14cyB0ZXh0LWdyYXktMzAwXCI+JHtlc2NhcGVIdG1sKGV2LmhlYWRsaW5lIHx8IEpTT04uc3RyaW5naWZ5KGV2KSl9PC9kaXY+YDtcbiAgICAgIH1cblxuICAgICAgYm94LmFwcGVuZENoaWxkKGl0ZW0pO1xuICAgIH0pO1xuICB9XG5cbiAgX3JlbmRlck5ld3MoZXZlbnRzKSB7XG4gICAgY29uc3QgYm94ID0gZG9jdW1lbnQuZ2V0RWxlbWVudEJ5SWQoXCJuZXdzLWZlZWRcIik7XG4gICAgaWYgKCFib3gpIHJldHVybjtcblxuICAgIGNvbnN0IG5ld3MgPSAoZXZlbnRzIHx8IFtdKS5maWx0ZXIoXG4gICAgICAoZXYpID0+IGV2LnR5cGUgPT09IFwibmV3c19ldmVudFwiIHx8IGV2LnR5cGUgPT09IFwicHJpY2VfdXBkYXRlZFwiLFxuICAgICk7XG4gICAgYm94LmlubmVySFRNTCA9IFwiXCI7XG5cbiAgICBpZiAoIW5ld3MubGVuZ3RoKSB7XG4gICAgICBib3guaW5uZXJIVE1MID0gYDxwIGNsYXNzPVwidGV4dC14cyB0ZXh0LWdyYXktNjAwIGZvbnQtbW9ubyB0ZXh0LWNlbnRlciBweS00XCI+Tm8gbmV3cyB5ZXQuPC9wPmA7XG4gICAgICByZXR1cm47XG4gICAgfVxuXG4gICAgbmV3cy5mb3JFYWNoKChldikgPT4ge1xuICAgICAgY29uc3QgaXRlbSA9IGRvY3VtZW50LmNyZWF0ZUVsZW1lbnQoXCJkaXZcIik7XG4gICAgICBpdGVtLmNsYXNzTmFtZSA9IGBuZXdzLWl0ZW0gJHtcbiAgICAgICAgZXYudHlwZSA9PT0gXCJuZXdzX2V2ZW50XCJcbiAgICAgICAgICA/IChldi5pbXBhY3QgfHwgMCkgPj0gMFxuICAgICAgICAgICAgPyBcInBvc2l0aXZlXCJcbiAgICAgICAgICAgIDogXCJuZWdhdGl2ZVwiXG4gICAgICAgICAgOiBcIm5ldXRyYWxcIlxuICAgICAgfWA7XG5cbiAgICAgIGlmIChldi50eXBlID09PSBcIm5ld3NfZXZlbnRcIikge1xuICAgICAgICBpdGVtLmlubmVySFRNTCA9IGBcbiAgICAgICAgICA8ZGl2IGNsYXNzPVwiZmxleCBpdGVtcy1jZW50ZXIganVzdGlmeS1iZXR3ZWVuIG1iLTFcIj5cbiAgICAgICAgICAgIDxzcGFuIGNsYXNzPVwidGV4dC1bMTBweF0gZm9udC1tb25vIHVwcGVyY2FzZSB0cmFja2luZy13aWRlc3QgdGV4dC1lbWVyYWxkLTQwMFwiPlJvdW5kICR7ZXYucm91bmQgfHwgXCJcdTIwMTRcIn08L3NwYW4+XG4gICAgICAgICAgICA8c3BhbiBjbGFzcz1cInRleHQtWzEwcHhdIGZvbnQtbW9ubyB0ZXh0LWdyYXktNTAwXCI+JHtlc2NhcGVIdG1sKChldi50YXJnZXRzIHx8IFtdKS5qb2luKFwiLCBcIikpfTwvc3Bhbj5cbiAgICAgICAgICA8L2Rpdj5cbiAgICAgICAgICA8ZGl2IGNsYXNzPVwidGV4dC1zbSB0ZXh0LWdyYXktMjAwXCI+JHtlc2NhcGVIdG1sKGV2LmhlYWRsaW5lIHx8IFwiXCIpfTwvZGl2PlxuICAgICAgICBgO1xuICAgICAgfSBlbHNlIHtcbiAgICAgICAgaXRlbS5pbm5lckhUTUwgPSBgXG4gICAgICAgICAgPGRpdiBjbGFzcz1cImZsZXggaXRlbXMtY2VudGVyIGp1c3RpZnktYmV0d2VlblwiPlxuICAgICAgICAgICAgPHNwYW4gY2xhc3M9XCJ0ZXh0LXhzIGZvbnQtbW9ubyB0ZXh0LWN5YW4tNDAwXCI+JHtlc2NhcGVIdG1sKGV2LnRpY2tlciB8fCBcIlwiKX08L3NwYW4+XG4gICAgICAgICAgICA8c3BhbiBjbGFzcz1cInRleHQteHMgZm9udC1tb25vIHRleHQtd2hpdGVcIj4kJHtwYXJzZUZsb2F0KGV2LnByaWNlIHx8IDApLnRvRml4ZWQoMil9PC9zcGFuPlxuICAgICAgICAgIDwvZGl2PlxuICAgICAgICBgO1xuICAgICAgfVxuXG4gICAgICBib3guYXBwZW5kQ2hpbGQoaXRlbSk7XG4gICAgfSk7XG4gIH1cblxuICBfdXBkYXRlQ2hhdEluZGljYXRvcihwaGFzZSkge1xuICAgIGNvbnN0IGVsID0gZG9jdW1lbnQuZ2V0RWxlbWVudEJ5SWQoXCJjaGF0LXBoYXNlLWluZGljYXRvclwiKTtcbiAgICBpZiAoIWVsKSByZXR1cm47XG5cbiAgICBjb25zdCBvcGVuID0gW1wibmVnb3RpYXRpb25cIiwgXCJkaXNjbG9zdXJlXCIsIFwiZmluaXNoZWRcIl0uaW5jbHVkZXMocGhhc2UpO1xuICAgIGVsLnRleHRDb250ZW50ID0gb3BlbiA/IFwiT3BlblwiIDogXCJDbG9zZWRcIjtcbiAgICBlbC5zdHlsZS5jb2xvciA9IG9wZW4gPyBcIiMxMGI5ODFcIiA6IFwiIzZiNzI4MFwiO1xuICB9XG5cbiAgX3Nob3dQaGFzZU92ZXJsYXkocGhhc2UpIHtcbiAgICBjb25zdCBvdmVybGF5ID0gZG9jdW1lbnQuZ2V0RWxlbWVudEJ5SWQoXCJwaGFzZS1vdmVybGF5XCIpO1xuICAgIGNvbnN0IGJhZGdlID0gZG9jdW1lbnQuZ2V0RWxlbWVudEJ5SWQoXCJwaGFzZS1vdmVybGF5LWJhZGdlXCIpO1xuICAgIGNvbnN0IGRlc2MgPSBkb2N1bWVudC5nZXRFbGVtZW50QnlJZChcInBoYXNlLW92ZXJsYXktZGVzY1wiKTtcbiAgICBpZiAoIW92ZXJsYXkgfHwgIWJhZGdlKSByZXR1cm47XG5cbiAgICBjb25zdCBkZXNjcyA9IHtcbiAgICAgIG5ld3M6IFwiTWFya2V0IG5ld3MganVzdCBsYW5kZWQuXCIsXG4gICAgICBhY3Rpb25fc3VibWlzc2lvbjogXCJTdWJtaXQgeW91ciBtb3ZlLlwiLFxuICAgICAgbmVnb3RpYXRpb246IFwiQ2hhdCBpcyBvcGVuLlwiLFxuICAgICAgcmVzb2x1dGlvbjogXCJSZXNvbHZpbmcgYWN0aW9ucy5cIixcbiAgICAgIGRpc2Nsb3N1cmU6IFwiUmVzdWx0cyBwb3N0ZWQuXCIsXG4gICAgICBmaW5pc2hlZDogXCJGaW5hbCByYW5raW5ncyByZWFkeS5cIixcbiAgICB9O1xuXG4gICAgYmFkZ2UudGV4dENvbnRlbnQgPSBwaGFzZUxhYmVsKHBoYXNlKTtcbiAgICBiYWRnZS5jbGFzc05hbWUgPSBgcGhhc2UtYmFkZ2UgcGhhc2UtJHtwaGFzZX0gdGV4dC0yeGwgcHgtOCBweS00IGZvbnQtbW9ubyB0cmFja2luZy13aWRlc3QgdXBwZXJjYXNlYDtcbiAgICBpZiAoZGVzYykgZGVzYy50ZXh0Q29udGVudCA9IGRlc2NzW3BoYXNlXSB8fCBcIlwiO1xuXG4gICAgdGhpcy5fc2hvdyhvdmVybGF5LCBcImZsZXhcIik7XG4gICAgY2xlYXJUaW1lb3V0KHRoaXMucGhhc2VPdmVybGF5VGltZXIpO1xuICAgIGlmIChwaGFzZSAhPT0gXCJmaW5pc2hlZFwiKSB7XG4gICAgICB0aGlzLnBoYXNlT3ZlcmxheVRpbWVyID0gc2V0VGltZW91dCgoKSA9PiB0aGlzLl9oaWRlKG92ZXJsYXkpLCAyNTAwKTtcbiAgICB9XG4gIH1cblxuICAvLyBcdTI1MDBcdTI1MDAgY291bnRkb3duIFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFxuXG4gIF9zdGFydENvdW50ZG93bihkZWFkbGluZSkge1xuICAgIHRoaXMuX2NsZWFyQ291bnRkb3duKCk7XG5cbiAgICBjb25zdCBlbCA9IGRvY3VtZW50LmdldEVsZW1lbnRCeUlkKFwiY291bnRkb3duLXRpbWVyXCIpO1xuICAgIGNvbnN0IHZhbEVsID0gZG9jdW1lbnQuZ2V0RWxlbWVudEJ5SWQoXCJjb3VudGRvd24tdmFsdWVcIik7XG4gICAgaWYgKCFlbCB8fCAhdmFsRWwpIHJldHVybjtcblxuICAgIHRoaXMuX3Nob3coZWwsIFwiZmxleFwiKTtcblxuICAgIGNvbnN0IHRpY2sgPSAoKSA9PiB7XG4gICAgICBjb25zdCBkaWZmID0gTWF0aC5tYXgoXG4gICAgICAgIDAsXG4gICAgICAgIE1hdGguZmxvb3IoKGRlYWRsaW5lLmdldFRpbWUoKSAtIERhdGUubm93KCkpIC8gMTAwMCksXG4gICAgICApO1xuICAgICAgY29uc3QgbSA9IE1hdGguZmxvb3IoZGlmZiAvIDYwKTtcbiAgICAgIGNvbnN0IHMgPSBkaWZmICUgNjA7XG4gICAgICB2YWxFbC50ZXh0Q29udGVudCA9IGAke219OiR7U3RyaW5nKHMpLnBhZFN0YXJ0KDIsIFwiMFwiKX1gO1xuICAgICAgZWwuY2xhc3NMaXN0LnRvZ2dsZShcImNvdW50ZG93bi11cmdlbnRcIiwgZGlmZiA8PSAxMCAmJiBkaWZmID4gMCk7XG4gICAgICBpZiAoZGlmZiA8PSAwKSB0aGlzLl9jbGVhckNvdW50ZG93bigpO1xuICAgIH07XG5cbiAgICB0aWNrKCk7XG4gICAgdGhpcy5jb3VudGRvd24gPSBzZXRJbnRlcnZhbCh0aWNrLCAxMDAwKTtcbiAgfVxuXG4gIF9jbGVhckNvdW50ZG93bigpIHtcbiAgICBpZiAodGhpcy5jb3VudGRvd24pIGNsZWFySW50ZXJ2YWwodGhpcy5jb3VudGRvd24pO1xuICAgIHRoaXMuY291bnRkb3duID0gbnVsbDtcblxuICAgIGNvbnN0IGVsID0gZG9jdW1lbnQuZ2V0RWxlbWVudEJ5SWQoXCJjb3VudGRvd24tdGltZXJcIik7XG4gICAgaWYgKGVsKSB7XG4gICAgICB0aGlzLl9oaWRlKGVsKTtcbiAgICAgIGVsLmNsYXNzTGlzdC5yZW1vdmUoXCJjb3VudGRvd24tdXJnZW50XCIpO1xuICAgIH1cbiAgfVxuXG4gIC8vIFx1MjUwMFx1MjUwMCBsZWFkZXJib2FyZCBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcblxuICBfcmVuZGVyTGVhZGVyYm9hcmQobGVhZGVyYm9hcmQpIHtcbiAgICBjb25zdCBsaXN0ID0gZG9jdW1lbnQuZ2V0RWxlbWVudEJ5SWQoXCJsZWFkZXJib2FyZC1saXN0XCIpO1xuICAgIGlmICghbGlzdCkgcmV0dXJuO1xuXG4gICAgbGlzdC5pbm5lckhUTUwgPSBcIlwiO1xuICAgIGxlYWRlcmJvYXJkLmZvckVhY2goKGVudHJ5LCBpKSA9PiB7XG4gICAgICBjb25zdCBkaXYgPSBkb2N1bWVudC5jcmVhdGVFbGVtZW50KFwiZGl2XCIpO1xuICAgICAgZGl2LmNsYXNzTmFtZSA9IFwiZmxleCBpdGVtcy1jZW50ZXIgZ2FwLTMgcC0zIHJvdW5kZWQtbGcgY2FyZCBmYWRlLWluLXVwXCI7XG5cbiAgICAgIGNvbnN0IG1lZGFscyA9IFtcIlx1RDgzRVx1REQ0N1wiLCBcIlx1RDgzRVx1REQ0OFwiLCBcIlx1RDgzRVx1REQ0OVwiXTtcbiAgICAgIGRpdi5pbm5lckhUTUwgPSBgXG4gICAgICAgIDxzcGFuIGNsYXNzPVwidGV4dC14bCB3LTggdGV4dC1jZW50ZXJcIj4ke21lZGFsc1tpXSB8fCBgIyR7aSArIDF9YH08L3NwYW4+XG4gICAgICAgIDxzcGFuIGNsYXNzPVwiZm9udC1tb25vIGZvbnQtc2VtaWJvbGQgdGV4dC1zbSB0ZXh0LXdoaXRlIGZsZXgtMVwiPiR7ZXNjYXBlSHRtbChlbnRyeS51c2VybmFtZSl9PC9zcGFuPlxuICAgICAgICA8c3BhbiBjbGFzcz1cImZvbnQtbW9ubyB0ZXh0LXNtICR7aSA9PT0gMCA/IFwidGV4dC1lbWVyYWxkLTQwMCBmb250LWJvbGRcIiA6IFwidGV4dC1ncmF5LTMwMFwifVwiPlxuICAgICAgICAgICQke3BhcnNlRmxvYXQoZW50cnkubmV0X3dvcnRoIHx8IDApLnRvTG9jYWxlU3RyaW5nKFwiZW4tVVNcIiwge1xuICAgICAgICAgICAgbWluaW11bUZyYWN0aW9uRGlnaXRzOiAyLFxuICAgICAgICAgICAgbWF4aW11bUZyYWN0aW9uRGlnaXRzOiAyLFxuICAgICAgICAgIH0pfVxuICAgICAgICA8L3NwYW4+XG4gICAgICBgO1xuICAgICAgbGlzdC5hcHBlbmRDaGlsZChkaXYpO1xuICAgIH0pO1xuICB9XG5cbiAgLy8gXHUyNTAwXHUyNTAwIGFjdGlvbiBtb2RhbCBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcblxuICBfb3BlbkFjdGlvbk1vZGFsKGRhdGFzZXQpIHtcbiAgICBjb25zdCBtb2RhbCA9IGRvY3VtZW50LmdldEVsZW1lbnRCeUlkKFwiYWN0aW9uLW1vZGFsXCIpO1xuICAgIGNvbnN0IHRpdGxlRWwgPSBkb2N1bWVudC5nZXRFbGVtZW50QnlJZChcIm1vZGFsLXRpdGxlXCIpO1xuICAgIGNvbnN0IGRlc2NFbCA9IGRvY3VtZW50LmdldEVsZW1lbnRCeUlkKFwibW9kYWwtZGVzY1wiKTtcbiAgICBjb25zdCBlcnJFbCA9IGRvY3VtZW50LmdldEVsZW1lbnRCeUlkKFwibW9kYWwtZXJyb3JcIik7XG4gICAgY29uc3QgdGlja2VyRGl2ID0gZG9jdW1lbnQuZ2V0RWxlbWVudEJ5SWQoXCJmaWVsZC10aWNrZXJcIik7XG4gICAgY29uc3QgcXR5RGl2ID0gZG9jdW1lbnQuZ2V0RWxlbWVudEJ5SWQoXCJmaWVsZC1xdWFudGl0eVwiKTtcbiAgICBjb25zdCB0YXJnZXREaXYgPSBkb2N1bWVudC5nZXRFbGVtZW50QnlJZChcImZpZWxkLXRhcmdldFwiKTtcbiAgICBjb25zdCBjb3N0RGl2ID0gZG9jdW1lbnQuZ2V0RWxlbWVudEJ5SWQoXCJjb3N0LWVzdGltYXRlXCIpO1xuICAgIGNvbnN0IHRpY2tlckJ0bnMgPSBkb2N1bWVudC5nZXRFbGVtZW50QnlJZChcInRpY2tlci1idXR0b25zXCIpO1xuICAgIGNvbnN0IHRhcmdldEJ0bnMgPSBkb2N1bWVudC5nZXRFbGVtZW50QnlJZChcInRhcmdldC1idXR0b25zXCIpO1xuICAgIGNvbnN0IHF0eUlucHV0ID0gZG9jdW1lbnQuZ2V0RWxlbWVudEJ5SWQoXCJpbnB1dC1xdWFudGl0eVwiKTtcblxuICAgIGlmICghbW9kYWwpIHJldHVybjtcblxuICAgIGlmICh0aXRsZUVsKSB0aXRsZUVsLnRleHRDb250ZW50ID0gZGF0YXNldC5sYWJlbCB8fCBkYXRhc2V0LmFjdGlvbiB8fCBcIlwiO1xuICAgIGlmIChkZXNjRWwpIGRlc2NFbC50ZXh0Q29udGVudCA9IGRhdGFzZXQuZGVzYyB8fCBcIlwiO1xuICAgIGlmIChlcnJFbCkge1xuICAgICAgZXJyRWwudGV4dENvbnRlbnQgPSBcIlwiO1xuICAgICAgdGhpcy5faGlkZShlcnJFbCk7XG4gICAgfVxuXG4gICAgY29uc3QgbmVlZHNUaWNrZXIgPSBkYXRhc2V0Lm5lZWRzVGlja2VyID09PSBcInRydWVcIjtcbiAgICBjb25zdCBuZWVkc1F0eSA9IGRhdGFzZXQubmVlZHNRdWFudGl0eSA9PT0gXCJ0cnVlXCI7XG4gICAgY29uc3QgbmVlZHNUYXJnZXQgPSBkYXRhc2V0Lm5lZWRzVGFyZ2V0ID09PSBcInRydWVcIjtcblxuICAgIG5lZWRzVGlja2VyID8gdGhpcy5fc2hvdyh0aWNrZXJEaXYsIFwiYmxvY2tcIikgOiB0aGlzLl9oaWRlKHRpY2tlckRpdik7XG4gICAgbmVlZHNRdHkgPyB0aGlzLl9zaG93KHF0eURpdiwgXCJibG9ja1wiKSA6IHRoaXMuX2hpZGUocXR5RGl2KTtcbiAgICBuZWVkc1RhcmdldCA/IHRoaXMuX3Nob3codGFyZ2V0RGl2LCBcImJsb2NrXCIpIDogdGhpcy5faGlkZSh0YXJnZXREaXYpO1xuICAgIHRoaXMuX2hpZGUoY29zdERpdik7XG5cbiAgICBpZiAobmVlZHNUaWNrZXIgJiYgdGlja2VyQnRucykge1xuICAgICAgdGlja2VyQnRucy5pbm5lckhUTUwgPSBcIlwiO1xuICAgICAgdGhpcy5jb21wYW5pZXMuZm9yRWFjaCgoYykgPT4ge1xuICAgICAgICBjb25zdCBidG4gPSBkb2N1bWVudC5jcmVhdGVFbGVtZW50KFwiYnV0dG9uXCIpO1xuICAgICAgICBidG4udHlwZSA9IFwiYnV0dG9uXCI7XG4gICAgICAgIGJ0bi5jbGFzc05hbWUgPSBcInRpY2tlci1waWxsXCI7XG4gICAgICAgIGJ0bi5kYXRhc2V0LnRpY2tlciA9IGMudGlja2VyO1xuICAgICAgICBidG4uaW5uZXJIVE1MID0gYDxzcGFuIGNsYXNzPVwiZm9udC1ib2xkXCI+JHtlc2NhcGVIdG1sKGMudGlja2VyKX08L3NwYW4+XG4gICAgICAgICAgICAgICAgICAgICAgICAgPHNwYW4gY2xhc3M9XCJ0ZXh0LWdyYXktNTAwIHRleHQteHMgbWwtMVwiPiQke3BhcnNlRmxvYXQoYy5wcmljZSkudG9GaXhlZCgyKX08L3NwYW4+YDtcbiAgICAgICAgYnRuLmFkZEV2ZW50TGlzdGVuZXIoXCJjbGlja1wiLCAoZSkgPT4ge1xuICAgICAgICAgIGUucHJldmVudERlZmF1bHQoKTtcbiAgICAgICAgICB0aWNrZXJCdG5zXG4gICAgICAgICAgICAucXVlcnlTZWxlY3RvckFsbChcIi50aWNrZXItcGlsbFwiKVxuICAgICAgICAgICAgLmZvckVhY2goKGIpID0+IGIuY2xhc3NMaXN0LnJlbW92ZShcInNlbGVjdGVkXCIpKTtcbiAgICAgICAgICBidG4uY2xhc3NMaXN0LmFkZChcInNlbGVjdGVkXCIpO1xuICAgICAgICAgIGlmIChuZWVkc1F0eSlcbiAgICAgICAgICAgIHRoaXMuX3VwZGF0ZUNvc3RFc3RpbWF0ZShkYXRhc2V0LmFjdGlvbiwgYy50aWNrZXIsIHF0eUlucHV0Py52YWx1ZSk7XG4gICAgICAgIH0pO1xuICAgICAgICB0aWNrZXJCdG5zLmFwcGVuZENoaWxkKGJ0bik7XG4gICAgICB9KTtcbiAgICB9XG5cbiAgICBpZiAobmVlZHNUYXJnZXQgJiYgdGFyZ2V0QnRucykge1xuICAgICAgdGFyZ2V0QnRucy5pbm5lckhUTUwgPSBcIlwiO1xuICAgICAgdGhpcy5wbGF5ZXJzLmZvckVhY2goKHApID0+IHtcbiAgICAgICAgaWYgKFN0cmluZyhwLnVzZXJfaWQpID09PSBTdHJpbmcodGhpcy5jZmcudXNlcklkKSkgcmV0dXJuO1xuXG4gICAgICAgIGNvbnN0IGJ0biA9IGRvY3VtZW50LmNyZWF0ZUVsZW1lbnQoXCJidXR0b25cIik7XG4gICAgICAgIGJ0bi50eXBlID0gXCJidXR0b25cIjtcbiAgICAgICAgYnRuLmNsYXNzTmFtZSA9IFwicGxheWVyLXBpbGxcIjtcbiAgICAgICAgYnRuLmRhdGFzZXQudGFyZ2V0SWQgPSBwLnVzZXJfaWQ7XG4gICAgICAgIGJ0bi5pbm5lckhUTUwgPSBgXG4gICAgICAgICAgPGRpdiBjbGFzcz1cInctMiBoLTIgcm91bmRlZC1mdWxsICR7cC5saXF1aWRpdHlfZnJvemVuID8gXCJiZy1ibHVlLTQwMFwiIDogXCJiZy1lbWVyYWxkLTQwMFwifVwiPjwvZGl2PlxuICAgICAgICAgIDxzcGFuIGNsYXNzPVwiZm9udC1tb25vIHRleHQtc20gdGV4dC1ncmF5LTIwMCBmbGV4LTFcIj4ke2VzY2FwZUh0bWwocC51c2VybmFtZSl9PC9zcGFuPlxuICAgICAgICAgICR7cC5saXF1aWRpdHlfZnJvemVuID8gYDxzcGFuIGNsYXNzPVwidGV4dC14cyB0ZXh0LWJsdWUtNDAwIGZvbnQtbW9ub1wiPlx1Mjc0NCBmcm96ZW48L3NwYW4+YCA6IFwiXCJ9XG4gICAgICAgICAgJHtwLnJlZ3VsYXRvcnlfaGVhdCA+IDAgPyBgPHNwYW4gY2xhc3M9XCJ0ZXh0LXhzIHRleHQtYW1iZXItNDAwIGZvbnQtbW9ub1wiPlx1MjY5NiAke3AucmVndWxhdG9yeV9oZWF0fTwvc3Bhbj5gIDogXCJcIn1cbiAgICAgICAgYDtcbiAgICAgICAgYnRuLmFkZEV2ZW50TGlzdGVuZXIoXCJjbGlja1wiLCAoZSkgPT4ge1xuICAgICAgICAgIGUucHJldmVudERlZmF1bHQoKTtcbiAgICAgICAgICB0YXJnZXRCdG5zXG4gICAgICAgICAgICAucXVlcnlTZWxlY3RvckFsbChcIi5wbGF5ZXItcGlsbFwiKVxuICAgICAgICAgICAgLmZvckVhY2goKGIpID0+IGIuY2xhc3NMaXN0LnJlbW92ZShcInNlbGVjdGVkXCIpKTtcbiAgICAgICAgICBidG4uY2xhc3NMaXN0LmFkZChcInNlbGVjdGVkXCIpO1xuICAgICAgICB9KTtcbiAgICAgICAgdGFyZ2V0QnRucy5hcHBlbmRDaGlsZChidG4pO1xuICAgICAgfSk7XG4gICAgfVxuXG4gICAgaWYgKHF0eUlucHV0KSB7XG4gICAgICBxdHlJbnB1dC52YWx1ZSA9IFwiXCI7XG4gICAgICBxdHlJbnB1dC5vbmlucHV0ID0gKCkgPT4ge1xuICAgICAgICBjb25zdCB0aWNrZXIgPSB0aWNrZXJCdG5zPy5xdWVyeVNlbGVjdG9yKFwiLnNlbGVjdGVkXCIpPy5kYXRhc2V0LnRpY2tlcjtcbiAgICAgICAgdGhpcy5fdXBkYXRlQ29zdEVzdGltYXRlKGRhdGFzZXQuYWN0aW9uLCB0aWNrZXIsIHF0eUlucHV0LnZhbHVlKTtcbiAgICAgIH07XG4gICAgfVxuXG4gICAgY29uc3QgZm9ybSA9IGRvY3VtZW50LmdldEVsZW1lbnRCeUlkKFwiYWN0aW9uLWZvcm1cIik7XG4gICAgaWYgKGZvcm0pIGZvcm0uZGF0YXNldC5hY3Rpb25UeXBlID0gZGF0YXNldC5hY3Rpb247XG5cbiAgICB0aGlzLl9zaG93KG1vZGFsLCBcImZsZXhcIik7XG4gIH1cblxuICBfY2xvc2VNb2RhbCgpIHtcbiAgICB0aGlzLl9oaWRlKGRvY3VtZW50LmdldEVsZW1lbnRCeUlkKFwiYWN0aW9uLW1vZGFsXCIpKTtcbiAgICB0aGlzLl9oaWRlKGRvY3VtZW50LmdldEVsZW1lbnRCeUlkKFwibW9kYWwtZXJyb3JcIikpO1xuICB9XG5cbiAgX3VwZGF0ZUNvc3RFc3RpbWF0ZShhY3Rpb24sIHRpY2tlciwgcXR5KSB7XG4gICAgY29uc3QgY29zdERpdiA9IGRvY3VtZW50LmdldEVsZW1lbnRCeUlkKFwiY29zdC1lc3RpbWF0ZVwiKTtcbiAgICBjb25zdCBjb3N0VmFsRWwgPSBkb2N1bWVudC5nZXRFbGVtZW50QnlJZChcImNvc3QtdmFsdWVcIik7XG4gICAgY29uc3QgY2FzaFJlbUVsID0gZG9jdW1lbnQuZ2V0RWxlbWVudEJ5SWQoXCJjYXNoLXJlbWFpbmluZ1wiKTtcblxuICAgIGlmIChcbiAgICAgICFjb3N0RGl2IHx8XG4gICAgICAhdGlja2VyIHx8XG4gICAgICAhcXR5IHx8XG4gICAgICAhW1wiYnV5XCIsIFwic2hvcnRcIiwgXCJhY3F1aXJlX3N0YWtlXCJdLmluY2x1ZGVzKGFjdGlvbilcbiAgICApIHtcbiAgICAgIHRoaXMuX2hpZGUoY29zdERpdik7XG4gICAgICByZXR1cm47XG4gICAgfVxuXG4gICAgY29uc3QgY29tcGFueSA9IHRoaXMuY29tcGFuaWVzLmZpbmQoKGMpID0+IGMudGlja2VyID09PSB0aWNrZXIpO1xuICAgIGlmICghY29tcGFueSkge1xuICAgICAgdGhpcy5faGlkZShjb3N0RGl2KTtcbiAgICAgIHJldHVybjtcbiAgICB9XG5cbiAgICBjb25zdCBwcmljZSA9IHBhcnNlRmxvYXQoY29tcGFueS5wcmljZSk7XG4gICAgY29uc3QgcSA9IHBhcnNlSW50KHF0eSwgMTApIHx8IDA7XG4gICAgY29uc3QgdG90YWwgPSBwcmljZSAqIHE7XG4gICAgY29uc3QgbXlDYXNoID0gdGhpcy5teVN0YXRlID8gcGFyc2VGbG9hdCh0aGlzLm15U3RhdGUuY2FzaCkgOiAwO1xuXG4gICAgdGhpcy5fc2hvdyhjb3N0RGl2LCBcImJsb2NrXCIpO1xuXG4gICAgaWYgKGNvc3RWYWxFbCkge1xuICAgICAgY29zdFZhbEVsLnRleHRDb250ZW50ID0gYCQke3RvdGFsLnRvTG9jYWxlU3RyaW5nKFwiZW4tVVNcIiwge1xuICAgICAgICBtaW5pbXVtRnJhY3Rpb25EaWdpdHM6IDIsXG4gICAgICAgIG1heGltdW1GcmFjdGlvbkRpZ2l0czogMixcbiAgICAgIH0pfWA7XG4gICAgfVxuXG4gICAgaWYgKGNhc2hSZW1FbCkge1xuICAgICAgY2FzaFJlbUVsLnRleHRDb250ZW50ID0gYCQke01hdGgubWF4KDAsIG15Q2FzaCAtIHRvdGFsKS50b0xvY2FsZVN0cmluZyhcbiAgICAgICAgXCJlbi1VU1wiLFxuICAgICAgICB7XG4gICAgICAgICAgbWluaW11bUZyYWN0aW9uRGlnaXRzOiAyLFxuICAgICAgICAgIG1heGltdW1GcmFjdGlvbkRpZ2l0czogMixcbiAgICAgICAgfSxcbiAgICAgICl9YDtcbiAgICAgIGNhc2hSZW1FbC5zdHlsZS5jb2xvciA9IG15Q2FzaCAtIHRvdGFsIDwgMCA/IFwiI2VmNDQ0NFwiIDogXCIjZDFkNWRiXCI7XG4gICAgfVxuICB9XG5cbiAgX3N1Ym1pdEFjdGlvbigpIHtcbiAgICBjb25zdCBmb3JtID0gZG9jdW1lbnQuZ2V0RWxlbWVudEJ5SWQoXCJhY3Rpb24tZm9ybVwiKTtcbiAgICBjb25zdCBzdWJtaXRCdG4gPSBkb2N1bWVudC5nZXRFbGVtZW50QnlJZChcIm1vZGFsLXN1Ym1pdFwiKTtcbiAgICBpZiAoIWZvcm0pIHJldHVybjtcblxuICAgIGNvbnN0IGFjdGlvblR5cGUgPSBmb3JtLmRhdGFzZXQuYWN0aW9uVHlwZTtcbiAgICBpZiAoIWFjdGlvblR5cGUpIHJldHVybjtcblxuICAgIGNvbnN0IHBhcmFtcyA9IHt9O1xuXG4gICAgY29uc3Qgc2VsZWN0ZWRUaWNrZXIgPSBkb2N1bWVudC5xdWVyeVNlbGVjdG9yKFxuICAgICAgXCIjdGlja2VyLWJ1dHRvbnMgLnRpY2tlci1waWxsLnNlbGVjdGVkXCIsXG4gICAgKTtcbiAgICBpZiAoc2VsZWN0ZWRUaWNrZXIpIHBhcmFtcy50aWNrZXIgPSBzZWxlY3RlZFRpY2tlci5kYXRhc2V0LnRpY2tlcjtcblxuICAgIGNvbnN0IHF0eUlucHV0ID0gZG9jdW1lbnQuZ2V0RWxlbWVudEJ5SWQoXCJpbnB1dC1xdWFudGl0eVwiKTtcbiAgICBjb25zdCBxdHlQYXJlbnQgPSBkb2N1bWVudC5nZXRFbGVtZW50QnlJZChcImZpZWxkLXF1YW50aXR5XCIpO1xuICAgIGNvbnN0IHF0eVZpc2libGUgPSBxdHlQYXJlbnQgJiYgcXR5UGFyZW50LnN0eWxlLmRpc3BsYXkgIT09IFwibm9uZVwiO1xuICAgIGlmIChxdHlWaXNpYmxlICYmIHF0eUlucHV0KSB7XG4gICAgICBjb25zdCBxID0gcGFyc2VJbnQocXR5SW5wdXQudmFsdWUsIDEwKTtcbiAgICAgIGlmICghcSB8fCBxIDw9IDApIHtcbiAgICAgICAgdGhpcy5fc2hvd01vZGFsRXJyb3IoXCJQbGVhc2UgZW50ZXIgYSB2YWxpZCBxdWFudGl0eS5cIik7XG4gICAgICAgIHJldHVybjtcbiAgICAgIH1cbiAgICAgIHBhcmFtcy5xdWFudGl0eSA9IHE7XG4gICAgfVxuXG4gICAgY29uc3Qgc2VsZWN0ZWRUYXJnZXQgPSBkb2N1bWVudC5xdWVyeVNlbGVjdG9yKFxuICAgICAgXCIjdGFyZ2V0LWJ1dHRvbnMgLnBsYXllci1waWxsLnNlbGVjdGVkXCIsXG4gICAgKTtcbiAgICBpZiAoc2VsZWN0ZWRUYXJnZXQpIHBhcmFtcy50YXJnZXRfdXNlcl9pZCA9IHNlbGVjdGVkVGFyZ2V0LmRhdGFzZXQudGFyZ2V0SWQ7XG5cbiAgICBjb25zdCB0aWNrZXJQYXJlbnQgPSBkb2N1bWVudC5nZXRFbGVtZW50QnlJZChcImZpZWxkLXRpY2tlclwiKTtcbiAgICBpZiAoXG4gICAgICB0aWNrZXJQYXJlbnQgJiZcbiAgICAgIHRpY2tlclBhcmVudC5zdHlsZS5kaXNwbGF5ICE9PSBcIm5vbmVcIiAmJlxuICAgICAgIXBhcmFtcy50aWNrZXJcbiAgICApIHtcbiAgICAgIHRoaXMuX3Nob3dNb2RhbEVycm9yKFwiUGxlYXNlIHNlbGVjdCBhIGNvbXBhbnkuXCIpO1xuICAgICAgcmV0dXJuO1xuICAgIH1cblxuICAgIGNvbnN0IHRhcmdldFBhcmVudCA9IGRvY3VtZW50LmdldEVsZW1lbnRCeUlkKFwiZmllbGQtdGFyZ2V0XCIpO1xuICAgIGlmIChcbiAgICAgIHRhcmdldFBhcmVudCAmJlxuICAgICAgdGFyZ2V0UGFyZW50LnN0eWxlLmRpc3BsYXkgIT09IFwibm9uZVwiICYmXG4gICAgICAhcGFyYW1zLnRhcmdldF91c2VyX2lkXG4gICAgKSB7XG4gICAgICB0aGlzLl9zaG93TW9kYWxFcnJvcihcIlBsZWFzZSBzZWxlY3QgYSB0YXJnZXQgcGxheWVyLlwiKTtcbiAgICAgIHJldHVybjtcbiAgICB9XG5cbiAgICBzdWJtaXRCdG4uZGlzYWJsZWQgPSB0cnVlO1xuICAgIHN1Ym1pdEJ0bi50ZXh0Q29udGVudCA9IFwiU3VibWl0dGluZy4uLlwiO1xuXG4gICAgdGhpcy5jaGFubmVsXG4gICAgICAucHVzaChcInN1Ym1pdF9hY3Rpb25cIiwgeyBhY3Rpb25fdHlwZTogYWN0aW9uVHlwZSwgcGFyYW1zIH0pXG4gICAgICAucmVjZWl2ZShcIm9rXCIsICgpID0+IHtcbiAgICAgICAgdGhpcy5fY2xvc2VNb2RhbCgpO1xuICAgICAgICBzdWJtaXRCdG4uZGlzYWJsZWQgPSBmYWxzZTtcbiAgICAgICAgc3VibWl0QnRuLnRleHRDb250ZW50ID0gXCJDb25maXJtIEFjdGlvblwiO1xuXG4gICAgICAgIGRvY3VtZW50LnF1ZXJ5U2VsZWN0b3JBbGwoXCIuYWN0aW9uLWJ0blwiKS5mb3JFYWNoKChiKSA9PiB7XG4gICAgICAgICAgYi5kaXNhYmxlZCA9IHRydWU7XG4gICAgICAgICAgYi5jbGFzc0xpc3QuYWRkKFwic3VibWl0dGVkXCIpO1xuICAgICAgICB9KTtcblxuICAgICAgICB0aGlzLl9zaG93KGRvY3VtZW50LmdldEVsZW1lbnRCeUlkKFwiYWN0aW9uLXN1Ym1pdHRlZC1iYWRnZVwiKSwgXCJmbGV4XCIpO1xuICAgICAgICBUb2FzdC5zaG93KFwiQWN0aW9uIHN1Ym1pdHRlZCEgV2FpdGluZyBmb3Igb3RoZXIgcGxheWVycy5cIiwgXCJzdWNjZXNzXCIpO1xuICAgICAgfSlcbiAgICAgIC5yZWNlaXZlKFwiZXJyb3JcIiwgKGUpID0+IHtcbiAgICAgICAgc3VibWl0QnRuLmRpc2FibGVkID0gZmFsc2U7XG4gICAgICAgIHN1Ym1pdEJ0bi50ZXh0Q29udGVudCA9IFwiQ29uZmlybSBBY3Rpb25cIjtcbiAgICAgICAgY29uc3QgcmVhc29uID1cbiAgICAgICAgICB0eXBlb2YgZS5yZWFzb24gPT09IFwic3RyaW5nXCIgPyBlLnJlYXNvbiA6IEpTT04uc3RyaW5naWZ5KGUucmVhc29uKTtcbiAgICAgICAgdGhpcy5fc2hvd01vZGFsRXJyb3IoXCJSZWplY3RlZDogXCIgKyByZWFzb24pO1xuICAgICAgfSk7XG4gIH1cblxuICBfc2hvd01vZGFsRXJyb3IobXNnKSB7XG4gICAgY29uc3QgZWwgPSBkb2N1bWVudC5nZXRFbGVtZW50QnlJZChcIm1vZGFsLWVycm9yXCIpO1xuICAgIGlmICghZWwpIHJldHVybjtcbiAgICBlbC50ZXh0Q29udGVudCA9IG1zZztcbiAgICB0aGlzLl9zaG93KGVsLCBcImJsb2NrXCIpO1xuICB9XG5cbiAgX3Nob3coZWwsIGRpc3BsYXkgPSBcImJsb2NrXCIpIHtcbiAgICBpZiAoIWVsKSByZXR1cm47XG4gICAgZWwuY2xhc3NMaXN0LnJlbW92ZShcImhpZGRlblwiKTtcbiAgICBlbC5zdHlsZS5kaXNwbGF5ID0gZGlzcGxheTtcbiAgfVxuXG4gIF9oaWRlKGVsKSB7XG4gICAgaWYgKCFlbCkgcmV0dXJuO1xuICAgIGVsLmNsYXNzTGlzdC5hZGQoXCJoaWRkZW5cIik7XG4gICAgZWwuc3R5bGUuZGlzcGxheSA9IFwibm9uZVwiO1xuICB9XG5cbiAgLy8gXHUyNTAwXHUyNTAwIGNoYXQgXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXG5cbiAgX3NlbmRDaGF0TWVzc2FnZSgpIHtcbiAgICBjb25zdCBpbnB1dCA9IGRvY3VtZW50LmdldEVsZW1lbnRCeUlkKFwiY2hhdC1pbnB1dFwiKTtcbiAgICBpZiAoIWlucHV0KSByZXR1cm47XG5cbiAgICBjb25zdCBjb250ZW50ID0gaW5wdXQudmFsdWUudHJpbSgpO1xuICAgIGlmICghY29udGVudCkgcmV0dXJuO1xuXG4gICAgdGhpcy5jaGFubmVsXG4gICAgICAucHVzaChcInNlbmRfbWVzc2FnZVwiLCB7IGNvbnRlbnQgfSlcbiAgICAgIC5yZWNlaXZlKFwib2tcIiwgKCkgPT4ge1xuICAgICAgICBpbnB1dC52YWx1ZSA9IFwiXCI7XG4gICAgICB9KVxuICAgICAgLnJlY2VpdmUoXCJlcnJvclwiLCAoZSkgPT4ge1xuICAgICAgICBUb2FzdC5zaG93KGBNZXNzYWdlIHJlamVjdGVkOiAke0pTT04uc3RyaW5naWZ5KGUucmVhc29uKX1gLCBcImVycm9yXCIpO1xuICAgICAgfSk7XG4gIH1cblxuICBfYXBwZW5kQ2hhdChtc2cpIHtcbiAgICBjb25zdCBjb250YWluZXIgPSBkb2N1bWVudC5nZXRFbGVtZW50QnlJZChcImNoYXQtbWVzc2FnZXNcIik7XG4gICAgaWYgKCFjb250YWluZXIpIHJldHVybjtcblxuICAgIGNvbnN0IHBsYWNlaG9sZGVyID0gY29udGFpbmVyLnF1ZXJ5U2VsZWN0b3IoXCJwXCIpO1xuICAgIGlmIChwbGFjZWhvbGRlciAmJiBwbGFjZWhvbGRlci5jbGFzc0xpc3QuY29udGFpbnMoXCJ0ZXh0LWNlbnRlclwiKSkge1xuICAgICAgcGxhY2Vob2xkZXIucmVtb3ZlKCk7XG4gICAgfVxuXG4gICAgY29uc3QgZWwgPSBkb2N1bWVudC5jcmVhdGVFbGVtZW50KFwiZGl2XCIpO1xuICAgIGVsLmNsYXNzTmFtZSA9IFwiZmxleCBnYXAtMS41IGZhZGUtaW4tdXBcIjtcblxuICAgIGNvbnN0IHRpbWUgPSBtc2cuaW5zZXJ0ZWRfYXRcbiAgICAgID8gbmV3IERhdGUobXNnLmluc2VydGVkX2F0KS50b0xvY2FsZVRpbWVTdHJpbmcoW10sIHtcbiAgICAgICAgICBob3VyOiBcIjItZGlnaXRcIixcbiAgICAgICAgICBtaW51dGU6IFwiMi1kaWdpdFwiLFxuICAgICAgICB9KVxuICAgICAgOiBcIlwiO1xuXG4gICAgZWwuaW5uZXJIVE1MID0gYFxuICAgICAgPHNwYW4gY2xhc3M9XCJ0ZXh0LWdyYXktNjAwIGZvbnQtbW9ubyB0ZXh0LXhzIHNocmluay0wIG10LTAuNVwiPiR7dGltZX08L3NwYW4+XG4gICAgICA8ZGl2IGNsYXNzPVwibWluLXctMFwiPlxuICAgICAgICA8c3BhbiBjbGFzcz1cImZvbnQtbW9ubyB0ZXh0LXhzIGZvbnQtc2VtaWJvbGQgdGV4dC1lbWVyYWxkLTQwMFwiPiR7ZXNjYXBlSHRtbChtc2cudXNlcm5hbWUgfHwgXCI/XCIpfTo8L3NwYW4+XG4gICAgICAgIDxzcGFuIGNsYXNzPVwidGV4dC1ncmF5LTMwMCB0ZXh0LXhzIG1sLTEgYnJlYWstd29yZHNcIj4ke2VzY2FwZUh0bWwobXNnLmNvbnRlbnQpfTwvc3Bhbj5cbiAgICAgIDwvZGl2PlxuICAgIGA7XG5cbiAgICBjb250YWluZXIuYXBwZW5kQ2hpbGQoZWwpO1xuICAgIGNvbnRhaW5lci5zY3JvbGxUb3AgPSBjb250YWluZXIuc2Nyb2xsSGVpZ2h0O1xuICB9XG59XG5cbi8vIFx1MjUwMFx1MjUwMFx1MjUwMCBIZWxwZXJzIFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFxuZnVuY3Rpb24gcGhhc2VMYWJlbChwaGFzZSkge1xuICBjb25zdCBsYWJlbHMgPSB7XG4gICAgd2FpdGluZzogXCJXQUlUSU5HXCIsXG4gICAgbmV3czogXCJORVdTXCIsXG4gICAgYWN0aW9uX3N1Ym1pc3Npb246IFwiU1VCTUlUIEFDVElPTlwiLFxuICAgIG5lZ290aWF0aW9uOiBcIk5FR09USUFUSU9OXCIsXG4gICAgcmVzb2x1dGlvbjogXCJSRVNPTFVUSU9OXCIsXG4gICAgZGlzY2xvc3VyZTogXCJESVNDTE9TVVJFXCIsXG4gICAgZmluaXNoZWQ6IFwiRklOSVNIRURcIixcbiAgfTtcblxuICByZXR1cm4gKFxuICAgIGxhYmVsc1twaGFzZV0gfHxcbiAgICBTdHJpbmcocGhhc2UgfHwgXCJcdTIwMTRcIilcbiAgICAgIC5yZXBsYWNlQWxsKFwiX1wiLCBcIiBcIilcbiAgICAgIC50b1VwcGVyQ2FzZSgpXG4gICk7XG59XG5cbmZ1bmN0aW9uIGVzY2FwZUh0bWwoc3RyKSB7XG4gIHJldHVybiBTdHJpbmcoc3RyID8/IFwiXCIpXG4gICAgLnJlcGxhY2VBbGwoXCImXCIsIFwiJmFtcDtcIilcbiAgICAucmVwbGFjZUFsbChcIjxcIiwgXCImbHQ7XCIpXG4gICAgLnJlcGxhY2VBbGwoXCI+XCIsIFwiJmd0O1wiKVxuICAgIC5yZXBsYWNlQWxsKCdcIicsIFwiJnF1b3Q7XCIpXG4gICAgLnJlcGxhY2VBbGwoXCInXCIsIFwiJiMzOTtcIik7XG59XG5cbi8vIFN0YXJ0IG9ubHkgYWZ0ZXIgdGhlIHdob2xlIGZpbGUgaGFzIGxvYWRlZFxuaWYgKGRvY3VtZW50LnJlYWR5U3RhdGUgPT09IFwibG9hZGluZ1wiKSB7XG4gIGRvY3VtZW50LmFkZEV2ZW50TGlzdGVuZXIoXCJET01Db250ZW50TG9hZGVkXCIsICgpID0+IGJvb3QoY29uZmlnKSwge1xuICAgIG9uY2U6IHRydWUsXG4gIH0pO1xufSBlbHNlIHtcbiAgYm9vdChjb25maWcpO1xufVxuIl0sCiAgIm1hcHBpbmdzIjogIjs7QUFDTyxNQUFJLFVBQVUsQ0FBQyxVQUFVO0FBQzlCLFFBQUcsT0FBTyxVQUFVLFlBQVc7QUFDN0IsYUFBTztJQUNULE9BQU87QUFDTCxVQUFJQSxXQUFVLFdBQVc7QUFBRSxlQUFPO01BQU07QUFDeEMsYUFBT0E7SUFDVDtFQUNGO0FDUk8sTUFBTSxhQUFhLE9BQU8sU0FBUyxjQUFjLE9BQU87QUFDeEQsTUFBTSxZQUFZLE9BQU8sV0FBVyxjQUFjLFNBQVM7QUFDM0QsTUFBTSxTQUFTLGNBQWMsYUFBYTtBQUMxQyxNQUFNLGNBQWM7QUFDcEIsTUFBTSxnQkFBZ0IsRUFBQyxZQUFZLEdBQUcsTUFBTSxHQUFHLFNBQVMsR0FBRyxRQUFRLEVBQUM7QUFDcEUsTUFBTSxrQkFBa0I7QUFDeEIsTUFBTSxrQkFBa0I7QUFDeEIsTUFBTSxpQkFBaUI7SUFDNUIsUUFBUTtJQUNSLFNBQVM7SUFDVCxRQUFRO0lBQ1IsU0FBUztJQUNULFNBQVM7RUFDWDtBQUNPLE1BQU0saUJBQWlCO0lBQzVCLE9BQU87SUFDUCxPQUFPO0lBQ1AsTUFBTTtJQUNOLE9BQU87SUFDUCxPQUFPO0VBQ1Q7QUFFTyxNQUFNLGFBQWE7SUFDeEIsVUFBVTtJQUNWLFdBQVc7RUFDYjtBQUNPLE1BQU0sYUFBYTtJQUN4QixVQUFVO0VBQ1o7QUNyQkEsTUFBcUIsT0FBckIsTUFBMEI7SUFDeEIsWUFBWSxTQUFTLE9BQU8sU0FBUyxTQUFRO0FBQzNDLFdBQUssVUFBVTtBQUNmLFdBQUssUUFBUTtBQUNiLFdBQUssVUFBVSxXQUFXLFdBQVc7QUFBRSxlQUFPLENBQUM7TUFBRTtBQUNqRCxXQUFLLGVBQWU7QUFDcEIsV0FBSyxVQUFVO0FBQ2YsV0FBSyxlQUFlO0FBQ3BCLFdBQUssV0FBVyxDQUFDO0FBQ2pCLFdBQUssT0FBTztJQUNkOzs7OztJQU1BLE9BQU8sU0FBUTtBQUNiLFdB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S0FBSztBQUNqQyxVQUFJLENBQUM7QUFBUztBQUVkLFdBQUssUUFDRixLQUFLLGdCQUFnQixFQUFFLFFBQVEsQ0FBQyxFQUNoQyxRQUFRLE1BQU0sTUFBTTtBQUNuQixjQUFNLFFBQVE7QUFBQSxNQUNoQixDQUFDLEVBQ0EsUUFBUSxTQUFTLENBQUMsTUFBTTtBQUN2QixjQUFNLEtBQUsscUJBQXFCLEtBQUssVUFBVSxFQUFFLE1BQU0sS0FBSyxPQUFPO0FBQUEsTUFDckUsQ0FBQztBQUFBLElBQ0w7QUFBQSxJQUVBLFlBQVksS0FBSztBQUNmLFlBQU0sWUFBWSxTQUFTLGVBQWUsZUFBZTtBQUN6RCxVQUFJLENBQUM7QUFBVztBQUVoQixZQUFNLGNBQWMsVUFBVSxjQUFjLEdBQUc7QUFDL0MsVUFBSSxlQUFlLFlBQVksVUFBVSxTQUFTLGFBQWEsR0FBRztBQUNoRSxvQkFBWSxPQUFPO0FBQUEsTUFDckI7QUFFQSxZQUFNLEtBQUssU0FBUyxjQUFjLEtBQUs7QUFDdkMsU0FBRyxZQUFZO0FBRWYsWUFBTSxPQUFPLElBQUksY0FDYixJQUFJLEtBQUssSUFBSSxXQUFXLEVBQUUsbUJBQW1CLENBQUMsR0FBRztBQUFBLFFBQy9DLE1BQU07QUFBQSxRQUNOLFFBQVE7QUFBQSxNQUNWLENBQUMsSUFDRDtBQUVKLFNBQUcsWUFBWTtBQUFBLHNFQUNtRDtBQUFBO0FBQUEseUVBRUcsV0FBVyxJQUFJLFlBQVksR0FBRztBQUFBLCtEQUN4QyxXQUFXLElBQUksT0FBTztBQUFBO0FBQUE7QUFJakYsZ0JBQVUsWUFBWSxFQUFFO0FBQ3hCLGdCQUFVLFlBQVksVUFBVTtBQUFBLElBQ2xDO0FBQUEsRUFDRjtBQUdBLFdBQVMsV0FBVyxPQUFPO0FBQ3pCLFVBQU0sU0FBUztBQUFBLE1BQ2IsU0FBUztBQUFBLE1BQ1QsTUFBTTtBQUFBLE1BQ04sbUJBQW1CO0FBQUEsTUFDbkIsYUFBYTtBQUFBLE1BQ2IsWUFBWTtBQUFBLE1BQ1osWUFBWTtBQUFBLE1BQ1osVUFBVTtBQUFBLElBQ1o7QUFFQSxXQUNFLE9BQU8sS0FBSyxLQUNaLE9BQU8sU0FBUyxRQUFHLEVBQ2hCLFdBQVcsS0FBSyxHQUFHLEVBQ25CLFlBQVk7QUFBQSxFQUVuQjtBQUVBLFdBQVMsV0FBVyxLQUFLO0FBQ3ZCLFdBQU8sT0FBTyxvQkFBTyxFQUFFLEVBQ3BCLFdBQVcsS0FBSyxPQUFPLEVBQ3ZCLFdBQVcsS0FBSyxNQUFNLEVBQ3RCLFdBQVcsS0FBSyxNQUFNLEVBQ3RCLFdBQVcsS0FBSyxRQUFRLEVBQ3hCLFdBQVcsS0FBSyxPQUFPO0FBQUEsRUFDNUI7QUFHQSxNQUFJLFNBQVMsZUFBZSxXQUFXO0FBQ3JDLGFBQVMsaUJBQWlCLG9CQUFvQixNQUFNLEtBQUssTUFBTSxHQUFHO0FBQUEsTUFDaEUsTUFBTTtBQUFBLElBQ1IsQ0FBQztBQUFBLEVBQ0gsT0FBTztBQUNMLFNBQUssTUFBTTtBQUFBLEVBQ2I7IiwKICAibmFtZXMiOiBbImNsb3N1cmUiLCAiX2EiLCAiX2IiXQp9Cg==
