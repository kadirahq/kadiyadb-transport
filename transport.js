var net = require('net');
var url = require('url');

function connect(_addr, callback) {
  var sock = new net.Socket();
  var addr = url.parse(_addr);
  var conn = new Connection();

  function onConnect() {
    sock.removeListener('error', onError);

    conn._socket = sock;
    conn.connected = true;
    sock.on('data', conn._onData.bind(conn));
    callback(null, conn);

    registerReconnect();
  }

  function onError(err) {
    sock = this;
    setTimeout(function() {
      sock.connect(addr.port, addr.hostname);
    }, 1000);
  }

  function registerReconnect() {
    conn._socket.once('close', function() {
      conn.connected = false;
      var newsock = new net.Socket();

      newsock.once('connect', function() {
        newsock.removeListener('error', onError);

        conn._socket = newsock;
        conn.connected = true;
        newsock.on('data', conn._onData.bind(conn));
      });

      newsock.on('error', onError.bind(newsock));
      newsock.connect(addr.port, addr.hostname);
    })
  }

  sock.once('connect', onConnect);
  sock.on('error', onError.bind(sock));
  sock.connect(addr.port, addr.hostname);
}

function Connection() {
  this.connected = false;
  this._responses = [];
  this._readcalls = [];
  this._buffPri = new Buffer(8192);
  this._buffSec = new Buffer(8192);
  this._buffEnd = 0;
  this._reset();
}

Connection.prototype.recv = function(decoder, callback) {
  if (this._responses.length) {
    var res = this._responses.pop();
    callback(null, res);
  } else {
    this._readcalls.push(callback);
  }
};

Connection.prototype.send = function(encoder, req) {
  var data = encoder.encode(req).toBuffer();
  var size = new Buffer(4);
  size.writeUInt32LE(data.length);
  this._socket.write(size);
  this._socket.write(data);
};

Connection.prototype._onData = function(data) {
  var size = this._buffEnd + data.length;
  if (this._data.length < size) {
    this._grow(size);
  }

  data.copy(this._buffPri, this._buffEnd);
  var buffer = this._buffPri.slice(0, size);
  var offset = 0;

  while (true) {
    if (buffer.length < offset + 4) {
      // not enough bytes left for res size
      break
    }

    var size = buffer.readUInt32LE(offset);
    if (buffer.length < offset + 4 + size) {
      // not enough bytes left for res data
      break
    }

    var res = buffer.slice(offset + 4, offset + 4 + size);
    if (this._readcalls.length) {
      var callback = this._readcalls.pop();
      callback(null, res);
    } else {
      this._responses.push(res);
    }
  }

  // set remaining data as this._buffSec
  // and swap primary/secondary buffers
  buffer.copy(this._buffSec, 0, offset);
  this._buffEnd = buffer.length - offset;
  var tmpBuff = this._buffPri;
  this._buffPri = this._buffSec;
  this._buffSec = tmpBuff;
};

Connection.prototype._grow = function(size) {
  var buff = new Buffer(size);
  this._buffPri.copy(buff);
  this._buffPri = buff;
};

module.exports = {
  connect: connect,
  Connection: Connection,
};
