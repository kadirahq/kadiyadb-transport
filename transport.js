var net = require('net');
var url = require('url');

function connect(address, callback){
  var socket = new net.Socket();

  function onConnect () {
    var conn = new Connection(socket);
    callback(null, conn);
  }

  function onError (err) {
    socket.removeListener('connect', onConnect);
    callback(err);
  }

  address = url.parse(address);
  socket.once('connect', onConnect);
  socket.once('error', onError);
  socket.connect(address.port, address.hostname);
}

function Connection(socket){
  this._socket = socket;
  this._data = new Buffer(8192);
  this._dataTmp = new Buffer(8192);
  this._cursor = 0;
  this._items = [];
  this._batches = [];
  this._readBatchCallbacks = [];
  this._dataEnd = 0;
  this._socket.on('data', this._onData.bind(this));
}

Connection.prototype.ReadBatch = function(callback){
  if(this._batches.length){
    var batch = this._batches.pop()
    callback(batch.id, batch.type, batch.data);
  } else {
    this._readBatchCallbacks.push(callback);
  }
};

Connection.prototype.WriteBatch = function(id, type, reqBatch){
  var self = this;
  var header = new Buffer(13);
  var itemSizeBuf = new Buffer(4);

  header.fill(0);
  header.writeUInt32LE(id, 0);
  header.writeUInt8(type, 8);
  header.writeUInt32LE(reqBatch.length, 9);
  this._socket.write(header);

  reqBatch.forEach(function(item){
    itemSizeBuf.writeUInt32LE(item.length);
    self._socket.write(itemSizeBuf);
    self._socket.write(item);
  });
};

Connection.prototype._onData = function(data){
  var newSize = this._dataEnd + data.length;
  if(this._data.length < newSize) {
    this._growBuffer(newSize);
  }

  data.copy(this._data, this._dataEnd);
  var buffer = this._data.slice(0, newSize);

  var startOffset = 0;

  while(true){
    if(!this._current){ // Header has not been read
      if(buffer.length-startOffset < 13){
        // Header is not received yet
        break;
      }

      var id = buffer.readUInt32LE(startOffset)
      var type = buffer.readUInt8(startOffset+8)
      var size = buffer.readUInt32LE(startOffset+9)

      this._current = {
        id: id,
        type: type,
        size: size,
        read: 0
      }

      startOffset = startOffset + 13
    }

    while(this._current.size>this._current.read){
      if(!this._current.item){ // Not in the middle of reading an item
        // Start reading a new item
        if(buffer.length-startOffset < 4){
          break;
        }

        var itemSize = buffer.readUInt32LE(startOffset);
        startOffset += 4;
        this._current.item = {
          size: itemSize
        }
      }

      if(buffer.length-startOffset < this._current.item.size){ // Current item is not received yet
        break;
      }

      var item = this._current.item;

      var itemBuf = buffer.slice(startOffset, startOffset+item.size);
      startOffset = startOffset + item.size;

      this._items.push(itemBuf);

      this._current.read++;
      this._current.item = null;
    }

    if(this._current.size>this._current.read){
      // All messages are not read, need more data
      break;
    }

    // Call registered callbacks
    if(this._readBatchCallbacks.length){
      // there are callbacks call the first
      var cb = this._readBatchCallbacks.pop();
      cb(this._current.id, this._current.type, this._items);
    } else {
      this._batches.push({
        id: this._current.id,
        type: this._current.type,
        data: this._items
      });
    }

    this._items = [];
    this._current = null
  }

  // set remaining data as this._data
  buffer.copy(this._dataTmp, 0, startOffset);
  this._dataEnd = buffer.length - startOffset;

  // swap buffers
  var tmpBuff = this._data;
  this._data = this._dataTmp;
  this._dataTmp = tmpBuff;
}

Connection.prototype._growBuffer = function(size) {
  var buff = new Buffer(size);
  this._data.copy(buff);
  this._data = buff;
};

module.exports = {
  connect: connect,
  Connection: Connection,
};
