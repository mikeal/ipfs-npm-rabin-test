const util = require('util')
const cbor = require('dag-cbor-sync')(1024 * 4)
const CID = require('cids')
const Block = require('ipfs-block')
const crypto = require('crypto')
const multihashes = require('multihashes')

const sha2 = b => crypto.createHash('sha256').update(b).digest()

const mkblock = (obj) => {
  let buffer = cbor.serialize(obj)
  let hash = multihashes.encode(sha2(buffer), 'sha2-256')
  let cid = new CID(1, 'dag-cbor', hash)
  return new Block(buffer, cid)
}

module.exports = {
  serialize: cbor.serialize,
  deserialize: cbor.deserialize,
  mkblock
}
