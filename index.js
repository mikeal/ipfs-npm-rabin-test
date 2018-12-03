const bent = require('bent')
const rabin = require('rabin')
const deepcopy = require('deepcopy')
const streamChunker = require('stream-chunker')
const {mkblock} = require('./cbor')
const {rawfile, fixedChunker} = require('js-unixfsv2-draft')
const {PassThrough} = require('stream')
const fs = require('fs').promises
const path = require('path')
const zlib = require('zlib')
const cbor = require('./cbor')
const links = require('../dag-cbor-links')
const difference = require('lodash.difference')

const getpkg = bent('https://skimdb.npmjs.com/registry/', 'json')

const blockpath = path.join(__dirname, 'blocks')
const BlockStore = {
  set: block => {
    let f = path.join(blockpath, block.cid.toBaseEncodedString())
    return fs.writeFile(f, block.data)
  },
  get: cid => {
    if (typeof cid !== 'string') {
      cid = cid.toBaseEncodedString()
    }
    return fs.readFile(path.join(blockpath, cid))
  }
}

const create = async name => {
  try {
    await fs.mkdir(path.join(__dirname, 'blocks'))
  } catch (e) { /* ignore */ }
  let package = await getpkg('request')

  let tarballs = deepcopy(package)
  let rabins = deepcopy(package)

  for (let [version, value] of Object.entries(package.versions)) {
    let url = value.dist.tarball
    console.error('GET', url)
    let stream = await bent()(url)

    /* first pipe in both cases should be PassThrough
       to terminate backpressue
    */

    let tarball = stream
    .pipe(new PassThrough())
    .pipe(streamChunker(2.048e+6, {flush: true}))
    .pipe(new PassThrough({objectMode: true})) // objectMode ensures size

    let rbn = stream
    .pipe(new PassThrough())
    .pipe(new zlib.Gunzip())
    .pipe(rabin())
    .pipe(new PassThrough({objectMode: true}))

    for await (let block of rawfile(tarball)) {
      await BlockStore.set(block)
      /* set to the last cid which is the root of the file */
      tarballs.versions[version].dist.tarball = block.cid
    }
    for await (let block of rawfile(rbn)) {
      await BlockStore.set(block)
      /* set to the last cid which is the root of the file */
      rabins.versions[version].dist.tarball = block.cid
    }

    tarballs.versions[version].dist.size = stream.headers['content-length']
    rabins.versions[version].dist.size = stream.headers['content-length']
  }
  let block = await cbor.mkblock(tarballs)
  BlockStore.set(block)
  console.log('tarballs', block.cid.toBaseEncodedString())

  block = await cbor.mkblock(rabins)
  BlockStore.set(block)
  console.log('rabins', block.cid.toBaseEncodedString())
}

// create('request')
// tarballs zdpuB2gvzYjfAdpQ6SoibzHmkTZ8r3sNTwdKYqDXfrmaHywfK
// rabins zdpuAvFNbJEL846E9aNh5Y3SMh6nfeM9Lr9LHzmi9NYh9Dx2p

const compare = async (tarballs, rabins) => {
  let tarballsTotal = 0
  let tarballsTotalBlocks = 1
  let rabinsTotal = 0
  let rabinsTotalBlocks = 1
  let tarballBuffer = await BlockStore.get(tarballs)
  tarballsTotal += tarballBuffer.length
  tarballs = cbor.deserialize(tarballBuffer)

  rabinsBuffer = await BlockStore.get(rabins)
  rabinsTotal += rabinsBuffer.length
  rabins = cbor.deserialize(rabinsBuffer)

  for (let [, link] of links(tarballBuffer)) {
    let buffer = await BlockStore.get(link)
    tarballsTotal += buffer.length
    tarballsTotalBlocks += 1
    for (let [, rawlink] of links(buffer)) {
      let raw = await BlockStore.get(rawlink)
      tarballsTotal += raw.length
      tarballsTotalBlocks += 1
    }
  }

  for (let [, link] of links(rabinsBuffer)) {
    let buffer = await BlockStore.get(link)
    rabinsTotal += buffer.length
    rabinsTotalBlocks += 1
    for (let [, rawlink] of links(buffer)) {
      let raw = await BlockStore.get(rawlink)
      rabinsTotal += raw.length
      rabinsTotalBlocks += 1
    }
  }


  let versions = Object.keys(rabins.versions)
  let lastVersion

  let averageBlocks = []
  let averageSize = []
  let averageTarball = []

  for (let version of versions) {
    if (lastVersion) {
      let prev = rabins.versions[lastVersion].dist.tarball
      let curr = rabins.versions[version].dist.tarball
      prev = Array.from(links(await BlockStore.get(prev)))
      curr = Array.from(links(await BlockStore.get(curr)))
      prev = prev.map(tuple => tuple[1].toBaseEncodedString())
      curr = curr.map(tuple => tuple[1].toBaseEncodedString())

      let diff = difference(curr, prev)
      averageBlocks.push(diff.length)
      let buffers = await Promise.all(diff.map(cid => BlockStore.get(cid)))
      averageSize.push(
        buffers.map(block => block.length).reduce((x,y) => x + y, 0)
      )
      console.log(version, 'has', diff.length, 'new blocks of', averageSize[averageSize.length-1], 'bytes instead of', rabins.versions[version].dist.size)

      averageTarball.push(parseInt(rabins.versions[version].dist.size))
    }
    lastVersion = version
  }

  console.log({tarballsTotal, tarballsTotalBlocks})
  console.log({rabinsTotal, rabinsTotalBlocks})
  averageBlocks = averageBlocks.reduce((x,y) => x + y, 0) / averageBlocks.length
  averageSize = averageSize.reduce((x,y) => x + y, 0) / averageSize.length

  console.log(
    {averageRabinBlocks: averageBlocks,
      averageTarballBlocks: tarballsTotalBlocks / versions.length,
      averageRabin: averageSize,
      averageTarball: tarballsTotal / versions.length
    })
}

compare('zdpuB2gvzYjfAdpQ6SoibzHmkTZ8r3sNTwdKYqDXfrmaHywfK', 'zdpuAvFNbJEL846E9aNh5Y3SMh6nfeM9Lr9LHzmi9NYh9Dx2p')