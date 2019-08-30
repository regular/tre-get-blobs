#!/usr/bin/env node

/* Usage: get-blobs --config PATH-TO-SSB-CONFIG
*/

const client = require('tre-cli-client')
const pull = require('pull-stream')
const bytes = require('human-size')
const paramap = require('pull-paramap')

client( (err, ssb) => {
  if (err) {
    console.error(err.message)
    process.exit(1)
  }
  let count = 0, totalBytes = 0
  let sync = false
  const localBlobs = new Set()

  function show() {
    console.log(`${count} blobs, ${bytes(totalBytes)}`)
  }

  const drain = pull.drain(()=>{}, err=>{
    if (err && err !== true) {
      console.error(err.message)
      process.exit(1)
    }
  })
  pull(
    ssb.blobs.ls({size: true, live: true, sync: true}),
    pull.through(x => {
      if (x.sync) {
        sync = true
        show()
        want()
        return
      }
      localBlobs.add(x.id)
      count++
      totalBytes += x.size
      if (sync) show()
    }),
    drain
  )
  
  function want() {
    pull(
      ssb.revisions.links({to: '&'}),
      pull.map(kv=>{
        const [rel, blob, msg] = kv.key
        const desc = `${kv.value.value.content.name} ${rel}`
        return {blob, desc}
      }),
      pull.unique(({blob})=>blob),
      paramap( ({blob, desc}, cb)=>{
        if (localBlobs.has(blob)) return cb(null, blob)
        console.log(`Requesting ${desc} ${blob} ...`)
        ssb.blobs.want(blob, (err, has)=>{
          if (err) return cb(err)
          if (!has) return cb(new Error('Failed to get blob ' + blob))
          cb(null, blob)
        })
      }, 100, false),
      pull.onEnd(err => {
        drain.abort()
        if (err) {
          console.error(err.message)
          process.exit(1)
        }
        ssb.close()
        console.log('done')
      })
    )
  }
})
