'use strict';

const es = require('event-stream');
const find_options = require('./query');
const { ObjectId } = require('mongodb');
const moment = require('moment');

function storage(env, ctx) {

  function list(opts, fn) {
    const sortOption = opts && opts.sort || { date: -1 };
    const limitOption = opts && opts.count ? parseInt(opts.count) : undefined;

    api().find(query_for(opts)).sort(sortOption).limit(limitOption).toArray(fn);
  }

  function remove(opts, fn) {
    api().deleteMany(query_for(opts)).then(result => {
      ctx.bus.emit('data-update', {
        type: 'entries',
        op: 'remove',
        count: result.deletedCount,
        changes: opts.find._id
      });

      ctx.bus.emit('data-received');
      fn(null, result);
    }).catch(fn);
  }

  function map() {
    return es.map(function iter(item, next) {
      return next(null, item);
    });
  }

  function persist(fn) {
    function done(err, result) {
      if (err) { return fn(err, result); }
      create(result, fn);
    }
    return es.pipeline(map(), es.writeArray(done));
  }

  function create(docs, fn) {
    const operations = docs.map(doc => {
      const _sysTime = moment(doc.dateString).isValid() ? moment.parseZone(doc.dateString) : moment(doc.date);
      doc.utcOffset = _sysTime.utcOffset();
      doc.sysTime = _sysTime.toISOString();
      if (doc.dateString) doc.dateString = doc.sysTime;

      const query = (doc.sysTime && doc.type) ? { sysTime: doc.sysTime, type: doc.type } : doc;
      return {
        updateOne: {
          filter: query,
          update: doc,
          upsert: true
        }
      };
    });

    api().bulkWrite(operations).then(result => {
      ctx.bus.emit('data-update', {
        type: 'entries',
        op: 'update',
        changes: ctx.ddata.processRawDataForRuntime(docs) // assuming this function can handle multiple docs
      });

      ctx.bus.emit('data-received');
      fn(null, docs);
    }).catch(fn);
  }

  function getEntry(id, fn) {
    api().findOne({ _id: new ObjectId(id) }).then(entry => {
      fn(null, entry);
    }).catch(fn);
  }

  function query_for(opts) {
    return find_options(opts, storage.queryOpts);
  }

  function api() {
    return ctx.store.collection(env.entries_collection);
  }

  api.list = list;
  api.map = map;
  api.create = create;
  api.remove = remove;
  api.persist = persist;
  api.query_for = query_for;
  api.getEntry = getEntry;
  api.aggregate = require('./aggregate')({}, api);
  api.indexedFields = [
    'date',
    'type',
    'sgv',
    'mbg',
    'sysTime',
    'dateString',
    { 'type': 1, 'date': -1, 'dateString': 1 }
  ];
  return api;
}

storage.queryOpts = {
  walker: {
    date: parseInt,
    sgv: parseInt,
    filtered: parseInt,
    unfiltered: parseInt,
    rssi: parseInt,
    noise: parseInt,
    mbg: parseInt
  },
  useEpoch: true
};

storage.storage = storage;
module.exports = storage;
