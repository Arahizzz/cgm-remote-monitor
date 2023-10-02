'use strict';

const find_options = require('./query');
const consts = require('../constants');
const { ObjectId } = require('mongodb');

function storage(collection, ctx) {

  function create(obj, fn) {
    obj.created_at = (new Date()).toISOString();
    api().insertOne(obj).then(result => {
      fn(null, result.ops[0]);
    }).catch(fn);
    ctx.bus.emit('data-received');
  }

  function save(obj, fn) {
    obj._id = new ObjectId(obj._id);
    if (!obj.created_at) {
      obj.created_at = (new Date()).toISOString();
    }
    api().replaceOne({ _id: obj._id }, obj).then(() => {
      fn(null, obj);
    }).catch(fn);
    ctx.bus.emit('data-received');
  }

  function list(fn, count) {
    const limit = count !== null ? count : Number(consts.PROFILES_DEFAULT_COUNT);
    return api().find({}).limit(limit).sort({ startDate: -1 }).toArray(fn);
  }

  function list_query(opts, fn) {
    storage.queryOpts = {
      walker: {},
      dateField: 'startDate'
    };

    const query = query_for(opts);
    const sort = opts && opts.sort && query_sort(opts) || { startDate: -1 };
    const limit = opts && opts.count ? parseInt(opts.count) : undefined;

    return api().find(query).sort(sort).limit(limit).toArray(fn);
  }

  function query_for(opts) {
    return find_options(opts, storage.queryOpts);
  }

  function query_sort(opts) {
    if (opts && opts.sort) {
      const sortKeys = Object.keys(opts.sort);
      for (let i = 0; i < sortKeys.length; i++) {
        opts.sort[sortKeys[i]] = opts.sort[sortKeys[i]] === '1' ? 1 : -1;
      }
      return opts.sort;
    }
  }

  function last(fn) {
    return api().find().sort({ startDate: -1 }).limit(1).toArray(fn);
  }

  function remove(_id, fn) {
    const objId = new ObjectId(_id);
    api().deleteOne({ '_id': objId }).then(result => {
      fn(null, result);
    }).catch(fn);
    ctx.bus.emit('data-received');
  }

  function api() {
    return ctx.store.collection(collection);
  }

  api.list = list;
  api.list_query = list_query;
  api.create = create;
  api.save = save;
  api.remove = remove;
  api.last = last;
  api.indexedFields = ['startDate'];

  return api;
}

module.exports = storage;
