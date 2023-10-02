'use strict';

const { ObjectId } = require('mongodb');

function storage(env, ctx) {

  function create(obj, fn) {
    obj.created_at = (new Date()).toISOString();
    api().insertOne(obj).then(result => {
      fn(null, result.ops);
    }).catch(err => {
      console.log('Data insertion error', err.message);
      fn(err.message, null);
    });
  }

  function save(obj, fn) {
    try {
      obj._id = new ObjectId(obj._id);
    } catch (err) {
      console.error(err);
      obj._id = new ObjectId();
    }
    obj.created_at = (new Date()).toISOString();
    api().replaceOne({ _id: obj._id }, obj).then(result => {
      fn(null, result);
    }).catch(fn);
  }

  function list(fn) {
    api().find({}).toArray(fn);
  }

  function listquickpicks(fn) {
    api().find({ $and: [{ 'type': 'quickpick' }, { 'hidden': 'false' }] }).sort({ 'position': 1 }).toArray(fn);
  }

  function listregular(fn) {
    api().find({ 'type': 'food' }).toArray(fn);
  }

  function remove(_id, fn) {
    const objId = new ObjectId(_id);
    api().deleteOne({ '_id': objId }).then(result => {
      fn(null, result);
    }).catch(fn);
  }

  function api() {
    return ctx.store.collection(env.food_collection);
  }

  api.list = list;
  api.listquickpicks = listquickpicks;
  api.listregular = listregular;
  api.create = create;
  api.save = save;
  api.remove = remove;
  api.indexedFields = ['type', 'position', 'hidden'];

  return api;
}

module.exports = storage;
