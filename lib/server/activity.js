'use strict';

const moment = require('moment');
const find_options = require('./query');

function storage(collection, ctx) {

  function create(statuses, fn) {
    if (!Array.isArray(statuses)) { statuses = [statuses]; }

    const r = [];
    let errorOccurred = false;

    statuses.forEach((obj, i) => {
      if (errorOccurred) return;

      const d = moment(obj.created_at).isValid() ? moment.parseZone(obj.created_at) : moment();
      obj.created_at = d.toISOString();
      obj.utcOffset = d.utcOffset();

      api().insertOne(obj).then(results => {
        if (!obj._id) obj._id = results.insertedId;
        r.push(obj);

        ctx.bus.emit('data-update', {
          type: 'devicestatus',
          op: 'update',
          changes: ctx.ddata.processRawDataForRuntime([obj])
        });

        if (i === statuses.length - 1) {
          fn(null, r);
          ctx.bus.emit('data-received');
        }
      }).catch(err => {
        console.log('Error inserting the device status object', err.message);
        errorOccurred = true;
        fn(err.message, null);
      });
    });
  }

  function last(fn) {
    list({ count: 1 }, (err, entries) => {
      fn(err, entries && entries.length > 0 ? entries[0] : null);
    });
  }

  function query_for(opts) {
    return find_options(opts, storage.queryOpts);
  }

  function list(opts, fn) {
    const sortOption = opts && opts.sort || { created_at: -1 };
    const limitOption = opts && opts.count ? parseInt(opts.count) : undefined;

    api().find(query_for(opts)).sort(sortOption).limit(limitOption).toArray(fn);
  }

  function remove(opts, fn) {
    api().deleteMany(query_for(opts)).then(result => {
      ctx.bus.emit('data-update', {
        type: 'devicestatus',
        op: 'remove',
        count: result.deletedCount,
        changes: opts.find._id
      });
      fn(null, result);
    }).catch(fn);
  }

  function api() {
    return ctx.store.collection(collection);
  }

  api.list = list;
  api.create = create;
  api.query_for = query_for;
  api.last = last;
  api.remove = remove;
  api.aggregate = require('./aggregate')({}, api);
  api.indexedFields = ['created_at', 'NSCLIENT_ID'];

  return api;
}

storage.queryOpts = {
  dateField: 'created_at'
};

module.exports = storage;
