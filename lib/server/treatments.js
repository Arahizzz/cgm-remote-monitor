'use strict';

const _ = require('lodash');
const async = require('async');
const moment = require('moment');
const find_options = require('./query');
const { ObjectId } = require('mongodb');

function storage(env, ctx) {

  function create(objOrArray, fn) {
    function done(err, result) {
      ctx.bus.emit('data-received');
      fn(err, result);
    }

    if (_.isArray(objOrArray)) {
      const allDocs = [];
      const errs = [];
      async.eachSeries(objOrArray, function(obj, callback) {
        upsert(obj, function upserted(err, docs) {
          allDocs.push(...docs);
          errs.push(err);
          callback(err, docs);
        });
      }, function() {
        const filteredErrs = _.compact(errs);
        done(filteredErrs.length > 0 ? filteredErrs : null, allDocs);
      });
    } else {
      upsert(objOrArray, done);
    }
  }

  function upsert(obj, fn) {
    const results = prepareData(obj);
    const query = {
      created_at: results.created_at,
      eventType: obj.eventType
    };

    api().updateOne(query, { $set: obj }, { upsert: true }).then(updateResults => {
      if (updateResults.upsertedId) {
        obj._id = updateResults.upsertedId._id;
      }
      if (obj.preBolus) {
        const pbTreat = {
          created_at: new Date(new Date(results.created_at).getTime() + (obj.preBolus * 60000)).toISOString(),
          eventType: obj.eventType,
          carbs: results.preBolusCarbs
        };
        if (obj.notes) {
          pbTreat.notes = obj.notes;
        }
        api().updateOne({ created_at: pbTreat.created_at }, { $set: pbTreat }, { upsert: true }).then(updateResults => {
          if (updateResults.upsertedId) {
            pbTreat._id = updateResults.upsertedId._id;
          }
          const treatments = _.compact([obj, pbTreat]);
          ctx.bus.emit('data-update', {
            type: 'treatments',
            op: 'update',
            changes: ctx.ddata.processRawDataForRuntime(treatments)
          });
          fn(null, treatments);
        }).catch(fn);
      } else {
        ctx.bus.emit('data-update', {
          type: 'treatments',
          op: 'update',
          changes: ctx.ddata.processRawDataForRuntime([obj])
        });
        fn(null, [obj]);
      }
    }).catch(fn);
  }

  function list(opts, fn) {
    const sortOption = opts && opts.sort || { created_at: -1 };
    const limitOption = opts && opts.count ? parseInt(opts.count) : undefined;
    api().find(query_for(opts)).sort(sortOption).limit(limitOption).toArray(fn);
  }

  function query_for(opts) {
    return find_options(opts, storage.queryOpts);
  }

  function remove(opts, fn) {
    api().deleteMany(query_for(opts)).then(result => {
      ctx.bus.emit('data-update', {
        type: 'treatments',
        op: 'remove',
        count: result.deletedCount,
        changes: opts.find._id
      });
      ctx.bus.emit('data-received');
      fn(null, result);
    }).catch(fn);
  }

  function save(obj, fn) {
    try {
      obj._id = new ObjectId(obj._id);
    } catch (err) {
      console.error(err);
      obj._id = new ObjectId();
    }
    prepareData(obj);
    api().replaceOne({ _id: obj._id }, obj).then(result => {
      ctx.bus.emit('data-update', {
        type: 'treatments',
        op: 'update',
        changes: ctx.ddata.processRawDataForRuntime([obj])
      });
      fn(null, result);
    }).catch(fn);
    ctx.bus.emit('data-received');
  }

  function api() {
    return ctx.store.collection(env.treatments_collection);
  }

  api.list = list;
  api.create = create;
  api.query_for = query_for;
  api.indexedFields = [
    'created_at',
    'eventType',
    'insulin',
    'carbs',
    'glucose',
    'enteredBy',
    'boluscalc.foods._id',
    'notes',
    'NSCLIENT_ID',
    'percent',
    'absolute',
    'duration',
    { 'eventType': 1, 'duration': 1, 'created_at': 1 }
  ];
  api.remove = remove;
  api.save = save;
  api.aggregate = require('./aggregate')({}, api);

  return api;
}

function prepareData(obj) {

  // Convert all dates to UTC dates

  // TODO remove this -> must not create new date if missing
  const d = moment(obj.created_at).isValid() ? moment.parseZone(obj.created_at) : moment();
  obj.created_at = d.toISOString();

  var results = {
    created_at: obj.created_at
    , preBolusCarbs: ''
  };

  const offset = d.utcOffset();
  obj.utcOffset = offset;
  results.offset = offset;

  obj.glucose = Number(obj.glucose);
  obj.targetTop = Number(obj.targetTop);
  obj.targetBottom = Number(obj.targetBottom);
  obj.carbs = Number(obj.carbs);
  obj.insulin = Number(obj.insulin);
  obj.duration = Number(obj.duration);
  obj.percent = Number(obj.percent);
  obj.absolute = Number(obj.absolute);
  obj.relative = Number(obj.relative);
  obj.preBolus = Number(obj.preBolus);

  //NOTE: the eventTime is sent by the client, but deleted, we only store created_at
  var eventTime;
  if (obj.eventTime) {
    eventTime = new Date(obj.eventTime).toISOString();
    results.created_at = eventTime;
  }

  obj.created_at = results.created_at;
  if (obj.preBolus && obj.preBolus !== 0 && obj.carbs) {
    results.preBolusCarbs = obj.carbs;
    delete obj.carbs;
  }

  if (obj.eventType === 'Announcement') {
    obj.isAnnouncement = true;
  }

  // clean data
  delete obj.eventTime;

  function deleteIfEmpty (field) {
    if (!obj[field] || obj[field] === 0) {
      delete obj[field];
    }
  }

  function deleteIfNaN (field) {
    if (isNaN(obj[field])) {
      delete obj[field];
    }
  }

  deleteIfEmpty('targetTop');
  deleteIfEmpty('targetBottom');
  deleteIfEmpty('carbs');
  deleteIfEmpty('insulin');
  deleteIfEmpty('percent');
  deleteIfEmpty('relative');
  deleteIfEmpty('notes');
  deleteIfEmpty('preBolus');

  deleteIfNaN('absolute');
  deleteIfNaN('duration');

  if (obj.glucose === 0 || isNaN(obj.glucose)) {
    delete obj.glucose;
    delete obj.glucoseType;
    delete obj.units;
  }

  return results;
}

storage.queryOpts = {
  walker: {
    insulin: parseInt
    , carbs: parseInt
    , glucose: parseInt
    , notes: find_options.parseRegEx
    , eventType: find_options.parseRegEx
    , enteredBy: find_options.parseRegEx
  }
  , dateField: 'created_at'
};

module.exports = storage;
