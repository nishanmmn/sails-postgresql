'use strict';
var _ = require('lodash')
  , Promise = require('bluebird')
  , utils = require('./utils')
  , hop = utils.object.hasOwnProperty
  , pg = require('pg')
  , url = require('url')
  , async = require('async')
  , Errors = require('waterline-errors').adapter
  , Sequel = require('waterline-sequel')
  , Processor = require('./processor')
  , cursor = require('waterline-cursor')
  , squel = require('squel')
  ;

const connections = new Map();
var connectionOverrides = {};
var clients = {};
var clientsCounter = 0;

var sqlOptions = {
  parameterized: true,
  caseSensitive: true,
  escapeCharacter: '"',
  casting: true,
  canReturnValues: true,
  escapeInserts: true,
  declareDeleteAlias: false
};

squel.cls.DefaultQueryBuilderOptions.tableAliasQuoteCharacter = '"';
var squelPostgres = squel.useFlavour('postgres');
squelPostgres.cls.DefaultQueryBuilderOptions.tableAliasQuoteCharacter = '"';

function spawnConnection(connectionName, logic, cb) {
  var connectionObject = connections.get(connectionName);
  if (!connectionObject) {
    return cb(Errors.InvalidConnection);
  } else {
    // If the connection details were supplied as a URL use that. Otherwise,
    // connect using the configuration object as is.
    var connectionConfig = connectionObject.config;
    if (_.has(connectionConfig, 'url')) {
      var connectionUrl = url.parse(connectionConfig.url);
      connectionUrl.query = _.omit(connectionConfig, 'url');
      connectionConfig = url.format(connectionUrl);
    }

    // Grab a client instance from the client pool
    pg.connect(connectionConfig, function (err, client, done) {
      if (err) {
        // console.error("Error creating a connection to Postgresql: " + err);
        // If connection to posgresql fails starting, provide general troubleshooting information,
        // sharpened with a few simple heuristics:
        console.error('');
        console.error('Error creating a connection to Postgresql using the following settings:\n', connectionConfig);
        console.error('');
        console.error('* * *\nComplete error details:\n', err);
        console.error('');
        console.error('');

        console.error('Troubleshooting tips:');
        console.error('');

        // Used below to indicate whether the error is potentially related to config
        // (in which case we'll display a generic message explaining how to configure all the things)
        var isPotentiallyConfigRelated;
        isPotentiallyConfigRelated = true;

        // Determine whether localhost is being used
        var usingLocalhost = !!(function () {
          try {
            var LOCALHOST_REGEXP = /(localhost|127\.0\.0\.1)/;
            if ((connectionConfig.hostname && connectionConfig.hostname.match(LOCALHOST_REGEXP)) ||
              (connectionConfig.host && connectionConfig.host.match(LOCALHOST_REGEXP))) {
              return true;
            }
          }
          catch (e) {
          }
        })();
        if (usingLocalhost) {
          console.error(
            ' -> You appear to be trying to use a Postgresql install on localhost.',
            'Maybe the database server isn\'t running, or is not installed?'
          );
          console.error('');
        }

        if (isPotentiallyConfigRelated) {
          console.error(
            ' -> Is your Postgresql configuration correct?  Maybe your `poolSize` configuration is set too high?',
            'e.g. If your Postgresql database only supports 20 concurrent connections, you should make sure',
            'you have your `poolSize` set as something < 20 (see http://stackoverflow.com/a/27387928/486547).',
            'The default `poolSize` is 10.',
            'To override default settings, specify the desired properties on the relevant Postgresql',
            '"connection" config object where the host/port/database/etc. are configured.',
            'If you\'re using Sails, this is generally located in `config/connections.js`,',
            'or wherever your environment-specific database configuration is set.'
          );
          console.error('');
        }

        // TODO: negotiate "Too many connections" error
        var tooManyConnections = true;
        if (tooManyConnections) {
          console.error(
            ' -> Maybe your `poolSize` configuration is set too high?',
            'e.g. If your Postgresql database only supports 20 concurrent connections, you should make sure',
            'you have your `poolSize` set as something < 20 (see http://stackoverflow.com/a/27387928/486547).',
            'The default `poolSize` is 10.');
          console.error('');
        }

        if (tooManyConnections && !usingLocalhost) {
          console.error(
            ' -> Do you have multiple Sails instances sharing the same Postgresql database?',
            'Each Sails instance may use up to the configured `poolSize` # of connections.',
            'Assuming all of the Sails instances are just copies of one another (a reasonable best practice)',
            'we can calculate the actual # of Postgresql connections used (C) ' +
            'by multiplying the configured `poolSize` (P)',
            'by the number of Sails instances (N).',
            'If the actual number of connections (C) exceeds the total # of **AVAILABLE** connections to your',
            'Postgresql database (V), then you have problems.  If this applies to you, try reducing your `poolSize`',
            'configuration. A reasonable `poolSize` setting would be V/N.'
          );
          console.error('');
        }

        // TODO: negotiate the error code here to make the heuristic more helpful
        var isSSLRelated = !usingLocalhost;
        if (isSSLRelated) {
          console.error(' -> Are you using an SSL-enabled Postgresql host like Heroku?',
            'Make sure to set `ssl` to `true` (see http://stackoverflow.com/a/22177218/486547)'
          );
          console.error('');
        }

        console.error('');

        // be sure to release connection
        done();

        return cb(err);
      } else {
        logic(client, function (_err, result) {
          // release client connection
          done();
          return cb(_err, result);
        });
      }
    });
  }
}

function handleQueryError(err) {
  var formattedErr;

  // Check for uniqueness constraint violations:
  if (err.code === '23505') {
    // Manually parse the error response and extract the relevant bits,
    // then build the formatted properties that will be passed directly to
    // WLValidationError in Waterline core.
    var matches = err.detail.match(/Key \((.*)\)=\((.*)\) already exists\.$/);
    if (matches && matches.length) {
      formattedErr = {};
      formattedErr.code = 'E_UNIQUE';
      formattedErr.invalidAttributes = {};
      formattedErr.invalidAttributes[matches[1]] = [{
        value: matches[2],
        rule: 'unique'
      }];
    }
  }

  return formattedErr || err;
}

function _getPK(connectionName, collectionName) {
  var collectionDefinition;

  try {
    collectionDefinition = connections.get(connectionName).collections[collectionName].definition;
    var pk;

    pk = _.find(Object.keys(collectionDefinition), function _findPK(key) {
      var attrDef = collectionDefinition[key];
      if (attrDef && attrDef.primaryKey) {
        return key;
      } else {
        return false;
      }
    });

    if (!pk) {
      pk = 'id';
    }
    return pk;
  }
  catch (e) {
    throw new Error('Unable to determine primary key for collection `' + collectionName + '` because ' +
      'an error was encountered acquiring the collection definition:\n' + require('util').inspect(e, false, null));
  }
}

function __DESCRIBE__(table, collection, client, cb) {

  // Build query to get a bunch of info from the information_schema
  // It's not super important to understand it only that it returns the following fields:
  // [Table, #, Column, Type, Null, Constraint, C, consrc, F Key, Default]
  var query = 'SELECT ' +
    "x.nspname || '.' || x.relname as \"Table\", x.attnum as \"#\", x.attname as \"Column\", x.\"Type\"," +
    ' case ' +
    'x.attnotnull when true ' +
    "then 'NOT NULL' else '' end as \"NULL\", r.conname as \"Constraint\", r.contype as \"C\", " +
    "r.consrc, fn.nspname || '.' || f.relname as \"F Key\", d.adsrc as \"Default\" FROM (" +
    'SELECT ' +
    'c.oid, a.attrelid, a.attnum, n.nspname, c.relname, ' +
    'a.attname, pg_catalog.format_type(a.atttypid, a.atttypmod) as "Type", a.attnotnull ' +
    'FROM pg_catalog.pg_attribute a, pg_namespace n, pg_class c ' +
    'WHERE a.attnum > 0 AND NOT a.attisdropped AND a.attrelid = c.oid ' +
    "and c.relkind not in ('S') " +
    "and c.relnamespace = n.oid and n.nspname not in ('pg_catalog','pg_toast','information_schema')) x " +
    'left join pg_attrdef d on d.adrelid = x.attrelid and d.adnum = x.attnum ' +
    'left join pg_constraint r on r.conrelid = x.oid and r.conkey[1] = x.attnum ' +
    'left join pg_class f on r.confrelid = f.oid ' +
    'left join pg_namespace fn on f.relnamespace = fn.oid ' +
    "where x.relname = '" + table + "' order by 1,2;";

  // Get Sequences to test if column auto-increments
  var autoIncrementQuery = 'SELECT ' +
    't.relname as related_table, a.attname as related_column, s.relname as sequence_name ' +
    'FROM pg_class s ' +
    'JOIN pg_depend d ON d.objid = s.oid JOIN pg_class t ON d.objid = s.oid AND d.refobjid = t.oid ' +
    'JOIN pg_attribute a ON (d.refobjid, d.refobjsubid) = (a.attrelid, a.attnum) ' +
    'JOIN pg_namespace n ON n.oid = s.relnamespace ' +
    "WHERE s.relkind = 'S' AND n.nspname = 'public';";

  // Get Indexes
  var indiciesQuery = 'SELECT n.nspname as "Schema", c.relname as "Name", ' +
    "CASE c.relkind WHEN 'r' THEN 'table' " +
    "WHEN 'v' THEN 'view' WHEN 'i' THEN 'index' WHEN 'S' THEN 'sequence' WHEN 's' THEN 'special' WHEN 'f' THEN " +
    "'foreign table' END as \"Type\", " +
    'pg_catalog.pg_get_userbyid(c.relowner) as "Owner", c2.relname as "Table" ' +
    'FROM pg_catalog.pg_class c LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace ' +
    'LEFT JOIN pg_catalog.pg_index i ON i.indexrelid = c.oid ' +
    'LEFT JOIN pg_catalog.pg_class c2 ON i.indrelid = c2.oid ' +
    "WHERE c.relkind IN ('i','') AND n.nspname <> 'pg_catalog' AND n.nspname <> 'information_schema' " +
    "AND n.nspname !~ '^pg_toast' AND pg_catalog.pg_table_is_visible(c.oid) ORDER BY 1,2;";

  // Run Info Query
  client.query(query, function (err, result) {
    if (err) {
      return cb(handleQueryError(err));
    }
    if (result.rows.length === 0) {
      return cb();
    }

    // Run Query to get Auto Incrementing sequences
    client.query(autoIncrementQuery, function (err, aResult) {
      if (err) {
        return cb(handleQueryError(err));
      }

      aResult.rows.forEach(function (row) {
        if (row.related_table !== table) {
          return;
        }

        // Look through query results and see if related_column exists
        result.rows.forEach(function (column) {
          if (column.Column !== row.related_column) {
            return;
          }
          column.autoIncrement = true;
        });
      });

      // Run Query to get Indexed values
      client.query(indiciesQuery, function (err, iResult) {
        if (err) {
          return cb(handleQueryError(err));
        }

        // Loop through indicies and see if any match
        iResult.rows.forEach(function (column) {
          var key = column.Name.split('_index_')[1];

          // Look through query results and see if key exists
          result.rows.forEach(function (column) {
            if (column.Column !== key) {
              return;
            }
            column.indexed = true;
          });
        });

        // Normalize Schema
        var normalizedSchema = utils.normalizeSchema(result.rows);

        // Set Internal Schema Mapping
        collection.schema = normalizedSchema;

        cb(null, normalizedSchema);
      });
    });
  });
}


squel.cls.QueryBuilder.prototype.run = function (client, table) {
  var query = this.toParam();
  client.query(query.text, query.values, function __QUERY__(err, result) {
    if (err) {
      return cb(handleQueryError(err));
    }

    var values = result.rows;
    if (table) {
      values = _.map(result.rows, function (row) {
        return processor.cast(table, row);
      })
    }
    return cb(null, values);
  });
};

var adapter = {
  identity: 'sails-postgresql',

  registerConnection: function (connection, collections, cb) {
    if (!connection.identity) {
      return cb(Errors.IdentityMissing);
    }
    if (connections.has(connection.identity)) {
      return cb(Errors.IdentityDuplicate);
    }

    // Store any connection overrides
    connectionOverrides[connection.identity] = {};

    // Look for the WL Next key
    if (hop(connection, 'wlNext')) {
      connectionOverrides[connection.identity].wlNext = _.cloneDeep(connection.wlNext);
    }

    // Store the connection
    connections.set(connection.identity, {
      config: connection,
      collections: collections
    });

    // Always call describe
    spawnConnection(connection.identity, (client, _cb) => {
      Promise
        .map(
          Object.keys(collections),
          colName => Promise.fromNode(__cb => __DESCRIBE__(colName, collections[colName], client, __cb))
        )
        .asCallback(_cb);
    }, cb)
  },

  // Teardown
  teardown: function (conn, cb) {
    if (typeof conn == 'function') {
      cb = conn;
      conn = null;
    }
    if (conn == null) {
      connections.clear();
      return cb();
    }
    if (connections.has(conn)) {
      connections.delete(conn);
    } else {
      return cb();
    }
    cb();
  },

  // Raw Query Interface
  query: adapterMethod(function (client, connectionName, table, query, data, cb) {
    if (_.isFunction(data)) {
      cb = data;
      data = null;
    }

    if (data) {
      client.query(query, data, cb);
    } else {
      client.query(query, cb);
    }
  }),

  // Describe a table
  describe: function (connectionName, table, cb) {
    let connection = connections.get(connectionName);
    if (!connection) {
      return cb(new Error('Invalid connectionName: ${connectionName}'));
    } else {
      // let {[table]: collection} = connection.collections;
      spawnConnection(
        connectionName,
        (client, _cb) => __DESCRIBE__(table, collection, client, _cb),
        cb
      );
    }

  },

  // Create a new table
  define: function (connectionName, table, definition, cb) {
    // Create a describe method to run after the define.
    // Ensures the define connection is properly closed.
    var that = this;
    var describe = function (err) {
      if (err) {
        return cb(err);
      }

      // Describe (sets schema)
      that.describe(connectionName, table.replace(/["']/g, ''), cb);
    };

    spawnConnection(connectionName, function __DEFINE__(client, cb) {
      // Escape Table Name
      table = utils.escapeName(table);

      // Iterate through each attribute, building a query string
      var _schema = utils.buildSchema(definition);

      // Check for any Index attributes
      var indexes = utils.buildIndexes(definition);

      // Build Query
      var query = 'CREATE TABLE ' + table + ' (' + _schema + ')';

      // Run Query
      client.query(query, function __DEFINE__(err) {
        if (err) {
          return cb(handleQueryError(err));
        }

        // Build Indexes
        function buildIndex(name, cb) {
          // Strip slashes from table name, used to namespace index
          var cleanTable = table.replace(/['"]/g, '');

          // Build a query to create a namespaced index tableName_key
          var query = 'CREATE INDEX ' + utils.escapeName(cleanTable + '_' + name) + ' on ' + table +
            ' (' + utils.escapeName(name) + ');';
          // Run Query
          client.query(query, function (err) {
            if (err) {
              return cb(handleQueryError(err));
            }
            cb();
          });
        }

        // Build indexes in series
        async.eachSeries(indexes, buildIndex, cb);
      });
    }, describe);
  },

  // Drop a table
  drop: adapterMethod(function (client, connectionName, table, relations, cb) {
    if (typeof relations === 'function') {
      cb = relations;
      relations = [];
    }

    function dropTable(item, next) {
      // Build Query
      var query = 'DROP TABLE ' + utils.escapeName(item) + ';';

      // Run Query
      client.query(query, function __DROP__(err, result) {
        if (err) {
          result = null;
        }
        next(null, result);
      });
    }

    async.eachSeries(relations, dropTable, function (err) {
      if (err) {
        return cb(err);
      }
      dropTable(table, cb);
    });
  }),

  // Add a column to a table
  addAttribute: function (connectionName, table, attrName, attrDef, cb) {
    spawnConnection(connectionName, function __ADD_ATTRIBUTE__(client, cb) {
      // Escape Table Name
      table = utils.escapeName(table);

      // Setup a Schema Definition
      var attrs = {};
      attrs[attrName] = attrDef;

      var _schema = utils.buildSchema(attrs);

      // Build Query
      var query = 'ALTER TABLE ' + table + ' ADD COLUMN ' + _schema;

      // Run Query
      client.query(query, function __ADD_ATTRIBUTE__(err, result) {
        if (err) {
          return cb(handleQueryError(err));
        }
        cb(null, result.rows);
      });
    }, cb);
  },

  // Remove a column from a table
  removeAttribute: function (connectionName, table, attrName, cb) {
    spawnConnection(connectionName, function __REMOVE_ATTRIBUTE__(client, cb) {
      // Escape Table Name
      table = utils.escapeName(table);

      // Build Query
      var query = 'ALTER TABLE ' + table + ' DROP COLUMN "' + attrName + '" RESTRICT';

      // Run Query
      client.query(query, function __REMOVE_ATTRIBUTE__(err, result) {
        if (err) {
          return cb(handleQueryError(err));
        }
        cb(null, result.rows);
      });
    }, cb);
  },

  // Add a new row to the table
  create: adapterMethod(function (client, connectionName, table, data, cb) {
    var connectionObject = connections.get(connectionName);
    var collection = connectionObject.collections[table];

    // Build up a SQL Query
    var schema = collection.waterline.schema;
    var processor = new Processor(schema);

    // Mixin WL Next connection overrides to sqlOptions
    var overrides = connectionOverrides[connectionName] || {};
    var options = _.cloneDeep(sqlOptions);
    if (hop(overrides, 'wlNext')) {
      options.wlNext = overrides.wlNext;
    }

    var sequel = new Sequel(schema, options);

    var incrementSequences = [];
    var query;

    // Build a query for the specific query strategy
    try {
      query = sequel.create(table, data);
    } catch (e) {
      return cb(e);
    }

    // Loop through all the attributes being inserted and check if a sequence was used
    Object.keys(collection.schema).forEach(function (schemaKey) {
      if (!utils.object.hasOwnProperty(collection.schema[schemaKey], 'autoIncrement')) {
        return;
      }
      if (Object.keys(data).indexOf(schemaKey) < 0) {
        return;
      }
      incrementSequences.push(schemaKey);
    });

    // Run Query
    client.query(query.query, query.values, function __CREATE__(err, result) {
      if (err) {
        return cb(handleQueryError(err));
      }

      // Cast special values
      var values = processor.cast(table, result.rows[0]);

      // Set Sequence value to defined value if needed
      if (incrementSequences.length === 0) {
        return cb(null, values);
      }

      function setSequence(item, next) {
        var sequenceName = "'\"" + table + '_' + item + '_seq' + "\"'";
        var sequenceValue = values[item];
        var sequenceQuery = 'SELECT setval(' + sequenceName + ', ' + sequenceValue + ', true)';

        client.query(sequenceQuery, function (err) {
          if (err) {
            return next(err);
          }
          next();
        });
      }

      async.each(incrementSequences, setSequence, function (err) {
        if (err) {
          return cb(err);
        }
        cb(null, values);
      });
    });
  }),

  // Add a multiple rows to the table
  createEach: adapterMethod(function (client, connectionName, table, records, cb) {
    // Don't bother if there are no records to create.
    if (records.length === 0) {
      return cb();
    }

    var connectionObject = connections.get(connectionName);
    var collection = connectionObject.collections[table];

    // Build up a SQL Query
    var schema = collection.waterline.schema;
    var processor = new Processor(schema);

    // Mixin WL Next connection overrides to sqlOptions
    var overrides = connectionOverrides[connectionName] || {};
    var options = _.cloneDeep(sqlOptions);
    if (hop(overrides, 'wlNext')) {
      options.wlNext = overrides.wlNext;
    }

    var sequel = new Sequel(schema, options);
    var incrementSequences = [];

    // Loop through all the attributes being inserted and check if a sequence was used
    Object.keys(collection.schema).forEach(function (schemaKey) {
      if (!utils.object.hasOwnProperty(collection.schema[schemaKey], 'autoIncrement')) {
        return;
      }
      incrementSequences.push({
        key: schemaKey,
        value: 0
      });
    });

    // Collect Query Results
    var results = [];

    // Simple way for now, in the future make this more awesome
    async.each(records, function (data, cb) {
      var query;

      // Build a query for the specific query strategy
      try {
        query = sequel.create(table, data);
      } catch (e) {
        return cb(e);
      }

      // Run Query
      client.query(query.query, query.values, function __CREATE_EACH__(err, result) {
        if (err) {
          return cb(handleQueryError(err));
        }

        // Cast special values
        var values = processor.cast(table, result.rows[0]);

        results.push(values);
        if (incrementSequences.length === 0) {
          return cb(null, values);
        }

        function checkSequence(item, next) {
          var currentValue = item.value;
          var sequenceValue = values[item.key];

          if (currentValue < sequenceValue) {
            item.value = sequenceValue;
          }
          next();
        }

        async.each(incrementSequences, checkSequence, function (err) {
          if (err) {
            return cb(err);
          }
          cb(null, values);
        });
      });
    }, function (err) {
      if (err) {
        return cb(err);
      }
      if (incrementSequences.length === 0) {
        return cb(null, results);
      }

      function setSequence(item, next) {
        if (sequenceValue === 0) {
          return next();
        }
        var sequenceName = "'\"" + table + '_' + item.key + '_seq' + "\"'";
        var sequenceValue = item.value;
        var sequenceQuery = 'SELECT setval(' + sequenceName + ', ' + sequenceValue + ', true)';

        client.query(sequenceQuery, function (err) {
          if (err) {
            return next(err);
          }
          next();
        });
      }

      async.each(incrementSequences, setSequence, function (err) {
        if (err) {
          return cb(err);
        }
        cb(null, results);
      });
    });
  }),

  // Native Join Support
  join: adapterMethod(function (client, connectionName, table, options, done) {
    // Populate associated records for each parent result
    // (or do them all at once as an optimization, if possible)
    cursor({
      instructions: options,
      nativeJoins: true,

      /**
       * Find some records directly (using only this adapter)
       * from the specified collection.
       *
       * @param collectionName
       * @param  {Object}   criteria
       * @param  {Function} _cb
       */
      $find: function (collectionName, criteria, _cb) {
        return adapter.find(conn, collectionIdentity, criteria, _cb, client);
      },

      /**
       * Look up the name of the primary key field
       * for the collection with the specified identity.
       *
       * @return {String}
       * @param collectionName
       */
      $getPK: function (collectionName) {
        if (!collectionName) {
          return;
        }
        return _getPK(connectionName, collectionName);
      },

      /**
       * Given a strategy type, build up and execute a SQL query for it.
       *
       * @param next
       * @param options
       * @param next
       */

      $populateBuffers: function populateBuffers(options, next) {
        var buffers = options.buffers;
        var instructions = options.instructions;

        // Grab the collection by looking into the connection
        var connectionObject = connections.get(connectionName);
        var collection = connectionObject.collections[table];

        var parentRecords = [];
        var cachedChildren = {};

        // Grab Connection Schema
        var schema = {};

        Object.keys(connectionObject.collections).forEach(function (coll) {
          schema[coll] = connectionObject.collections[coll].schema;
        });

        // Build Query
        var _schema = collection.waterline.schema;

        // Mixin WL Next connection overrides to sqlOptions
        var overrides = connectionOverrides[connectionName] || {};
        var _options = _.cloneDeep(sqlOptions);
        if (hop(overrides, 'wlNext')) {
          _options.wlNext = overrides.wlNext;
        }

        var sequel = new Sequel(_schema, _options);
        var _query;

        // Build a query for the specific query strategy
        try {
          _query = sequel.find(table, instructions);
        } catch (e) {
          return next(e);
        }

        async.auto({
            processParent: function (next) {
              client.query(_query.query[0], _query.values[0], function __FIND__(err, result) {
                if (err) {
                  return next(handleQueryError(err));
                }

                parentRecords = result.rows;

                var splitChildren = function (parent, next) {
                  var cache = {};

                  _.keys(parent).forEach(function (key) {
                    // Check if we can split this on our special alias identifier '___' and if
                    // so put the result in the cache
                    var split = key.split('___');
                    if (split.length < 2) {
                      return;
                    }

                    if (!hop(cache, split[0])) {
                      cache[split[0]] = {};
                    }
                    cache[split[0]][split[1]] = parent[key];
                    delete parent[key];
                  });

                  // Combine the local cache into the cachedChildren
                  if (_.keys(cache).length > 0) {
                    _.keys(cache).forEach(function (pop) {
                      if (!hop(cachedChildren, pop)) {
                        cachedChildren[pop] = [];
                      }
                      cachedChildren[pop] = cachedChildren[pop].concat(cache[pop]);
                    });
                  }

                  next();
                };

                // Pull out any aliased child records that have come from a hasFK association
                async.eachSeries(parentRecords, splitChildren, function (err) {
                  if (err) {
                    return next(err);
                  }
                  buffers.parents = parentRecords;
                  next();
                });
              });
            },

            // Build child buffers.
            // For each instruction, loop through the parent records and build up a
            // buffer for the record.
            buildChildBuffers: ['processParent', function (next) {
              async.each(_.keys(instructions.instructions), function (population, nextPop) {
                var populationObject = instructions.instructions[population];
                var popInstructions = populationObject.instructions;
                var pk = _getPK(connectionName, popInstructions[0].parent);

                var alias = populationObject.strategy.strategy === 1 ?
                  popInstructions[0].parentKey : popInstructions[0].alias;

                // Use eachSeries here to keep ordering
                async.eachSeries(parentRecords, function (parent, nextParent) {
                  var buffer = {
                    attrName: population,
                    parentPK: parent[pk],
                    pkAttr: pk,
                    keyName: alias
                  };

                  var records = [];

                  // Check for any cached parent records
                  if (hop(cachedChildren, alias)) {
                    cachedChildren[alias].forEach(function (cachedChild) {
                      var childVal = popInstructions[0].childKey;
                      var parentVal = popInstructions[0].parentKey;

                      if (cachedChild[childVal] !== parent[parentVal]) {
                        return;
                      }

                      // If null value for the parentVal, ignore it
                      if (parent[parentVal] === null) {
                        return;
                      }

                      records.push(cachedChild);
                    });
                  }

                  if (records.length > 0) {
                    buffer.records = records;
                  }

                  buffers.add(buffer);
                  nextParent();
                }, nextPop);
              }, next);
            }],

            processChildren: ['buildChildBuffers', function (next) {
              // Remove the parent query
              _query.query.shift();

              async.each(_query.query, function (q, next) {
                var qs = '';
                var pk;

                if (!Array.isArray(q.instructions)) {
                  pk = _getPK(connectionName, q.instructions.parent);
                } else if (q.instructions.length > 1) {
                  pk = _getPK(connectionName, q.instructions[0].parent);
                }

                parentRecords.forEach(function (parent) {
                  if (_.isNumber(parent[pk])) {
                    qs += q.qs.replace('^?^', parent[pk]) + ' UNION ALL ';
                  } else {
                    qs += q.qs.replace('^?^', "'" + parent[pk] + "'") + ' UNION ALL ';
                  }
                });

                // Remove the last UNION ALL
                qs = qs.slice(0, -11);

                // Add a final sort to the Union clause for integration
                if (parentRecords.length > 1) {
                  qs += ' ORDER BY ';

                  if (!Array.isArray(q.instructions)) {
                    _.keys(q.instructions.criteria.sort).forEach(function (sortKey) {
                      var direction = q.instructions.criteria.sort[sortKey] === 1 ? 'ASC' : 'DESC';
                      qs += '"' + sortKey + '"' + ' ' + direction + ', ';
                    });
                  } else if (q.instructions.length === 2) {
                    _.keys(q.instructions[1].criteria.sort).forEach(function (sortKey) {
                      var direction = q.instructions[1].criteria.sort[sortKey] === 1 ? 'ASC' : 'DESC';
                      qs += '"' + sortKey + '"' + ' ' + direction + ', ';
                    });
                  }

                  // Remove the last comma
                  qs = qs.slice(0, -2);
                }
                client.query(qs, q.values, function __FIND__(err, result) {
                  if (err) {
                    return next(handleQueryError(err));
                  }

                  var groupedRecords = {};

                  result.rows.forEach(function (row) {
                    if (!Array.isArray(q.instructions)) {
                      if (!hop(groupedRecords, row[q.instructions.childKey])) {
                        groupedRecords[row[q.instructions.childKey]] = [];
                      }

                      groupedRecords[row[q.instructions.childKey]].push(row);
                    } else {
                      // Grab the special "foreign key" we attach and make sure to remove it
                      var fk = '___' + q.instructions[0].childKey;

                      if (!hop(groupedRecords, row[fk])) {
                        groupedRecords[row[fk]] = [];
                      }

                      var data = _.cloneDeep(row);
                      delete data[fk];
                      groupedRecords[row[fk]].push(data);

                      // Ensure we don't have duplicates in here
                      groupedRecords[row[fk]] = _.uniq(groupedRecords[row[fk]], q.instructions[1].childKey);
                    }
                  });

                  buffers.store.forEach(function (buffer) {
                    if (buffer.attrName !== q.attrName) {
                      return;
                    }
                    var records = groupedRecords[buffer.belongsToPKValue];
                    if (!records) {
                      return;
                    }
                    if (!buffer.records) {
                      buffer.records = [];
                    }
                    buffer.records = buffer.records.concat(records);
                  });

                  next();
                });
              }, function () {
                next();
              });
            }]
          },
          function (err) {
            if (err) {
              return next(err);
            }
            next();
          });
      }
    }, done);
  }),

  // Select Query Logic
  find: adapterMethod(function (client, connectionName, table, options, cb) {
    // Grab Connection Schema
    var schema = {};
    var connectionObject = connections.get(connectionName);
    var collection = connectionObject.collections[table];

    Object.keys(connectionObject.collections).forEach(function (coll) {
      schema[coll] = connectionObject.collections[coll].schema;
    });

    // Build Query
    var _schema = _.mapValues(collection.waterline.schema, function (schema) {
      delete schema.attributes.transactionId;
      return schema;
    });

    var processor = new Processor(_schema);

    // Mixin WL Next connection overrides to sqlOptions
    var overrides = connectionOverrides[connectionName] || {};
    var _options = _.cloneDeep(sqlOptions);
    if (hop(overrides, 'wlNext')) {
      _options.wlNext = overrides.wlNext;
    }
    _options = _.assign(_options, options._options || {});
    options = _.omit(options, '_options');

    var sequel = new Sequel(_schema, _options);
    var _query;

    // Build a query for the specific query strategy
    try {
      _query = sequel.find(table, options);
    } catch (e) {
      return cb(e);
    }

    client.query(_query.query[0], _query.values[0], function __FIND__(err, result) {
      if (err) {
        return cb(handleQueryError(err));
      }

      // Cast special values
      var values = [];

      result.rows.forEach(function (row) {
        values.push(processor.cast(table, row));
      });

      return cb(null, values);
    });
  }),

  // Stream one or more models from the collection
  stream: function (connectionName, table, options, stream) {
    var connectionObject = connections.get(connectionName);
    var collection = connectionObject.collections[table];

    var client = new pg.Client(connectionObject.config);
    client.connect();

    var schema = {};

    Object.keys(connectionObject.collections).forEach(function (coll) {
      schema[coll] = connectionObject.collections[coll].schema;
    });

    // Build Query
    var _schema = collection.schema;
    var queryObj = new Query(_schema, schema);
    var query = queryObj.find(table, options);

    // Run Query
    var dbStream = client.query(query.query, query.values);

    //can stream row results back 1 at a time
    dbStream.on('row', function (row) {
      stream.write(row);
    });

    dbStream.on('error', function () {
      stream.end(); // End stream
      client.end(); // Close Connection
    });

    //fired after last row is emitted
    dbStream.on('end', function () {
      stream.end(); // End stream
      client.end(); // Close Connection
    });
  },

  // Update one or more models in the collection
  update: adapterMethod(function (client, connectionName, table, options, data, cb) {
    var connectionObject = connections.get(connectionName);
    var collection = connectionObject.collections[table];

    var _schema = collection.waterline.schema;
    var processor = new Processor(_schema);

    // Mixin WL Next connection overrides to sqlOptions
    var overrides = connectionOverrides[connectionName] || {};
    var _options = _.cloneDeep(sqlOptions);
    if (hop(overrides, 'wlNext')) {
      _options.wlNext = overrides.wlNext;
    }

    var sequel = new Sequel(_schema, _options);
    var query;

    // Build a query for the specific query strategy
    try {
      query = sequel.update(table, options, data);
    } catch (e) {
      return cb(e);
    }

    // Run Query
    client.query(query.query, query.values, function __UPDATE__(err, result) {
      if (err) {
        return cb(handleQueryError(err));
      }

      // Cast special values
      var values = [];

      result.rows.forEach(function (row) {
        values.push(processor.cast(table, row));
      });

      cb(null, values);
    });
  }),

  // Delete one or more models from the collection
  destroy: adapterMethod(function (client, connectionName, table, options, cb) {
    var connectionObject = connections.get(connectionName);
    var collection = connectionObject.collections[table];

    var _schema = collection.waterline.schema;

    // Mixin WL Next connection overrides to sqlOptions
    var overrides = connectionOverrides[connectionName] || {};
    var _options = _.cloneDeep(sqlOptions);
    if (hop(overrides, 'wlNext')) {
      _options.wlNext = overrides.wlNext;
    }

    var sequel = new Sequel(_schema, _options);
    var query;

    // Build a query for the specific query strategy
    try {
      query = sequel.destroy(table, options);
    } catch (e) {
      return cb(e);
    }

    // Run Query
    client.query(query.query, query.values, function __DELETE__(err, result) {
      if (err) {
        return cb(handleQueryError(err));
      }
      cb(null, result.rows);
    });
  }),

  transaction: function (connectionName, table, transaction, callback) {
    clientsCounter++;
    var id = clientsCounter;
    return (new Promise(function (resolve, reject) {
      spawnConnection(connectionName, function (client, done) {
        client._id = id;
        clients[id] = client;
        client.query('BEGIN', function (err) {
          if (err) {
            done(err);
          } else {
            transaction(
              id,
              function (res) {
                client.query('COMMIT', function (err) {
                  done(err, res);
                });
              },
              function (err, res) {
                client.query('ROLLBACK', function (_err) {
                  if (_err) {
                    sails.log.error(_err);
                  }
                  done(err, res);
                });
              }
            );
          }
        });
      }, function (err, res) {
        delete clients[id];
        clientsCounter--;
        if (err) {
          reject(err);
        } else {
          resolve(res);
        }
      });
    })).nodeify(callback);
  },
  squelQuery: adapterMethod(function (client, connectionName, table, options, cb) {
    var builder = options.builder;
    var cast = options.cast;
    if (_.isFunction(options)) {
      builder = options;
      cast = true;
    }
    var query = builder(squelPostgres).toParam();
    client.query(query.text, query.values, function __QUERY__(err, result) {
      if (err) {
        return cb(handleQueryError(err));
      }

      // Cast special values
      var values = result.rows;
      if (cast) {
        var connectionObject = connections.get(connectionName);
        var collection = connectionObject.collections[table];

        var _schema = collection.waterline.schema;
        var processor = new Processor(_schema);
        values = _.map(values, function (row) {
          return processor.cast(table, row);
        });
      }
      cb(null, values);
    });
  })
};

function adapterMethod(logic) {
  return function () {
    var cb = _.last(arguments);
    var connectionName = _.first(arguments);
    var data = arguments[2];
    var args = _(arguments).rest().initial().value();

    function _logic(client, cb) {
      logic.apply(null, [client, connectionName].concat(args).concat([cb]));
    }

    var id = data.transactionId || (data.where && data.where.transactionId);
    if (id) {
      var client = clients[data.transactionId || data.where.transactionId];
      delete data.transactionId;
      if (data.where) {
        delete data.where.transactionId;
      }
      if (client) {
        _logic(client, cb);
      } else {
        cb(new Error('Transaction ' + id + ' does not exist'));
      }
    } else {
      spawnConnection(connectionName, _logic, cb);
    }
  };
}

module.exports = adapter;
