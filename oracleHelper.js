//REQUIRED MODULES

var oracle = require( 'oracledb' );
oracle.autoCommit = true;
oracle.outFormat = oracle.OBJECT
oracle.maxRows = 1000

//PRIVATE VARIABLES

var db_data = {}
var prefs = {
    default_db_name: "",
    log_sql: false,
    log_errors: true
}

//PUBLIC CONFIG METHODS

var config = function ( config ) {
    if ( config.log_sql ) {
        prefs.log_sql = config.log_sql;
    }
    if ( config.log_errors ) {
        prefs.log_errors = config.log_errors
    }
    if ( config.default_db ) {
        prefs.default_db_name = config.default_db
        console.log( "%s is now the current Database.", default_db )
    }
}

var connect = function ( config, cb ) {
    oracle.createPool( {
        user: config.user,
        password: config.password,
        connectString: create_conn_string( config ),
        poolMax: config.connectionLimit
    }, function ( err, pool ) {
        if ( err ) {
            handle_error( err )
            cb( null )
        } else {

            var db_name = config.database
            prefs.default_db_name = db_name
            db_data[ db_name ] = {
                pool: pool,
                db_type: "oracle"
            }
            console.log( "connected to Oracle database sucessfully: " + db_name )

            get_db_columns( pool, db_name, function ( table_info ) {
                db_data[ db_name ].tables = table_info
                cb()
            } )

        }
    } )
}

//PUBLIC QUERY METHODS

var format = function ( string, values ) {
    var final_string = ""
    var string_parts = string.split( "?" )
    for ( var i = 0; i < string_parts.length; i++ ) {
        final_string += string_parts[ i ]
        if ( values.constructor === Array ) {
            if ( i != string_parts.length - 1 ) final_string += values[ i ]
        } else {
            if ( i != string_parts.length - 1 ) final_string += values
        }
    }
    return final_string
}

var query = function ( query_obj, cb ) {
    var query_obj = clean_query_obj( query_obj )
    var pool = db_data[ query_obj.db_name ].pool
    if ( !query_obj.values ) query_obj.values = []
    if ( prefs.log_sql ) console.log( "\n\t" + query_obj.sql )
    pool.getConnection( function ( err, conn ) {
        if ( err ) {
            handle_error( err )
            cb( err )
        } else {
            conn.execute( query_obj.sql, query_obj.values, function ( err, res ) {
                conn.close( function () {
                    if ( err ) {
                        handle_error( err, "SQL: " + query_obj.sql )
                        if ( cb ) cb( err, [] )
                    } else {
                        if ( res.rows != undefined ) {
                            res.rows = convert_obj_to_lowercase( res.rows )
                            if ( cb ) cb( null, res.rows )
                        } else {
                            if ( cb ) cb( null, res )
                        }
                    }
                } )
            } )
        }
    } )
}

var get = function ( query_obj, cb ) {
    var query_obj = clean_query_obj( query_obj )
    var table_pk = db_data[ query_obj.db_name ].tables[ query_obj.table ].pk
    query_obj.sql = format( "select * from ? where ? = ?", [ query_obj.table, table_pk, query_obj.id ] )
    query( query_obj, function ( err, rows ) {
        if ( rows.length > 0 ) {
            cb( err, rows[ 0 ] )
        } else {
            cb( err, null )
        }
    } )
}

var all = function ( query_obj, cb ) {
    var query_obj = clean_query_obj( query_obj )
    query_obj.sql = "select * from " + query_obj.table
    query( query_obj, cb )
}

var find = function ( query_obj, cb ) {
    var query_obj = build_find_statement( query_obj )
    query( query_obj, cb )
}

var findOne = function ( query_obj, cb ) {
    var query_obj = build_find_statement( query_obj )
    query_obj.sql += " and ROWNUM <= 1"
    query( query_obj, function ( err, rows, cols ) {
        if ( !rows || rows.length == 0 ) {
            cb( err, null, null )
        } else {
            cb( err, rows[ 0 ], cols )
        }
    } )
}

var build_find_statement = function ( query_obj ) {
    var query_obj = clean_query_obj( query_obj )
    var conditions = query_obj.find
    query_obj.sql = format( "select * from ? where ", [ query_obj.table ] )
    for ( field in conditions ) {
        if ( typeof conditions[ field ] === 'object' ) {
            for ( operator in conditions[ field ] ) {
                var value = conditions[ field ][ operator ]
                query_obj.sql += format( " ( ? ? '?') AND ", [ field, operator, value ] )
            }
        } else {
            var value = conditions[ field ]
            query_obj.sql += format( " ( ? = '?') AND ", [ field, value ] )
        }
    }
    query_obj.sql = query_obj.sql.substring( 0, query_obj.sql.length - 4 )
    return query_obj
}

var create = function ( query_obj, cb ) {
    var query_obj = clean_query_obj( query_obj )
    var table_columns = db_data[ query_obj.db_name ].tables[ query_obj.table ].columns
    var table_pk = db_data[ query_obj.db_name ].tables[ query_obj.table ].pk
    query_obj.sql = format( "insert into ? (", [ query_obj.table ] )
    for ( var i = 0; i < table_columns.length; i++ ) {
        var column = table_columns[ i ].name
        if ( column != table_pk ) {
            if ( i != 0 ) query_obj.sql += ", "
            query_obj.sql += column
        }
    }
    query_obj.sql += ") values ("
    for ( var i = 0; i < table_columns.length; i++ ) {
        var column = table_columns[ i ].name
        if ( column != table_pk ) {
            if ( i != 0 ) query_obj.sql += ", "
            if ( query_obj.object[ column.toLowerCase() ] != undefined ) {
                var value = query_obj.object[ column.toLowerCase() ]
                query_obj.sql += format( "'?'", value )
            } else if ( query_obj.object[ column ] != undefined ) {
                var value = query_obj.object[ column ]
                query_obj.sql += format( "'?'", value )
            } else {
                query_obj.sql += "null"
            }
        }
    }
    query_obj.sql += ")"
    query( query_obj, cb )
}

var update = function ( query_obj, cb ) {
    var query_obj = clean_query_obj( query_obj )
    var pk_column_name = db_data[ query_obj.db_name ].tables[ query_obj.table ].pk
    var id = query_obj.object[ pk_column_name ]
    query_obj.object = clean_object_for_insertion( query_obj.table, query_obj.object, query_obj.db_name )
    query_obj.sql = format( "update ? set ", table )
    for ( field in object ) {
        if ( i != 0 ) query_obj.sql += ", "
        query_obj.sql += format( "? = '?''", [ field, object[ field ] ] )
    }
    query_obj.sql += format( " where ?? = ? ", [ pk_column_name, id ] )
    query( query_obj, cb );
}

var remove = function ( query_obj, cb ) {
    var query_obj = clean_query_obj( query_obj )
    var pk_column_name = db_data[ query_obj.db_name ].tables[ query_obj.table ].pk
    query_obj.sql = format( "delete from ? where ? = ?", [ query_obj.table, pk_column_name, query_obj.id ] )
    query( query_obj, cb );
}

var reverse_populate = function ( query_obj, cb ) {
    var reverse_populate = require( './reverse_populate' )
    var query_obj = clean_query_obj( query_obj )
    reverse_populate( query_obj, cb )
}

var populate = function ( query_obj, cb ) {
    var populate = require( './populate' )
    var query_obj = clean_query_obj( query_obj )
    populate( query_obj, db_data, cb )
}

/////////////////////////////////////////////////////////////////////////////////////////////////
//PRIVATE METHODS

var create_conn_string = function ( config ) {
    if ( config.conn_string == undefined ) {
        return config.host + ":" + config.port + "/" + config.service_name
    } else {
        return config.conn_string
    }
}

var convert_obj_to_lowercase = function ( obj, cb ) {
    //converts an objects members to lowercase and all its children no matter how deep cause recurssion
    var response
    if ( obj == null ) return null
    if ( obj.constructor === Array ) {
        //ARRAY
        response = []
        for ( var i = 0; i < obj.length; i++ ) {
            response[ i ] = convert_obj_to_lowercase( obj[ i ] )
        }
    } else if ( typeof obj === 'object' && obj != null ) {
        //OBJ
        response = {}
        for ( var prop in obj ) {
            if ( obj.hasOwnProperty( prop ) ) {
                response[ prop.toLowerCase() ] = convert_obj_to_lowercase( obj[ prop ] )
            }
        }
    } else {
        response = obj
    }
    return response
}

var handle_error = function ( err, extra_info ) {
    if ( prefs.log_errors ) {
        console.log( "\nSQL-HELPER ERROR:" )
        if ( err ) console.log( "\t", err )
        if ( extra_info ) console.log( "\t", extra_info )
    }
}

var get_db_columns = function ( pool, db_name, cb ) {
    pool.getConnection( function ( err, connection ) {
            if ( err ) handle_error( err )
            var sql = "select * from ( select table_name, column_name, null IS_PK from USER_TAB_COLUMNS union " +
                " select table_name, COLUMN_NAME, 'true' IS_PK FROM ALL_CONS_COLUMNS  WHERE CONSTRAINT_NAME IN " +
                " ( SELECT CONSTRAINT_NAME FROM ALL_CONSTRAINTS where CONSTRAINT_TYPE = 'P' ) " +
                " ) order by table_name"
                //var sql = "SELECT table_name, column_name, data_type, data_length FROM USER_TAB_COLUMNS",
            connection.execute(
                    sql,
                    function ( err, res ) {
                        if ( !err ) {
                            //console.log( rows )
                            var response = {}
                            var rows = res.rows
                            for ( var i = 0; i < rows.length; i++ ) {
                                var column_name = rows[ i ].COLUMN_NAME.toLowerCase()
                                var table = rows[ i ].TABLE_NAME.toLowerCase()
                                var type // = rows[ i ].DATA_TYPE
                                var isPK = ( rows[ i ].IS_PK == "true" )

                                //create object for new table in db_data
                                if ( response[ table ] == undefined ) {
                                    response[ table ] = {
                                        columns: []
                                    }
                                }

                                if ( isPK ) {
                                    response[ table ].pk = column_name;
                                } else {
                                    var column = {
                                        name: column_name,
                                        type: type
                                    }
                                    response[ table ].columns.push( column )
                                }

                            }
                            console.log( "--Database sucessfully parsed for column names" )
                            if ( cb != undefined ) cb( response )
                        } else {
                            console.log( "--Database DID NOT sucessfully parsed for column names" )
                            handle_error( err )
                            if ( cb != undefined ) cb( null )
                        }
                    } ) //execute()
        } ) //getConneciton()
}

var clean_object_for_insertion = function ( table_name, dirty_object, db_name ) {
    var table_columns = db_data[ db_name ].tables[ table_name ].columns
    if ( table_columns != undefined ) {
        var pk_name = db_data[ db_name ].tables[ table_name ].pk
        table_columns.push( {
            name: pk_name
        } )
        var clean_object = {}
        for ( var i = 0; i < table_columns.length; i++ ) {
            var column_name = table_columns[ i ].name
            if ( dirty_object[ column_name ] != undefined ) {
                clean_object[ column_name ] = dirty_object[ column_name ]
            }
        }
        return clean_object
    } else {
        handle_error( "couldn't clean object" )
        return dirty_object
    }
}

var clean_query_obj = function ( query_obj ) {
    var isNull = ( !query_obj.db_name )
    var isUndefined = ( query_obj.db_name == undefined )
    if ( isNull || isUndefined ) {
        query_obj.db_name = prefs.default_db_name
    }
    var db_name = query_obj.db_name
    query_obj.db_type = db_data[ db_name ].db_type
    if ( query_obj.values == undefined ) {
        query_obj.values = null
    }
    return query_obj
}

//EXPORT OF PUBLIC METHODS TO USER
module.exports = {
    connect: connect,
    config: config,
    query: query,
    populate: populate,
    reverse_populate: reverse_populate,
    create: create,
    get: get,
    all: all,
    find: find,
    findOne: findOne,
    format: format,
    remove: remove,
    update: update
}
