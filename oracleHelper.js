//REQUIRED MODULES

var oracle = require( 'oracledb' );
oracle.autoCommit = true;
oracle.outFormat = oracle.OBJECT
oracle.maxRows = 1000

//PRIVATE VARIABLES

var db_data = {}
var default_db_name = ""
var log_sql = false;
var log_errors = true;

//PRIVATE METHODS

var format = function ( string, values ) {
    var final_string = ""
    var string_parts = string.split( "?" )
    for ( var i = 0; i < string_parts.length; i++ ) {
        final_string += string_parts[ i ]
        if ( i != string_parts.length - 1 ) final_string += values[ i ]
    }
    return final_string
}

var get_db_columns = function ( pool, db_name, cb ) {
    pool.getConnection( function ( err, connection ) {
            if ( err ) console.log( err )
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
                                var column_name = rows[ i ].COLUMN_NAME
                                var table = rows[ i ].TABLE_NAME
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
                            if ( cb != undefined ) cb( response )
                        } else {
                            console.log( err )
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
        console.log( "couldn't clean object" )
        return dirty_object
    }
}

var clean_query_obj = function ( query_obj ) {
    var isNull = ( !query_obj.db_name )
    var isUndefined = ( query_obj.db_name == undefined )
    if ( isNull || isUndefined ) {
        query_obj.db_name = default_db_name
    }
    var db_name = query_obj.db_name
    query_obj.db_type = db_data[ db_name ].db_type
    if ( query_obj.values == undefined ) {
        query_obj.values = null
    }
    return query_obj
}

//PUBLIC CONFIG METHODS

var config = function ( config ) {
    if ( config.log_sql ) {
        log_sql = config.log_sql;
    }
    if ( config.log_errors ) {
        log_errors = config.log_errors
    }
    if ( config.default_db ) {
        default_db_name = config.default_db
        console.log( "%s is now the current Database.", default_db )
    }
}

var connect = function ( config, cb ) {
    var db_name = config.database
    var connString = config.host + ":" + config.port + "/" + config.service_name
    oracle.createPool( {
        user: config.user,
        password: config.password,
        connectString: connString,
        poolMax: config.connectionLimit
    }, function ( err, pool ) {
        if ( err ) {
            console.log( err )
            cb( null )
        } else {
            console.log( "connected to Oracle database sucessfully: " + db_name )

            get_db_columns( pool, db_name, function ( table_info ) {
                default_db_name = db_name
                db_data[ db_name ] = {
                    pool: pool,
                    db_type: "oracle",
                    tables: table_info
                }
                cb()

            } )
        }
    } )
}

//PUBLIC QUERY METHODS

var query = function ( query_obj, cb ) {
    query_obj = clean_query_obj( query_obj )
    var db_name = query_obj.db_name
    var pool = db_data[ db_name ].pool
    var sql = query_obj.sql
    var values = query_obj.values
    if ( !values ) values = []
    if ( log_sql ) console.log( sql )
    pool.getConnection( function ( err, conn ) {
        if ( err ) {
            console.log( err )
            cb( err )
        } else {
            conn.execute( sql, values, function ( err, res ) {
                if ( err ) {
                    console.log( err )
                    cb( err )
                } else {
                    if ( res.rows != undefined ) {
                        cb( null, res.rows )
                    } else {
                        cb( null, res )
                    }
                }
                conn.close()
            } )
        }
    } )
}

var get = function ( query_obj, cb ) {
    query_obj = clean_query_obj( query_obj )
    var table = query_obj.table
    var db_name = query_obj.db_name
    var table_pk = db_data[ db_name ].tables[ table ].pk
    var id = query_obj.id
    query_obj.sql = format( "select * from ? where ? = ?", [ table, table_pk, id ] )
    query( query_obj, function ( err, rows ) {
        if ( rows.length > 0 ) {
            cb( err, rows[ 0 ] )
        } else {
            cb( err, null )
        }
    } )
}

var all = function ( query_obj, cb ) {
    query_obj = clean_query_obj( query_obj )
    query_obj.sql = "select * from " + query_obj.table

    query( query_obj, cb )
}

var find = function ( query_obj, cb ) {
    query_obj = buildFindStatement( query_obj )
    query( query_obj, cb );
}

var findOne = function ( query_obj, cb ) {
    query_obj = buildFindStatement( query_obj )
    query_obj.sql += " and ROWNUM <= 1"

    query( query_obj, function ( err, rows, cols ) {
        if ( cols == 0 ) {
            cb( err, null, null )
        } else {
            cb( err, rows[ 0 ], cols )
        }
    } );
}

var buildFindStatement = function ( query_obj ) {
    query_obj = clean_query_obj( query_obj )
    var table = query_obj.table
    var conditions = query_obj.find
    query_obj.sql = format( "select * from ? where ", [ table ] )
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
    query_obj = clean_query_obj( query_obj )
    var table = query_obj.table
    var db_name = query_obj.db_name
    var object = query_obj.object
    var table_columns = db_data[ db_name ].tables[ table ].columns
    var table_pk = db_data[ db_name ].tables[ table ].pk
    var sql = format( "insert into ? (", [ table ] )
    for ( var i = 0; i < table_columns.length; i++ ) {
        var column = table_columns[ i ].name
        if ( column != table_pk ) {
            sql += format( "?, ", [ column ] )
        }
    }
    sql = sql.substring( 0, sql.length - 2 ) + ") values ("
    for ( var i = 0; i < table_columns.length; i++ ) {
        var column = table_columns[ i ].name
        if ( column != table_pk ) {
            var value
            if ( object[ column.toLowerCase() ] != undefined ) {
                value = object[ column.toLowerCase() ]
            } else if ( object[ column ] != undefined ) {
                value = object[ column ]
            } else {
                value = "null"
            }
            sql += format( "'?', ", [ value ] )
        }
    }
    sql = sql.substring( 0, sql.length - 2 ) + ")"
    query_obj.sql = sql
    query( query_obj, cb )
}

var update = function ( query_obj, cb ) {
    query_obj = clean_query_obj( query_obj )
    var table = query_obj.table
    var object = query_obj.object
    var db = query_obj.db_name
    var pk_column_name = db_data[ db_name ].tables[ table ].pk
    var id = object[ pk_column_name ]
    object = clean_object_for_insertion( table, object, db )
    query_obj.sql = format( "update ? set ", [ table ] )
    for ( field in object ) {
        if ( i != 0 ) query_obj.sql += ", "
        query_obj.sql += format( "? = '?''", [ field, object[ field ] ] )
    }
    query_obj.sql += format( " where ?? = ? ", [ pk_column_name, id ] )
    query( query_obj, cb );
}

var remove = function ( query_obj, cb ) {
    query_obj = clean_query_obj( query_obj )
    var id = query_obj.id
    var table = obj.table
    var db_name = query_obj.db_name
    var pk_column_name = db_data[ db_name ].tables[ table ].pk
    query_obj.sql = format( "delete from ? where ? = ?", [ table, pk_column_name, id ] )
    query( query_obj, cb );
}


//POPULATE PUBLIC METHOD FOLLOWED BY ITS HELPERS -- located here due to amount of private methods

var populate = function ( query_obj, cb ) {
    query_obj = clean_query_obj( query_obj )
    var structure = query_obj.structure
    var extra_sql = query_obj.sql
    var db_name = query_obj.db_name

    var sql = "select "
    sql += build_select_sql( structure, db_name )
    sql += "\n null from " + structure.table + "\n ";
    sql += build_join_sql( structure )
    if ( extra_sql != undefined ) sql += extra_sql

    query( {
        sql: sql
    }, function ( err, rows ) {
        if ( rows.length > 0 || err ) {
            obj = build_object( rows, structure, db_name, null, null )
            cb( err, obj )
        } else {
            cb( err, null, null )
        }

    } )
}

var to_sql = function ( table, column ) {
    //oracle has limits on alias length of 30. To avoid and still have unique name did the below.
    //Can still have ununique identifier in rare ocasions
    return ( table.substr( table.length - 2 ) + table.length + "_" + column )
}

var build_select_sql = function ( structure, db_name ) {
    var sql = ""
    var table = structure.table;
    var columns = db_data[ db_name ].tables[ table ].columns;
    var pk = db_data[ db_name ].tables[ table ].pk;
    //add table columns to sql select statement
    for ( var i = 0; i < columns.length; i++ ) {
        sql += " " + table + "." + columns[ i ].name + " " + to_sql( table, columns[ i ].name ) + ", \n"
    }

    if ( structure.children != undefined ) {
        for ( var i = 0; i < structure.children.length; i++ ) {
            var child = structure.children[ i ]
            sql += build_select_sql( child, db_name )
        }
    }

    return sql
}

var build_join_sql = function ( structure ) {
    var sql = ""
    if ( structure.children != undefined ) {
        for ( var i = 0; i < structure.children.length; i++ ) {
            var parent_table = structure.table
            var child_obj = structure.children[ i ]
            var child_table = child_obj.table
            var fk = child_table + "." + child_obj.fk
            sql += " left join " + child_table + " on " + fk + "=" + parent_table + ".id \n"
            sql += build_join_sql( child_obj )
        }
    }
    return sql
}

var build_object = function ( data, structure, db_name, parent_id, parent_fk ) {
    var obj = []
    var table = structure.table //srid
    var table_id_column = db_data[ db_name ].tables[ table ].pk
    table_id_column = to_sql( table, table_id_column ) //srid__id

    var unique_ids = []
    for ( var i = 0; i < data.length; i++ ) {
        //go row by row looking for children
        var fk_val
        var is_child = false;
        if ( structure.fk != undefined ) {
            var fk_column = to_sql( structure.table, structure.fk ) //srid__compound_id_fk
            fk_val = data[ i ][ fk_column ] //1
            is_child = ( fk_val == parent_id )
        }
        if ( parent_id == null ) {
            is_child = true
        }

        var child_id = data[ i ][ table_id_column ] //1

        var notUsedYet = ( unique_ids.indexOf( child_id ) == -1 )

        if ( notUsedYet && is_child ) {
            unique_ids.push( child_id )
            var cur_obj = cleanObject( data[ i ], table, db_name )
                //look for children
            if ( structure.children != undefined ) {
                for ( var j = 0; j < structure.children.length; j++ ) {
                    var child = structure.children[ j ]
                    cur_obj[ child.table ] = build_object( data, child, db_name, child_id )
                }
            }
            obj.push( cur_obj )
        }
    }
    return obj
}

var cleanObject = function ( data, table, db_name ) {
    var columns = db_data[ db_name ].tables[ table ].columns
    var id = db_data[ db_name ].tables[ table ].pk
    var obj = {};
    for ( var i = 0; i < columns.length; i++ ) {
        var column = to_sql( table, columns[ i ].name )
        if ( data[ column ] ) {
            obj[ columns[ i ].name ] = data[ column ]
        }
    }
    return obj
}


//EXPORT OF PUBLIC METHODS TO USER
module.exports = {
    connect: connect,
    config: config,
    query: query,
    populate: populate,
    create: create,
    get: get,
    all: all,
    find: find,
    findOne: findOne,
    format: format,
    update: remove
}
