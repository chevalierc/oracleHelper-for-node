var sqlHelper = require( './oracleHelper' );
var db_data = {}

var populate = function ( query_obj, passed_db_data, cb ) {
    db_data = passed_db_data
    var structure = query_obj.structure
    var extra_sql = query_obj.sql
    var db_name = query_obj.db_name

    var sql = "select "
    sql += build_select_sql( structure, db_name )
    sql += "\n null from " + structure.table + "\n ";
    sql += build_join_sql( structure )
    if ( extra_sql != undefined ) sql += extra_sql

    sqlHelper.query( {
        sql: sql
    }, function ( err, rows ) {
        if ( err || rows.length > 0 ) {
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

module.exports = populate
