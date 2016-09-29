var Promise = require( 'promise' );
var sqlHelper = require( './oracleHelper' );

var reverse_populate = function ( query_obj, cb ) {
    var structure = query_obj.structure
    var object = query_obj.object
    console.log( "\nUPLOAD OBJ TO DB" )
    handle_parent( object, structure ).then( function () {
        get_parent_id( object, structure ).then( function ( res ) {
            console.log( "DONE WITH OBJ UPLOAD" )
            delete_orphans( structure )
            cb( null, res )
        }, function ( err ) {
            cb( err, null )
        } )
    }, function ( err ) {
        cb( err, null )
    } )
}

var handle_parent = function ( object, structure, pk ) {
    console.log( "HANDLE PARENT", structure.table, pk )
    return new Promise( function ( resolve, reject ) {
        if ( structure.replace == true ) {
            //replace
            create_or_replace( structure, object, pk ).then( function ( id ) {
                handle_children( object, structure, id ).then( function () {
                    return resolve()
                }, function ( err ) {
                    return reject( err )
                } )
            }, function ( err ) {
                return reject( err )
            } )
        } else {
            //ignore
            create_or_find( structure, object, pk ).then( function ( id ) {
                handle_children( object, structure, id ).then( function () {
                        return resolve()
                    }, function ( err ) {
                        return reject( err )
                    } )
                    // return resolve() //maybee
            }, function ( err ) {
                return reject( err )
            } )
        }
    } )
}

var handle_children = function ( object, structure, pk ) {
    console.log( "HANDLE CHILDREN", structure.table, pk )
    return new Promise( function ( resolve, reject ) {
        if ( structure.children == undefined ) {
            console.log( "UNDEFINED CHILDREN" )
            return resolve()
        } else {
            var promises = []
            for ( var i = 0; i < structure.children.length; i++ ) {
                var child_structure = structure.children[ i ]
                var child_name = structure.children[ i ].table
                var child_obj = object[ child_name ]
                if ( child_obj == undefined ) break
                if ( child_obj.constructor === Array ) {
                    for ( var j = 0; j < child_obj.length; j++ ) {
                        var promise = handle_parent( child_obj[ j ], child_structure, pk )
                        promises.push( promise )
                    }
                } else {
                    if ( child_obj ) {
                        var promise = handle_parent( child_obj, child_structure, pk )
                        promises.push( promise )
                    } else {
                        return resolve()
                    }
                }
            }
            Promise.all( promises ).then( function ( res ) {
                return resolve( res )
            }, function ( err ) {
                return reject( err )
            } )
        }
    } )
}

var create_or_find = function ( structure, object, pk ) {
    console.log( "CREATE OR FIND", structure.table, pk )
    return new Promise( function ( resolve, reject ) {
        var table = structure.table
        if ( missing_unique_value( structure, object ) ) return reject()
        sqlHelper.findOne( {
            table: table,
            find: build_unique_value_find( structure, object, pk )
        }, function ( err, response ) {
            if ( err ) return reject( err )
            if ( response ) {
                //OBJECT ALLREADY EXISTS IN DB
                return resolve( response.id )
            } else {
                //OBJECT DOESNT ALLREADY EXISTS IN DB
                object = add_fk( structure, object, pk )
                create( table, object ).then( function () {
                    create_or_find( structure, object, pk ).then( function ( response ) {
                        return resolve( response )
                    }, function ( err ) {
                        return reject( err )
                    } )
                }, function ( err ) {
                    return reject( err )
                } )
            }
        } )
    } )
}

var create_or_replace = function ( structure, object, pk ) {
    console.log( "CREATE OR REPLACE", structure.table, pk )
    return new Promise( function ( resolve, reject ) {
        var table = structure.table
        if ( missing_unique_value( structure, object ) ) return reject()
        sqlHelper.findOne( {
            table: table,
            find: build_unique_value_find( structure, object, pk )
        }, function ( err, response ) {
            if ( err ) return reject( err )
            if ( response ) {
                //OBJECT ALLREADY EXISTS IN DB
                remove( table, response.id ).then( function () {
                    create_or_replace( structure, object, pk ).then( function ( response ) {
                        resolve( response )
                    }, function ( err ) {
                        return reject( err )
                    } )
                }, function ( err ) {
                    return reject( err )
                } )
            } else {
                //OBJECT DOESNT ALLREADY EXIST IN DB
                object = add_fk( structure, object, pk )
                create( table, object ).then( function () {
                    sqlHelper.findOne( {
                        table: table,
                        find: build_unique_value_find( structure, object, pk )
                    }, function ( err, response ) {
                        if ( err ) return reject( err )
                        resolve( response.id )
                    } )
                }, function ( err ) {
                    return reject( err )
                } )
            }
        } )
    } )
}

var remove = function ( table, id ) {
    return new Promise( function ( resolve, reject ) {
        sqlHelper.remove( {
            table: table,
            id: id
        }, function ( err, res ) {
            if ( err ) return reject( err )
            return resolve()
        } )
    } )
}

var create = function ( table, object ) {
    return new Promise( function ( resolve, reject ) {
        sqlHelper.create( {
            table: table,
            object: object
        }, function ( err, res ) {
            if ( err ) return reject( err )
            return resolve()
        } )
    } )
}

var get_parent_id = function ( object, structure ) {
    console.log( "GET PARENT ID" )
    return new Promise( function ( resolve, reject ) {
        sqlHelper.findOne( {
            table: structure.table,
            find: build_unique_value_find( structure, object )
        }, function ( err, response ) {
            if ( err ) reject( err )
            console.log( response )
            if ( response.id == undefined ) reject( err )
            resolve( {
                id: response.id
            } )
        } )
    } )
}

var delete_orphans = function ( structure ) {
    if ( structure.children = !undefined ) {
        for ( var i = 0; i < structure.children.length; i++ ) {
            delete_orpans( structure.children[ i ] )
        }
    }
}

//HELPERS

var add_fk = function ( structure, object, pk ) {
    if ( pk ) object[ structure.fk ] = pk
    return object
}

var build_unique_value_find = function ( structure, object, pk ) {
    var res = {}
    var unique_values = structure.unique_values
    if ( unique_values == undefined ) return res
    for ( var i = 0; i < unique_values.length; i++ ) {
        var value = unique_values[ i ]
        res[ value ] = object[ value ]
    }
    if ( pk ) res[ structure.fk ] = pk
    return res
}

var missing_unique_value = function ( structure, object ) {
    var unique_values = structure.unique_values
    if ( unique_values == undefined ) return true
    for ( var i = 0; i < unique_values.length; i++ ) {
        var value = unique_values[ i ]
        if ( object[ value ] == undefined ) return true
        if ( object[ value ] == null ) return true
        if ( object[ value ] == "null" ) return true
        if ( object[ value ] == "" ) return true
    }
    return false
}

var delete_orphans = function ( structure ) {
    console.log( "DELETE ORPHANS" )
    if ( structure.children != undefined ) {
        for ( var i = 0; i < structure.children.length; i++ ) {
            var child = structure.children[ i ].table
            var parent = structure.table
            var fk = structure.children[ i ].fk
            var sql = "delete from ? where ?.? not in (select id from ?)"
            sql = sqlHelper.format( sql, [ child, child, fk, parent ] )
            delete_orphans( structure.children[ i ] )
            sqlHelper.query( {
                sql: sql
            }, function ( err, res ) {
                if ( err ) console.log( err )
            } )
        }
    }
}

module.exports = reverse_populate
