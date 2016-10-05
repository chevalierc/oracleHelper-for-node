var chai = require( 'chai' );
var assert = chai.assert;
var expect = chai.expect;

var sqlHelper = require( './oracleHelper.js' )
var db_config = require( '../app/config/config' ).database

describe( 'Connecting to the database', function () {
    this.timeout( 15000 );
    var table_info

    it( 'Should connect to the database', function ( done ) {
        sqlHelper.connect( db_config, function ( db_data ) {

            sqlHelper.config( {
                log_sql: true
            } )

            expect( db_data[ db_config.database ] ).to.not.be.undefined;
            expect( db_data[ db_config.database ].pool ).to.not.be.undefined;

            table_info = db_data[ db_config.database ].tables

            done()
        } );
    } )

    it( 'Should parse the Database for column names correctly', function () {
        expect( table_info ).to.not.be.undefined;
        // expect(table_info)
    } )

} )

describe( 'Be able to do basic querying', function () {
    this.timeout( 15000 );

    before( function ( done ) {
        sqlHelper.query( {
            // sql: "CREATE TABLE :tablename( foo varchar2(32), bar varchar(32) )",
            sql: "create table foobar (id number(10) primary key, foo varchar2(32), bar varchar2(32) ) "
        }, function ( err, res ) {
            sqlHelper.connect( db_config, function ( db_data ) {
                //reconnect to get db-data
                expect( db_data[ db_config.database ].tables.foobar.pk ).to.be.equal( "id" )
                expect( db_data[ db_config.database ].tables.foobar.columns.length ).to.be.equal( 3 )
                done()
            } );
        } )
    } );

    var foobar = {
        id: 1,
        foo: "42",
        bar: "42"
    }
    var foobar_from_db
    var foobar_id

    it( 'Should be able to execute create()', function ( done ) {
        sqlHelper.create( {
            table: "foobar",
            object: foobar
        }, function ( err, rows, cols ) {
            expect( err ).to.be.null;
            done()
        } )
    } )

    it( 'Should be able to execute all()', function ( done ) {
        sqlHelper.all( {
            table: "foobar"
        }, function ( err, rows, cols ) {
            expect( err ).to.be.null
            expect( rows[ 0 ] ).to.not.be.undefined
            expect( rows[ 0 ] ).to.deep.equal( foobar )
            expect( rows[ 0 ].id ).to.not.be.null
            expect( rows[ 0 ].id ).to.not.be.undefined
            foobar_id = rows[ 0 ].id
            done()
        } )
    } )

    it( 'Should be able to execute get()', function ( done ) {
        sqlHelper.get( {
            table: "foobar",
            id: foobar_id
        }, function ( err, foobar, cols ) {
            expect( err ).to.be.null
            expect( foobar.id ).to.not.be.undefined
            expect( foobar.foo ).to.equal( '42' )
            expect( foobar.bar ).to.equal( '42' )
            foobar_from_db = foobar
            done()
        } )
    } )

    it( 'Should be able to execute update()', function ( done ) {
        foobar_from_db.foo = '43'
        sqlHelper.update( {
            table: "foobar",
            object: foobar_from_db
        }, function ( err, rows, cols ) {
            expect( err ).to.not.be.undefined
            sqlHelper.get( {
                table: "foobar",
                id: foobar_id
            }, function ( err, foobar, cols ) {
                expect( err ).to.be.null
                expect( foobar.foo ).to.equal( '43' )
                foobar_from_db = foobar
                done()
            } )
        } )
    } )

    it( 'Should be able to execute find()', function ( done ) {
        sqlHelper.find( {
            table: "foobar",
            find: {
                foo: 43
            }
        }, function ( err, rows, cols ) {
            expect( err ).to.be.null
            expect( rows[ 0 ] ).to.deep.equal( foobar_from_db )
            done()
        } )
    } )

    it( 'Should be able to execute findOne()', function ( done ) {
        foobar_from_db.id = 2
        sqlHelper.create( {
            table: "foobar",
            object: foobar_from_db
        }, function ( err, rows, cols ) {
            sqlHelper.findOne( {
                table: "foobar",
                find: {
                    foo: 43
                }
            }, function ( err, foobar, cols ) {
                expect( err ).to.be.null
                expect( foobar ).to.have.property( 'foo' );
                done()
            } )
        } )
    } )

    it( 'Should be able to execute remove()', function ( done ) {
        sqlHelper.remove( {
            table: "foobar",
            id: 1
        }, function ( err, rows, cols ) {
            sqlHelper.find( {
                table: "foobar",
                id: 1
            }, function ( err, rows, cols ) {
                expect( err ).to.be.null
                expect( rows.length ).to.be.equal( 0 )
                done()
            } )
        } )
    } )

    after( function ( done ) {
        sqlHelper.query( {
            sql: "DROP TABLE foobar"
        }, function ( err, res ) {
            done()
        } )
    } );

} )

describe( 'Be able to do the populate functions', function () {
    this.timeout( 20000 );
    before( function ( done ) {
        sqlHelper.query( {
            // sql: "CREATE TABLE :tablename( foo varchar2(32), bar varchar(32) )",
            sql: "create table foo (id number(10) primary key, foo_level number(2) ) "
        }, function ( err, res ) {
            sqlHelper.query( {
                // sql: "CREATE TABLE :tablename( foo varchar2(32), bar varchar(32) )",
                sql: "create table bar (id number(10) primary key, foo_id_fk number(10), bar_level number(2) ) "
            }, function ( err, res ) {
                sqlHelper.connect( db_config, function ( db_data ) {
                    //reconnect to get db-data
                    expect( db_data[ db_config.database ].tables.foo.pk ).to.be.equal( "id" )
                    expect( db_data[ db_config.database ].tables.bar.pk ).to.be.equal( "id" )
                    done()
                } )
            } )
        } )
    } );

    var join_schema = {
        table: "foo",
        unique_values: [ "foo_level" ],
        children: [ {
            table: "bar",
            unique_values: [ "bar_level" ],
            fk: "foo_id_fk",
        } ]
    }

    var object = {
        id: 1,
        foo_level: 42,
        bar: [ {
            id: 1,
            bar_level: 22
        }, {
            id: 2,
            bar_level: 23
        } ]
    }

    it( 'Should be able to reverse_populate()', function ( done ) {
        sqlHelper.reverse_populate( {
            structure: join_schema,
            object: object
        }, function ( err, rows, cols ) {
            sqlHelper.all( {
                table: "foo"
            }, function ( err, rows, cols ) {
                expect( err ).to.be.null
                expect( rows.length ).to.be.equal( 1 )
                sqlHelper.all( {
                    table: "bar"
                }, function ( err, rows, cols ) {
                    expect( err ).to.be.null
                    expect( rows.length ).to.be.equal( 2 )
                    expect( rows[ 0 ].foo_id_fk ).to.be.equal( 1 )
                    done()
                } )
            } )
        } )
    } )

    var populate_return

    it( 'Should be able to populate()', function ( done ) {
        sqlHelper.populate( {
            structure: join_schema,
            sql: "where foo.id = 1"
        }, function ( err, rows, cols ) {
            expect( err ).to.be.null
            expect( rows ).to.not.be.undefined
            expect( rows[0].bar.length ).to.be.equal( 2 )
            populate_return = rows[0]
            done()
        } )
    } )

    it( 'Should be able to reverse_populate() with the response of populate', function ( done ) {
        sqlHelper.reverse_populate( {
            structure: join_schema,
            object: populate_return
        }, function ( err, rows, cols ) {
            sqlHelper.all( {
                table: "foo"
            }, function ( err, rows, cols ) {
                expect( err ).to.be.null
                expect( rows.length ).to.be.equal( 1 )
                sqlHelper.all( {
                    table: "bar"
                }, function ( err, rows, cols ) {
                    expect( err ).to.be.null
                    expect( rows.length ).to.be.equal( 2 )
                    expect( rows[ 0 ].foo_id_fk ).to.be.equal( 1 )
                    done()
                } )
            } )
        } )
    } )
