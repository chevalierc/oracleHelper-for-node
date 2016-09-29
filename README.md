# oracleHelper-for-node
###Utility for simplifying work with Oracle databases in the Node.JS enviroment

###About:

Built to make DB interaction as simple as possible, while still handling all basic application needs.
Automaticly detects primary key and table columns when needed then stores them for future method calls.

###Caveats:

DOES NOT PREVENT SQL-INJECTION at the moment

Not fully tested at the moment.

Population() function requires column names to be 26 charachters at most. No table name can have the same last 2 charachters and length

###Install:

Requires [oracledb official node driver](https://github.com/oracle/node-oracledb) that isn't the easiest to install. Have fun.

###Prefer mysql?

If you prefer mysql (as I do), I created an identical [helper for mysql](https://github.com/chevalierc/mysqlHelper-for-node) that is injection-proof. The usage is identical with the exception of how query() is used with values.

#Basic Usage
```
var dbConfig = database: {
        host: "localhost",
        user: "admin",
        password: 'pw',
        database: "pinballMachine",
        service_name: 'pinball service
        connectionLimit: 20,
    }
    
sqlHelper.connect( dbConfig );

sqlHelper.config( {
    log_sql: true,
    log_errors: true
} );

var id = 233
sqlhelper.get({
        table:"users",
        id: id
}, function(err, rows, cols){
    if(!err){
        var user = rows[0]
    }
})
```
#All Functions

##Configuration

The most recent connected database will be your default database. You can change that using the config file. It will be used for any query you do not specify a database in the query object. 

```
var config = {
        host: "localhost",
        user: "admin",
        password: 'pw',
        database: "pinballMachine",
        connectionLimit: 20,
}
sqlHelper.connect( config, function(){
        query_db()
)

//The Callback is recomended but not required. During connection the helper grabs table columns information for its use in several methods, specificaly: get(), create(), update(), remove() & populate()
```
```
sqlHelper.config( {
    log_sql: true,                      //log executed sql
    log_errors: true,                   //log errors
    default_db: "customer_backup_db"    //default Databse, that will be used if a database is not specified in a query_object
} )
```

##Querying

All methods follow the format: `sqlHelper.method( query_object, callback )`. If you wish to query on a specific database add its name to the `query_object` with the parameter `db_name`.

See example for how to use a find_object

`sqlHelper.query( {sql, values}, callback)`

`sqlHelper.create( {table, object}, callback)`

`sqlHelper.update( {table, object}, callback)`

`sqlHelper.remove( {table, id}, cb)`

`sqlHelper.get( {table, id}, callback)`

`sqlHelper.find( {table, find_object} , callback)`

`sqlHelper.findOne( {tableName, find_object}, callback)`

`sqlHeper.all( {table}, callback)`

`sqlhelper.populate( {join_structure, sql, values}, callback)` Run all the sql commands to create an object from a complex join

`sqlhelper.reverse_populate( {join_structure, object}, callback)` Run all the sql commands to take a js object and recreate it in the db

##Object Manipulation

`sqlHelper.pivot(object, pivot_column )`

`sqlHelper.join(parentObject, childrenArray, foreignKey)`

`sqlHelper.format(string, arrayOfValues) \\array of values replaces ? in string`

#More Examples

##FindObject Example
```
var find_obj = {
        gender: "male",
        age: {">": 21},
        name: {"!=": "Jeff"}
}
mysqlHelper.find({
        table: "users",
        find_object: find_obj
}, function(err, rows, cols){
        console.log(rows)
}
```

##Populate Example (extreme joining)

```
var join_structure = {
        table: "car",
        children: [ {
            table: "wheel",
            fk: "car_fk",
            children: [ {
                table: "bolt",
                fk: "wheel_fk"
            }, {
                table: "rubber",
                fk: "wheel_fk"
            } ]
        } ]
}
var extra_sql = "where card.make = ?"
var values = "Volvo"

mysqlHelper.populate( {
        structure: join_structure,
        sql: extra_sql,
        values: values
}, function(err, cars, cols){
        console.log(cars)
}
```

##Reverse Populate

Will create all entries in the db for an object that spans many tables as well as handle one-to-many joins

Note: The same join structure will work for `populate()` and `reverse_populate()`
```
var join_structure = {
        table: "car",
        unique_values: [ "license" ],
        replace: true,
        children: [ {
            table: "wheel",
            fk: "car_fk"
        } ]
}
var my_car = {
        license: "abcdefg",
        color: red,
        body: large,
        wheel: [{
                tread: " winter",
                position: "fl"
        },{
                tread: " winter",
                position: "fr"
        }]

mysqlHelper.reverse_populate( {
        structure: join_structure,
        object: my_car
}, function(err, res, cols){
        var car_id = res.id
        console.log(card_id)
}
```


##Query with values Example
Using the values array you can avoid sqlInjection.
```
mysqlHelper.query({
        sql: "Select * from :table where id = :id",
        values: ["users", user_id]
}, function(err, rows, cols){
        console.log(rows)
}
```

##Multiple Database Conections
```
var dbConfig1 = database: {
        host: "localhost",
        user: "admin",
        password: 'pw',
        database: "pinballMachine",
        connectionLimit: 20,
    }
var dbConfig2 = database: {
        host: "localhost",
        user: "admin",
        password: 'pw',
        database: "sodaPopDatabase",
        connectionLimit: 30,
    }
    
sqlHelper.connect( dbConfig1 );
sqlHelper.connect( dbConfig2 );

mysqlHelper.query({
        db_name: "pinballMachine",
        sql: "Select * from users"
}, function(err, rows, cols){
        console.log(rows)
}
```


