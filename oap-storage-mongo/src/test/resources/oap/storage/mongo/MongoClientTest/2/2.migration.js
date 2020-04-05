if( testB )
    db.test.insert( {_id: "test", b: NumberInt(10), c: NumberInt(20)} );

if( testS == "true" )
    db.test.insert( {_id: "test2", b: 20, c: "vv"} );
