db.test.update({_id: "test"}, {$inc: {c: NumberInt(5)}});
db.test.update({_id: "test"}, {$inc: {c: NumberInt(-2)}});

db.test.update({_id: "test2"}, {z: 10});

db.test.update({_id: "test3"}, {$inc: {v: NumberInt(1)}}, {upsert: true});
