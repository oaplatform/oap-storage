db.test.update({_id: "test3"}, {$inc: {v: NumberInt(1)}}, {upsert: true});
