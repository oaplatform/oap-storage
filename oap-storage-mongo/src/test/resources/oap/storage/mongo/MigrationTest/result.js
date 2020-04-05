conn = new Mongo("localhost:27017");
db = conn.getDB("testdb");

// ========== PARAMETERS ==========
var param1 = true;
var param2 = "string";
var param3 = true;
var param4 = "string2";

// ========== INCLUDE: /oap/storage/mongo/MigrationTest/lib.migration.js ==========
var lib = "lib";

// ========== SCRIPT: /oap/storage/mongo/MigrationTest/s1.migration.js ==========
(function() {
var s1 = "s1";
})();

// ========== SCRIPT: /oap/storage/mongo/MigrationTest/s2.migration.js ==========
(function() {
var s2 = "s2";
})();
