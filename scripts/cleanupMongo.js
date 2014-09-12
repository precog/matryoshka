// run with mongo shell:
//
// $ mongo cleanupMongo.js
//
// $ mongo
// > r2-ds045089:PRIMARY> load('cleanupMongo.js')


function dbSize() {
  return db.stats().fileSize/(1024*1024) + " MiB";
}

print("DB file size: " + dbSize())
print()

print("Dropping all collections...")
var droppedCount = 0;
var colls = db.getCollectionNames();
for (i in colls) {
  var name = colls[i];
  if (!name.startsWith("system.")) {
    var col = db.getCollection(name);
    //print("dropping " + name + " (" + col.totalSize()/(1024*1024.0) + " MB)");
    var dropped = col.drop();
    if (!dropped) print("  drop failed: " + name + "; " + db.getLastError());
    else droppedCount += 1;
  }
}
print("  dropped " + droppedCount + " collections")
print()

print("Recovering unused space...")
db.repairDatabase()
print("  done")
print()

print("DB file size: " + dbSize())
print()

