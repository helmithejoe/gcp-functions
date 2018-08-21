/* globals exports, require */
/* jshint node: true */
/* jshint strict: false */
/* jshint esversion: 6 */

"use strict";
const crc32 = require("fast-crc32c");
const gcs = require('@google-cloud/storage')();
const stream = require("stream");
const unzipper = require("unzipper");

const bucketName = {
   src: "lincgroup-bi-test-receive",
   dst: "lincgroup-bi-test"
};

exports.processZip = function(event, callback) {
  const file = event.data;
  console.log(`Processing Zip: ${file.name}`);

  var srcBucket = gcs.bucket(bucketName.src);
  var dstBucket = gcs.bucket(bucketName.dst);

  var gcsSrcObject = srcBucket.file(file.name);
  //var prefix = (new Date()).getTime();
  var prefix = '';

  gcsSrcObject.createReadStream()
  .pipe(unzipper.Parse())
  .on("entry", function(entry) {
      var filePath = entry.path;
      var type = entry.type;
      var size = entry.size;
      console.log(`Found ${type}: ${filePath}`);
      var gcsDstObject = dstBucket.file(`${prefix}/${filePath}`);
      entry
        .pipe(gcsDstObject.createWriteStream())
        .on('error', function(err) {
          console.log(`File Error: ${err}`);
        })
        .on('finish', function() {
          console.log("File Extracted");
        });
  })
  .promise()
  .then(() => {
    console.log("Zip Processed");
    callback();
  },(err) => {
    console.log(`Zip Error: ${err}`);
  });
};
