/**
 * Background Cloud Function to be triggered by Cloud Storage.
 *
 * @param {object} event The Cloud Functions event.
 */
// Get a reference to the Cloud Storage component
const storage = require('@google-cloud/storage')();
// Get a reference to the BigQuery component
const bigquery = require('@google-cloud/bigquery')();
const fs = require('fs');

const project = 'lincgroup-bi-test';
const datasetName = 'lincgroup_bi_test';
const schemaDir = 'SCHEMA';

var schemaStr;
var options;

/**
 * Helper method to get a handle on a BigQuery table. Automatically creates the
 * dataset and table if necessary.
 */
function getTable(datasetName, tableName, params) {
    const dataset = bigquery.dataset(datasetName);

    return dataset.get({
            autoCreate: true
        })
        .then(([dataset]) => dataset.createTable(tableName, params));
}

function getSchema(file) {
    if (!file.bucket) {
        throw new Error('Bucket not provided. Make sure you have a "bucket" property in your request');
    }
    if (!file.name) {
        throw new Error('Filename not provided. Make sure you have a "name" property in your request');
    }
    console.log('Schema: ' + schemaDir + '/' + file.name);

    storage.bucket(file.bucket).file(schemaDir + '/' + file.name)
        .download()
        .then(function(data){
             if (data)
                 return data.toString('utf-8');
         })
        .then(function(data){
             if (data) {
                 console.log(data);
                 schemaStr = data;
             }
         })
        .catch(function(e){ reject(e); })
}

exports.bqGCSLoad = function(event) {
    const file = event.data;
    const tableName = file.name.replace('.csv', '');

    if (file.resourceState === 'not_exists') {
        console.log(`File ${file.name} deleted.`);

        return 'Ok';
    } else if (file.metageneration === '1') {
        // metageneration attribute is updated on metadata changes.
        // on create value is 1
        console.log(`File ${file.name} uploaded.`);

        return Promise.resolve()
            /*
            .then(() => {
                //load schema
                getSchema(file);

                options = {
                    schema: schemaStr
                };
            })
            */
            .then(() => {

                console.log('Options: ' + JSON.stringify(options));

                return getTable(datasetName, tableName, options);
            })
            .then(([table]) => {
                console.log(`Table created. Now to load data`);
                const fileObj = storage.bucket(file.bucket).file(file.name);
                const metadata = {
                    sourceFormat: 'CSV'
                }
                //return table.import(fileObj, metadata);
                return table.load(fileObj, metadata).
                    then(results => {
                      const job = results[0];

                      console.log(job.status.state + ':' + file.bucket + '/' + file.name);

                      const errors = job.status.errors;
                      if (errors && errors.length > 0) {
                        throw errors;
                      }
                    })
            })
            //.then(([job]) => job.promise())
            //.then(() => {
            //    console.log(`Insert job complete for ${file.name}`);

                /*
                const sourceTable = '[' + project + ':' + datasetName + '.' + tableName + ']';
                const destTable = bigquery.dataset(datasetName).table(tableName + '_CLEANSED');

                const query = `SELECT HOUR(time) AS hour, COUNT(time) AS num_ticks
                          FROM ${sourceTable}
                          WHERE time BETWEEN TIMESTAMP("2014-01-16 00:00:00.000") AND TIMESTAMP("2014-01-16 23:59:59.999")
                          GROUP BY hour ORDER BY hour ASC;`;
                var options = {
                    destination: destTable,
                    query: query
                }

                return bigquery.startQuery(options);
                */

            //})
            //.then(([job]) => job.promise())
            //.then(() => {
                /*
                console.log(`Query finished for ${tableName}. Now to start the export to GCS.`);
                const exportFile = storage.bucket('cleansed-data').file(tableName + '_CLEANSED.CSV');

                return bigquery.dataset(datasetName).table(tableName + '_CLEANSED').export(exportFile);
                */
            //})
            //.then(([job]) => job.promise())
            .then(() => console.log(`BigQuery actions and export completed for ${file.name}.`))
            .catch((err) => {
                console.log(`Job failed for ${file.name}`);
                return Promise.reject(err);
            })
    } else {
        console.log(`File ${file.name} metadata updated.`);

        return 'Ok';
    }
};
