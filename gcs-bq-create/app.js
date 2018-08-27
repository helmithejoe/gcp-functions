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
const datasetName = 'lincgroup_bi_test2';

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

exports.bqGCSCreateDisable = function(event) {
    return true;
}

exports.bqGCSCreate = function(event) {
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
            .then(() => {
                storage.bucket(file.bucket).file(file.name)
                    .download()
                    .then(function(data){
                         if (data) {
                             const schemaStr = data.toString('utf-8');
                             const options = {
                                 schema: schemaStr
                             };
                             console.log('Options: ' + JSON.stringify(options));
                             return getTable(datasetName, tableName, options);
                         }
                     })

            })
            .then(() => {
                console.log(`Table ${file.name} created.`);
            })
            .catch((err) => {
                console.log(`Job failed for ${file.name}`);
                return Promise.reject(err);
            })
    } else {
        console.log(`File ${file.name} metadata updated.`);
        return 'Ok';
    }
};
