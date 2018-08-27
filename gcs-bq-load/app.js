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
            .then(() => {
                const fileObj = storage.bucket(file.bucket).file(file.name);
                const metadata = {
                    sourceFormat: 'CSV',
                    writeDisposition: 'WRITE_APPEND',
                    autodetect: false,
                    skipLeadingRows: 0
                }
                return bigquery.dataset(datasetName).table(tableName)
                    .load(fileObj, metadata).
                        then(results => {
                          const job = results[0];
                          console.log(job.status.state + ':' + file.bucket + '/' + file.name);
                          const errors = job.status.errors;
                          if (errors && errors.length > 0) {
                            throw errors;
                          }
                      });
            })
            .then(() => console.log(`BigQuery load completed for ${file.name}.`))
            .catch((err) => {
                console.log(`Job failed for ${file.name}`);
                return Promise.reject(err);
            })
    } else {
        console.log(`File ${file.name} metadata updated.`);

        return 'Ok';
    }
};
