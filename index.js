const {Storage} = require('@google-cloud/storage');
const { error } = require('console');
const csv = require('csv-parser');
const {BigQuery} = require('@google-cloud/bigquery');

const bq = new BigQuery();
const dataSetId = 'weather_etl_v5';
const tableId = 'final_weatherTable';

exports.readObservation = (file, context) => {
    // console.log(`  Event: ${context.eventId}`);
    // console.log(`  Event Type: ${context.eventType}`);
    // console.log(`  Bucket: ${file.bucket}`);
    // console.log(`  File: ${file.name}`);

    const gcs = new Storage();

    const dataFile = gcs.bucket(file.bucket).file(file.name);

    dataFile.createReadStream()

    .on('error', () => {
        //Handle the error
        console.error(error);
    })
    .pipe(csv())

    .on('data', (row) => {
        //Log row data
        //console.log(row);
        modifyDict(row, file.name);
        printDict(row);
        
    })

    .on('end', () => {
        //Handle end of csv
        console.log("CSV End!");
    })
}

//Function to modify the row data
function modifyDict(row, fileName) {
    for (let key in row) {
        let value = parseFloat(row[key]);
        if (value == -9999) {
            row[key] = null;
        }
        if (key == "airtemp" || key == "dewpoint" || key == "pressure" || key == "windspeed" || key == "precip1hour" || key == "precip6hour") {
            if (row[key] != null) {
                row[key] = value / 10;
            }
        }

        if (key == "year" || key == "month" || key == "day" || key == "hour" || key == "sky") {
            if (row[key] != null) {
                row[key] = parseInt(row[key]);
            }
        }

        const stationCode = fileName.split('.')[0];
        row['station'] = stationCode;
    }

}

//Function to print the row data
function printDict(row) {
    for (let key in row) {
        console.log(`${key} : ${row[key]}`);
    }

    writeToBQ(row);
}


//Function to write to big query
async function writeToBQ(obj) {
    let finalRows = [];
    finalRows.push(obj);

    await bq
    .dataset(dataSetId)
    .table(tableId)
    .insert(finalRows)
    .then( () => {
        finalRows.forEach((row) => {
            console.log(`Inserted ${row}!`)
        })
    })
    .catch( (err) => {
        console.log(`Error: ${err}`)
    })
}
