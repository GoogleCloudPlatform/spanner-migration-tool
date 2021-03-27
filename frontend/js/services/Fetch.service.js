/**
 * Interacts with the backend and inplements the transforms
 */
const Fetch = (() => {

    // config - header, authorization, JWT token, callback 
    let makeFetchCall = (method, url, payload, config, callback) => {
        // logic to talk to the backend
        // Start the site loader
        return new Promise((resolve, reject) => {
            fetch(url).then((response) => {
                    response = { name: 'Amaaa', occupation: 'Artist', currentTab : "reportTab",open: 'no', funcc: () => console.log('upppp') }

                    setTimeout(() => {
                        resolve(response);
                    }, 0);
                })
                .catch((err) => {
                    resolve('hi')
                        // reject(err, ' Error in making the fetch call ', err);
                })
                .finally(() => {
                    // stop the loader here
                });
        });
    }

    let transforminintialData = (response) => {
        // logic to clean the data
        return response;
    }

    return {
        getData: () => {
            return makeFetchCall('GET', "/").then((response) => {
                    // the success logic
                    return transforminintialData(response);
                })
                .catch((err) => {
                    console.log('Error in initialisign ');
                })
        }
    }
})();

export default Fetch;