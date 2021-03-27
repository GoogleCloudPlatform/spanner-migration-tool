/**
 * Interacts with the backend and inplements the transforms
 */
const Fetch = (() => {
    // config - header, authorization, JWT token, callback 
    let makeFetchCall = (method, url, payload, config, callback) => {
        // logic to talk to the backend
        // Start the site loader
        return new Promise((resolve, reject) => {
            fetch(url, {
                method: method,
                headers: {
                    'Accept': 'application/json',
                    'Content-Type': 'application/json'
                },
                body: JSON.stringify(payload)
            })
                .then((response) => {
                    console.log(response);
                    setTimeout(() => {
                        resolve(response);
                    }, 0);
                })
                .catch((err) => {
                    resolve('hi')
                })
                .finally(() => {
                    // stop the loader here
                });
        });


        // return new Promise((resolve, reject) => {
        //     fetch(url).then((response) => {
        //         response = { name: 'Amaaa', occupation: 'Artist', open: 'no', funcc: () => console.log('upppp') }

        //         setTimeout(() => {
        //             resolve(response);
        //         }, 0);
        //     })
        //         .catch((err) => {
        //             resolve('hi')
        //             // reject(err, ' Error in making the fetch call ', err);
        //         })
        //         .finally(() => {
        //             // stop the loader here
        //         });
        // });
    }

    let transformingIntialData = (response) => {
        // logic to clean the data
        return response;
    }

    return {
        getData: () => {
            return makeFetchCall('GET', "/").then((response) => {
                // the success logic
                return transformingIntialData(response);
            })
                .catch((err) => {
                    console.log('Error in initialisign ');
                });
        },
        showSnackbar: (message, bgClass) => {
            var snackbar = document.getElementById("snackbar");
            snackbar.className = "show" + bgClass;
            snackbar.innerHTML = message;
            setTimeout(function () {
                snackbar.className = snackbar.className.replace("show", "");
            }, 3000);
        },
        getAppData: (method, url, payload, config, callback) => {
            return makeFetchCall(method, url, payload, config, callback).then((response) => {
                console.log(response);
                return response;
            });
        }
    }
})();

export default Fetch;