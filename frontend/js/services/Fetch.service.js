import { showSnackbar } from './../helpers/SchemaConversionHelper.js';

/**
 * Interacts with the backend and implements the transforms
 */
const Fetch = (() => {
    let makeFetchCall = (method, url, payload, config, callback, snakbar) => {
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
                setTimeout(() => {
                    resolve(response);
                }, 0);
            })
            .catch((err) => {
                showSnackbar(err, 'redBg');
            })
            .finally(() => {
                // stop the loader here
            });
        });
    }

    return {
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
                return response;
            });
        }
    }
})();

export default Fetch;