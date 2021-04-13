import { showSnackbar } from './../helpers/SchemaConversionHelper.js';
import Actions from './Action.service.js';

/**
 * Interacts with the backend and implements the transforms
 */
const Fetch = (() => {
    let makeFetchCall = (method, url, payload, config, callback, snakbar) => {
        Actions.showSpinner()
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
                showSnackbar(err, ' redBg');
            })
            .finally(() => {
                // stop the loader here
                Actions.hideSpinner()
            });
        });
    }

    return {
        getAppData: (method, url, payload, config, callback) => {
            return makeFetchCall(method, url, payload, config, callback).then((response) => {
                return response;
            });
        }
    }
})();

export default Fetch;