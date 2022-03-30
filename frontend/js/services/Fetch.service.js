// Copyright 2020 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

import { showSnackbar } from './../helpers/SchemaConversionHelper.js';
import Actions from './Action.service.js';

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
                showSnackbar(err, ' redBg');
            })
            .finally(() => {
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