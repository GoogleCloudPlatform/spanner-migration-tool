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

import "../LoadingSpinner.component.js";

test("Loading spinner component render fine", () => {
  document.body.innerHTML = "<hb-loading-spinner></hb-loading-spinner>";
  let loadingSpinner = document.querySelector("hb-loading-spinner");
  expect(loadingSpinner).not.toBe(null);
  expect(loadingSpinner.innerHTML).not.toBe(null);
  expect(loadingSpinner.innerHTML).not.toBe("");
  expect(document.querySelector("#toggle-spinner")).not.toBe(null);
  expect(document.querySelector("#spinner")).not.toBe(null);
});
