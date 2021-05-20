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
