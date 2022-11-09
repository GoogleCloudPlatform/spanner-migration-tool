# Ui

This project was generated with [Angular CLI](https://github.com/angular/angular-cli) version 13.2.3.

## Updating the UI

The `dist/ui` directory is mounted inside the harbourbridge binary using the `go embed` library.
After making any changes to the UI, perform the following steps to update Harbourbridge with the UI changes:

1. Run `ng build` inside the `ui/` directory. This places the generated angular artifacts in the `ui/dist/ui` directory.
2. Run `make build` from the root directory to generate the harbourbridge binary.
3. Run `./harbourbridge web` to run the web UI.
4. Navigate to `http://localhost:8080/` to access the UI.

Note: Do not forget to commit the changes to the `dist/` directory once the updates to the UI have been made.

## Development server

Run `ng serve` for a dev server. Navigate to `http://localhost:8080/`. The app will automatically reload if you change any of the source files.

## Code scaffolding

Run `ng generate component component-name` to generate a new component. You can also use `ng generate directive|pipe|service|class|guard|interface|enum|module`.

## Build

Run `ng build` to build the project. The build artifacts will be stored in the `dist/` directory.

## Running unit tests

Run `ng test` to execute the unit tests via [Karma](https://karma-runner.github.io).

## Running end-to-end tests

Run `ng e2e` to execute the end-to-end tests via a platform of your choice. To use this command, you need to first add a package that implements end-to-end testing capabilities.

## Further help

To get more help on the Angular CLI use `ng help` or go check out the [Angular CLI Overview and Command Reference](https://angular.io/cli) page.
