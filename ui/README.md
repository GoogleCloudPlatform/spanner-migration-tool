# Ui

This project uses [Angular](https://angular.dev) 21, built and tested via `@angular-devkit/architect-cli` and `@angular/build`.

## Updating the UI

The `dist/ui` directory is mounted inside the Spanner migration tool binary using the `go embed` library.
After making any changes to the UI, perform the following steps to update Spanner migration tool with the UI changes:

1. Run `npm run build` (`architect ui:build`) inside the `ui/` directory. This places the generated Angular artifacts in the `ui/dist/ui` directory.
2. Run `make build` from the root directory to generate the Spanner migration tool binary.
3. Run `./spanner-migration-tool web` to run the web UI.
4. Navigate to `http://localhost:8080/` to access the UI.

Note: Do not forget to commit the changes to the `dist/` directory once the updates to the UI have been made.

## Development server

Run `npm start` (`architect ui:serve`) for a dev server. Navigate to `http://localhost:4200/`. The app will automatically reload if you change any of the source files.

## Code scaffolding

`@angular/cli` is not installed locally; run Angular schematics via `npx @angular/cli@21 generate component component-name` (or `directive|pipe|service|class|guard|interface|enum|module`).

## Build

Run `npm run build` (`architect ui:build`) to build the project. The build artifacts will be stored in the `dist/ui/` directory.

## Running unit tests

Run `npm test` (`architect ui:test`, or `npm test -- --watch=false` for a single headless run) to execute the unit tests via [Vitest](https://vitest.dev).

## Running end-to-end tests

Start the dev server (`npm start`) and run `npx cypress run` to execute the end-to-end tests via [Cypress](https://www.cypress.io).

## Further help

To inspect available Architect targets run `npx architect --help`, or see the [Angular CLI and Builder Reference](https://angular.dev/tools/cli).
