// Home screen component
const HomeComponent = {
  render: () => homeScreen()
}

// Edit Schema screen component
const SchemaComponent = {
  render: () => schemaReport()
}

// Instructions Component
const InstructionsComponent = {
  render: () => renderInstructionsHtml()
}

// Error component (for any unrecognized path)
const ErrorComponent = {
  render: () => {
    return `
      <section>
        <h1>Error</h1>
      </section>
    `;
  }
}

// Pre defined paths
const paths = {
  defaultPath: '/',
  connectToDb: '/schema-report-connect-to-db',
  loadDbDump: '/schema-report-load-db-dump',
  importDb: '/schema-report-import-db',
  resumeSession: '/schema-report-resume-session',
  instructions: '/instructions'
}

// Pre defined routes
const routes = [
  { path: paths.defaultPath, component: HomeComponent, },
  { path: paths.connectToDb, component: SchemaComponent, },
  { path: paths.loadDbDump, component: SchemaComponent, },
  { path: paths.importDb, component: SchemaComponent, },
  { path: paths.resumeSession, component: SchemaComponent, },
  { path: paths.instructions, component: InstructionsComponent, }
];

// function to fetch browser url
const parseLocation = () => location.hash.slice(1).toLowerCase() || paths.defaultPath;

// function to find component based on browser url
const findComponentByPath = (path, routes) => {
  return routes.find(r => {
    return r.path.match(new RegExp(`^\\${path}$`, 'gm'))
  }) || undefined;
}

// function to render the html based on path
const router = () => {
  const path = parseLocation();
  const { component = ErrorComponent } = findComponentByPath(path, routes) || {};
  getComponentFlag = renderComponent({'path': path, 'event': window.event.type});
  if (!getComponentFlag) {
    document.getElementById('app').innerHTML = component.render();
  }
};

window.addEventListener('hashchange', router);
window.addEventListener('load', router);