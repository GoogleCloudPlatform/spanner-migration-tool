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

// Pre defined routes 
const routes = [
  { path: '/', component: HomeComponent, },
  { path: '/schema-report-connect-to-db', component: SchemaComponent, },
  { path: '/schema-report-load-db-dump', component: SchemaComponent, },
  { path: '/schema-report-import-db', component: SchemaComponent, },
  { path: '/schema-report-resume-session', component: SchemaComponent, },
  { path: '/instructions', component: InstructionsComponent, }
];

// function to fetch browser url
const parseLocation = () => location.hash.slice(1).toLowerCase() || '/';

// function to find component based on browser url
const findComponentByPath = (path, routes) => routes.find(r => r.path.match(new RegExp(`^\\${path}$`, 'gm'))) || undefined;

// function to render the html based on path
const router = () => {
  const path = parseLocation();
  const { component = ErrorComponent } = findComponentByPath(path, routes) || {};
  getComponentFlag = getComponent({'path': path, 'event': window.event.type});
  if (!getComponentFlag) {
    document.getElementById('app').innerHTML = component.render();
  }
};

window.addEventListener('hashchange', router);
window.addEventListener('load', router);